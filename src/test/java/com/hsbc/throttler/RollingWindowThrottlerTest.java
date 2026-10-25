package com.hsbc.throttler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hsbc.throttler.Throttler.ThrottleResult.DO_NOT_PROCEED;
import static com.hsbc.throttler.Throttler.ThrottleResult.PROCEED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RollingWindowThrottlerTest {

    // No permit becomes free during a test with this window.
    private static final Duration LONG_WINDOW = Duration.ofMinutes(1);
    // The timer gives permits during a test with this window.
    private static final Duration SHORT_WINDOW = Duration.ofMillis(50);
    private static final Executor DIRECT = Runnable::run;

    @Test
    void rejects_invalid_arguments() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RollingWindowThrottler(0, LONG_WINDOW));
        assertThatIllegalArgumentException().isThrownBy(() -> new RollingWindowThrottler(-1, LONG_WINDOW));
        assertThatIllegalArgumentException().isThrownBy(() -> new RollingWindowThrottler(1, Duration.ZERO));
        assertThatIllegalArgumentException().isThrownBy(() -> new RollingWindowThrottler(1, Duration.ofSeconds(-1)));
        assertThatNullPointerException().isThrownBy(() -> new RollingWindowThrottler(1, null));
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, LONG_WINDOW);
        assertThatNullPointerException().isThrownBy(() -> throttler.notifyWhenCanProceed(null, () -> {
        }));
        assertThatNullPointerException().isThrownBy(() -> throttler.notifyWhenCanProceed(DIRECT, null));
    }

    @Test
    void gives_n_permits_then_refuses() {
        RollingWindowThrottler throttler = new RollingWindowThrottler(3, LONG_WINDOW);

        assertThat(throttler.shouldProceed()).isEqualTo(PROCEED);
        assertThat(throttler.shouldProceed()).isEqualTo(PROCEED);
        assertThat(throttler.shouldProceed()).isEqualTo(PROCEED);
        assertThat(throttler.shouldProceed()).isEqualTo(DO_NOT_PROCEED);
        assertThat(throttler.shouldProceed()).isEqualTo(DO_NOT_PROCEED);
    }

    @Test
    void frees_a_permit_one_window_after_it_was_given() {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, SHORT_WINDOW);
        long start = System.nanoTime();
        assertThat(throttler.shouldProceed()).isEqualTo(PROCEED);

        while (throttler.shouldProceed() == DO_NOT_PROCEED) {
            Thread.onSpinWait();
        }

        assertThat(System.nanoTime() - start).isGreaterThanOrEqualTo(SHORT_WINDOW.toNanos());
    }

    @Test
    void calls_the_executor_at_once_on_the_calling_thread_when_a_permit_is_free() {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, LONG_WINDOW);
        List<Thread> executorThreads = new ArrayList<>();
        List<String> calls = new ArrayList<>();

        throttler.notifyWhenCanProceed(task -> {
            executorThreads.add(Thread.currentThread());
            task.run();
        }, () -> calls.add("called"));

        assertThat(calls).containsExactly("called");
        assertThat(executorThreads).containsExactly(Thread.currentThread());
        assertThat(throttler.shouldProceed()).isEqualTo(DO_NOT_PROCEED);
    }

    @Test
    void refuses_shouldProceed_while_a_subscriber_waits() {
        RollingWindowThrottler throttler = new RollingWindowThrottler(2, LONG_WINDOW);
        List<String> calls = new ArrayList<>();
        throttler.shouldProceed();
        throttler.shouldProceed();
        throttler.notifyWhenCanProceed(DIRECT, () -> calls.add("subscriber"));

        assertThat(throttler.shouldProceed()).isEqualTo(DO_NOT_PROCEED);
        assertThat(calls).isEmpty();
    }

    @Test
    void real_timer_never_notifies_early_and_keeps_fifo_order() throws InterruptedException {
        RollingWindowThrottler throttler = new RollingWindowThrottler(2, SHORT_WINDOW);
        long start = System.nanoTime();
        throttler.shouldProceed();
        throttler.shouldProceed();
        List<Integer> order = new CopyOnWriteArrayList<>();
        long[] grantedAt = new long[4];
        CountDownLatch done = new CountDownLatch(4);
        for (int i = 0; i < 4; i++) {
            int id = i;
            throttler.notifyWhenCanProceed(DIRECT, () -> {
                grantedAt[id] = System.nanoTime();
                order.add(id);
                done.countDown();
            });
        }

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(order).containsExactly(0, 1, 2, 3);
        assertThat(grantedAt[0] - start).isGreaterThanOrEqualTo(SHORT_WINDOW.toNanos());
        assertThat(grantedAt[1] - start).isGreaterThanOrEqualTo(SHORT_WINDOW.toNanos());
        // The second pair waits for a second window.
        assertThat(grantedAt[2] - start).isGreaterThanOrEqualTo(2 * SHORT_WINDOW.toNanos());
        assertThat(grantedAt[3] - start).isGreaterThanOrEqualTo(2 * SHORT_WINDOW.toNanos());
        assertThat(grantedAt[3] - start).isLessThan(Duration.ofSeconds(5).toNanos());
    }

    @Test
    void runs_the_callback_on_the_executor_of_the_request() throws InterruptedException {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, SHORT_WINDOW);
        List<String> calls = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        throttler.shouldProceed();

        throttler.notifyWhenCanProceed(task -> {
            calls.add("executor");
            task.run();
        }, () -> {
            calls.add("callback");
            done.countDown();
        });

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(calls).containsExactly("executor", "callback");
    }

    @Test
    void keeps_the_permit_when_the_executor_rejects_on_the_immediate_path() {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, LONG_WINDOW);

        throttler.notifyWhenCanProceed(task -> {
            throw new RejectedExecutionException("full");
        }, () -> {
        });

        assertThat(throttler.shouldProceed()).isEqualTo(DO_NOT_PROCEED);
    }

    @Test
    void real_timer_continues_when_the_executor_rejects() throws InterruptedException {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, SHORT_WINDOW);
        CountDownLatch done = new CountDownLatch(1);
        throttler.shouldProceed();

        throttler.notifyWhenCanProceed(task -> {
            throw new RejectedExecutionException("full");
        }, () -> {
        });
        throttler.notifyWhenCanProceed(DIRECT, done::countDown);

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void real_timer_survives_a_callback_that_throws() throws InterruptedException {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, SHORT_WINDOW);
        CountDownLatch done = new CountDownLatch(1);
        throttler.shouldProceed();

        throttler.notifyWhenCanProceed(DIRECT, () -> {
            throw new IllegalStateException("boom");
        });
        throttler.notifyWhenCanProceed(DIRECT, done::countDown);

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void real_timer_wakes_up_again_after_it_went_idle() throws InterruptedException {
        RollingWindowThrottler throttler = new RollingWindowThrottler(1, SHORT_WINDOW);
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);
        throttler.shouldProceed();
        throttler.notifyWhenCanProceed(DIRECT, first::countDown);
        assertThat(first.await(10, TimeUnit.SECONDS)).isTrue();

        throttler.notifyWhenCanProceed(DIRECT, second::countDown);

        assertThat(second.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void many_threads_never_get_more_than_max_permits() {
        int maxPermits = 100;
        RollingWindowThrottler throttler = new RollingWindowThrottler(maxPermits, LONG_WINDOW);
        AtomicInteger proceeded = new AtomicInteger();

        try (ExecutorService pool = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().factory())) {
            for (int t = 0; t < 8; t++) {
                pool.execute(() -> {
                    for (int i = 0; i < 1_000; i++) {
                        if (throttler.shouldProceed() == PROCEED) {
                            proceeded.incrementAndGet();
                        }
                    }
                });
            }
        }

        assertThat(proceeded).hasValue(maxPermits);
    }
}
