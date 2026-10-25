package com.hsbc.eventbus.multithread;

import com.hsbc.eventbus.ConflatableEvent;
import com.hsbc.eventbus.Event;
import com.hsbc.eventbus.TestEvents.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class MultiThreadedEventBusTest {

    enum ExecutorKind {
        FIXED_POOL(() -> Executors.newFixedThreadPool(4)),
        SMALL_FIXED_POOL(() -> Executors.newFixedThreadPool(2)),
        VIRTUAL_THREADS(Executors::newVirtualThreadPerTaskExecutor);

        private final Supplier<ExecutorService> factory;

        ExecutorKind(Supplier<ExecutorService> factory) {
            this.factory = factory;
        }
    }

    private final List<ExecutorService> executors = new ArrayList<>();

    private ExecutorService executor(ExecutorKind kind) {
        ExecutorService service = kind.factory.get();
        executors.add(service);
        return service;
    }

    private MultiThreadedEventBus bus(ExecutorKind kind) {
        return new MultiThreadedEventBus(executor(kind));
    }

    @AfterEach
    void shutDownExecutors() throws InterruptedException {
        for (ExecutorService service : executors) {
            service.shutdownNow();
            assertThat(service.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void rejects_invalid_arguments() {
        ExecutorService service = executor(ExecutorKind.FIXED_POOL);
        assertThatNullPointerException().isThrownBy(() -> new MultiThreadedEventBus(null));
        MultiThreadedEventBus bus = new MultiThreadedEventBus(service);
        assertThatNullPointerException().isThrownBy(() -> bus.publishEvent(null));
        assertThatNullPointerException().isThrownBy(() -> bus.addSubscriber(null, e -> {
        }));
        assertThatNullPointerException().isThrownBy(() -> bus.addSubscriber(Quote.class, null));
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void callback_runs_on_another_thread(ExecutorKind kind) throws InterruptedException {
        MultiThreadedEventBus bus = bus(kind);
        Thread[] callbackThread = new Thread[1];
        CountDownLatch done = new CountDownLatch(1);
        bus.addSubscriber(Quote.class, q -> {
            callbackThread[0] = Thread.currentThread();
            done.countDown();
        });

        bus.publishEvent(new Quote("A", 1));

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackThread[0]).isNotSameAs(Thread.currentThread());
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void filter_and_routing_work(ExecutorKind kind) throws InterruptedException {
        MultiThreadedEventBus bus = bus(kind);
        List<Event> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        bus.addSubscriberForFilteredEvents(Event.class, e -> e instanceof Quote q && q.price() > 1, e -> {
            received.add(e);
            done.countDown();
        });

        // Filters run on the producer thread, so the rejected events are never queued.
        bus.publishEvent(new Quote("A", 1));
        bus.publishEvent(new Control("x"));
        bus.publishEvent(new Quote("A", 2));

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly(new Quote("A", 2));
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void subscriber_added_after_a_publish_gets_the_next_events(ExecutorKind kind) throws InterruptedException {
        MultiThreadedEventBus bus = bus(kind);
        CountDownLatch first = new CountDownLatch(2);
        List<Quote> late = new CopyOnWriteArrayList<>();
        CountDownLatch lateDone = new CountDownLatch(1);
        bus.addSubscriber(Quote.class, q -> first.countDown());

        bus.publishEvent(new Quote("A", 1));
        bus.addSubscriber(Event.class, e -> {
            late.add((Quote) e);
            lateDone.countDown();
        });
        bus.publishEvent(new Quote("A", 2));

        assertThat(first.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(lateDone.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(late).containsExactly(new Quote("A", 2));
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void multi_producer_multi_consumer_delivers_each_event_once_in_producer_order(ExecutorKind kind) throws Exception {
        int producers = 8;
        int eventsPerProducer = 10_000;
        int subscribers = 4;
        MultiThreadedEventBus bus = bus(kind);
        CountDownLatch allDone = new CountDownLatch(subscribers);
        AtomicInteger violations = new AtomicInteger();
        int[][] expectedNext = new int[subscribers][producers];
        int[] counts = new int[subscribers];
        for (int s = 0; s < subscribers; s++) {
            int index = s;
            bus.addSubscriber(Sequenced.class, e -> {
                if (e.seq() != expectedNext[index][e.producer()]++) {
                    violations.incrementAndGet();
                }
                if (++counts[index] == producers * eventsPerProducer) {
                    allDone.countDown();
                }
            });
        }

        runProducers(producers, producer -> {
            for (int i = 0; i < eventsPerProducer; i++) {
                bus.publishEvent(new Sequenced(producer, i));
            }
        });

        assertThat(allDone.await(40, TimeUnit.SECONDS)).isTrue();
        assertThat(violations).hasValue(0);
        for (int s = 0; s < subscribers; s++) {
            assertThat(counts[s]).isEqualTo(producers * eventsPerProducer);
            for (int p = 0; p < producers; p++) {
                assertThat(expectedNext[s][p]).isEqualTo(eventsPerProducer);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void never_calls_subscriber_concurrently(ExecutorKind kind) throws Exception {
        int producers = 4;
        int eventsPerProducer = 5_000;
        MultiThreadedEventBus bus = bus(kind);
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maxInside = new AtomicInteger();
        int[] plainCounter = {0};
        CountDownLatch done = new CountDownLatch(1);
        bus.addSubscriber(Sequenced.class, e -> {
            maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
            if (++plainCounter[0] == producers * eventsPerProducer) {
                done.countDown();
            }
            inside.decrementAndGet();
        });

        runProducers(producers, producer -> {
            for (int i = 0; i < eventsPerProducer; i++) {
                bus.publishEvent(new Sequenced(producer, i));
            }
        });

        assertThat(done.await(40, TimeUnit.SECONDS)).isTrue();
        assertThat(maxInside).hasValue(1);
        assertThat(plainCounter[0]).isEqualTo(producers * eventsPerProducer);
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void conflates_to_latest_value_per_key(ExecutorKind kind) throws Exception {
        MultiThreadedEventBus bus = bus(kind);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        List<Event> processed = new ArrayList<>();
        bus.addSubscriber(Event.class, gatedRecorder(entered, release, finished, processed));

        bus.publishEvent(new Control("block"));
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        for (int seq = 1; seq <= 1000; seq++) {
            bus.publishEvent(new MarketDataSnapshot("AAPL", seq, seq));
            bus.publishEvent(new MarketDataSnapshot("MSFT", seq, seq));
        }
        bus.publishEvent(new Control("end"));
        release.countDown();

        assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(processed).containsExactly(
                new MarketDataSnapshot("AAPL", 1000, 1000),
                new MarketDataSnapshot("MSFT", 1000, 1000));
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void rejected_update_never_overwrites_accepted_pending_value(ExecutorKind kind) throws Exception {
        MultiThreadedEventBus bus = bus(kind);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        List<Event> processed = new ArrayList<>();
        bus.addSubscriberForFilteredEvents(Event.class,
                e -> !(e instanceof MarketDataSnapshot s) || s.seq() % 2 == 0,
                gatedRecorder(entered, release, finished, processed));

        bus.publishEvent(new Control("block"));
        assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
        for (int seq = 1; seq <= 999; seq++) {
            bus.publishEvent(new MarketDataSnapshot("AAPL", seq, seq));
        }
        bus.publishEvent(new Control("end"));
        release.countDown();

        assertThat(finished.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(processed).containsExactly(new MarketDataSnapshot("AAPL", 998, 998));
    }

    @Test
    void null_conflation_key_is_rejected_before_any_delivery() {
        MultiThreadedEventBus bus = bus(ExecutorKind.FIXED_POOL);
        List<Event> received = new CopyOnWriteArrayList<>();
        bus.addSubscriber(Event.class, received::add);
        ConflatableEvent badEvent = () -> null;

        assertThatNullPointerException().isThrownBy(() -> bus.publishEvent(badEvent));

        assertThat(received).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void failing_subscriber_does_not_stop_others(ExecutorKind kind) throws InterruptedException {
        MultiThreadedEventBus bus = bus(kind);
        List<Quote> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        bus.addSubscriber(Quote.class, q -> {
            throw new IllegalStateException("boom");
        });
        bus.addSubscriber(Quote.class, q -> {
            received.add(q);
            done.countDown();
        });
        Quote quote = new Quote("A", 1);

        bus.publishEvent(quote);

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly(quote);
    }

    @ParameterizedTest
    @EnumSource(ExecutorKind.class)
    void failing_subscriber_stays_subscribed(ExecutorKind kind) throws InterruptedException {
        MultiThreadedEventBus bus = bus(kind);
        CountDownLatch failingCalls = new CountDownLatch(10);
        CountDownLatch healthy = new CountDownLatch(10);
        bus.addSubscriber(Quote.class, q -> {
            failingCalls.countDown();
            throw new IllegalStateException();
        });
        bus.addSubscriber(Quote.class, q -> healthy.countDown());

        for (int i = 0; i < 10; i++) {
            bus.publishEvent(new Quote("A", i));
        }

        assertThat(healthy.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(failingCalls.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void rejected_execution_does_not_block_the_mailbox() throws InterruptedException {
        ToggleExecutor toggle = new ToggleExecutor(executor(ExecutorKind.FIXED_POOL));
        MultiThreadedEventBus bus = new MultiThreadedEventBus(toggle);
        List<Quote> received = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(2);
        bus.addSubscriber(Quote.class, q -> {
            received.add(q);
            done.countDown();
        });
        Quote first = new Quote("A", 1);
        Quote second = new Quote("A", 2);

        toggle.reject = true;
        bus.publishEvent(first);
        assertThat(received).isEmpty();

        toggle.reject = false;
        bus.publishEvent(second);

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(received).containsExactly(first, second);
    }

    private static void runProducers(int count, IntConsumer producer) {
        try (ExecutorService pool = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().factory())) {
            for (int p = 0; p < count; p++) {
                int id = p;
                pool.execute(() -> producer.accept(id));
            }
        }
    }

    private static Consumer<Event> gatedRecorder(
            CountDownLatch entered, CountDownLatch release, CountDownLatch finished, List<Event> processed) {
        return e -> {
            if (e instanceof Control control) {
                if (control.name().equals("block")) {
                    entered.countDown();
                    awaitUninterruptibly(release);
                } else {
                    finished.countDown();
                }
            } else {
                processed.add(e);
            }
        };
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class ToggleExecutor implements Executor {
        private final Executor delegate;
        volatile boolean reject;

        ToggleExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            if (reject) {
                throw new RejectedExecutionException("rejected on purpose");
            }
            delegate.execute(command);
        }
    }
}
