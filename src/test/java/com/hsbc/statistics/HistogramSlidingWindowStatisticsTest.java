package com.hsbc.statistics;

import com.hsbc.eventbus.EventBus;
import com.hsbc.eventbus.multithread.MultiThreadedEventBus;
import com.hsbc.eventbus.singlethread.SingleThreadedEventBus;
import com.hsbc.statistics.SlidingWindowStatistics.Statistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.within;

@Timeout(value = 60, unit = TimeUnit.SECONDS)
class HistogramSlidingWindowStatisticsTest {

    private final EventBus bus = new SingleThreadedEventBus();

    private HistogramSlidingWindowStatistics statistics(int windowSize) {
        return new HistogramSlidingWindowStatistics(windowSize, bus);
    }

    private static void addAll(SlidingWindowStatistics statistics, int... values) {
        for (int value : values) {
            statistics.add(value);
        }
    }

    @Nested
    class Construction {

        @Test
        void rejects_invalid_arguments() {
            assertThatIllegalArgumentException().isThrownBy(() -> statistics(0));
            assertThatIllegalArgumentException().isThrownBy(() -> statistics(-1));
            assertThatNullPointerException()
                    .isThrownBy(() -> new HistogramSlidingWindowStatistics(1, null))
                    .withMessage("bus");
        }

        @Test
        void rejects_null_subscription_arguments() {
            HistogramSlidingWindowStatistics statistics = statistics(1);
            assertThatNullPointerException()
                    .isThrownBy(() -> statistics.subscribeForStatistics(null, s -> { }))
                    .withMessage("criterion");
            assertThatNullPointerException()
                    .isThrownBy(() -> statistics.subscribeForStatistics((p, c) -> true, null))
                    .withMessage("subscriber");
            assertThatNullPointerException()
                    .isThrownBy(() -> statistics.subscribeForStatistics(null))
                    .withMessage("subscriber");
        }
    }

    @Nested
    class Values {

        @Test
        void empty_window_gives_nan() {
            Statistics latest = statistics(3).getLatestStatistics();

            assertThat(latest.getMean()).isNaN();
            assertThat(latest.getMode()).isNaN();
            for (int p = 0; p <= 100; p++) {
                assertThat(latest.getPctile(p)).isNaN();
            }
        }

        @Test
        void rejects_percentile_outside_range() {
            HistogramSlidingWindowStatistics statistics = statistics(3);
            statistics.add(1);

            assertThatIllegalArgumentException().isThrownBy(() -> statistics.getLatestStatistics().getPctile(-1));
            assertThatIllegalArgumentException().isThrownBy(() -> statistics.getLatestStatistics().getPctile(101));
            assertThatIllegalArgumentException().isThrownBy(() -> StatisticsSnapshot.EMPTY.getPctile(101));
        }

        @Test
        void window_not_full_uses_the_values_present() {
            HistogramSlidingWindowStatistics statistics = statistics(10);
            addAll(statistics, 10, 20);

            Statistics latest = statistics.getLatestStatistics();
            assertThat(latest.getMean()).isEqualTo(15);
            assertThat(latest.getPctile(0)).isEqualTo(10);
            assertThat(latest.getPctile(50)).isEqualTo(10);
            assertThat(latest.getPctile(51)).isEqualTo(20);
            assertThat(latest.getPctile(100)).isEqualTo(20);
        }

        @Test
        void eviction_removes_the_oldest_value() {
            HistogramSlidingWindowStatistics statistics = statistics(3);
            addAll(statistics, 1, 2, 3, 100);

            Statistics latest = statistics.getLatestStatistics();
            assertThat(latest.getMean()).isEqualTo(35);
            assertThat(latest.getPctile(0)).isEqualTo(2);
            assertThat(latest.getPctile(100)).isEqualTo(100);
        }

        @Test
        void window_of_one_follows_the_last_value() {
            HistogramSlidingWindowStatistics statistics = statistics(1);
            addAll(statistics, 5, -7);

            Statistics latest = statistics.getLatestStatistics();
            assertThat(latest.getMean()).isEqualTo(-7);
            assertThat(latest.getMode()).isEqualTo(-7);
            assertThat(latest.getPctile(0)).isEqualTo(-7);
            assertThat(latest.getPctile(100)).isEqualTo(-7);
        }

        @Test
        void small_values_are_exact() {
            int windowSize = 50;
            Random random = new Random(1);
            HistogramSlidingWindowStatistics statistics = statistics(windowSize);
            Deque<Integer> window = new ArrayDeque<>();

            for (int i = 0; i < 500; i++) {
                int value = random.nextInt(256) - 128;
                statistics.add(value);

                window.addLast(value);
                if (window.size() > windowSize) {
                    window.removeFirst();
                }

                assertExactStatistics(statistics.getLatestStatistics(), window);
            }
        }

        private void assertExactStatistics(Statistics actual, Collection<Integer> window) {
            int[] sorted = window.stream().mapToInt(Integer::intValue).sorted().toArray();
            for (int p = 0; p <= 100; p++) {
                assertThat(actual.getPctile(p)).isEqualTo(sorted[rank(p, sorted.length) - 1]);
            }
            assertThat(actual.getMode()).isEqualTo(exactMode(sorted));
        }

        @Test
        void mode_tie_gives_the_lowest_value() {
            HistogramSlidingWindowStatistics statistics = statistics(10);
            addAll(statistics, 5, -3, 5, -3, 9);

            assertThat(statistics.getLatestStatistics().getMode()).isEqualTo(-3);
        }

        @Test
        void mode_is_the_most_frequent_bucket() {
            HistogramSlidingWindowStatistics statistics = statistics(10);
            addAll(statistics, 1, 2, 2, 3);
            assertThat(statistics.getLatestStatistics().getMode()).isEqualTo(2);

            addAll(statistics, 1000, 1001, 1000);
            assertThat(statistics.getLatestStatistics().getMode()).isCloseTo(1000, within(1000 / 128.0 + 1));
        }

        @Test
        void large_values_are_within_one_percent() {
            HistogramSlidingWindowStatistics statistics = statistics(10);
            addAll(statistics, Integer.MIN_VALUE, -1_000_000, 1_000_000, Integer.MAX_VALUE);

            Statistics latest = statistics.getLatestStatistics();
            assertThat(latest.getPctile(0)).isCloseTo(Integer.MIN_VALUE, within(Integer.MAX_VALUE * 0.01));
            assertThat(latest.getPctile(26)).isCloseTo(-1_000_000, within(10_000.0));
            assertThat(latest.getPctile(75)).isCloseTo(1_000_000, within(10_000.0));
            assertThat(latest.getPctile(100)).isCloseTo(Integer.MAX_VALUE, within(Integer.MAX_VALUE * 0.01));
        }

        private int rank(int pctile, int size) {
            return Math.max(1, (int) Math.ceil(pctile * size / 100.0));
        }

        private double exactMode(int[] sorted) {
            int best = sorted[0];
            int bestCount = 0;
            for (int i = 0; i < sorted.length; ) {
                int j = i;
                while (j < sorted.length && sorted[j] == sorted[i]) {
                    j++;
                }
                if (j - i > bestCount) {
                    bestCount = j - i;
                    best = sorted[i];
                }
                i = j;
            }
            return best;
        }
    }

    @Nested
    class Snapshots {

        @Test
        void snapshot_keeps_its_values_after_more_adds() {
            HistogramSlidingWindowStatistics statistics = statistics(3);
            addAll(statistics, 10, 20);
            Statistics before = statistics.getLatestStatistics();

            addAll(statistics, 1000, 2000, 3000);

            assertThat(before.getMean()).isEqualTo(15);
            assertThat(before.getPctile(100)).isEqualTo(20);
            assertThat(statistics.getLatestStatistics()).isNotSameAs(before);
        }
    }

    @Nested
    class Subscription {

        @Test
        void level_criterion_gets_each_matching_update() {
            HistogramSlidingWindowStatistics statistics = statistics(2);
            List<Double> means = new ArrayList<>();
            statistics.subscribeForStatistics((previous, current) -> current.getMean() > 10, s -> means.add(s.getMean()));

            addAll(statistics, 5, 20, 30, 1);

            assertThat(means).containsExactly(12.5, 25.0, 15.5);
        }

        @Test
        void default_subscription_gets_every_update() {
            HistogramSlidingWindowStatistics statistics = statistics(2);
            List<Double> means = new ArrayList<>();
            statistics.subscribeForStatistics(s -> means.add(s.getMean()));

            addAll(statistics, 1, 2, 3);

            assertThat(means).containsExactly(1.0, 1.5, 2.5);
        }

        @Test
        void edge_criterion_fires_once_per_crossing() {
            HistogramSlidingWindowStatistics statistics = statistics(1);
            List<Double> means = new ArrayList<>();
            statistics.subscribeForStatistics(
                    (previous, current) -> !(previous.getMean() >= 10) && current.getMean() >= 10,
                    s -> means.add(s.getMean()));

            addAll(statistics, 20, 30, 5, 3, 15, 16, 2, 10);

            assertThat(means).containsExactly(20.0, 15.0, 10.0);
        }

        @Test
        void criterion_gets_the_previous_statistics() {
            HistogramSlidingWindowStatistics statistics = statistics(5);
            List<Double> previousMeans = new ArrayList<>();
            statistics.subscribeForStatistics((previous, current) -> {
                previousMeans.add(previous.getMean());
                return false;
            }, s -> { });

            addAll(statistics, 4, 6);

            assertThat(previousMeans).containsExactly(Double.NaN, 4.0);
        }

        @Test
        void instances_on_a_shared_bus_get_only_their_own_updates() {
            HistogramSlidingWindowStatistics first = statistics(2);
            HistogramSlidingWindowStatistics second = statistics(2);
            List<Double> firstMeans = new ArrayList<>();
            List<Double> secondMeans = new ArrayList<>();
            first.subscribeForStatistics(s -> firstMeans.add(s.getMean()));
            second.subscribeForStatistics(s -> secondMeans.add(s.getMean()));

            first.add(1);
            second.add(100);
            first.add(3);

            assertThat(firstMeans).containsExactly(1.0, 2.0);
            assertThat(secondMeans).containsExactly(100.0);
        }
    }

    @Nested
    class WithMultiThreadedBus {

        private ExecutorService executor;

        @AfterEach
        void shutDown() throws InterruptedException {
            if (executor != null) {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }

        @Test
        void subscriber_gets_only_the_latest_statistics() {
            Queue<Runnable> tasks = new ArrayDeque<>();
            HistogramSlidingWindowStatistics statistics =
                    new HistogramSlidingWindowStatistics(10, new MultiThreadedEventBus(tasks::add));
            List<Double> means = new ArrayList<>();
            statistics.subscribeForStatistics(s -> means.add(s.getMean()));

            addAll(statistics, 10, 20, 30);
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }

            assertThat(means).containsExactly(20.0);
        }

        @Test
        void subscriber_runs_on_another_thread() throws InterruptedException {
            executor = Executors.newSingleThreadExecutor();
            HistogramSlidingWindowStatistics statistics =
                    new HistogramSlidingWindowStatistics(10, new MultiThreadedEventBus(executor));
            AtomicReference<Thread> callbackThread = new AtomicReference<>();
            CountDownLatch received = new CountDownLatch(1);
            statistics.subscribeForStatistics(s -> {
                callbackThread.set(Thread.currentThread());
                received.countDown();
            });

            statistics.add(1);

            assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(callbackThread.get()).isNotSameAs(Thread.currentThread());
        }
    }
}
