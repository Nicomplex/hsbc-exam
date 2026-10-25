package com.hsbc.statistics;

import com.hsbc.eventbus.EventBus;

import java.util.function.BiPredicate;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class HistogramSlidingWindowStatistics implements SlidingWindowStatistics {

    private static final int SUB_BUCKET_BITS = 7;
    private static final int EXACT_LIMIT = 1 << SUB_BUCKET_BITS;
    private static final int HALF_SUB_BUCKETS = EXACT_LIMIT >> 1;
    private static final int FIRST_EXPONENT = SUB_BUCKET_BITS;
    private static final int LAST_EXPONENT = 30;
    private static final int BUCKETS_PER_SIGN =
            EXACT_LIMIT + (LAST_EXPONENT - FIRST_EXPONENT + 1) * HALF_SUB_BUCKETS;
    private static final int BUCKET_COUNT = 2 * BUCKETS_PER_SIGN;
    private static final double[] BUCKET_MIDDLES = bucketMiddles();

    private final int[] window;
    private final int[] bucketCounts = new int[BUCKET_COUNT];
    private final EventBus bus;
    private int head;
    private int count;
    private long sum;
    private volatile Statistics latest = StatisticsSnapshot.EMPTY;

    public HistogramSlidingWindowStatistics(int windowSize, EventBus bus) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive: " + windowSize);
        }
        this.window = new int[windowSize];
        this.bus = requireNonNull(bus, "bus");
    }

    @Override
    public void add(int measurement) {
        if (count == window.length) {
            int oldest = window[head];
            sum -= oldest;
            bucketCounts[bucketIndex(oldest)]--;
        } else {
            count++;
        }
        window[head] = measurement;
        head = head + 1 == window.length ? 0 : head + 1;
        sum += measurement;
        bucketCounts[bucketIndex(measurement)]++;

        Statistics previous = latest;
        Statistics current = snapshot();
        latest = current;
        bus.publishEvent(new StatisticsUpdate(this, previous, current));
    }

    @Override
    public void subscribeForStatistics(
            BiPredicate<? super Statistics, ? super Statistics> criterion,
            Consumer<? super Statistics> subscriber) {
        requireNonNull(criterion, "criterion");
        requireNonNull(subscriber, "subscriber");
        bus.addSubscriberForFilteredEvents(
                StatisticsUpdate.class,
                update -> update.source() == this && criterion.test(update.previous(), update.current()),
                update -> subscriber.accept(update.current()));
    }

    @Override
    public Statistics getLatestStatistics() {
        return latest;
    }

    /**
     * Calculates the mode and the 101 percentiles (nearest rank) with one scan of the buckets.
     */
    private StatisticsSnapshot snapshot() {
        double[] percentiles = new double[StatisticsSnapshot.PERCENTILE_COUNT];
        long total = count;
        int nextPctile = 0;
        long nextRank = 1;
        long cumulative = 0;
        int modeCount = 0;
        double mode = Double.NaN;
        for (int i = 0; i < BUCKET_COUNT; i++) {
            int bucketCount = bucketCounts[i];
            if (bucketCount == 0) {
                continue;
            }
            cumulative += bucketCount;
            double middle = BUCKET_MIDDLES[i];
            if (bucketCount > modeCount) {
                modeCount = bucketCount;
                mode = middle;
            }
            while (nextPctile < percentiles.length && cumulative >= nextRank) {
                percentiles[nextPctile] = middle;
                nextPctile++;
                nextRank = Math.max(1, (nextPctile * total + 99) / 100);
            }
            if (cumulative == total) {
                break;
            }
        }
        return new StatisticsSnapshot((double) sum / total, mode, percentiles);
    }

    /**
     * Gives the bucket of a value, in value order.
     * A negative value uses {@code ~value}, because {@code -value} overflows for {@link Integer#MIN_VALUE}.
     */
    private static int bucketIndex(int value) {
        int magnitude = value >= 0 ? value : ~value;
        int bucket = magnitudeBucket(magnitude);
        return value >= 0 ? BUCKETS_PER_SIGN + bucket : BUCKETS_PER_SIGN - 1 - bucket;
    }

    /**
     * Keeps the 7 most significant bits of the magnitude.
     * A magnitude below 128 is exact.
     */
    private static int magnitudeBucket(int magnitude) {
        if (magnitude < EXACT_LIMIT) {
            return magnitude;
        }
        int exponent = 31 - Integer.numberOfLeadingZeros(magnitude);
        int shift = exponent - (SUB_BUCKET_BITS - 1);
        return EXACT_LIMIT
                + (exponent - FIRST_EXPONENT) * HALF_SUB_BUCKETS
                + ((magnitude >>> shift) - HALF_SUB_BUCKETS);
    }

    /**
     * Gives the middle value of each bucket.
     */
    private static double[] bucketMiddles() {
        double[] middles = new double[BUCKET_COUNT];
        for (int bucket = 0; bucket < BUCKETS_PER_SIGN; bucket++) {
            long lowest = bucket;
            long width = 1;
            if (bucket >= EXACT_LIMIT) {
                int offset = bucket - EXACT_LIMIT;
                int shift = offset / HALF_SUB_BUCKETS + 1;
                lowest = (long) (HALF_SUB_BUCKETS + offset % HALF_SUB_BUCKETS) << shift;
                width = 1L << shift;
            }
            double magnitudeMiddle = lowest + (width - 1) / 2.0;
            middles[BUCKETS_PER_SIGN + bucket] = magnitudeMiddle;
            middles[BUCKETS_PER_SIGN - 1 - bucket] = -magnitudeMiddle - 1;
        }
        return middles;
    }
}
