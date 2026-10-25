package com.hsbc.statistics;

import java.util.Arrays;

final class StatisticsSnapshot implements SlidingWindowStatistics.Statistics {

    static final int PERCENTILE_COUNT = 101;
    static final StatisticsSnapshot EMPTY = empty();

    private final double mean;
    private final double mode;
    private final double[] percentiles;

    StatisticsSnapshot(double mean, double mode, double[] percentiles) {
        this.mean = mean;
        this.mode = mode;
        this.percentiles = percentiles;
    }

    private static StatisticsSnapshot empty() {
        double[] percentiles = new double[PERCENTILE_COUNT];
        Arrays.fill(percentiles, Double.NaN);
        return new StatisticsSnapshot(Double.NaN, Double.NaN, percentiles);
    }

    @Override
    public double getMean() {
        return mean;
    }

    @Override
    public double getMode() {
        return mode;
    }

    @Override
    public double getPctile(int pctile) {
        if (pctile < 0 || pctile >= PERCENTILE_COUNT) {
            throw new IllegalArgumentException("pctile must be between 0 and 100: " + pctile);
        }
        return percentiles[pctile];
    }
}
