package com.hsbc.statistics;

import java.util.function.BiPredicate;
import java.util.function.Consumer;

public interface SlidingWindowStatistics {

    void add(int measurement);

    /**
     * Calls the subscriber with the current statistics after an {@link #add} if the criterion accepts the update.
     * The criterion gets the previous and the current statistics.
     */
    void subscribeForStatistics(
            BiPredicate<? super Statistics, ? super Statistics> criterion,
            Consumer<? super Statistics> subscriber);

    default void subscribeForStatistics(Consumer<? super Statistics> subscriber) {
        subscribeForStatistics((previous, current) -> true, subscriber);
    }

    Statistics getLatestStatistics();

    interface Statistics {

        double getMean();

        double getMode();

        double getPctile(int pctile);
    }
}
