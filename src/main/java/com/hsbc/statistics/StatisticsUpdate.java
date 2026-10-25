package com.hsbc.statistics;

import com.hsbc.eventbus.ConflatableEvent;
import com.hsbc.statistics.SlidingWindowStatistics.Statistics;

record StatisticsUpdate(SlidingWindowStatistics source, Statistics previous, Statistics current)
        implements ConflatableEvent {

    @Override
    public Object getConflationKey() {
        return source;
    }
}
