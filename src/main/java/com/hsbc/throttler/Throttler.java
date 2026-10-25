package com.hsbc.throttler;

import java.util.concurrent.Executor;

public interface Throttler {

    enum ThrottleResult {
        PROCEED,
        DO_NOT_PROCEED
    }

    /**
     * Checks if the caller can proceed (poll).
     * {@code PROCEED} takes one permit.
     */
    ThrottleResult shouldProceed();

    /**
     * Asks to be told when the caller can proceed (push).
     * The throttler takes one permit, then calls {@code executor.execute(callback)}.
     * Each call is a one-shot request for one permit.
     *
     * @param executor runs the callback, not null
     * @param callback the action to run when the permit is given, not null
     */
    void notifyWhenCanProceed(Executor executor, Runnable callback);
}
