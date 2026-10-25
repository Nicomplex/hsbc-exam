package com.hsbc.eventbus;

/**
 * An event where only the latest value for a key matters.
 */
public interface ConflatableEvent extends Event {

    /**
     * Not null, with stable {@code equals} and {@code hashCode}.
     */
    Object getConflationKey();
}
