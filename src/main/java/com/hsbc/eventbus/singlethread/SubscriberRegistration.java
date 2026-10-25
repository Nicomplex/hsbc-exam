package com.hsbc.eventbus.singlethread;

import com.hsbc.eventbus.Event;

import java.lang.System.Logger;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

final class SubscriberRegistration<T extends Event> {

    private static final Logger LOGGER = System.getLogger(SubscriberRegistration.class.getName());

    private final Class<T> eventType;
    private final Predicate<? super T> filter;
    private final Consumer<? super T> consumer;

    SubscriberRegistration(Class<T> eventType, Predicate<? super T> filter, Consumer<? super T> consumer) {
        this.eventType = requireNonNull(eventType, "eventType");
        this.filter = filter;
        this.consumer = requireNonNull(consumer, "consumer");
    }

    boolean handles(Class<?> eventClass) {
        return eventType.isAssignableFrom(eventClass);
    }

    void deliver(Event event) {
        try {
            T typedEvent = eventType.cast(event);
            if (filter == null || filter.test(typedEvent)) {
                consumer.accept(typedEvent);
            }
        } catch (Exception e) {
            LOGGER.log(WARNING, "Subscriber failed for event " + event, e);
        }
    }
}
