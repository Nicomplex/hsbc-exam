package com.hsbc.eventbus;

import java.util.function.Consumer;
import java.util.function.Predicate;

public interface EventBus {

    void publishEvent(Event event);

    default <T extends Event> void addSubscriber(Class<T> eventType, Consumer<? super T> subscriber) {
        addSubscriberForFilteredEvents(eventType, null, subscriber);
    }

    <T extends Event> void addSubscriberForFilteredEvents(
            Class<T> eventType, Predicate<? super T> filter, Consumer<? super T> subscriber);
}
