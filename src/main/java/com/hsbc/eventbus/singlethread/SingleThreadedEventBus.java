package com.hsbc.eventbus.singlethread;

import com.hsbc.eventbus.Event;
import com.hsbc.eventbus.EventBus;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public final class SingleThreadedEventBus implements EventBus {

    private final List<SubscriberRegistration<?>> registrations = new ArrayList<>();
    private final ArrayDeque<Event> pendingEvents = new ArrayDeque<>();
    private boolean dispatching;

    @Override
    public void publishEvent(Event event) {
        pendingEvents.addLast(requireNonNull(event, "event"));
        if (dispatching) {
            return;
        }
        dispatching = true;
        try {
            Event nextEvent;
            while ((nextEvent = pendingEvents.pollFirst()) != null) {
                dispatch(nextEvent);
            }
        } finally {
            dispatching = false;
        }
    }

    private void dispatch(Event event) {
        for (SubscriberRegistration<?> registration : route(event.getClass())) {
            registration.deliver(event);
        }
    }

    private List<SubscriberRegistration<?>> route(Class<?> eventClass) {
        List<SubscriberRegistration<?>> matches = new ArrayList<>();
        for (SubscriberRegistration<?> registration : registrations) {
            if (registration.handles(eventClass)) {
                matches.add(registration);
            }
        }
        return matches;
    }

    @Override
    public <T extends Event> void addSubscriberForFilteredEvents(
            Class<T> eventType,
            Predicate<? super T> filter,
            Consumer<? super T> subscriber) {
        registrations.add(new SubscriberRegistration<>(eventType, filter, subscriber));
    }
}
