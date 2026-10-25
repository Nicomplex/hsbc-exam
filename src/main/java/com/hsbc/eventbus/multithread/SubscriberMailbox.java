package com.hsbc.eventbus.multithread;

import com.hsbc.eventbus.Event;

import java.lang.System.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

final class SubscriberMailbox<T extends Event> {

    private static final Logger LOGGER = System.getLogger(SubscriberMailbox.class.getName());

    private record ConflationMarker(Object key) {
    }

    private final Class<T> eventType;
    private final Predicate<? super T> filter;
    private final Consumer<? super T> consumer;

    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<Object, Event> conflatedValues = new ConcurrentHashMap<>();
    private final AtomicBoolean scheduled = new AtomicBoolean();

    SubscriberMailbox(Class<T> eventType, Predicate<? super T> filter, Consumer<? super T> consumer) {
        this.eventType = requireNonNull(eventType, "eventType");
        this.filter = filter;
        this.consumer = requireNonNull(consumer, "consumer");
    }

    boolean handles(Class<?> eventClass) {
        return eventType.isAssignableFrom(eventClass);
    }

    boolean accepts(Event event) {
        try {
            return filter == null || filter.test(eventType.cast(event));
        } catch (Exception e) {
            LOGGER.log(WARNING, "Filter failed for event " + event, e);
            return false;
        }
    }

    void deliver(Event event) {
        try {
            consumer.accept(eventType.cast(event));
        } catch (Exception e) {
            LOGGER.log(WARNING, "Subscriber failed for event " + event, e);
        }
    }

    void offer(Event event, Object conflationKey) {
        if (conflationKey == null) {
            queue.offer(event);
        } else if (conflatedValues.put(conflationKey, event) == null) {
            queue.offer(new ConflationMarker(conflationKey));
        }
    }

    Event pollNext() {
        Object item;
        while ((item = queue.poll()) != null) {
            if (item instanceof ConflationMarker(Object key)) {
                Event latest = conflatedValues.remove(key);
                if (latest != null) {
                    return latest;
                }
            } else {
                return (Event) item;
            }
        }
        return null;
    }

    boolean isEmpty() {
        return queue.isEmpty();
    }

    boolean tryAcquire() {
        return scheduled.compareAndSet(false, true);
    }

    void release() {
        scheduled.set(false);
    }
}
