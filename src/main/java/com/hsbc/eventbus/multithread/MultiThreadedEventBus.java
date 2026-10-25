package com.hsbc.eventbus.multithread;

import com.hsbc.eventbus.ConflatableEvent;
import com.hsbc.eventbus.Event;
import com.hsbc.eventbus.EventBus;

import java.lang.System.Logger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

public final class MultiThreadedEventBus implements EventBus {

    private static final Logger LOGGER = System.getLogger(MultiThreadedEventBus.class.getName());
    private static final int DRAIN_BATCH_SIZE = 100;

    private final Executor executor;
    private final List<SubscriberMailbox<?>> mailboxes = new CopyOnWriteArrayList<>();
    private volatile Map<Class<?>, SubscriberMailbox<?>[]> routes = new ConcurrentHashMap<>();

    public MultiThreadedEventBus(Executor executor) {
        this.executor = requireNonNull(executor, "executor");
    }

    @Override
    public void publishEvent(Event event) {
        requireNonNull(event, "event");
        Object conflationKey = event instanceof ConflatableEvent conflatable
                ? requireNonNull(conflatable.getConflationKey(), "conflationKey")
                : null;
        for (SubscriberMailbox<?> mailbox : routesFor(event.getClass())) {
            if (mailbox.accepts(event)) {
                mailbox.offer(event, conflationKey);
                if (mailbox.tryAcquire()) {
                    submit(mailbox);
                }
            }
        }
    }

    @Override
    public <T extends Event> void addSubscriberForFilteredEvents(
            Class<T> eventType, Predicate<? super T> filter, Consumer<? super T> subscriber) {
        mailboxes.add(new SubscriberMailbox<>(eventType, filter, subscriber));
        routes = new ConcurrentHashMap<>();
    }

    private SubscriberMailbox<?>[] routesFor(Class<?> eventClass) {
        Map<Class<?>, SubscriberMailbox<?>[]> current = routes;
        SubscriberMailbox<?>[] targets = current.get(eventClass);
        return targets != null ? targets : current.computeIfAbsent(eventClass, this::route);
    }

    private SubscriberMailbox<?>[] route(Class<?> eventClass) {
        return mailboxes.stream().filter(m -> m.handles(eventClass)).toArray(SubscriberMailbox<?>[]::new);
    }

    private void submit(SubscriberMailbox<?> mailbox) {
        try {
            executor.execute(() -> drain(mailbox));
        } catch (RejectedExecutionException e) {
            mailbox.release();
            LOGGER.log(WARNING, "Executor rejected a drain task", e);
        }
    }

    private void drain(SubscriberMailbox<?> mailbox) {
        try {
            Event event;
            for (int i = 0; i < DRAIN_BATCH_SIZE && (event = mailbox.pollNext()) != null; i++) {
                mailbox.deliver(event);
            }
        } finally {
            mailbox.release();
        }
        if (!mailbox.isEmpty() && mailbox.tryAcquire()) {
            submit(mailbox);
        }
    }
}
