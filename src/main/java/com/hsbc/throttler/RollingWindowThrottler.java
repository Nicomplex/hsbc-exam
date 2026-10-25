package com.hsbc.throttler;

import java.lang.System.Logger;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.Logger.Level.WARNING;
import static java.util.Objects.requireNonNull;

/**
 * Allows at most {@code maxPermits} permits in any rolling window.
 * A ring buffer keeps the time of the last {@code maxPermits} permits.
 * Waiting subscribers come before pollers, in FIFO order.
 * While subscribers wait, only the timer thread gives permits.
 */
public final class RollingWindowThrottler implements Throttler {

    private static final Logger LOGGER = System.getLogger(RollingWindowThrottler.class.getName());
    private static final long NO_DEADLINE = Long.MAX_VALUE;
    private static final long TAKEN = 0;

    private record Subscriber(Executor executor, Runnable callback) {
    }

    private final long windowNanos;

    private final ReentrantLock lock = new ReentrantLock();
    private final long[] permitTimes;
    private int nextSlot;
    private final ArrayDeque<Subscriber> subscribers = new ArrayDeque<>();
    private volatile boolean hasSubscribers;
    private Thread timer;

    /**
     * @param maxPermits the number of permits in one window, greater than 0
     * @param window     the length of the window, greater than 0
     * @throws NullPointerException     if the window is null
     * @throws IllegalArgumentException if an argument is not greater than 0
     */
    public RollingWindowThrottler(int maxPermits, Duration window) {
        validateParams(maxPermits, window);
        this.windowNanos = window.toNanos();
        this.permitTimes = new long[maxPermits];
        Arrays.fill(permitTimes, System.nanoTime() - windowNanos);
    }

    private static void validateParams(int maxPermits, Duration window) {
        requireNonNull(window, "window");
        if (maxPermits <= 0) {
            throw new IllegalArgumentException("maxPermits must be greater than 0: " + maxPermits);
        }
        if (!window.isPositive()) {
            throw new IllegalArgumentException("window must be greater than 0: " + window);
        }
    }

    @Override
    public ThrottleResult shouldProceed() {
        if (hasSubscribers) {
            return ThrottleResult.DO_NOT_PROCEED;
        }
        lock.lock();
        try {
            return subscribers.isEmpty() && tryTakePermit() == TAKEN
                    ? ThrottleResult.PROCEED
                    : ThrottleResult.DO_NOT_PROCEED;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void notifyWhenCanProceed(Executor executor, Runnable callback) {
        requireNonNull(executor, "executor");
        requireNonNull(callback, "callback");
        Subscriber subscriber = new Subscriber(executor, callback);
        lock.lock();
        try {
            if (!subscribers.isEmpty()) {
                subscribers.add(subscriber);
                return;
            }
            if (tryTakePermit() != TAKEN) {
                subscribers.add(subscriber);
                hasSubscribers = true;
                wakeTimer();
                return;
            }
        } finally {
            lock.unlock();
        }
        dispatch(subscriber);
    }

    /**
     * Takes a permit if one is free. The caller holds the lock.
     *
     * @return {@code 0} if the permit is taken, else the nanoseconds until the next permit is free
     */
    private long tryTakePermit() {
        long now = System.nanoTime();
        // Do not compare raw nanoTime values, for example now < oldest + window: nanoTime can overflow.
        // Only the difference of 2 nanoTime values is correct, so subtract first.
        long elapsed = now - permitTimes[nextSlot];
        if (elapsed < windowNanos) {
            return windowNanos - elapsed;
        }
        permitTimes[nextSlot] = now;
        if (++nextSlot == permitTimes.length) {
            nextSlot = 0;
        }
        return TAKEN;
    }

    private void wakeTimer() {
        if (timer == null) {
            timer = new Thread(this::runTimer, "throttler-timer");
            timer.setDaemon(true);
            timer.start();
        }
        LockSupport.unpark(timer);
    }

    private void runTimer() {
        while (true) {
            LockSupport.parkNanos(grantToSubscribers());
            // Nobody interrupts the timer. Clear the flag anyway, to avoid a busy loop.
            Thread.interrupted();
        }
    }

    /**
     * Gives permits to the subscribers at the head of the queue, in FIFO order.
     *
     * @return the nanoseconds until the next permit is free, or {@link #NO_DEADLINE} if no subscriber is left
     */
    private long grantToSubscribers() {
        while (true) {
            Subscriber subscriber;
            lock.lock();
            try {
                if (subscribers.isEmpty()) {
                    return NO_DEADLINE;
                }
                long waitNanos = tryTakePermit();
                if (waitNanos != TAKEN) {
                    return waitNanos;
                }
                subscriber = subscribers.poll();
                if (subscribers.isEmpty()) {
                    hasSubscribers = false;
                }
            } finally {
                lock.unlock();
            }
            dispatch(subscriber);
        }
    }

    private static void dispatch(Subscriber subscriber) {
        try {
            subscriber.executor().execute(subscriber.callback());
        } catch (RuntimeException e) {
            LOGGER.log(WARNING, "Executor or callback failed after a permit was given", e);
        }
    }
}
