package com.hsbc.eventbus.singlethread;

import com.hsbc.eventbus.Event;
import com.hsbc.eventbus.TestEvents.BaseEvent;
import com.hsbc.eventbus.TestEvents.ChildEvent;
import com.hsbc.eventbus.TestEvents.PriceEvent;
import com.hsbc.eventbus.TestEvents.Quote;
import com.hsbc.eventbus.TestEvents.Trade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class SingleThreadedEventBusTest {

    private SingleThreadedEventBus bus;

    @BeforeEach
    void setUp() {
        bus = new SingleThreadedEventBus();
    }

    @Nested
    class Basics {

        @Test
        void delivers_event_on_publishing_thread() {
            List<Thread> threads = new ArrayList<>();
            List<Quote> received = new ArrayList<>();
            bus.addSubscriber(Quote.class, q -> {
                threads.add(Thread.currentThread());
                received.add(q);
            });

            Quote quote = new Quote("AAPL", 10);
            bus.publishEvent(quote);

            assertThat(received).containsExactly(quote);
            assertThat(threads).containsExactly(Thread.currentThread());
        }

        @Test
        void ignores_event_without_subscriber() {
            List<Quote> received = new ArrayList<>();
            bus.addSubscriber(Quote.class, received::add);

            bus.publishEvent(new Trade("AAPL", 10, 5));

            assertThat(received).isEmpty();
        }

        @Test
        void rejects_null_arguments() {
            assertThatNullPointerException().isThrownBy(() -> bus.publishEvent(null));
            assertThatNullPointerException().isThrownBy(() -> bus.addSubscriber(null, e -> { }));
            assertThatNullPointerException().isThrownBy(() -> bus.addSubscriber(Quote.class, null));
            assertThatNullPointerException().isThrownBy(
                    () -> bus.addSubscriberForFilteredEvents(Quote.class, e -> true, null));
        }

        @Test
        void null_filter_accepts_every_event() {
            List<Quote> received = new ArrayList<>();
            bus.addSubscriberForFilteredEvents(Quote.class, null, received::add);
            Quote quote = new Quote("A", 1);

            bus.publishEvent(quote);

            assertThat(received).containsExactly(quote);
        }

        @Test
        void filters_events() {
            List<Quote> received = new ArrayList<>();
            bus.addSubscriberForFilteredEvents(Quote.class, q -> q.price() > 10, received::add);

            bus.publishEvent(new Quote("AAPL", 5));
            bus.publishEvent(new Quote("AAPL", 15));

            assertThat(received).containsExactly(new Quote("AAPL", 15));
        }

        @Test
        void throwing_filter_skips_the_event() {
            List<Quote> received = new ArrayList<>();
            bus.addSubscriberForFilteredEvents(Quote.class, q -> {
                throw new IllegalStateException("bad filter");
            }, received::add);

            bus.publishEvent(new Quote("AAPL", 5));

            assertThat(received).isEmpty();
        }
    }

    @Nested
    class PolymorphicRouting {

        @Test
        void routes_through_class_hierarchy() {
            List<Event> base = new ArrayList<>();
            List<Event> child = new ArrayList<>();
            bus.addSubscriber(BaseEvent.class, base::add);
            bus.addSubscriber(ChildEvent.class, child::add);
            BaseEvent baseEvent = new BaseEvent();
            ChildEvent childEvent = new ChildEvent();

            bus.publishEvent(baseEvent);
            bus.publishEvent(childEvent);

            assertThat(base).containsExactly(baseEvent, childEvent);
            assertThat(child).containsExactly(childEvent);
        }

        @Test
        void routes_through_interface() {
            List<PriceEvent> received = new ArrayList<>();
            bus.addSubscriber(PriceEvent.class, received::add);
            Quote quote = new Quote("A", 1);
            Trade trade = new Trade("A", 1, 2);

            bus.publishEvent(quote);
            bus.publishEvent(trade);
            bus.publishEvent(new BaseEvent());

            assertThat(received).containsExactly(quote, trade);
        }

        @Test
        void event_subscriber_receives_everything() {
            List<Event> received = new ArrayList<>();
            bus.addSubscriber(Event.class, received::add);
            Quote quote = new Quote("A", 1);
            ChildEvent child = new ChildEvent();

            bus.publishEvent(quote);
            bus.publishEvent(child);

            assertThat(received).containsExactly(quote, child);
        }

        @Test
        void registration_receives_each_event_once() {
            // Quote reaches Event, PriceEvent and Quote. One registration must see it once.
            List<Event> received = new ArrayList<>();
            bus.addSubscriber(Event.class, received::add);
            bus.addSubscriber(PriceEvent.class, e -> { });
            bus.addSubscriber(Quote.class, e -> { });

            bus.publishEvent(new Quote("A", 1));

            assertThat(received).hasSize(1);
        }

        @Test
        void two_registrations_are_two_subscribers() {
            List<Event> received = new ArrayList<>();
            bus.addSubscriber(BaseEvent.class, received::add);
            bus.addSubscriber(ChildEvent.class, received::add);

            bus.publishEvent(new ChildEvent());

            assertThat(received).hasSize(2);
        }

        @Test
        void same_consumer_registered_twice_is_called_twice() {
            List<Event> received = new ArrayList<>();
            bus.addSubscriber(Quote.class, received::add);
            bus.addSubscriber(Quote.class, received::add);

            bus.publishEvent(new Quote("A", 1));

            assertThat(received).hasSize(2);
        }
    }

    @Nested
    class Reentrancy {

        @Test
        void publish_in_callback_keeps_strict_fifo() {
            List<String> log = new ArrayList<>();
            bus.addSubscriber(Quote.class, q -> {
                log.add("A:" + q.symbol());
                if (q.symbol().equals("E1")) {
                    bus.publishEvent(new Quote("E2", 0));
                }
            });
            bus.addSubscriber(Quote.class, q -> log.add("B:" + q.symbol()));

            bus.publishEvent(new Quote("E1", 0));

            assertThat(log).containsExactly("A:E1", "B:E1", "A:E2", "B:E2");
        }

        @Test
        void added_subscriber_misses_current_event_but_gets_next() {
            List<String> added = new ArrayList<>();
            boolean[] done = {false};
            bus.addSubscriber(Quote.class, q -> {
                if (!done[0]) {
                    done[0] = true;
                    bus.addSubscriber(Quote.class, e -> added.add(e.symbol()));
                }
            });

            bus.publishEvent(new Quote("E1", 0));
            bus.publishEvent(new Quote("E2", 0));

            assertThat(added).containsExactly("E2");
        }

        @Test
        void error_during_dispatch_leaves_bus_usable() {
            List<String> received = new ArrayList<>();
            bus.addSubscriber(Quote.class, q -> {
                if (q.symbol().equals("boom")) {
                    throw new AssertionError("fatal");
                }
                received.add(q.symbol());
            });

            try {
                bus.publishEvent(new Quote("boom", 0));
            } catch (AssertionError expected) {
                // Errors reach the caller.
            }
            bus.publishEvent(new Quote("next", 0));

            assertThat(received).containsExactly("next");
        }
    }

    @Nested
    class FaultIsolation {

        @Test
        void failing_subscriber_does_not_stop_others() {
            List<Quote> received = new ArrayList<>();
            bus.addSubscriber(Quote.class, q -> {
                throw new IllegalStateException("boom");
            });
            bus.addSubscriber(Quote.class, received::add);
            Quote quote = new Quote("A", 1);

            bus.publishEvent(quote);

            assertThat(received).containsExactly(quote);
        }

        @Test
        void failing_subscriber_stays_subscribed() {
            int[] calls = {0};
            bus.addSubscriber(Quote.class, q -> {
                calls[0]++;
                throw new IllegalStateException();
            });

            for (int i = 0; i < 10; i++) {
                bus.publishEvent(new Quote("A", i));
            }

            assertThat(calls[0]).isEqualTo(10);
        }
    }
}
