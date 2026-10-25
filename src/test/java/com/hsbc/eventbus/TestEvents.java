package com.hsbc.eventbus;

public final class TestEvents {

    private TestEvents() {
    }

    public interface PriceEvent extends Event {
    }

    public record Quote(String symbol, double price) implements PriceEvent {
    }

    public record Trade(String symbol, double price, long quantity) implements PriceEvent {
    }

    public record Control(String name) implements Event {
    }

    public record Sequenced(int producer, int seq) implements Event {
    }

    public record MarketDataSnapshot(String symbol, long seq, double price) implements ConflatableEvent {
        @Override
        public Object getConflationKey() {
            return symbol;
        }
    }

    public static class BaseEvent implements Event {
    }

    public static class ChildEvent extends BaseEvent {
    }
}
