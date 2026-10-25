# HSBC take-home exercise

## Build

- Requirements: Java 25 and Maven 3.9.
- Build and run the tests: `mvn verify`

## Time log

| Section                      | Status | Time  |
|------------------------------|--------|-------|
| 1. `ProbabilisticRandomGen`  | Done   | 1 h   |
| 2. `EventBus`                | Done   | 2.5 h |
| 3. `Throttler`               | Done   | 2 h   |
| 4. `SlidingWindowStatistics` | Done   | 1 h   |

## 1. ProbabilisticRandomGen

Code: [`src/main/java/com/hsbc/random`](src/main/java/com/hsbc/random)

### Assumptions

- A probability is a relative weight.
  The sum does not need to be 1.
- Each probability is finite and greater than 0.
  Other values are a caller error, so the constructor fails fast.
- A number can occur more than once in the list.
  Its weights add up.
- The distribution does not change after construction.
- The generator is called much more often than it is built.
  So the sample cost is more important than the construction cost.

### Design decisions

- Algorithm: Vose's alias method.
  - Construction: O(N) time, O(N) memory.
  - `nextFromSample()`: O(1), with one random value, one comparison and 2 or 3 array reads.
- Creation: constructor.
  - All fields are final, so the object is immutable and thread-safe.
  - The object is valid after construction.
- State: flat primitive arrays, with no boxing.
- Random source: an optional `DoubleSupplier`.
  - The default is `ThreadLocalRandom`, not a `new Random()`, for performance.
    - `Random` updates an atomic seed with a compare-and-set (CAS) on each call.
      `ThreadLocalRandom` updates a plain field of the current thread, with no CAS.
    - When many threads share one `Random`, they compete for the same seed.
      `ThreadLocalRandom` has no shared state, so it scales with the number of threads.
  - The tests inject fixed values to check the table by hand.

#### How the alias method works

The constructor cuts the distribution into N columns of height 1.
Each column holds at most 2 numbers: its own number and one alias.

Example with `A = 0.5`, `B = 0.3`, `C = 0.2`, so N = 3:

```
          col 0     col 1     col 2
        +-------+ +-------+ +-------+  1.0
        |       | |   A   | |       |
        |       | +-------+ |   A   |  0.9
        |   A   | |       | |       |
        |       | |   B   | +-------+  0.6
        |       | |       | |   C   |
        +-------+ +-------+ +-------+  0.0
```

| Column | `numbers` | `prob` | `alias` |
|---|---|---|---|
| 0 | A | 1.0 | A |
| 1 | B | 0.9 | A |
| 2 | C | 0.6 | A |

A gets `(1.0 + 0.1 + 0.4) / 3 = 0.5`.

To sample:

1. Get `u` in `[0, 1)` and compute `position = u * N`.
2. The integer part of `position` picks the column.
3. If the fraction is less than `prob[column]`, return the number of the column.
   Else, return its alias.

Example: `u = 0.95` gives `position = 2.85`, so column 2.
`0.85 >= 0.6`, so the result is A.

### Known limits

- The distribution is immutable.
  To change it, create a new generator in O(N).

## 2. EventBus

Code: [`src/main/java/com/hsbc/eventbus`](src/main/java/com/hsbc/eventbus)

- `singlethread/SingleThreadedEventBus`: the callback runs on the publisher thread.
- `multithread/MultiThreadedEventBus`: many producers, many consumers, with conflation.

### Assumptions

- Events are immutable, so the bus can share one event between subscriber threads.
- A subscriber is not thread-safe.
- Most subscribers register at start-up, before the main event flow.
- A conflation key has stable `equals` and `hashCode`.
- The bus is in memory only.
  It does not persist events.

### Design decisions

#### API

- `publishEvent` takes an `Event` marker interface, not `Object`.
  The compiler rejects other types, and `ConflatableEvent` extends `Event`.
- A subscriber is a plain `Consumer`, and a filter is a `Predicate`.
  - The `Class` argument is necessary because of type erasure.
  - A null filter accepts every event.
- Routing is polymorphic.
  A subscriber for `C` receives `C` and its subtypes, once for each event.

#### SingleThreadedEventBus

- A publish inside a callback goes to a queue.
  The bus delivers it after the current event, so the order stays strict FIFO.

#### MultiThreadedEventBus

- Each subscriber has one mailbox.
  Only one drain task at a time drains a mailbox.
  So the bus never calls a subscriber concurrently, and the subscriber needs no lock.
- Conflation: a `ConflatableEvent` gives a key.
  The mailbox keeps only the latest value for each key.
  The key keeps its first position in the queue.
- Order: FIFO for each producer and each subscriber.
- Route cache: the bus keeps the matching mailboxes for each event class.
  A new subscriber resets the cache.
- Filters run on the producer thread, before the queue.
  So a rejected event cannot overwrite a conflated value.
  Filters must be thread-safe.
- `DRAIN_BATCH_SIZE` (100): a drain task delivers at most 100 events.
  Then it gives the thread to the other mailboxes.
- Failure isolation, in both buses:
  - The bus logs a throwing filter or subscriber with `System.Logger`.
  - The subscriber stays subscribed.

### Known limits

- No unsubscribe.
  - Possible extension: `addSubscriber` returns a `Subscription` handle with an `unsubscribe` method.
- Unbounded mailboxes.
  - A slow subscriber of non-conflatable events can fill the memory.
  - A bounded mailbox needs a back-pressure policy (block, drop or disconnect).
    That is a business decision.
- No eviction of a failing subscriber.
  - A subscriber that throws on each event stays subscribed, and the bus logs each failure.
  - Possible extension: an opt-in circuit breaker that tells the owner of the subscriber.

## 3. Throttler

Code: [`src/main/java/com/hsbc/throttler`](src/main/java/com/hsbc/throttler)

### Assumptions

- `PROCEED` takes the permit, so the check and the action are atomic.
- Many threads can call the throttler.
- Each `notifyWhenCanProceed` call asks for one permit, one time.
  The throttler takes the permit before the callback.
- Subscribers come before pollers, in FIFO order.
  While a subscriber waits, `shouldProceed()` returns `DO_NOT_PROCEED`.
- A late notification is acceptable. An early one is not.
- The configuration is immutable.

### Design decisions

- Algorithm: exact rolling window.
  - A ring buffer keeps the times of the last N permits.
  - A permit is free if `now - oldest >= window`.
  - O(1) time for each call, O(N) memory.
- Concurrency: one short lock around the ring buffer and the subscriber queue.
  - The lock never covers user code.
  - A `volatile` flag lets `shouldProceed()` refuse with no lock while subscribers wait.
- Clock: `System.nanoTime()`.
- Push: `notifyWhenCanProceed(Executor executor, Runnable callback)`.
  - The callback runs on the executor that the caller gives, for example its event loop.
- Timer: one lazy daemon thread with `LockSupport.parkNanos`.
  - While subscribers wait, only the timer gives permits.
    So the deadline is always `oldest + window`.
- Errors: the throttler logs + the permit stays taken.

### Known limits

- The throttler counts the time of the permit, not the time of the send.
  A delay before the send can bring 2 sends closer than the window.

## 4. SlidingWindowStatistics

Code: [`src/main/java/com/hsbc/statistics`](src/main/java/com/hsbc/statistics)

### Assumptions

- The window holds the last N measurements.
  Each measurement is an `int`.
- One thread only calls `add`.
  Any thread can call `getLatestStatistics()`.
- A relative error of about 1% is acceptable for the percentile and the mode.

### Design decisions

- Mean: exact, with a running `long` sum and a ring buffer.
- Percentile and mode: a histogram with log-linear buckets.
  - Values from -128 to 127 are exact.
  - For a larger value, the error is at most about 0.8%.
  - 3,328 buckets, about 13 KB.
  - The result is the middle of the bucket.
  - Mode: the most frequent bucket.
    The lowest bucket wins a tie.
- Each `add` builds one immutable snapshot and stores it in a `volatile` field.
  An empty window gives `NaN`.
- Push: the class reuses the `EventBus` from exercise 2.
  - The criterion is the bus filter.
    It gets the previous and the current statistics.
  - `StatisticsUpdate` is conflatable.
    A slow subscriber gets only the latest update.

### Known limits

- A second writer thread corrupts the state.
