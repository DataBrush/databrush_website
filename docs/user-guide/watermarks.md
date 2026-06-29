# Timestamps & Watermarks

Event-time processing requires knowing *when* each event occurred according to the source system, rather than when it was processed. Renoir separates the **timestamp** (a property of an individual element) from the **watermark** (a progress signal on the stream) to enable correct event-time windowing even when events arrive out of order.

> Timestamps and watermarks require the `timestamp` feature flag. Without it `Timestamp` resolves to `()` and `EventTimeWindow` and `TransactionWindow` are unavailable.

---

## The `StreamElement` Enum

Every value flowing through a Renoir pipeline is wrapped in a `StreamElement`:

```rust
pub enum StreamElement<Out> {
    Item(Out),
    Timestamped(Out, Timestamp),   // element carrying an event-time timestamp
    Watermark(Timestamp),          // progress signal: all future events have ts > this
    FlushBatch,                    // internal: end of a micro-batch
    FlushAndRestart,               // internal: end of one iteration cycle
    Terminate,                     // end of stream
}
```

`Timestamp` is an alias for `i64`. By convention it holds Unix milliseconds, but any monotonically increasing integer scale works as long as your window sizes use the same unit.

A `Watermark(t)` is a promise: *every element with timestamp ≤ t has already been emitted by this replica*. Once a watermark with value `t` has passed through a `WindowManager`, any window whose end time is ≤ `t` can safely close and emit its result without waiting for late-arriving elements.

---

## Assigning Timestamps and Watermarks

Raw sources (iterators, files, Kafka topics) produce plain `Item` variants. To enable event-time windowing you must attach timestamps and a watermark strategy using `add_timestamps`:

```rust
stream.add_timestamps(
    |element| element.event_time_ms,          // (1) timestamp extractor
    |element, &ts| Some(ts - 2_000),          // (2) watermark generator
)
```

| Parameter | Signature | Role |
|---|---|---|
| `timestamp_gen` | `FnMut(&T) -> Timestamp` | Extracts the event timestamp from each element |
| `watermark_gen` | `FnMut(&T, &Timestamp) -> Option<Timestamp>` | Optionally emits a watermark after this element; return `None` to skip |

`add_timestamps` converts every `Item(x)` into `Timestamped(x, ts)` and, whenever `watermark_gen` returns `Some(wm)`, injects a `Watermark(wm)` *after* the element that triggered it. Downstream operators therefore always see the element before they see the watermark that closes its window.

`add_timestamps` is available on both `Stream` and `KeyedStream`.

### Common watermark strategies

**Bounded out-of-orderness** — track the maximum observed timestamp and lag behind it by a fixed tolerance. This absorbs late arrivals that are no more than `tolerance` milliseconds behind the current front:

```rust
let mut max_ts = i64::MIN;
stream.add_timestamps(
    |el| el.ts,
    move |_el, &ts| {
        max_ts = max_ts.max(ts);
        Some(max_ts - 5_000)   // tolerate up to 5 seconds of lateness
    },
)
```

**Sparse watermark** — emit a watermark only every N elements to reduce overhead on high-throughput streams:

```rust
let (mut count, mut max_ts) = (0usize, i64::MIN);
stream.add_timestamps(
    |el| el.ts,
    move |_el, &ts| {
        max_ts = max_ts.max(ts);
        count += 1;
        if count % 1_000 == 0 { Some(max_ts - 2_000) } else { None }
    },
)
```

**In-order source** — if your source guarantees strictly increasing timestamps (e.g. a log file sorted by time), the watermark can simply follow each element:

```rust
stream.add_timestamps(|el| el.ts, |_el, &ts| Some(ts))
```

---

## How Watermarks Propagate

After `add_timestamps` has been applied, `Watermark` signals flow through the operator chain just like data. Every operator forwards watermarks unchanged unless it consumes them (as `WindowManager` does).

### The `WatermarkFrontier`

In parallel execution a block typically has multiple upstream replicas, each emitting watermarks at its own pace. The `Start` operator at the entry of every block maintains a `WatermarkFrontier` — a per-replica tracker that computes the minimum across all upstream watermarks:

```
Upstream replica 0 ──► Watermark(100) ─┐
Upstream replica 1 ──► Watermark(80)  ─┼──► WatermarkFrontier ──► Watermark(80)
Upstream replica 2 ──► Watermark(120) ─┘       min = 80
```

A watermark is forwarded downstream only once **all** upstream replicas have reported a watermark at least as large. This preserves the monotonicity guarantee: if `Watermark(t)` arrives at a `WindowManager`, it is safe to close all windows whose end time is ≤ `t`, because no upstream replica can still send an element with timestamp ≤ `t`.

The `WatermarkFrontier` is transparent to the user; it is inserted automatically by the runtime.

---

## Event-Time Windows in Practice

`EventTimeWindow` is the primary consumer of watermarks. It buffers `Timestamped` elements and emits window results only when a watermark advances past a window's end time.

```rust
use renoir::prelude::*;

let mut max_ts = i64::MIN;

env.stream(KafkaSource::new(...))
    .add_timestamps(
        |msg| msg.timestamp_ms,
        move |_, &ts| { max_ts = max_ts.max(ts); Some(max_ts - 2_000) },
    )
    .group_by(|msg| msg.user_id)
    .window(EventTimeWindow::tumbling(10_000))  // 10-second tumbling window
    .count()
    .drop_key()
    .collect_vec();
```

**Tumbling window** (`EventTimeWindow::tumbling(size)`): non-overlapping windows of fixed duration. An element with timestamp `t` belongs to exactly one window: `[floor(t/size)*size, floor(t/size)*size + size)`.

**Sliding window** (`EventTimeWindow::sliding(size, slide)`): overlapping windows. An element belongs to all windows `[k*slide, k*slide + size)` for each `k` such that `k*slide ≤ t < k*slide + size`. Sliding windows consume more memory because each element may be present in multiple accumulators simultaneously.

### What happens if watermarks never arrive?

If `watermark_gen` always returns `None`, or if `add_timestamps` is never called, `EventTimeWindow` will buffer elements indefinitely and **never emit any results**. This is the most common pitfall when connecting a real-time source to an event-time pipeline: always attach a watermark strategy.

---

## Dropping Timestamps

After a windowed aggregation you may no longer need event-time semantics. Calling `drop_timestamps()` converts `Timestamped(x, t)` back to `Item(x)` and discards any `Watermark` signals, preventing them from reaching downstream operators:

```rust
keyed_stream
    .window(EventTimeWindow::tumbling(10_000))
    .sum::<u64>()
    .drop_key()
    .drop_timestamps()   // back to plain Item values
    .window_all(CountWindow::tumbling(5))
    .sum::<u64>()
```
