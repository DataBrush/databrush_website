# Execution Model and Internals

This page explains how Renoir turns a user-written operator chain into a running parallel computation. It is not required reading for basic use, but is useful for performance tuning, debugging, and understanding compiler errors that arise at block boundaries.

---

## Blocks: the Unit of Deployment

A **block** is a contiguous sequence of operators that run in the same thread and exchange data without serialisation. Communication only happens at the **boundary between blocks**, where data is serialised and sent over a typed channel.

```
Block A                   Block B                   Block C
┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
│ Source            │   │ Start             │   │ Start             │
│ Map               │-->│ Fold (per-key)    │-->│ Collect           │
│ Filter            │   └─────────────────────┘   └─────────────────────┘
└─────────────────────┘
```

Each block can run as multiple **replicas** in parallel. Replicas of the same block process different partitions of the data independently and share no mutable state.

The `Block` struct carries the entire operator chain as a generic type parameter:

```rust
pub(crate) struct Block<OperatorChain: Operator> {
    pub id: BlockId,
    pub operators: OperatorChain,   // nested operator chain (compile-time type)
    pub batch_mode: BatchMode,
    pub scheduling: Scheduling,     // replication constraints
    pub iteration_ctx: Vec<...>,    // used by iterative workflows
}
```

---

## The Build Phase: From User Code to Job Graph

When you write a Renoir pipeline you are **describing** a computation, not executing it. Each method call on `Stream` or `KeyedStream` appends an operator to an in-memory chain held inside the current block.

Some operators force a **block split** because they need to redistribute data across threads or hosts. These operators:

1. Close the current block and hand it to the `Scheduler`.
2. Open a new block prefixed with a `Start` receiver.
3. Add an edge between the two blocks in the job graph.

Common block-splitting operators:

| Operator | Reason for split |
|---|---|
| `group_by` | Network shuffle to route each element to the replica responsible for its key |
| `key_by` | Same — when the key boundary crosses a parallelism boundary |
| `join` | Co-locates matching keys from two independent streams |
| `collect_vec`, `for_each`, sinks | Close the final block |

The resulting **job graph** is a directed acyclic graph (DAG) of `BlockId` nodes stored inside the `Scheduler`. Each edge carries the output type of the upstream block so the network layer can deserialise incoming messages with the correct type.

---

## The `ExchangeData` Trait

Data that crosses a block boundary must be serialised, transmitted over a channel, and deserialised on the other side. Renoir enforces this at the type level:

```rust
/// Marker trait for types that can be exchanged between blocks.
pub trait ExchangeData: Serialize + for<'a> Deserialize<'a> + Clone + Send + 'static {}

// Automatically implemented for every type satisfying the bounds.
impl<T: Serialize + for<'a> Deserialize<'a> + Clone + Send + 'static> ExchangeData for T {}
```

If your element type does not implement `serde::Serialize` + `serde::Deserialize`, the pipeline will fail to compile **at the point of the block split** — not at the sink. This gives a precise compile-time error rather than a runtime failure.

Within a single block, elements only need to satisfy the lighter `Data` bound (`Clone + Send + 'static`). Serialisability is only required at block boundaries.

Related traits used across the codebase:

| Trait | Bounds | Used for |
|---|---|---|
| `Data` | `Clone + Send + 'static` | Any element inside a block |
| `DataKey` | `Data + Hash + Eq` | Keys used by `group_by` / `key_by` |
| `ExchangeData` | `Data + Serialize + Deserialize` | Elements crossing block boundaries |
| `ExchangeDataKey` | `DataKey + ExchangeData` | Keys that also cross block boundaries |

---

## Block Replication and Worker Assignment

After `execute_blocking()` is called the `Scheduler` expands the job graph into an **execution graph** in three steps:

### 1. Replication

Each block is expanded into one or more `Coord` values — `(block_id, host_id, replica_id)` triples. The number of replicas depends on the block's `Replication` setting:

| Variant | Replicas |
|---|---|
| `Replication::Unlimited` *(default)* | One per logical CPU thread on the host |
| `Replication::Limited(n)` | At most `n` replicas |
| `Replication::One` | Exactly one (required for `window_all`, global sinks) |
| `Replication::Host` | One per host in a distributed deployment |

### 2. Network topology

`NetworkTopology` creates typed channels between every upstream `Coord` and every downstream `Coord`. For in-process communication these are lock-free queues; for remote communication they go through TCP or Unix sockets.

### 3. Worker spawning

For each local `Coord` the scheduler spawns a worker thread:

```
execute_blocking()
  ├── build_execution_graph()     // expand blocks → Coord set
  ├── network.build()             // create inter-block channels
  └── for each local Coord:
        spawn_worker(block, metadata)
          ├── operators.setup(metadata)   // init: open files, bind channels, …
          └── loop { operators.next() }   // pull StreamElements until Terminate
```

Each worker calls `operators.setup(metadata)` once to let operators initialise (connect to Kafka, open files, bind network receivers, etc.), then enters a tight loop calling `operators.next()` to pull `StreamElement` values through the chain until it sees `Terminate`.

---

## The `Start` Operator

Every block except source blocks begins with a `Start` operator, inserted automatically by `split_block`. `Start` is responsible for:

- **Receiving** `StreamElement` batches from all upstream replicas over the network layer.
- **Merging watermarks** across replicas via the `WatermarkFrontier` — a watermark is only forwarded once *all* upstream replicas have reported a watermark at least as large (see [Timestamps & Watermarks](/docs/user-guide/watermarks/)).
- **Forwarding control signals** (`FlushAndRestart`, `Terminate`) only after *all* upstream replicas have sent them, ensuring every block ends cleanly.

`Start` is invisible in user code; it appears at the front of every non-source block's operator chain at runtime.

---

## The `StreamContext` Internals

`StreamContext` is the user-facing handle. It wraps a `StreamContextInner` behind an `Arc<Mutex<...>>` so that multiple threads can concurrently register blocks during pipeline construction:

```rust
pub struct StreamContext {
    inner: Arc<Mutex<StreamContextInner>>,
}

pub(crate) struct StreamContextInner {
    config: Arc<RuntimeConfig>,
    block_count: CoordUInt,       // monotonically increasing BlockId counter
    scheduler: Option<Scheduler>,
}
```

The three key methods used by operators during the build phase:

| Method | What it does |
|---|---|
| `new_block(source, …)` | Creates a `Block` with a fresh `BlockId` and a `Start` receiver prepended |
| `close_block(block)` | Hands the completed block to the `Scheduler`; returns its `BlockId` |
| `connect_blocks(from, to)` | Adds a directed edge in the job graph |

Once `execute_blocking()` is called the `Scheduler` is consumed, the execution graph is built, worker threads are spawned, and the calling thread blocks until all workers terminate.

---

## Batching

To avoid the overhead of processing one element at a time, Renoir groups elements into micro-batches. A `FlushBatch` signal marks the end of each batch. The `BatchMode` of a block controls this:

| Mode | Behaviour |
|---|---|
| `BatchMode::Adaptive(max, timeout)` | Flush when the batch reaches `max` elements *or* when `timeout` elapses |
| `BatchMode::Fixed(n)` | Flush after exactly `n` elements |

Batching is transparent to user operators but affects latency. Larger batches improve throughput; smaller timeouts reduce end-to-end latency. For real-time pipelines, tune the `timeout` parameter of `Adaptive` mode to match your latency budget.
