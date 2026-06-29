# Connectors

Connectors are the entry and exit points of a Renoir pipeline. **Sources** feed data into a stream; **sinks** consume the final output.

## Feature flags

Renoir uses Cargo feature flags to control optional functionality. Some features are enabled by default.

### Connector features

| Connector | Feature flag | Notes |
|---|:---:|---|
| Iterator, Parallel Iterator, File, CSV, Channel | — | Built-in, no flag needed |
| Avro source / sink | `avro` | |
| Parquet source / sink | `parquet` | |
| Async stream source, async operators | `tokio` | See [Tokio](/docs/user-guide/tokio/) |
| Kafka source / sink | `rdkafka` | Experimental; implies `tokio` |

### Runtime features

| Feature | Default | Notes |
|---|:---:|---|
| `timestamp` | ✅ | Enables event-time timestamps and watermarks. Without it, `Timestamp` resolves to `()` and `EventTimeWindow`/`TransactionWindow` are unavailable. |
| `ssh` | ✅ | Enables SSH-based remote worker spawning for distributed deployment (via `libssh2`). |
| `clap` | ✅ | Enables `RuntimeConfig::from_args()` CLI argument parsing. |
| `profiler` | ❌ | Enables internal profiling instrumentation for performance debugging. When enabled, workers collect metrics (like throughput and network bytes) and print them to `stderr` prefixed with `__renoir_TRACING_DATA__` upon completion, which can be parsed for execution analysis. |

```toml title="Cargo.toml — enabling optional connectors"
[dependencies]
renoir = { version = "*", features = ["avro", "parquet", "rdkafka"] }
```

---

## Sources

### Iterator source

Reads items from any Rust iterator. The iterator is consumed by a **single replica**, so this source is not parallel. Use it for small or already-in-memory datasets.

```rust
let ctx = StreamContext::new_local();
let result = ctx.stream_iter(0..100u32)
    .filter(|n| n % 2 == 0)
    .collect_vec();
ctx.execute_blocking();
```

For a distributed deployment the iterator is only read on the **first host** and then sent over the network to other workers.

---

### Parallel iterator source

Like the iterator source, but the generating closure is called **once per replica** so each replica produces its own slice of the data. This makes the source as parallel as your operator graph.

The closure receives `(id: u64, instances: u64)` — the replica index and total replica count — and must return an iterator over the shard for that replica.

```rust
let n: u64 = 1_000_000;
let result = ctx.stream_par_iter(move |id, instances| {
    let chunk = (n + instances - 1) / instances;
    let start = id * chunk;
    let end = (start + chunk).min(n);
    start..end
})
.map(|x| x * 2)
.collect_vec();
```

Integer ranges (`0..n`) implement `IntoParallelSource` directly, so the chunk-splitting logic above is written for you:

```rust
ctx.stream_par_iter(0u64..1_000_000)
```

---

### File source

Reads a **text file line by line**. The file is split into as many chunks as there are replicas so that each replica reads a contiguous portion of the file concurrently. Each line is emitted exactly once.

```rust
let result = ctx.stream_file("data/corpus.txt")
    .flat_map(|line| line.split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>())
    .group_by(|w| w.clone())
    .fold(0u64, |count, _| *count += 1)
    .collect_vec();
ctx.execute_blocking();
```

The path is anything that converts to `PathBuf`. Only regular files (with a known size) are supported; pipes or special files cannot be split.

---

### CSV source

Reads a CSV file and deserializes each row into a typed Rust struct using `serde`. Like the file source the file is partitioned and read in parallel.

The type parameter must derive `serde::Deserialize`.

```rust
use serde::Deserialize;

#[derive(Clone, Deserialize)]
struct Sale {
    product: String,
    units: u32,
    price: f64,
}

let result = ctx.stream_csv::<Sale>("data/sales.csv")
    .filter(|s| s.units > 0)
    .map(|s| (s.product.clone(), s.units as f64 * s.price))
    .group_by(|(product, _)| product.clone())
    .fold(0.0f64, |total, (_, revenue)| *total += revenue)
    .collect_vec();
ctx.execute_blocking();
```

**Configuration** — chain builder methods on `CsvSource` before passing it to `ctx.stream()`:

| Method | Default | Description |
|---|:---:|---|
| `has_headers(bool)` | `true` | Treat the first row as a header row |
| `delimiter(u8)` | `b','` | Field separator |
| `quote(u8)` | `b'"'` | Quote character |
| `double_quote(bool)` | `true` | Allow `""` as an escaped quote |
| `escape(Option<u8>)` | `None` | Alternative quote-escape byte |
| `flexible(bool)` | `false` | Allow rows with varying field counts |
| `trim(Trim)` | `Trim::None` | Strip whitespace from fields |
| `comment(Option<u8>)` | `None` | Treat lines starting with this byte as comments |

```rust
use renoir::operator::source::CsvSource;
use csv::Trim;

let source = CsvSource::<Sale>::new("data/sales.tsv")
    .delimiter(b'\t')
    .trim(Trim::All);
let result = ctx.stream(source).collect_vec();
```

---

### Avro source

> Requires feature flag: `avro`

Reads an [Apache Avro](https://avro.apache.org/) file and emits raw `apache_avro::types::Value` items. Chain `.from_avro_value::<T>()` to deserialize into a typed struct.

The `replication` parameter controls how many parallel replicas read the source.

```rust
use renoir::operator::source::AvroSource;
use renoir::Replication;
use serde::Deserialize;

#[derive(Clone, Deserialize)]
struct Event {
    user_id: u64,
    action: String,
}

let result = ctx
    .stream_avro_file(Replication::Unlimited, "data/events.avro")
    .from_avro_value::<Event>()
    .filter_map(|r| r.ok())
    .collect_vec();
ctx.execute_blocking();
```

For a custom reader (e.g., reading from an in-memory buffer or a network stream) use `stream_avro` and pass a factory closure:

```rust
ctx.stream_avro(Replication::One, |_id, _peers| {
    std::fs::File::open("data/events.avro").unwrap()
})
.from_avro_value::<Event>()
```

---

### Parquet source

> Requires feature flag: `parquet`

Reads an [Apache Parquet](https://parquet.apache.org/) file and emits `arrow::record_batch::RecordBatch` items. Call `.to_rows::<T>()` to convert batches into individual typed tuples using Arrow column types.

```rust
use renoir::operator::source::arrow::*; // UInt32Type, Float64Type, Utf8Type …

let result = ctx
    .stream_parquet_one("data/records.parquet")
    .to_rows::<(UInt32Type, Float64Type, Utf8Type)>()
    .filter_map(|r| r.ok())
    .collect_vec();
ctx.execute_blocking();
```

The source always uses `Replication::One` — the entire file is read by a single replica. Batches are 1 024 rows each.

---

### Kafka source

> Requires feature flag: `rdkafka` (implies `tokio`)
>
> **Experimental** — the interface may change in future releases.

Consumes messages from one or more Kafka topics. Each message is emitted as an `rdkafka::message::OwnedMessage`; extract the payload with `.payload()`.

```rust
use rdkafka::ClientConfig;
use renoir::Replication;

let mut config = ClientConfig::new();
config
    .set("bootstrap.servers", "localhost:9092")
    .set("group.id", "renoir-consumer")
    .set("enable.auto.commit", "true")
    .set("enable.partition.eof", "false");

ctx.stream_kafka(config, &["events"], Replication::Unlimited)
    .filter_map(|msg| {
        msg.payload()
            .and_then(|p| std::str::from_utf8(p).ok())
            .map(str::to_owned)
    })
    .for_each(|text| println!("{text}"));

ctx.execute_blocking();
```

**Replication and partitions** — if you use `Replication::Unlimited` together with event-time watermarks, make sure the number of Kafka partitions for the topic is at least as large as the number of replicas. Otherwise some replicas will never receive a message and watermarks will stall.

---

### Channel source

Feeds data into a stream via a Rust channel, useful when you need to push items from outside the Renoir execution graph (e.g., from a network handler or a GUI thread).

`ChannelSource::new` returns a `(Sender<T>, ChannelSource<T>)` pair. The sender can be cloned and shared across threads.

```rust
use renoir::operator::source::ChannelSource;
use renoir::Replication;

let (tx, source) = ChannelSource::new(/* channel capacity */ 64, Replication::One);
let result = ctx.stream(source).collect_vec();

// Push items from another thread before or during execution
std::thread::spawn(move || {
    for i in 0..10u32 {
        tx.send(i).unwrap();
    }
    // Dropping tx signals end-of-stream
});

ctx.execute_blocking();
println!("{:?}", result.get());
```

The channel is MPMC — multiple senders can push items and each item will be delivered to exactly one replica.

---

### Async stream source

> Requires feature flag: `tokio`

Wraps any `futures::Stream` as a Renoir source. The async stream is consumed by a single replica using `block_on`.

```rust
use renoir::prelude::*;

#[tokio::main]
async fn main() {
    let ctx = StreamContext::new_local();

    let async_stream = futures::stream::iter(0..50u32);
    let result = ctx.stream(AsyncStreamSource::new(async_stream))
        .map(|n| n * n)
        .collect_vec();

    ctx.execute().await;
    println!("{:?}", result.get());
}
```

---

## Sinks

### Collect to Vec

The most common sink — collects all stream items into a `Vec` on a single replica. The result is wrapped in a `StreamOutput<Vec<T>>`; call `.get()` after execution to retrieve it.

```rust
let output = ctx.stream_iter(0..10u32)
    .map(|n| n * 2)
    .collect_vec();

ctx.execute_blocking();

if let Some(v) = output.get() {
    println!("{v:?}");
}
```

`collect_vec_all()` is the multi-host variant: each host in a distributed deployment collects its own local results independently.

---

### Collect to any collection

`collect::<C>()` generalizes `collect_vec` to any type implementing `FromIterator`, such as `HashSet`, `BTreeMap`, or a custom collection.

```rust
use std::collections::HashSet;

let unique: StreamOutput<HashSet<String>> = ctx
    .stream_iter(vec!["a", "b", "a", "c", "b"])
    .map(str::to_owned)
    .collect::<HashSet<_>>();

ctx.execute_blocking();
println!("{:?}", unique.get()); // {"a", "b", "c"} (order may vary)
```

`collect_all::<C>()` is the multi-host variant of `collect::<C>()`. Each host in a distributed deployment collects its own local results into a collection of type `C` independently without repartitioning.

---

### Count

Counts the total number of items in the stream. The `collect_count` operator internally counts every element.

```rust
let count = ctx.stream_iter(0u64..1_000_000)
    .filter(|n| n % 7 == 0)
    .collect_count();

ctx.execute_blocking();
println!("divisible by 7: {}", count.get().unwrap());
```

---

### For each

Applies a closure to every item as a side effect. No result is returned to the host program.

```rust
ctx.stream_iter(0..5u32)
    .map(|n| n * n)
    .for_each(|n| println!("{n}"));

ctx.execute_blocking();
```

---

### Collect to channel

Sends stream items to a `flume` channel receiver. Unlike `collect_vec`, this lets you start processing results incrementally while the stream is still running.

```rust
let rx = ctx.stream_iter(0u32..100)
    .map(|n| n * 3)
    .collect_channel();

ctx.execute_blocking();

for item in rx {
    print!("{item} ");
}
```

`collect_channel_parallel()` skips the final repartitioning step — each replica writes to the channel directly, which is faster but means items arrive in an arbitrary interleaved order.

---

### CSV sink

Serializes each stream item to a CSV row using `serde`. Three path strategies are available:

| Method | Output |
|---|---|
| `write_csv_seq(dir_or_file, append)` | One numbered file per replica: `0000.csv`, `0001.csv`, … |
| `write_csv_one(path, append)` | All replicas write to the same file (repartitioned to one replica first) |
| `write_csv(make_path, append)` | Custom path closure — called once per replica with the replica index |

```rust
use serde::Serialize;

#[derive(Clone, Serialize)]
struct Record {
    id: u32,
    hex: String,
}

ctx.stream_par_iter(0u32..1000)
    .map(|i| Record { id: i, hex: format!("{i:08x}") })
    .write_csv_one("output/records.csv", /* append */ false);

ctx.execute_blocking();
```

When `append` is `false` the file is truncated before writing. When `true` headers are only written if the file is currently empty.

---

### Avro sink

> Requires feature flag: `avro`

Serializes stream items to [Apache Avro](https://avro.apache.org/) format. The schema is derived automatically via the `AvroSchema` derive macro.

```rust
use serde::{Deserialize, Serialize};
use apache_avro::AvroSchema;

#[derive(Clone, Serialize, Deserialize, AvroSchema)]
struct Event {
    user_id: u64,
    action: String,
}

ctx.stream_iter(events)
    .write_avro_one("output/events.avro");

ctx.execute_blocking();
```

Same three path strategies as the CSV sink (`write_avro_seq`, `write_avro_one`, `write_avro`).

---

### Parquet sink

> Requires feature flag: `parquet`

Writes stream items to [Apache Parquet](https://parquet.apache.org/) using Snappy compression. You must supply an Arrow `Schema` that matches the fields of your struct.

```rust
use arrow::datatypes::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Row {
    id: u32,
    value: f64,
    label: String,
}

let schema = Schema::new(vec![
    Field::new("id",    DataType::UInt32,   false),
    Field::new("value", DataType::Float64,  false),
    Field::new("label", DataType::Utf8,     false),
]);

ctx.stream_par_iter(0u32..500)
    .map(|i| Row { id: i, value: (i as f64).sqrt(), label: format!("item-{i}") })
    .write_parquet_one("output/rows.parquet", schema);

ctx.execute_blocking();
```

Same two path strategies as the other file sinks (`write_parquet_seq`, `write_parquet_one`).

---

### Kafka sink

> Requires feature flag: `rdkafka` (implies `tokio`)
>
> **Experimental** — the interface may change in future releases.

Publishes each stream item as a Kafka message. Items must be convertible to bytes (`AsRef<[u8]>`), so serialize them before calling the sink.

```rust
use rdkafka::ClientConfig;

let mut producer = ClientConfig::new();
producer
    .set("bootstrap.servers", "localhost:9092")
    .set("message.timeout.ms", "5000");

ctx.stream_iter(0u32..100)
    .map(|n| format!("{n:08X}"))   // String implements AsRef<[u8]>
    .write_kafka(producer, "output-topic");

ctx.execute_blocking();
```

---

## Caching / Checkpointing

Renoir supports **stream caching** — materialising the output of a pipeline so it can be replayed later without re-reading the original source. This is useful for interactive exploration, iterative model training, or splitting a pipeline into stages.

### `collect_cache`

Consumes the stream and stores all output elements in a `StreamCache`. The cache can later be used to create a new `Stream` in a fresh `StreamContext`.

```rust
use renoir::prelude::*;
use renoir::operator::cache::VecCacher;

let ctx = StreamContext::new_local();

// Stage 1: read and filter data, cache the result
let cache = ctx.stream_iter(0..100)
    .filter(|x| x % 3 == 0)
    .collect_cache::<VecCacher<_>>(());

ctx.execute_blocking();

// Stage 2: resume from cache in a new context
let ctx2 = StreamContext::new(cache.config());
let res = cache.stream_in(&ctx2)
    .filter(|x| x % 2 == 0)
    .collect_vec();

ctx2.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0, 6, 12, 18, 24, 30, 36, 42, 48,
    54, 60, 66, 72, 78, 84, 90, 96]);
```

> **Warning**: `StreamCache` methods (`stream_in`, `inner_cloned`) must only be called **after** the original `StreamContext` has finished executing. Calling them on an incomplete cache will panic.

The `VecCacher` stores items in a `Vec` in memory. The cache argument `()` is passed to the cacher's constructor. After execution, `cache.config()` returns the `RuntimeConfig` that should be used for the new context to ensure compatible parallelism settings.

### `cache` and `cache_vec`

Unlike `collect_cache`, which consumes the stream, `cache` and `cache_vec` are **non-consuming**. They split the stream, writing elements to a cache while returning the original stream so it can continue to be processed in the *same* pipeline context.

- `cache_vec()` stores elements in memory using `VecCacher` and returns `(CachedStream, Stream)`.
- `cache()` is the generic version that allows configuring a custom cacher.

```rust
let ctx = StreamContext::new_local();

// Split the stream: write to a cache while continuing processing
let (cache, s) = ctx.stream_iter(0..10)
    .filter(|x| x % 3 == 0)
    .cache_vec();

// Process the remaining stream immediately
let res1 = s.collect_vec();

ctx.execute_blocking();

// Now that the context has finished, we can resume from the cache
let ctx2 = StreamContext::new(cache.config());
let res2 = cache.stream_in(&ctx2)
    .filter(|x| x % 2 == 0)
    .collect_vec();

ctx2.execute_blocking();
```

### When to use caching

- **Interactive pipelines**: compute an expensive filtering/transformation once, then explore the result with multiple downstream queries.
- **Multi-stage workflows**: materialise intermediate results between logically separate pipeline stages.
- **Iterative algorithms**: cache the input dataset and replay it across multiple training epochs (see also the [Replay](operators/iterative.md#replay) operator for in-pipeline iteration).
