# Apache Flink to Renoir Migration Guide

> This guide assumes you have already set up an environment for Renoir and created a Cargo project following the [Installation Guide](install.md).

If you are coming from Apache Flink, you will find Renoir's programming paradigm very familiar. Both platforms model computation as a **directed dataflow graph of operators**, where elements flow through a series of transformations from one or more *Sources* to one or more *Sinks*.

This guide explains the conceptual alignment between Flink and Renoir, contrasts their architectures, and breaks down a migration path using practical examples.

---

## 1. Conceptual Alignment

The table below maps core Apache Flink concepts directly to their **Renoir** equivalents:

| Apache Flink Concept | Renoir Equivalent | Description |
| :--- | :--- | :--- |
| `StreamExecutionEnvironment` | `StreamContext` | The entry point for creating streams and managing dataflow execution. |
| `DataStream<T>` | `Stream<T>` | A sequential stream of elements processed sequentially. |
| `KeyedStream<K, T>` | `KeyedStream<K, T>` | A stream partitioned across worker tasks based on a key hash. |
| `map(MapFunction)` | `map(Fn)` | Transforms each element in the stream to another element. |
| `flatMap(FlatMapFunction)` | `flat_map(Fn)` | Transforms each element into zero, one, or multiple elements. |
| `filter(FilterFunction)` | `filter(Fn)` | Filters elements out of the stream based on a boolean predicate. |
| `keyBy(...)` | `group_by(...)` / `key_by(...)` | Partitions a stream based on a key extracted from each element. |
| `reduce(ReduceFunction)` | `reduce(...)` / `reduce_assoc(...)` | Combines elements of the stream into a single output element. |
| `WindowedStream` | `WindowedStream` | Represents a stream grouped into windows based on count or time. |
| `SinkFunction` / `print()` | `collect_vec()` / Sinks | Consumes elements of a stream and outputs or writes them. |
| `env.execute(...)` | `ctx.execute_blocking()` / `execute()` | Triggers the build and execution of the compiled dataflow graph. |

---

## 2. Architectural Differences

While the APIs feel remarkably similar, Flink and Renoir diverge significantly in implementation, resulting in drastically different performance profiles.

### JVM vs. Native Compiled Rust
* **Apache Flink** runs on the JVM. Objects streaming through Flink pipelines must be serialized/deserialized (using Kryo, Avro, or custom POJO serializers) when crossing network boundaries or operators, introducing substantial garbage collection (GC) and heap management overhead.
* **Renoir** compiles down to native machine code. It leverages Rust's strict static typing, zero-cost abstractions, and generics system. Items flow through local pipelines with zero heap allocations or serialization overhead where possible.

> :octicons-light-bulb-16: While in flink moving data across two worker on the same machine requires serialization/deserialization, in renoir is as simple as passing a pointer reference.

### Threading & Operator Blocks
* **Apache Flink** groups operators into chains and executes them inside Task Slots, relying on JVM threads and a complex scheduler.
* **Renoir** groups contiguous sequence-altering operators into **Blocks**. Blocks contain a pipeline of operators that process the stream sequentially. They are the scheduling and deployment units of Renoir and are distributed across OS thread pools (in local execution) or network nodes, communicating using muxed tcp channels.

---

## 3. Hands-on Comparison: WordCount

To see the direct comparison, let's look at a standard WordCount application implemented in both engines.

### Flink (Java API)

```java
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkWordCount {
    public static void main(String[] args) throws Exception {
        // Initialize the Flink execution context
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read dataset from a text file source
        DataStream<String> text = env.readTextFile("text-file.txt");

        // Split, normalize, group, and sum
        DataStream<Tuple2<String, Integer>> counts = text
            .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                    for (String token : value.split("\\s+")) {
                        if (token.length() > 0) {
                            out.collect(new Tuple2<>(token.toLowerCase(), 1));
                        }
                    }
                }
            })
            .keyBy(value -> value.f0)
            .sum(1);

        // Print results to stdout
        counts.print();

        // Trigger execution of the defined dataflow graph
        env.execute("Flink WordCount");
    }
}
```

### Renoir (Rust API)

```rust
use renoir::prelude::*;

fn main() {
    // 1. Initialize the local execution context
    let ctx = StreamContext::new_local();

    // 2. Read dataset from a file source
    let result = ctx
        .stream_file("text-file.txt")
        // 3. Map/FlatMap: Returns a standard Rust Iterator/collection
        .flat_map(|line| {
            line.split_whitespace()
                .map(|t| t.to_lowercase())
        })
        // 4. Partition and perform counting reduction
        .group_by_count(|word: &String| word.clone())
        // 5. Accumulate results locally 
        .collect_vec();

    // 6. Trigger dataflow execution (blocking on the current thread)
    ctx.execute_blocking();

    // 7. Retrieve the collected output
    if let Some(mut res) = result.get() {
        res.sort_by_key(|t| t.1);
        println!("{res:#?}");
    }
}
```

---

## 4. Key Differences in API Usage

### Iterators vs. Collectors
In Flink's `flatMap`, you must pass elements to a provided `Collector` object. In Renoir, operators like `flat_map` align with Rust's standard `Iterator` trait: you simply return a type that implements `IntoIterator` (such as a standard `Vec` or a mapped iterator).

### Explicit Execution & Result Fetching
In Flink, calling `.print()` or adding a sink configures the pipeline, and calling `env.execute()` starts a background run that streams directly to the sink. 

In Renoir, you must explicitly collect results (e.g. via `collect_vec()`). The collect operator returns a handle (a promise-like wrapper). The actual computation starts only when you call `ctx.execute_blocking()` (or its async equivalent `ctx.execute().await`). Once completed, you retrieve the data via `.get()` which returns an `Option`.
