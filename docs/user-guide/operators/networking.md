These operators control how elements are distributed across replicas and hosts. They are essential for load balancing and controlling parallelism in distributed pipelines.

## Shuffle
Perform a network shuffle sending each element to a **random** replica. Useful when the load is unbalanced (e.g. after a very skewed `group_by`).

```rust
let s = env.stream_iter(0..5);
let res = s.shuffle().map(|n| n * 2).collect_vec();
```

> `shuffle` triggers a block split — elements are serialised and sent over the network in a distributed deployment.

## Broadcast
Duplicate each element of the stream and forward it to **all** replicas of the next block. Every replica receives a complete copy of the data.

```rust
let s = env.stream_iter(0..10);
s.broadcast().for_each(|n| println!("{n}"));
```

> **Warning**: this is potentially very expensive as it duplicates every element. Use it only when all replicas truly need the complete dataset (e.g. broadcasting a small reference table for a map-side join).

## Replication
> ADVANCED OPERATOR

Change the maximum parallelism of the following operators. This controls how many replicas are spawned for the next block.

```rust
use renoir::Replication;

let s = env.stream_iter(0..100);
// Force subsequent operators to run on a single replica
let res = s.replication(Replication::One).map(|n| n * 2).collect_vec();
```

| Variant | Behaviour |
|---|---|
| `Replication::Unlimited` *(default)* | One replica per CPU core |
| `Replication::Limited(n)` | At most `n` replicas |
| `Replication::One` | Exactly one replica (required for `window_all`, global sinks) |
| `Replication::Host` | One replica per host in a distributed deployment |

> Some operators internally set the replication — for example `collect_vec` forces `Replication::One` to gather results to a single node.

## RepartitionBy
> ADVANCED OPERATOR

An advanced operator that combines repartitioning with a custom partition function. Elements are sent to replicas based on the hash returned by the provided function. Use this when you need custom routing logic that doesn't fit `group_by`.

```rust
use renoir::block::group_by_hash;
use renoir::Replication;

let s = env.stream_iter(0..100);
let res = s.repartition_by(Replication::Unlimited, group_by_hash).collect_vec();
```