There will be times where we need to merge multiple streams together to obtain the insights we need. **Renoir** provides a set of operators that allow you to combine multiple streams.

## Join

The most common operation is to join two streams together. The `join` operator allows you to join two streams based on a key. The key is evaluated using a closure (one for each stream) and two elements are joined together if the keys are equal.

This operator is equivalent to the SQL query `SELECT * FROM a JOIN b ON keyer1(a) = keyer2(b)`.

```rust
let s1 = ctx.stream_iter(0..5u8);
let s2 = ctx.stream_iter(0..5i32);
// Pair elements where (s1 % 5) == (s2 % 2)
let res = s1.join(s2, |n| (n % 5) as i32, |n| n % 2)
    .drop_key()
    .collect_vec();
```

### Join variants

**Renoir** offers three additional variants:

#### `left_join`

Keeps all elements from the left stream. Elements from the right stream that have no matching key produce `None` on the right side.

Equivalent to SQL `LEFT OUTER JOIN`.

```rust
let names = ctx.stream_iter(vec!["alice", "bob", "carol"]);
let scores = ctx.stream_iter(vec![("alice", 95), ("carol", 87)]);
// bob has no score → right side will be None
let res = names.left_join(scores, |n| *n, |(name, _)| *name)
    .collect_vec();
// [("alice", Some(("alice", 95))), ("bob", None), ("carol", Some(("carol", 87)))]
```

#### `outer_join`

Keeps all elements from both streams. Missing matches on either side become `None`.

Equivalent to SQL `FULL OUTER JOIN`.

```rust
let s1 = ctx.stream_iter(vec![1u32, 2, 3]);
let s2 = ctx.stream_iter(vec![2u32, 3, 4]);
let res = s1.outer_join(s2, |n| *n, |n| *n).collect_vec();
// (1, None), (Some(2), Some(2)), (Some(3), Some(3)), (None, 4)
```

#### `interval_join`

> Requires feature flag: `timestamp`

Joins two **timestamped** streams based on a time interval. An element from the left stream with timestamp `T` is matched to all elements from the right stream with timestamp `Q` such that `T - lower_bound <= Q <= T + upper_bound`.

The bounds are `Timestamp` values (alias for `i64`), not `Duration`. You must attach timestamps via `add_timestamps` before using this operator.

```rust
use renoir::operator::Timestamp;

let left  = ctx.stream_iter(left_events);
let right = ctx.stream_iter(right_events);

// Both streams must have timestamps attached via add_timestamps
let res = left
    .add_timestamps(|e| e.ts, |_, &ts| Some(ts))
    .interval_join(
        right.add_timestamps(|e| e.ts, |_, &ts| Some(ts)),
        5_000,   // lower_bound: 5 seconds in milliseconds
        5_000,   // upper_bound: 5 seconds in milliseconds
    )
    .collect_vec();
```

> Note: this operator is not parallelised — all elements are sent to a single node to perform the join.

### Join on `KeyedStream`

If your streams are already keyed (via `group_by`), you can join them directly — the existing partition key is reused and no additional keyer function is needed:

```rust
let s1 = ctx.stream_iter(events).group_by(|e| e.user_id);
let s2 = ctx.stream_iter(profiles).group_by(|p| p.user_id);
let res = s1.join(s2).drop_key().collect_vec();
```

Keyed streams also support `join_outer` for full outer joins:

```rust
let res = s1.join_outer(s2).collect_vec();
// Each item is (key, (Option<V1>, Option<V2>))
```

### `join_with` — Advanced Join Builder

> ADVANCED OPERATOR

A more experienced user may want to use the `join_with` operator to customise the join's **ship strategy** and **local strategy**:

**Ship strategies** control how data is distributed for the join:

- `ship_hash()` — both sides are shuffled by key hash (default for `join`)
- `ship_broadcast_right()` — the left side stays local, the right side is broadcast to all replicas. Useful when the right side is small.

**Local strategies** control how matching is done within a replica:

- `local_hash()` — build a hash map (default, fast for most cases)
- `local_sort_merge()` — sort both sides and merge (better for pre-sorted data)

```rust
let s1 = env.stream_iter(0..5u8);
let s2 = env.stream_iter(0..5i32);

// Use broadcast-right ship strategy with hash local strategy
let res = s1.join_with(s2, |n| (n % 5) as i32, |n| n % 2)
    .ship_broadcast_right()
    .local_hash()
    .inner();
```

See the [join_with API docs](https://deib-polimi.github.io/renoir/renoir/struct.Stream.html#method.join_with) for details.

---

## Merge

The `merge` operator concatenates two streams of the same type into one. Elements from both streams are interleaved in arrival order — no ordering guarantee is provided.

```rust
let s1 = ctx.stream_iter(0..10);
let s2 = ctx.stream_iter(10..20);
let res = s1.merge(s2).collect_vec();
```

Use `merge` when you want to process two independent data sources through a shared pipeline.

Merge also works on `KeyedStream`s with the same key and value types:

```rust
let s1 = env.stream_iter(0..3).group_by(|&n| n % 2);
let s2 = env.stream_iter(3..5).group_by(|&n| n % 2);
let res = s1.merge(s2).collect_vec();
```

---

## Zip

The `zip` operator pairs elements from two streams positionally: the first element of `s1` with the first of `s2`, the second with the second, and so on. The output stream ends when the shorter stream is exhausted; remaining elements from the longer stream are discarded.

```rust
let s1 = ctx.stream_iter(vec!['A', 'B', 'C', 'D']);
let s2 = ctx.stream_iter(vec![1, 2, 3]);
let res = s1.zip(s2).collect_vec();
// [('A', 1), ('B', 2), ('C', 3)]  — 'D' is dropped
```

> Elements after the end of the shorter stream are discarded.
