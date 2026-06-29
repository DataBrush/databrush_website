To obtain insights from a data stream it is often necessary to aggregate the data in some way. **Renoir** provides a set of operators that allow you to perform reductions and folds over the data stream to obtain the wanted information.

## Reduce
The `reduce` operator aggregates the data of a stream following a use defined function and emit a single value. The function should modify the accumulator which will at the end be the emitted value.

```rust
let s = env.stream_iter(0..5);
let res = s.reduce(|acc, value| acc + value).collect::<Vec<_>>();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
```

> Note that the type of the accumulator (and so the output) is the same as the type of the stream elements. If a different type is needed consider using `fold`.

## Reduce Associative
The `reduce_assoc` operator is a variant of the `reduce` operator that can be used when the reduction function is associative. This allows the operator to be executed in parallel and can be more efficient than the `reduce` operator.

The operator applies the reducing function in two steps:
 - **Local**: the function that will be executed on each replica.
 - **Global**: the function that will aggregate all the partial results obtained from the local functions.

```rust
let s = env.stream_iter(0..5);
let res = s.reduce_assoc(|acc, value| acc + value).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
```

> Note that the type of the accumulator (and so the output) is the same as the type of the stream elements. If a different type is needed condider using `fold_assoc`.

## Fold
The `fold` operator aggregates the data of a stream following a user-defined function and emits a single value. The function receives a mutable reference to the accumulator and the current item. It is similar to the `reduce` operator but it allows specifying an initial value and so the type for the accumulator can differ from the stream elements.

```rust
let s = env.stream_iter(0..5);
let res = s.fold(0, |acc, value| *acc += value).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
```

## Fold Associative
The `fold_assoc` operator is a variant of the `fold` operator that can be used when the reduction function is associative. Similar to the `reduce_assoc` this allows the operator to be executed in parallel and can be more efficient than the `fold` operator.

The operator requires two user-defined functions:
 - **Local**: the function that will be executed on each replica.
 - **Global**: the function that will aggregate all the partial results obtained from the local functions.

```rust
let s = env.stream_iter(0..5);
let res = s.fold_assoc(0, |acc, value| *acc += value, |acc, value| *acc += value).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0 + 1 + 2 + 3 + 4]);
```

---

## Group-By Reductions

For the most common aggregation pattern — partitioning a stream by key and then reducing each partition — **Renoir** provides a family of associative `group_by_*` operators. These are more efficient than calling `.group_by(keyer).fold(...)` because they perform a **local pre-aggregation** on each replica before shuffling the (much smaller) partial results over the network.

### `group_by_reduce`
Partition the stream and apply a reduction function to each group. Equivalent to `GROUP BY key` with a custom reducer.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_reduce(|&n| n % 2, |acc, value| *acc += value)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_unstable();
assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
```

### `group_by_fold`
Partition the stream and apply a fold operation with an initial value to each group. Use this when the output type differs from the input type.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_fold(|&n| n % 2, 0, |acc, value| *acc += value, |acc, value| *acc += value)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_unstable();
assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
```

### `group_by_sum`
Partition the stream and compute the sum per group. Equivalent to `SELECT SUM(value) ... GROUP BY key`.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_sum(|&n| n % 2, |n| n)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_unstable();
assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
```

### `group_by_avg`
Partition the stream and compute the average per group. Equivalent to `SELECT AVG(value) ... GROUP BY key`.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_avg(|&n| n % 2, |&n| n as f64)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_by_key(|(k, _)| *k);
assert_eq!(res, vec![(0, (0.0 + 2.0 + 4.0) / 3.0), (1, (1.0 + 3.0) / 2.0)]);
```

### `group_by_count`
Partition the stream and count the number of items per group. Equivalent to `SELECT COUNT(*) ... GROUP BY key`.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_count(|&n| n % 2)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_by_key(|(k, _)| *k);
assert_eq!(res, vec![(0, 3), (1, 2)]);
```

### `group_by_max_element`
Find the item with the largest value in each group. The comparison is done using the value returned by `get_value`, but the resulting items retain their original type.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_max_element(|&n| n % 2, |&n| n)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_unstable();
assert_eq!(res, vec![(0, 4), (1, 3)]);
```

### `group_by_min_element`
Find the item with the smallest value in each group.

```rust
let s = env.stream_iter(0..5);
let res = s
    .group_by_min_element(|&n| n % 2, |&n| n)
    .collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_unstable();
assert_eq!(res, vec![(0, 0), (1, 1)]);
```

---

## KeyedStream Reductions

If you already have a `KeyedStream` (e.g. from `group_by` or `key_by`), you can apply `fold` and `reduce` directly:

```rust
// Fold per key on a KeyedStream
let s = env.stream_iter(0..5).group_by(|&n| n % 2);
let res = s.fold(0, |acc, value| *acc += value).collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort_unstable();
assert_eq!(res, vec![(0, 0 + 2 + 4), (1, 1 + 3)]);
```

> The difference between `stream.group_by(keyer).fold(...)` and `stream.group_by_fold(keyer, ...)` is that the latter performs a **local pre-aggregation** step before the network shuffle, sending only one partial result per key per replica instead of every element. For large streams, prefer the associative `group_by_*` variants.

---

## Prefix Scans (Two-Pass Scans)

For running prefix aggregations (like cumulative sum or running state) across parallel worker nodes, **Renoir** provides two-pass scan operators: `fold_scan` and `reduce_scan`. These are implemented using iterations to calculate and propagate global state across parallel partitions.

### `fold_scan`

The `fold_scan` operator performs a custom two-pass scan. It requires a local fold function (for local accumulation), a global fold function (to merge local states globally), a global initial value, and a mapping function (to produce the final output using the global state).

```rust
// Compute running cumulative sum across the stream
let s = env.stream_iter(0..5);
let res = s.fold_scan(
    |local: &mut i32, x| *local += x,           // Local fold
    |global: &mut i32, local| *global += local, // Global merge
    0,                                          // Global initial state
    |x, global_state| x + global_state,         // Output map
).collect_vec();

env.execute_blocking();
```

### `reduce_scan`

A variant of `fold_scan` that is tailored for associative operations where the state has the same type as the input. It requires a first mapping function (to initialize the state from elements), an associative reduction function, and a second mapping function.

```rust
let s = env.stream_iter(0..5);
let res = s.reduce_scan(
    |x| x,                         // Initial state mapper
    |a, b| a + b,                  // Associative reduction
    |x, state| x + state,          // Output mapper
).collect_vec();

env.execute_blocking();
```

Both `fold_scan` and `reduce_scan` are also supported on `KeyedStream` where the operations are performed independently per key.
