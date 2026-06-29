Most of the time elements in a stream need to be grouped or partitioned in some way to retrieve the desired result. **Renoir** provides a set of operators that do exactly that.

## Grouping

### Group By
Probably the most common operation is to group together elements by some common key. The key can be extracted from the element itself or computed from it. The `group_by` operator takes a closure that returns the key for each element and returns a `KeyedStream` where the operators are evaluated over elements with the same key.

```rust
let s = env.stream_iter(0..5);
// partition even and odd elements
let keyed = s.group_by(|&n| n % 2); 
```

> After the partitioning all the elements will be sent to the network to balance the load but if the desired result is an aggregation in many cases is advisable to use an associative variant of the `group_by` operator like  `group_by_reduce` or `group_by_sum`, a complete list of the possible associative variant can be found in the [Reductions and Folds](reduction.md#group-by-reductions) page.

### Key By
> ADVANCED OPERATOR

Creates a new `KeyedStream` in the same way as `group_by`, but **without shuffling elements over the network**. Each replica independently assigns keys to the elements it already holds — elements with the same key that happen to live on different replicas are **not** merged.

This means subsequent keyed operators (e.g., `reduce`, `fold`) will produce one result *per replica*, not one result globally. **This can make operators misbehave** if you expect a global aggregation.

```rust
let s = env.stream_iter(0..5);
let res = s.key_by(|&n| n % 2).collect_vec();
```

### To Keyed

If you already have a `Stream` where the elements are key-value tuples `(K, V)`, you can convert it directly to a `KeyedStream` using the `to_keyed` operator without needing a keyer closure:

```rust
let s = env.stream_iter(vec![(0, "a"), (1, "b"), (0, "c")]);
let keyed = s.to_keyed();
```

### `group_by` vs `key_by` at a glance

| | `group_by` | `key_by` |
|---|---|---|
| Network shuffle | Yes — all elements with the same key end up on the same replica | No — keys stay local to each replica |
| Global aggregation | Correct — one result per key across all data | Incorrect for this purpose — one result per key **per replica** |
| Performance | Higher (network cost) | Lower (no shuffle) |
| When to use | Any time you need a correct global aggregation or join | Only when you know all elements for a key are already co-located (rare) |

If you are unsure which to use, use `group_by`. For aggregations on very large streams, prefer the **associative variants** (`group_by_reduce`, `group_by_sum`, etc.) which split the work into a local pre-aggregation step before the shuffle, reducing network traffic significantly. 

## Partitioning
### Route
Sometimes there is the need to send elements to different routes based on some condition. The `route` operator allows to create a series of routes and send the elements to the correct route based on the first met condition.
- Routes are created with the `add_route` method, a new stream is created for each route.
- Each element is routed to the first stream for which the routing condition evaluates to true.
- If no route condition is satisfied, the element is dropped

```rust
let mut routes = s.route()
 .add_route(|&i| i < 5)
 .add_route(|&i| i % 2 == 0)
 .build()
 .into_iter();
assert_eq!(routes.len(), 2);
// 0 1 2 3 4
routes.next().unwrap().for_each(|i| eprintln!("route1: {i}"));
// 6 8
routes.next().unwrap().for_each(|i| eprintln!("route2: {i}"));
// 5 7 9 ignored
env.execute_blocking();
```

### Split
Create multiple streams from a single stream where each split will have all the elements of the original stream. The `split` operator is useful when you need to apply different transformations to the same stream.

```rust
let s = env.stream_iter(0..5);
let mut splits = s.split(3);
let a = splits.pop().unwrap();
let b = splits.pop().unwrap();
let c = splits.pop().unwrap();
```

---

## Keyed Streams (`KeyedStream`)

When you partition a stream using `group_by`, `key_by`, or `to_keyed`, the resulting type is a `KeyedStream`. A `KeyedStream` is logically a set of streams, one for each unique key.

Many of the standard operators that exist on `Stream` are also available on `KeyedStream` (such as `map`, `filter`, `flat_map`, `fold`, `reduce`, etc.), but their signatures differ because they operate on keyed elements and have access to the key.

### Operator Signatures on `KeyedStream`

- **`map`**: The mapping closure receives a tuple containing a reference to the key and the element: `Fn((&K, I)) -> O`. The output is another `KeyedStream` with the same key but the transformed value:
  ```rust
  let s = env.stream_iter(0..5).group_by(|&n| n % 2);
  let res = s.map(|(key, n)| n * 10).collect_vec();
  ```

- **`filter`**: The predicate closure receives a reference to the key-value tuple: `Fn(&(K, I)) -> bool`.
  ```rust
  let s = env.stream_iter(0..10).group_by(|&n| n % 2);
  let res = s.filter(|&(key, n)| n % 3 == 0).collect_vec();
  ```

- **`filter_map`**: The closure receives a reference to the key and the value: `Fn((&K, I)) -> Option<O>`.
  ```rust
  let s = env.stream_iter(0..10).group_by(|&n| n % 2);
  let res = s.filter_map(|(key, n)| if n % 3 == 0 { Some(n * 4) } else { None }).collect_vec();
  ```

- **`flat_map`**: The closure receives the owned key-value pair `(K, I)` and returns an iterator: `Fn((K, I)) -> It`.
  ```rust
  let s = env.stream_iter(0..3).group_by(|&n| n % 2);
  let res = s.flat_map(|(key, n)| vec![n, n]).collect_vec();
  ```

### Stateful Keyed Operators

On a `KeyedStream`, stateful operators like `rich_map`, `rich_flat_map`, and `rich_filter_map` are **evaluated per-key**. This means the closure's state is cloned and maintained independently for each key (rather than just per replica as on `Stream::rich_map`).

- **`rich_map`**: `FnMut((&K, I)) -> O`
- **`rich_flat_map`**: `FnMut((&K, I)) -> It`
- **`rich_filter_map`**: `FnMut((&K, I)) -> Option<O>`

### Returning to a Normal Stream

If you want to discard the key or convert the `KeyedStream` back into a normal `Stream`, you can use:

- **`unkey`**: Converts a `KeyedStream<K, V>` to a normal `Stream<(K, V)>`.
  ```rust
  let stream = keyed.unkey(); // Stream<(K, I)>
  ```
- **`drop_key`**: Discards the key entirely and returns a `Stream<V>` containing just the values.
  ```rust
  let stream = keyed.drop_key(); // Stream<I>
  ```
