**Renoir** offers a multitude of operators to transform and manipulate data streams. In this section, we will see how to use the basic operators to perform sequential transformations on a stream.

A sequential transformation is an operation that is applied to each element of the stream once in sequence. This allows us to obtain the maximum level of parallelism and to distribute the load evenly among the available resources.

## Map
The `map` operator is used to apply a function to each element of the stream. The function can be any closure that takes an element of the stream as input and returns a new element.

```rust
// Multiply each element of the stream by 10
let res = s.map(|n| n * 10).collect_vec();
```
The map operator since it is applied to each element independently can use the full power of parallelism.

## RichMap
The `rich_map` operator is similar to the `map` operator but it allows to keep a state between the elements of the stream. The function passed to the `rich_map` operator can be stateful and maintain a state for each replica.

```rust
// Enumerate the elements of the stream
let res = s.rich_map({
    let mut id = 0;
    move |x| {
        id += 1;
 (id - 1, x)
 }
}).collect_vec();
```
> Note that the state is kept for each replica of the operator, so in this case, if we keep the parallelism there will be multiple elements with the same ID (one for each replica).

## Filter
The `filter` operator is used to keep only the elements of the stream that satisfy a certain condition. The function passed to the `filter` operator must return a boolean value.

```rust
// Keep only the even elements of the stream
let res = s.filter(|&n| n % 2 == 0).collect_vec();
```
The filter operator since it is applied to each element independently can use the full power of parallelism.

## FilterMap
The `filter_map` operator combines `filter` and `map` in a single step. The function returns `Option<T>` — elements that return `None` are dropped, and elements that return `Some(value)` are kept with the transformed value.

```rust
let s = env.stream_iter(0..10);
let res = s.filter_map(|n| if n % 2 == 0 { Some(n * 3) } else { None }).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0, 6, 12, 18, 24]);
```

## FlatMap
The `flat_map` operator applies a mapping function to each element and then flattens the result. Each element is transformed into zero or more output elements.

```rust
let s = env.stream_iter(0..3);
let res = s.flat_map(|n| vec![n, n]).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0, 0, 1, 1, 2, 2]);
```

## Flatten
The `flatten` operator is used to flatten a stream of collections of elements. It takes a stream of containers and returns a stream with all the contained elements.

```rust
let s = env.stream_iter((vec![
    vec![1, 2, 3],
    vec![],
    vec![4, 5],
].into_iter()));
let res = s.flatten().collect_vec();
env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![1, 2, 3, 4, 5]);
```

## Inspect
The `inspect` operator applies a function to every element as a side effect without modifying the stream. Useful for logging, debugging, or monitoring data as it passes through the pipeline.

```rust
let s = env.stream_iter(0..5);
s.inspect(|n| println!("Item: {}", n)).for_each(std::mem::drop);

env.execute_blocking();
```

---

## Sorting and Limiting

### SortedBy
The `sorted_by` operator sorts all items in the stream using a comparison function. This is a **blocking** operator — it retains all items until the stream ends, then emits them in sorted order.

```rust
use rand::seq::SliceRandom;
use rand::rng;

let mut vec: Vec<_> = (0..20).collect();
vec.shuffle(&mut rng());

let s = env.stream_iter(vec.into_iter());
let res = s.sorted_by(|a, b| a.cmp(b)).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), (0..20).collect::<Vec<_>>());
```

### Limit
Keep only the first `limit` items from the stream. An optional `offset` skips elements before counting. The order across partitions is unspecified.

```rust
let s = env.stream_iter((0..80).into_iter());
let res = s.limit(20, Some(40)).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), (40..60).collect::<Vec<_>>());
```

> Note: This is a blocking operator. All upstream computation runs to completion even if more than `limit` elements exist.

### SortedLimitBy
Combines sorting and limiting — sort the stream and keep only the first `limit` items after an optional `offset`. More efficient than sorting everything and then limiting, as it can discard items that would fall outside the limit during accumulation.

```rust
let s = env.stream_iter(shuffled_vec.into_iter());
let res = s.sorted_limit_by(|a, b| a.cmp(b), 20, Some(40)).collect_vec();
```

---

## Deduplication

### UniqueAssoc
Remove duplicate elements from the stream. The resulting stream will contain exactly one occurrence of each unique element. Uses an associative (distributed) approach: each replica first deduplicates locally, then the results are merged globally.

```rust
let s = env.stream_iter(vec![1, 2, 2, 3, 1, 3, 4]);
let res = s.unique_assoc().collect_vec();

env.execute_blocking();

let mut res = res.get().unwrap();
res.sort();
assert_eq!(res, vec![1, 2, 3, 4]);
```

> Requires the element type to implement `Hash + Eq + ExchangeData + Sync`.

### UniqueAssocByKey
Like `unique_assoc`, but deduplication is based on a key function rather than the full element. The first element seen for each key is kept.

```rust
let s = env.stream_iter(vec![(1, "a"), (2, "b"), (1, "c"), (3, "d")]);
let res = s.unique_assoc_by_key(|(k, _)| *k).collect_vec();
```

---

## Memoized Map

### MapMemo
Map elements using a function with result caching. Previously computed outputs for the same input are returned from cache without re-executing the function. Useful for expensive pure computations with repeated inputs.

```rust
let s = env.stream_iter((0..4).cycle().take(10));
let res = s.map_memo(|n| n * n, 5).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![0, 1, 4, 9, 0, 1, 4, 9, 0, 1]);
```

The second argument is the cache capacity (maximum number of entries to keep in memory). The cache is per-process and uses `quick_cache::sync::Cache`.

### MapMemoBy
Like `map_memo`, but takes a separate key function so the cache key can differ from the element itself.

```rust
let s = env.stream_iter(5..15);
let res = s.map_memo_by(|n| (n * n) % 7, |n| n % 7, 5).collect_vec();

env.execute_blocking();

assert_eq!(res.get().unwrap(), vec![4, 1, 0, 1, 4, 2, 2, 4, 1, 0]);
```

> For async variants of memoized map, see [Tokio Integration](/docs/user-guide/tokio/).

---

## Concatenation of Sequential Operators
To help the user write clean and readable code, **Renoir** offers a series of concatenations of previous operators in a single call. This allows writing complex transformations in a single line of code.

| Operator | Composition | Description |
|---|---|---|
| `flat_map` | `map` + `flatten` | Map each element, then flatten the result |
| `rich_flat_map` | `rich_map` + `flatten` | Stateful map + flatten |
| `rich_filter_map` | `rich_map` + `filter` + `map` | Stateful map, keeping only `Some` values |
| `filter_map` | `filter` + `map` | Stateless combined filter and map |

For a complete list of operators see the [API documentation](https://docs.rs/renoir/latest/renoir/struct.Stream.html).