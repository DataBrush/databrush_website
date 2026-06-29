There could be the possibility that you need to iterate over a stream multiple times to obtain the right insights or to perform a complex computation. **Renoir** provides a set of operators exactly for this purpose.

## Iterate

This operator allows to create an iterative workflow where the data cycles through the same set of operators multiple times. It returns two streams: the **state** stream (emits one element per iteration with the accumulated state) and the **items** stream (emits the final output elements after all iterations).

The operator takes the following parameters:

| Parameter | Description |
|---|---|
| `max_iterations` | Maximum number of iterations before stopping |
| `initial_state` | Initial value of the shared iteration state |
| `body` | Closure receiving the stream and current state, returning the transformed stream |
| `local_fold` | Aggregates each element into a local delta per replica |
| `global_fold` | Merges all replica deltas into the global state |
| `loop_condition` | Called after each iteration with the updated state — return `false` to stop early |

```rust
let s = ctx.stream_iter(0..3i32).shuffle();
let (state, items) = s.iterate(
    3,  // at most 3 iterations
    0,  // initial state = 0
    |s, state| s.map(|n| n + 10),
    |delta: &mut i32, n| *delta += n,
    |state, delta| *state += delta,
    |_state| true,  // always continue until max_iterations
);
let state = state.collect_vec();
let items = items.collect_vec();
ctx.execute_blocking();
```

### Practical example: converging sum

A common use of `iterate` is running a computation until a convergence criterion is met. The following example keeps doubling values until the global sum exceeds a threshold:

```rust
let s = ctx.stream_iter(vec![1i64, 2, 3, 4, 5]).shuffle();
let (state, items) = s.iterate(
    100,    // safety cap on iterations
    0i64,   // accumulated sum across iterations
    |s, _state| s.map(|n| n * 2),
    |delta: &mut i64, n| *delta += n,
    |state, delta| *state += delta,
    |state| *state < 1000,  // stop when sum exceeds 1000
);
```

## Replay

The `replay` operator is very similar to `iterate`, but instead of feeding the *output* of each iteration as input to the next, it replays the **original input stream** at the start of every iteration. Use it when your body needs the same baseline data each time (e.g., joining a dataset against a model that is updated each round).

```rust
let s = ctx.stream_iter(vec![1i32, 2, 3]).shuffle();
let (state, out) = s.replay(
    5,
    0i32,
    |s, state| s.map(|n| n + *state),
    |delta: &mut i32, n| *delta += n,
    |state, delta| *state += delta,
    |_state| true,
);
```

### When to use `iterate` vs `replay`

| | `iterate` | `replay` |
|---|---|---|
| Input each round | Output of previous round | Original source data |
| Typical use case | Refining/transforming data across rounds | Joining fixed data against evolving state |
| Example | Gradient descent steps | Training epochs over a fixed dataset |
