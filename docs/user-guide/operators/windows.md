When working with unbounded data streams is often necessary to consider only a subset of the data to perform some computation. **Renoir** provides a set of operators that allow you to define windows over the data stream to perform the computation only on the data that is part of the window.
In **Renoir** windows are defined by a descriptor `WinDescr` that allows the user to specify the type and the logic used to group the data inside the right window.

## Windows Descriptors
There are several types of descriptors that can be used:

- **CountWindow**: defines a window based on the number of elements in the window. It can be defines as `tumbling` or `sliding` windows.
- **EventTimeWindow**: defines a window based on the event timestamps of the data. It can be defines as `tumbling` or `sliding` windows.
- **ProcessingTimeWindow**: defines a window based on the wall clock at time of processing. It can be defines as `tumbling` or `sliding` windows.
- **SessionWindow**: defines a window that splits after if no element is received for a fixed wall clock duration.
- **TransactionWindow**: defines a window based on a user defined logic. A complete analysis of this descriptor can be found in the [TransactionWindow API Documentation](https://deib-polimi.github.io/renoir/renoir/operator/window/struct.TransactionWindow.html).


## Windows over a single stream
If the stream is NOT partitioned in any way like using `group_by` or `key_by` operators, the window is defined over the whole stream. 
The operator that allows you to define a window over the stream is `window_all`. The `window_all` operator takes a [descriptor](#windows-descriptors) of the window as an argument and return a windowed stream that can be used to perform computation over the window.

```Rust
let s = env.stream_iter(0..5usize);
let res = s
    .window_all(CountWindow::tumbling(2))
    // rest of the pipeline
```

> Note that since the window is defined over the whole stream, this operator cannot be parallelized. If possible partition the stream using `group_by` or `key_by` operators to allow parallel execution.

## Windows over a partitioned stream
If the stream is partitioned in some way like using `group_by` or `key_by` operators, the window is defined over each partition. We can define our windows using the `window` operator with the [descriptor](#windows-descriptors) that we want.

```Rust
let s = env.stream_iter(0..9);
let res = s
    .group_by(|&n| n % 2)
    .window(CountWindow::sliding(3, 2))
    // rest of the pipeline
```

To apply a window after joining two `KeyedStream`s over the same key, first join them and then apply the window to the resulting keyed stream:

```Rust
let s1 = env.stream_iter(0..9).group_by(|&n| n % 2);
let s2 = env.stream_iter(0..9).group_by(|&n| n % 2);

let res = s1
    .join(s2)
    .window(CountWindow::tumbling(2))
    // rest of the pipeline
```

## Operators over windows
Once the window is defined we can perform different operation to obtain the wanted information. Some of the possible operations are the standard `max`, `min`, `sum` or `count` but also more complex operations are available like the `fold` operator.

For a complete list of the available operators check the [API Documentation](https://deib-polimi.github.io/renoir/renoir/struct.WindowedStream.html#).

```rust
let s = env.stream_iter(0..5usize);
let res = s
    .window_all(CountWindow::tumbling(2))
    .fold(0, |acc, value| acc + value)
    .collect_vec();
```

---

## Under the Hood

### The `WindowedStream` Struct

Calling `.window(descr)` or `.window_all(descr)` does **not** immediately add an operator to the pipeline. Instead it wraps the underlying `KeyedStream` together with the chosen descriptor into a `WindowedStream`:

```rust
pub struct WindowedStream<Op, O, WinDescr>
where
    Op: Operator,
    Op::Out: KeyedItem,
    WinDescr: WindowDescription<<Op::Out as KeyedItem>::Value>,
{
    inner: KeyedStream<Op>,
    descr: WinDescr,
}
```

`WindowedStream` is a *builder*, not an operator. Only when you chain an aggregation method (`.fold()`, `.sum()`, `.count()`, …) does Renoir combine the descriptor and the accumulator into a concrete `WindowOperator` and append it to the block's operator chain.

### The `WindowAccumulator` Trait

A `WindowAccumulator` holds the incremental state for a single open window. It receives elements one at a time through `process()`, and when the window closes `output()` is called exactly once to produce the result:

```rust
pub trait WindowAccumulator: Clone + Send + 'static {
    type In: Data;
    type Out: Data;

    fn process(&mut self, el: &Self::In);
    fn output(self) -> Self::Out;
}
```

Because sliding windows can have several overlapping windows open simultaneously, the accumulator must be `Clone`, Renoir initialises one accumulator *per active window* by cloning the initial state. Built-in accumulators include `Fold`, `Count`, `Sum`, `Min`, `Max`, and `Collect`. You can express a custom accumulator by providing an initial value and an update function to `.fold()`.

### The `WindowManager` Trait

While the accumulator handles *what* to compute, the `WindowManager` decides *when*: it routes incoming elements to the correct windows and emits results when a window is ready to close.

```rust
pub trait WindowManager: Clone + Send {
    type In: Data;
    type Out: Data;
    type Output: IntoIterator<Item = WindowResult<Self::Out>>;

    fn process(&mut self, el: StreamElement<Self::In>) -> Self::Output;
    fn recycle(&self) -> bool { false }
}
```

`process()` receives full `StreamElement` values, not just data items but also watermarks and control signals. A single call may return zero, one, or many `WindowResult`s: for example, a single watermark can close several windows at once. `recycle()` returns `true` when the manager has no active state and can be evicted from the per-key map, freeing memory.

For keyed streams the actual runtime type is `KeyedWindowManager`: a `HashMap` mapping each key to its own `WindowManager`, so windows are tracked independently per key.

The `WindowDescription` trait acts as a factory that, given an accumulator, produces the corresponding manager:

```rust
pub trait WindowDescription<T> {
    type Manager<A: WindowAccumulator<In = T>>: WindowManager<In = T, Out = A::Out>;
    fn build<A: WindowAccumulator<In = T>>(&self, accumulator: A) -> Self::Manager<A>;
}
```

When you write `.window(CountWindow::tumbling(3)).fold(0, |a, x| a + x)`, Renoir calls `descr.build(FoldAccumulator::new(0, f))` internally, producing a `WindowOperator` that is inserted into the current block's operator chain.

---

## Converting a `WindowedStream` Back to a `Stream`

`WindowedStream` has no `.into_stream()` method, this is intentional. A `WindowedStream` is a *pending* operation with no output type yet: the element type is only determined once you choose an aggregation.

To obtain a plain `Stream` you must commit to an accumulation. Any aggregation on a `WindowedStream` produces a `KeyedStream<(Key, Output)>`. You can then call `.drop_key()` to strip the key:

```rust
let result: Stream<usize> = s
    .group_by(|x| x % 4)
    .window(CountWindow::tumbling(3))
    .sum::<usize>()   // KeyedStream<(u32, usize)>
    .drop_key();      // Stream<usize>
```

If you need the key alongside the value, use `.unkey()` to obtain `Stream<(Key, Value)>`, or continue chaining `KeyedStream` operators.

There is no way to forward raw, un-aggregated window contents as a `Stream`, the accumulator is the only bridge back to the stream world.

---

## Windows over Real-Time (Unbounded) Streams

All window types work with both bounded and unbounded sources, but closing conditions differ in an important way.

With a **bounded** source (iterator, file), the stream ends with a `Terminate` signal. When the operator chain sees `Terminate`, all open windows flush immediately regardless of whether their closing condition was met.

With an **unbounded** source (Kafka, channel, async stream), the stream never terminates. Windows must close based on their own logic:

| Window type | Closes on | Works unbounded? |
|---|---|---|
| `CountWindow` | Element count reached | Yes |
| `SessionWindow` | Inactivity gap elapsed | Yes |
| `ProcessingTimeWindow` | Wall-clock time elapsed | Yes |
| `EventTimeWindow` | Watermark advances past end | Only with a watermark generator |

For `EventTimeWindow` on a live source you **must** attach timestamps and a watermark strategy via `add_timestamps`, without watermarks, event-time windows accumulate elements indefinitely and never emit. See the [Timestamps & Watermarks](/docs/user-guide/watermarks/) guide for how to instrument a real-time source correctly.
