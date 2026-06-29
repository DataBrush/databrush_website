# Tokio Feature

By default Renoir uses standard OS threads for both workers and network I/O. Enabling the `tokio` feature switches the runtime to use Tokio throughout, which unlocks async execution and a set of async operators.

## Enabling the feature

```toml title="Cargo.toml"
[dependencies]
renoir = { version = "*", features = ["tokio"] }
```

## What changes with `tokio` enabled

**Runtime behavior**

| Area | Without `tokio` | With `tokio` |
|---|:---:|:---:|
| Worker threads | OS threads | `tokio::task::spawn_blocking` |
| Network stack | Synchronous I/O | Tokio async I/O (`tokio::net`) |
| `execute_blocking()` | Joins OS threads directly | Creates a Tokio multi-thread runtime internally, then blocks |
| `execute().await` | <span style="background:#f5f5f5;color:#9e9e9e;padding:1px 7px;border-radius:3px;font-size:.85em">not available</span> | <span style="background:#e8f5e9;color:#2e7d32;padding:1px 7px;border-radius:3px;font-size:.85em">available</span> — runs inside an existing async context |

**Operators unlocked**

| Operator | Without `tokio` | With `tokio` |
|---|:---:|:---:|
| `map_async` | <span style="background:#f5f5f5;color:#9e9e9e;padding:1px 7px;border-radius:3px;font-size:.85em">not available</span> | <span style="background:#e8f5e9;color:#2e7d32;padding:1px 7px;border-radius:3px;font-size:.85em">available</span> |
| `map_async_memo` | <span style="background:#f5f5f5;color:#9e9e9e;padding:1px 7px;border-radius:3px;font-size:.85em">not available</span> | <span style="background:#e8f5e9;color:#2e7d32;padding:1px 7px;border-radius:3px;font-size:.85em">available</span> |
| `map_async_memo_by` | <span style="background:#f5f5f5;color:#9e9e9e;padding:1px 7px;border-radius:3px;font-size:.85em">not available</span> | <span style="background:#e8f5e9;color:#2e7d32;padding:1px 7px;border-radius:3px;font-size:.85em">available</span> |
| Async stream source | <span style="background:#f5f5f5;color:#9e9e9e;padding:1px 7px;border-radius:3px;font-size:.85em">not available</span> | <span style="background:#e8f5e9;color:#2e7d32;padding:1px 7px;border-radius:3px;font-size:.85em">available</span> |

## Running the context

### In a blocking `main`

`execute_blocking()` works exactly the same as without the feature. When tokio is enabled it internally creates a multi-threaded Tokio runtime, so you do not need to create one yourself.

```rust
use renoir::prelude::*;

fn main() {
    let ctx = StreamContext::new_local();
    // ... streams and operators
    ctx.execute_blocking();
}
```

### In an async `main`

With the `tokio` feature you can drive the context from inside an existing Tokio runtime using `execute().await`. This is useful when Renoir is part of a larger async application.

```rust
use renoir::prelude::*;

#[tokio::main]
async fn main() {
    let ctx = StreamContext::new_local();
    // ... streams and operators
    ctx.execute().await;
}
```

## Async operators

### `map_async`

Applies an async closure to each element of the stream. Up to 4 futures run concurrently by default (controlled by `BatchMode`).

```rust
use renoir::prelude::*;

#[tokio::main]
async fn main() {
    let ctx = StreamContext::new_local();
    let result = ctx
        .stream_iter(0..10u64)
        .map_async(|n| async move {
            // simulate an async operation, e.g. an HTTP call or DB query
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            n * 2
        })
        .collect_vec();
    ctx.execute().await;
    if let Some(v) = result.get() {
        println!("{v:?}");
    }
}
```

### `map_async_memo`

Like `map_async`, but caches results by element value. Repeated elements skip the async call and return the cached output. Useful when the async operation is expensive and inputs repeat frequently.

```rust
let result = ctx
    .stream_iter(words)
    .map_async_memo(|word| async move { fetch_embedding(word).await }, 1024)
    .collect_vec();
```

The second argument is the cache capacity (number of entries to keep in memory).

### `map_async_memo_by`

Like `map_async_memo` but lets you supply a separate key function, so the cache key can differ from the element itself.

```rust
let result = ctx
    .stream_iter(events)
    .map_async_memo_by(
        |event| async move { fetch_user_profile(event.user_id).await },
        |event| event.user_id,
        512,
    )
    .collect_vec();
```
