# Interactive Development with Jupyter

For exploratory data analysis, rapid prototyping, or showcasing examples, you can write and execute **Renoir** data streams interactively in a Jupyter Notebook using Rust's evaluation kernel.

---

## Prerequisites

Before setting up Jupyter, ensure you have the Rust toolchain installed. If not, follow the [Installation Guide](/docs/getting-started/install/).

---

## 1. Install Evcxr Jupyter Kernel

[**Evcxr**](https://github.com/evcxr/evcxr) is a Rust evaluation context providing a REPL (similar to Python's IPython) and a Jupyter Kernel.

To install the kernel, follow these steps:

1. **Install the `evcxr_jupyter` binary**:
    ```bash
    cargo install --locked evcxr_jupyter
    ```
2. **Register the Jupyter Kernel**:
    ```bash
    evcxr_jupyter --install
    ```

    > :octicons-light-bulb-16: Ensure that Cargo's binary folder (`$HOME/.cargo/bin`) is in your environment `PATH` variable so Jupyter can locate the `evcxr_jupyter` executable.

---

## 2. Using the Jupyter Kernel in VS Code

You can run Jupyter Notebooks directly inside Visual Studio Code:

1. **Install Jupyter in your Python environment**:
   Make sure you have python and the standard `jupyter` package installed:
   ```bash
   pip install jupyter
   ```
2. **Install the VS Code Extensions**:
   Install the [VS Code Jupyter Extension](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter).
3. **Create a Notebook**:
   Create a new file with the extension `.ipynb` (e.g., `renoir_demo.ipynb`) and open it in VS Code.
4. **Select the Rust Kernel**:
    - Click **Select Kernel** in the top-right corner of the editor.
    - Choose **Jupyter Kernel...**
    - Select the **Rust** kernel installed by `evcxr_jupyter` in the previous step.

---

## 3. Importing Renoir & Jupyter Directives

In an Evcxr-powered cell, you can import dependencies and configure compilation settings using special colon-prefixed directives.

### Key Evcxr Directives

* **`:dep`**: Imports external crates from crates.io or Git repositories. Follows the same syntax as a dependency line in `Cargo.toml`.
  ```rust
  :dep renoir = { git = "https://github.com/deib-polimi/renoir" }
  ```
* **`:cache`**: Sets a compilation cache size (in MiB). Caching compilation segments speeds up interactive executions.
  ```rust
  :cache 500
  ```
* **`:opt`**: Sets the optimization level (`0` to `3`). While level `0` compiles fastest, level `2` or `3` runs the streaming dataflow much faster.
  ```rust
  :opt 2
  ```
* **`:vars`**: Prints a table listing all variables in scope alongside their types and values.
  ```rust
  :vars
  ```

---

## 4. Complete Interactive Example

Add the following snippets to consecutive cells in your Jupyter Notebook.

### Cell 1: Setup and Imports
```rust
:cache 500
:opt 2
:dep renoir = { git = "https://github.com/deib-polimi/renoir" }

use renoir::prelude::*;
```

### Cell 2: Creating a Local Dataflow
```rust
let ctx: StreamContext = StreamContext::new_local();

// Compute squares for a range of integers in parallel
let result: StreamOutput<Vec<(i32, i32)>> = ctx.stream_par_iter(0..100)
    .map(|x| (x, x * x))
    .collect_vec();

// Execute the pipeline synchronously 
ctx.execute_blocking();

// Retrieve the collected results
let output: Option<Vec<(i32, i32)>>  = result.get();
```

### Cell 3: Print and Inspect
```rust
// Display variables currently defined in the session
:vars
```

| Variable | Type | Value |
| :--- | :--- | :--- |
| `output` | `Option<Vec<(i32, i32)>>` | `Some(...)` |

### Cell 4: Display Results

```rust
println!("First 5 squared pairs: {:?}", &output.as_ref().unwrap()[0..5]);
```

**Expected Cell Output:**
```txt
First 5 squared pairs: [(0, 0), (1, 1), (2, 4), (3, 9), (4, 16)]
```
