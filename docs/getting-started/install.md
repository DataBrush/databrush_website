# Installation & Setup

To start building high-performance data processing pipelines with **Renoir** (also known as *Noir*), you first need to set up the **Rust** toolchain on your system. 

---

## 1. Installing Rust

We highly recommend using `rustup`, the official Rust installer and version manager. It manages multiple toolchains and platform-specific targets seamlessly.

=== "Linux & macOS"

    Open a terminal and run the following command to download and install the Rust toolchain:
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
    This script will guide you through the default installation. Once completed, restart your terminal or run `source "$HOME/.cargo/env"` to apply the changes.

=== "Windows"

    1. Download the `rustup-init.exe` installer from [rustup.rs](https://rustup.rs/).
    2. Run the installer and follow the on-screen instructions.
    3. Make sure you have the C++ build tools installed. The installer will prompt you to install them via the Visual Studio Build Tools if they are missing.

=== "Alternative: Package Manager"

    You can also install Rust using your operating system's package manager, though the version may be outdated:
    
    * **Ubuntu/Debian**: `sudo apt install rustc cargo`
    * **Arch Linux**: `sudo pacman -S rust`
    * **Homebrew (macOS)**: `brew install rust`

---

## 2. Configure PATH Environment Variable

To run compiler binaries and CLI tools installed via Cargo, you must add Cargo's binary directory (`~/.cargo/bin`) to your shell's `PATH`.

Choose the configuration corresponding to your preferred shell:

=== "Bash"

    ```bash
    echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.bashrc
    source ~/.bashrc
    ```

=== "Zsh"

    ```bash
    echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.zshrc
    source ~/.zshrc
    ```

=== "Fish"

    ```fish
    fish_add_path ~/.cargo/bin
    ```

---

## 3. Create a New Renoir Project

Once the Rust toolchain is successfully configured, let's create a new binary application and add the `renoir` library to its dependencies.

```bash
# Create a new binary cargo project
cargo new --bin renoir-quickstart
cd renoir-quickstart

# Add Renoir from crates.io registry
cargo add renoir
```

> [!NOTE]
> Alternatively, if you want to use the latest development version directly from the Git repository, you can add it using:
> `cargo add renoir --git https://github.com/deib-polimi/renoir`

---

## 4. Your First Renoir Program

Let's verify that everything works correctly by writing a simple streaming application. We'll take a range of integers, distribute them, perform basic transformations (`map` and `filter`), and then collect the results.

Open `src/main.rs` in your favorite editor and replace its contents with the following code:

```rust
use renoir::prelude::*;

fn main() {
    // 1. Initialize a local StreamContext
    // This creates an execution environment leveraging all CPU cores.
    let ctx = StreamContext::new_local();

    // 2. Define the streaming pipeline
    let input_range = 0..100;
    
    let result = ctx
        // Stream the iterator elements
        .stream_iter(input_range)
        // Re-distribute/shuffle items uniformly among available processing cores
        .shuffle()
        // Keep only numbers divisible by 3 or 5
        .filter(|&x| x % 3 == 0 || x % 5 == 0)
        // Double each number
        .map(|x| x * 2)
        // Collect the results back into a vector on the master/main node
        .collect_vec();

    println!("Starting dataflow execution...");

    // 3. Execute the pipeline
    // This starts execution across all parallel worker threads and blocks until completion.
    let start_time = std::time::Instant::now();
    ctx.execute_blocking();
    let elapsed = start_time.elapsed();

    // 4. Retrieve and inspect the results
    if let Some(mut output) = result.get() {
        // Sort output for deterministic printing
        output.sort();
        println!("Execution completed in {:?}", elapsed);
        println!("Result size: {}", output.len());
        println!("Sample output: {:?}", &output[0..10]);
    } else {
        println!("No results collected (or not on the master node).");
    }
}
```

### Running the Application

Execute your program using cargo in release mode for optimal performance:

```bash
cargo run --release
```

#### Expected Output

Upon successful execution, you should see output similar to this:

```txt
Starting dataflow execution...
Execution completed in 4.2ms
Result size: 47
Sample output: [0, 6, 10, 12, 18, 20, 24, 30, 36, 40]
```

---

> :octicons-light-bulb-16: **Bonus Tip**: Set up your development environment (including VS Code tips and cargo settings) by visiting the [Development Environment Guide](../appendix/editor.md).
