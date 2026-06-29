If you are already set-up with a development environment you are comfortable with we recommend staying with what you find to be the most productive. Otherwise if this is your first experience programming with Rust or looking for ideas to enhance your productivity, this page will show you how you can set up an effective development environment for Rust and Renoir.

# Setting up an environment using VS Code

First, we recommend working in a UNIX system if possible, if you are on Windows you may try using Windows Subsystem for Linux (WSL) for a better experience.

As for IDE, we use [VS Code](https://code.visualstudio.com/) for working with Rust (Note: there is also a WSL extension if you are on windows and want to work in WSL)

The [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer) extension will get you most of the way with what you could need to work with Rust, there is also an [official guide](https://code.visualstudio.com/docs/languages/rust) on how to setup vs code for Rust.

Other than that we also recommend the [Error Lens](https://marketplace.visualstudio.com/items?itemName=usernamehw.errorlens) extension  which integrates well with rust-analyzer to show compiler errors and hints directly inline (Enable the hint diagnostic level in the settings CTRL+, for more details)
The [Even Better TOML](https://marketplace.visualstudio.com/items?itemName=tamasfe.even-better-toml) and [Crates](https://marketplace.visualstudio.com/items?itemName=serayuzgur.crates) extension for working with the Cargo.toml: This extension is currently unmaintained and we are looking for alternative recommendations.

Finally for CLI tools, remember to check `cargo clippy --all --all-targets` for good coding practices (you can use the `--fix` flag to automatically apply the corrections too) `cargo fmt --all` to format the code and [cargo-edit](https://github.com/killercup/cargo-edit) for the cargo upgrade command, which together with cargo add can be used to manage the Cargo.toml from the terminal

When evaluating performance run with `--release` flag

> If you feel like the rust-analyzer extension is overloading with information by showing all type hints, you can disable some of them, for example in our configs we have set the following
> + Imports>Granularity>Group: module
> + Inlay Hints>Chaining Hints>Enable: false 
> +  Inlay Hints>Type Hints>Enable: false

### More suggested extensions

+ [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb)
+ [Jupyter](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) if working with [evcxr](https://github.com/evcxr/evcxr)