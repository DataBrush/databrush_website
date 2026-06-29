# Deployment & Execution

Renoir supports running computations on a single machine or across a cluster of machines. This section covers the available deployment modes, how to configure them, and containerized deployment options.

The information about the environment in which the computation will run is stored in a `StreamContext`. The context can contain multiple streams and operators, and is the object that rules the execution of all the streams within it.

---

## Local Deployment

To run a computation on a single machine, you can create a `StreamContext` with the `new_local()` method. This method creates a context that will run the stream using all the available CPU cores of the machine.

```rust
let ctx = StreamContext::new_local();
// ... Streams and operators
```

If you want to specify a custom number of threads/workers to use, you can create a custom `RuntimeConfig` by using the `local(..)` method:

```rust
let config = RuntimeConfig::local(4).unwrap();
let ctx = StreamContext::new(config);
// ... Streams and operators
```

---

## Distributed Deployment

To run a computation on multiple machines, you can create a `StreamContext` with the `remote(..)` method. This method takes the path to a configuration file (TOML) containing information about the cluster.

```rust
let config = RuntimeConfig::remote("path/to/config.toml").unwrap();
config.spawn_remote_workers();
let ctx = StreamContext::new(config);
// ... Streams and operators
```

> [!IMPORTANT]
> When executing in a distributed environment, you **must** call `spawn_remote_workers()` on the config before executing any streams.

### Cluster Configuration

The configuration file must contain connection details for the various machines in the cluster. For example:

```toml
# config.toml
[[host]]
address = "host1.lan"
base_port = 9500
num_cores = 16

[[host]]
address = "host2.lan"
base_port = 9500
num_cores = 24
ssh = { username = "renoir", key_file = "/home/renoir/.ssh/id_ed25519" }
```

Once configured, your pipeline will be automatically distributed across both machines.

### Configuration Host Options
Each `[[host]]` entry supports the following keys:
- `address`: The hostname or IP address of the machine.
- `base_port`: The starting port for communication between operators on different machines.
- `num_cores`: The number of CPU cores available on the machine.
- `ssh`: Optional object to store SSH connection details (requires the `ssh` feature flag):
    - `ssh_port`: Port to connect to the machine (default: 22).
    - `username`: SSH username.
    - `password`: Password for password-based authentication.
    - `key_file`: Path to the private key file.
    - `key_passphrase`: Passphrase for the private key file.

---

## Context from Arguments

To easily switch between environments without recompiling your program, you can parse runtime settings from command-line arguments:

```rust
let (config, args) = RuntimeConfig::from_args();
let ctx = StreamContext::new(config);
// ... Streams and operators
```

This enables you to specify local or remote execution directly from your terminal:

```bash
# Run locally with 4 threads
cargo run -- --local 4

# Run on a distributed cluster using a config file
cargo run -- --remote path/to/config.toml
```

---

## Docker / Containerized Deployment

Renoir applications compile to static, self-contained binaries, making them ideal for containerization with Docker.

### Packaging Your Application

Here is a recommended multi-stage `Dockerfile` to produce a minimal production image:

```dockerfile
# Stage 1: Build the binary
FROM rust:1.80 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

# Stage 2: Create a minimal runner image
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/renoir-quickstart /app/renoir-app
EXPOSE 9500
ENTRYPOINT ["/app/renoir-app"]
```

### Running with Docker
When deploying with Docker across multiple hosts:
1. Make sure to publish and expose the configured `base_port` (and any subsequent ports used by peer connections).
2. The network configuration in `config.toml` must use hostnames or container IPs that are mutually reachable across all hosts.

---

## Kubernetes

In Kubernetes environments, you can deploy Renoir workers as Pods.

- **Headless Service**: Renoir relies on peer-to-peer TCP connections between all replicas. To facilitate direct pod-to-pod routing, configure a **Headless Service** (`clusterIP: None`) so each Pod can resolve and connect to other replicas directly using Pod DNS names.
- **Worker Spawning**: When running inside Kubernetes, dynamic worker spawning via SSH is typically not desired. Instead, pre-deploy worker Pods with identical binary versions and configure them to join the cluster using static Pod DNS entries or service IPs in your `config.toml`.
