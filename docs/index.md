---
hide:
    - toc
    - feedback
---
# Welcome to Renoir

[Preprint](https://arxiv.org/abs/2306.04421)

### REactive Network of Operators In Rust

[API Docs](https://deib-polimi.github.io/renoir/renoir/)

Renoir *(short: Noir)  [/ʁənwaʁ/, /nwaʁ/]* is a distributed data processing platform based on the dataflow paradigm that provides an ergonomic programming interface, similar to that of Apache Flink, but has much better performance characteristics.

Renoir converts each job into a dataflow graph of
operators and groups them in blocks. Blocks contain a sequence of operors which process the data sequentially without repartitioning it. They are the deployment unit used by the system and can be distributed and executed on multiple systems.

The common layout of a Renoir program starts with the creation of a `StreamContext`, then one or more `Source`s are initialised creating a `Stream`. The graph of operators is composed using the methods of the `Stream` object, which follow a similar approach to Rust's `Iterator` trait allowing ergonomically define a processing workflow through method chaining.

## Explore the Documentation

<!-- <div class="grid cards" markdown> -->

-  **Getting Started**: Install the framework, configure your environment, and run your first build. [Start here](getting-started/install/)
-  **Core Concepts**: Understand Renoir's programming model and how streams, operators, and blocks fit together.    [Learn the basics](user-guide/core-concepts/)
-  **Code Examples**: See the framework in action with copy-pasteable snippets and common use cases.    [View examples](user-guide/examples/)
-  **Operators**: Explore the full operator catalogue: sequential, reduction, windowing, partitioning, iterative, and multi-stream.    [Browse operators](user-guide/operators/sequential/)
-  **Connectors**: Configure sources and sinks to feed data into and out of your pipelines.    [View connectors](user-guide/connectors/)
-  **Deployment & Execution**: Deploy and run Renoir computations locally in parallel or distribute them across a cluster.    [Deploy & Run](user-guide/deployment/)
-  **Jupyter Notebooks**: Write and execute Renoir streams interactively using Rust's Jupyter kernel.    [Interactive dev](getting-started/jupyter/)
-  **Flink Migration**: Migrate an existing Apache Flink job to Renoir with a concept-by-concept guide.    [Migrate from Flink](getting-started/flink-migration/)
-  **Tokio Integration**: Enable the Tokio feature to use async operators and an async runtime.    [Async with Tokio](user-guide/tokio/)
-  **Timestamps & Watermarks**: Assign event-time timestamps, define watermark strategies, and understand how watermarks propagate across parallel replicas.    [Event-time processing](user-guide/watermarks/)
-   **Internals & Execution Model**: Understand how blocks are built, replicated, and assigned to workers — and why `ExchangeData` matters at block boundaries.    [Under the hood](user-guide/internals/)
-   **Editor Setup**: Configure VS Code for a productive Rust and Renoir development environment.    [Set up editor](appendix/editor/)
<!-- 
</div> -->
