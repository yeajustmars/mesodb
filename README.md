# MesoDB

MesoDB is a high-performance, bitemporal Datalog database written entirely in Rust. It combines the expressive, graph-relational power of Datalog with the vectorized, zero-copy analytics of Apache Arrow and DataFusion.

Designed for both heavy transactional ingestion and complex historical analytics, MesoDB leverages a custom lock-free Copy-On-Write (COW) B+Tree for sub-microsecond point lookups, while seamlessly falling back to a vectorized SQL execution engine for massive analytical aggregations.

## 🚀 Key Features

*   **Bitemporal Time Travel:** Built-in auditing and historical querying. MesoDB retains all historical states non-destructively. You can query the database exactly as it existed at any microsecond in the past using point-in-time (`as_of`) snapshots or across-time `history` queries.
*   **Zero-Copy Execution:** Powered by Apache Arrow memory models. Data remains in native columnar formats from disk (Parquet) through the query engine (DataFusion) and over the network (Arrow Flight gRPC).
*   **7-Tier Fast-Path Router:** Simple point lookups, bitmap intersections, and range scans dynamically bypass the DataFusion SQL compiler, hitting memory-mapped B-Trees for $O(1)$ to $O(\log N)$ microsecond response times.
*   **Lock-Free Read Isolation:** A lock-free `Arc<WorldView>` pointer architecture guarantees that active readers are *never* blocked by the transactor or background compaction threads.
*   **Log-Structured Compaction:** High-throughput sequential Write-Ahead Log (WAL) buffered into MemTables, merged seamlessly into Parquet by background compactor threads.

## 📦 Workspace Structure

MesoDB is structured as a Rust Cargo Workspace containing several interconnected crates:

### 1. `mesodb-core`
The heart of the database. This crate can be used as an embedded database library within any Rust application. It contains the Transactor, the B+Tree and Roaring Bitmap tiering indices, the Datalog AST parser, and the DataFusion query planner.
> 📖 **Architecture Deep Dive:** Please refer to [core.md](core.md) for exhaustive documentation on the internal storage engine, index structures, and the bitemporal concurrency model.

### 2. `mesodb-server`
A standalone, production-ready server binary. It exposes the core engine over the network using two high-performance protocols:
*   **Axum HTTP API:** For standard web clients, serving RESTful JSON and EDN payloads (`/transact`, `/query`, `/schema`).
*   **Tonic Arrow Flight gRPC:** A specialized, zero-copy data streaming protocol for analytics clients. It streams binary Arrow `RecordBatches` directly from the engine's memory over the wire, bypassing serialization overhead.

### 3. `mesodb-bench`
A rigorous `criterion` benchmarking suite designed to guard against performance regressions. It includes:
*   `ingestion.rs`: High-throughput WAL and bitemporal collision stress tests.
*   `fast_path.rs`: Microsecond-latency routing validations.
*   `query.rs`: DataFusion relational join and aggregation profiling.
*   `flight.rs`: Arrow IPC network throughput tests.

### 4. `mesodb-cli` *(Work in Progress)*
A command-line interface for connecting to `mesodb-server` instances, managing schemas, and executing interactive Datalog queries.

## 🛠️ Configuration Profiles

MesoDB can be tuned for different environments using presets:

*   **Embedded:** Strict low-memory profile (<50MB heap target) for IoT or resource-constrained sidecars. Forces strict OS-level fsyncs.
*   **Balanced (Default):** Suitable for standard cloud microservices. 64MB MemTable buffers with moderate background thread pooling.
*   **Server:** High-throughput dedicated hardware profile. Features 512MB RAM buffers, aggressive background WAL syncs, and heavy thread allocation for Parquet compaction.

## ⚡ Quick Start

### Running the Server

Start the MesoDB server using Cargo. By default, it binds the HTTP API to port `8000` and the Arrow Flight gRPC service to port `50051`.

```bash
cargo run --release --bin mesodb-server
```

### Transacting Data (HTTP JSON)

You can transact data using standard JSON via the `/transact` endpoint. MesoDB uses standard Datalog EAV (Entity-Attribute-Value) fact structures.

```bash
curl -X POST http://localhost:8000/transact \
  -H "Content-Type: application/json" \
  -d '{
    "facts": [
      { "e": 1, "ident": ":user/name", "v": "Alice", "op": true },
      { "e": 1, "ident": ":user/age", "v": 30, "op": true },
      { "e": 2, "ident": ":user/name", "v": "Bob", "op": true }
    ]
  }'
```

### Querying Data (HTTP EDN or JSON)

Execute Datalog queries using the `/query` endpoint. Use the `Accept` header to negotiate between JSON and EDN formats.

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{
    "query": "[:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age] [(> ?age 25)]]"
  }'
```

## 🗺️ Roadmap: Vector 4

The current major engineering phase (Vector 4) is focused on **Zero-Copy Network Streaming**. The objective is to refactor the Axum HTTP and Tonic gRPC handlers to stream query results chunk-by-chunk, eliminating monolithic heap-allocated response strings. This will cap the server's RAM footprint strictly to the size of a single active Arrow `RecordBatch`, ensuring infinite horizontal read scalability regardless of query result cardinality.
