use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub compactor: CompactorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// How many datoms the active MemTable can hold before it is sealed
    /// and a new WorldView is published.
    pub memtable_max_rows: usize,
    /// Allow JIT schema creation during transactions.
    pub allow_jit_schema: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompactorConfig {
    /// Number of background threads dedicated to Parquet merging and interval closing.
    pub worker_threads: usize,
    /// If this many sealed MemTables are waiting in RAM to be written to disk,
    /// the Transactor will experience backpressure to prevent OOM errors.
    pub backpressure_threshold: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                memtable_max_rows: 50_000, // Small, fast snapshots
                allow_jit_schema: true,
            },
            compactor: CompactorConfig {
                worker_threads: 2,
                backpressure_threshold: 10, // Protects laptop memory
            },
        }
    }
}
