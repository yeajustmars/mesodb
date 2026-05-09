// mesodb-core/src/config.rs
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub compactor: CompactorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// Max rows in a MemTable before rotating to a frozen batch
    pub memtable_rotation_threshold: usize,
    /// Pre-allocated capacity for new MemTables
    pub memtable_initial_capacity: usize,
    /// Whether to allow the database to automatically create attributes
    /// when they are first encountered in a transaction.
    pub allow_jit_schema: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompactorConfig {
    /// Number of background threads for Parquet conversion/merging
    pub worker_threads: usize,
    /// How many frozen batches to keep in RAM before forcing a disk flush
    pub max_frozen_batches_in_ram: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                memtable_rotation_threshold: 100_000,
                memtable_initial_capacity: 10_000,
                allow_jit_schema: true, // Default to 'on' for developer velocity
            },
            compactor: CompactorConfig {
                worker_threads: 4,
                max_frozen_batches_in_ram: 5,
            },
        }
    }
}
