// mesodb-core/src/config.rs

use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

use crate::error::MesoError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    /// Strict low-memory profile (<50MB heap target) for embedded, IoT, or resource-constrained sidecars.
    Embedded,
    /// Balanced default profile suitable for standard cloud services (64MB MemTable buffer).
    #[default]
    Balanced,
    /// High-throughput profile for dedicated database servers with abundant RAM (512MB MemTable, multi-threaded workers).
    Server,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub storage: StorageConfig,
    pub compactor: CompactorConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum WalSyncMode {
    /// Forces an OS-level fsync after every single transaction.
    /// Maximum ACID compliance (zero data loss on power failure), but limits write throughput.
    #[default]
    Strict,
    /// Writes to the WAL but lets the OS determine when to actually sync to disk.
    /// Massive write throughput gains, at the risk of losing milliseconds of data on hard kernel panic.
    Background,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// How many datoms the active MemTable can hold before it is sealed
    /// and a new WorldView is published.
    pub memtable_max_rows: usize,

    /// The maximum size in bytes the MemTable can reach before being sealed.
    /// This prevents out-of-memory errors on very wide schemas or large string insertions.
    pub memtable_max_bytes: usize,

    /// How aggressively the Write-Ahead Log syncs to physical disk.
    pub wal_sync_mode: WalSyncMode,

    /// Allow JIT schema creation during transactions.
    pub allow_jit_schema: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactorConfig {
    /// Number of background threads dedicated to Parquet merging and interval closing.
    pub worker_threads: usize,

    /// If this many sealed MemTables are waiting in RAM to be written to disk,
    /// the Transactor will experience backpressure and block new writes to prevent OOM errors.
    pub backpressure_threshold: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::for_profile(Profile::Balanced)
    }
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self::for_profile(Profile::Balanced)
    }
}

impl StorageConfig {
    pub fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Embedded => Self {
                memtable_max_rows: 10_000,
                memtable_max_bytes: 16 * 1024 * 1024, // 16 MB (Leaves ample margin for <50MB total heap budget)
                wal_sync_mode: WalSyncMode::Strict,
                allow_jit_schema: true,
            },
            Profile::Balanced => Self {
                memtable_max_rows: 50_000,
                memtable_max_bytes: 64 * 1024 * 1024, // 64 MB
                wal_sync_mode: WalSyncMode::Strict,
                allow_jit_schema: true,
            },
            Profile::Server => Self {
                memtable_max_rows: 500_000,
                memtable_max_bytes: 512 * 1024 * 1024, // 512 MB
                wal_sync_mode: WalSyncMode::Background,
                allow_jit_schema: true,
            },
        }
    }
}

impl CompactorConfig {
    pub fn for_profile(profile: Profile) -> Self {
        match profile {
            Profile::Embedded => Self {
                worker_threads: 1,
                backpressure_threshold: 2, // Low tolerance for un-compacted tables in RAM
            },
            Profile::Balanced => Self {
                worker_threads: 2,
                backpressure_threshold: 10,
            },
            Profile::Server => Self {
                worker_threads: 8,
                backpressure_threshold: 32, // Can buffer bursty writes in RAM during peak traffic
            },
        }
    }
}

impl Config {
    /// Generates a configuration preset for a specific operational target profile.
    pub fn for_profile(profile: Profile) -> Self {
        Self {
            storage: StorageConfig::for_profile(profile),
            compactor: CompactorConfig::for_profile(profile),
        }
    }

    /// Convenience shortcut for embedded/low-memory environments (<50MB heap target).
    pub fn embedded() -> Self {
        Self::for_profile(Profile::Embedded)
    }

    /// Convenience shortcut for balanced default cloud deployments.
    pub fn balanced() -> Self {
        Self::for_profile(Profile::Balanced)
    }

    /// Convenience shortcut for high-throughput server deployments with abundant RAM.
    pub fn server() -> Self {
        Self::for_profile(Profile::Server)
    }

    /// Loads a configuration from a TOML file path.
    /// If the file does not exist, it falls back to the Default configuration.
    pub fn load_or_default<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let p = path.as_ref();
        if !p.exists() {
            return Ok(Self::default());
        }

        let contents = fs::read_to_string(p).map_err(MesoError::Io)?;
        Self::from_toml(&contents)
    }

    /// Loads configuration directly from a TOML string (useful for tests and benchmarks).
    pub fn from_toml(s: &str) -> Result<Self, MesoError> {
        toml::from_str(s).map_err(|e| MesoError::Serialization(format!("TOML Parse Error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedded_profile_limits() {
        let config = Config::embedded();
        assert_eq!(config.storage.memtable_max_bytes, 16 * 1024 * 1024);
        assert_eq!(config.compactor.worker_threads, 1);
        assert_eq!(config.compactor.backpressure_threshold, 2);
    }

    #[test]
    fn test_server_profile_limits() {
        let config = Config::server();
        assert_eq!(config.storage.memtable_max_bytes, 512 * 1024 * 1024);
        assert_eq!(config.storage.wal_sync_mode, WalSyncMode::Background);
        assert_eq!(config.compactor.worker_threads, 8);
    }

    #[test]
    fn test_parse_toml_config() {
        let toml_str = r#"
        [storage]
        memtable_max_rows = 100000
        memtable_max_bytes = 134217728
        wal_sync_mode = "background"
        allow_jit_schema = false

        [compactor]
        worker_threads = 4
        backpressure_threshold = 20
        "#;

        let config = Config::from_toml(toml_str).unwrap();

        assert_eq!(config.storage.memtable_max_rows, 100_000);
        assert_eq!(config.storage.memtable_max_bytes, 134_217_728); // 128 MB
        assert_eq!(config.storage.wal_sync_mode, WalSyncMode::Background);
        assert!(!config.storage.allow_jit_schema);

        assert_eq!(config.compactor.worker_threads, 4);
        assert_eq!(config.compactor.backpressure_threshold, 20);
    }

    #[test]
    fn test_partial_toml_config_uses_defaults() {
        let toml_str = r#"
        [compactor]
        worker_threads = 8
        "#;

        let config = Config::from_toml(toml_str).unwrap();

        // Should use defaults (Balanced) for everything missing
        assert_eq!(config.storage.memtable_max_rows, 50_000);
        assert_eq!(config.storage.wal_sync_mode, WalSyncMode::Strict);

        // Should use the explicitly provided values
        assert_eq!(config.compactor.worker_threads, 8);
        assert_eq!(config.compactor.backpressure_threshold, 10);
    }
}
