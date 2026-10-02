// mesodb-cli/src/engine/mod.rs

use async_trait::async_trait;
use thiserror::Error;

pub mod embedded;
pub mod remote;

/// Strict, typed errors for internal engine operations.
/// These will bubble up to `main.rs` where `color-eyre` will format them for the user.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EngineError {
    #[error("Network connection failed: {0}")]
    Connection(String),
    #[error("Query execution failed: {0}")]
    Query(String),
    #[error("Transaction failed: {0}")]
    Transaction(String),
    #[error("Internal engine error: {0}")]
    Internal(String),
}

// -----------------------------------------------------------------------------
// Data Structures
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    pub format: String,
    pub as_of: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub raw_output: String, // To be expanded with proper Table/JSON parsing structs
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxReport {
    pub tx_id: u64,
    pub datoms_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerStatusReport {
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    pub bytes_freed: u64,
}

// -----------------------------------------------------------------------------
// Core Interface
// -----------------------------------------------------------------------------

/// Unified interface for executing MesoDB commands locally or over the network.
#[async_trait]
pub trait MesoEngine: Send + Sync {
    async fn query(&self, datalog: &str, options: QueryOptions)
    -> Result<QueryResult, EngineError>;
    async fn transact(&self, edn_facts: &str) -> Result<TxReport, EngineError>;
    async fn server_status(&self) -> Result<ServerStatusReport, EngineError>;
    async fn trigger_compaction(&self) -> Result<CompactionReport, EngineError>;
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_error_display_formatting() {
        let conn_err = EngineError::Connection("timeout".to_string());
        assert_eq!(conn_err.to_string(), "Network connection failed: timeout");

        let query_err = EngineError::Query("syntax error".to_string());
        assert_eq!(
            query_err.to_string(),
            "Query execution failed: syntax error"
        );

        let tx_err = EngineError::Transaction("conflict".to_string());
        assert_eq!(tx_err.to_string(), "Transaction failed: conflict");

        let internal_err = EngineError::Internal("disk full".to_string());
        assert_eq!(internal_err.to_string(), "Internal engine error: disk full");
    }

    #[test]
    fn test_query_options_instantiation() {
        let opts = QueryOptions {
            format: "table".to_string(),
            as_of: Some(123456789),
        };

        assert_eq!(opts.format, "table");
        assert_eq!(opts.as_of, Some(123456789));
    }
}
