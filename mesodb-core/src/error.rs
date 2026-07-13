// mesodb-core/src/error.rs

use std::io;
use thiserror::Error;

use crate::{schema::ValueType, types::Value};

#[derive(Error, Debug)]
pub enum MesoError {
    #[error("Arrow Error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("DataFusion Execution Error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("I/O Error: {0}")]
    Io(#[from] io::Error),

    #[error("Parse Error: {0}")]
    ParseError(String),

    #[error("Parquet Error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Query Planning Error: {0}")]
    PlanError(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Schema mismatch: Expected {expected:?}, got {found:?}")]
    TypeMismatch { expected: ValueType, found: Value },

    #[error("Schema validation failed: Attribute '{0}' is undefined.")]
    UndefinedAttribute(String),

    #[error(
        "Unique constraint violation: Value '{value}' for attribute '{attr}' is already held by Entity {owner}"
    )]
    UniqueConstraintViolation {
        attr: String,
        value: String,
        owner: u64,
    },

    #[error("Invalid Query: {0}")]
    InvalidQuery(String),
}

impl MesoError {
    /// Helper to access the IO kind if the error is an IO variant
    pub fn io_kind(&self) -> Option<std::io::ErrorKind> {
        match self {
            MesoError::Io(e) => Some(e.kind()),
            _ => None,
        }
    }
}
