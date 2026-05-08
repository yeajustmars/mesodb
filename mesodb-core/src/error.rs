use std::io;

use thiserror::Error;

use crate::parser::ParseError;
use crate::schema::ValueType;
use crate::types::Value;

#[derive(Error, Debug)]
pub enum MesoError {
    #[error("Schema validation failed: Attribute '{0}' is undefined.")]
    UndefinedAttribute(String),

    #[error("Schema mismatch: Expected {expected:?}, got {found:?}")]
    TypeMismatch { expected: ValueType, found: Value },

    #[error(
        "Unique constraint violation: Value '{value}' for attribute '{attr}' is already held by Entity {owner}"
    )]
    UniqueConstraintViolation {
        attr: String,
        value: String,
        owner: u64,
    },

    #[error("I/O Error: {0}")]
    Io(#[from] io::Error),

    #[error("Query Planning Error: {0}")]
    PlanError(String),

    #[error("DataFusion Execution Error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    // Inside pub enum MesoError { ... }
    #[error("Parse Error: {0}")]
    Parser(#[from] ParseError),

    #[error("Arrow Error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, MesoError>;
