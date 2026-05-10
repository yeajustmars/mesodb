use thiserror::Error;

#[derive(Error, Debug)]
pub enum MesoError {
    #[error("I/O Error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Arrow Error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(
        "Unique constraint violation: Value '{value}' for attribute '{attr}' is already held by Entity {owner}"
    )]
    UniqueConstraintViolation {
        attr: String,
        value: String,
        owner: u64,
    },
}
