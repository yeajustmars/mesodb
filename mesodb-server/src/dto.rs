// mesodb-server/src/dto.rs

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Deserialize)]
pub struct TransactionRequest {
    pub facts: Vec<WireFact>,
    pub tx_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct WireFact {
    pub e: u64,
    pub ident: String,
    pub v: JsonValue,
    pub op: bool,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub as_of: Option<i64>,
    pub format: Option<String>,
    pub rules: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
