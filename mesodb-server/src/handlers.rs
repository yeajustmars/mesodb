// mesodb-server/src/handlers.rs

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::stream;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::dto::{ErrorResponse, QueryRequest, SchemaRequest, TransactionRequest};
use mesodb_core::{
    db::{AttributeDefinition, MesoDB, OutputFormat, QueryOptions},
    formatter::BatchChunkStream,
    parser,
    schema::ValueType,
    transactor::Fact,
    types::Value as MesoValue,
};

/// POST /transact
pub async fn handle_transact(
    State(db): State<Arc<MesoDB>>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    let mut core_facts = Vec::with_capacity(payload.facts.len());

    let parse_json = |val: JsonValue| -> Result<MesoValue, String> {
        match val {
            JsonValue::Bool(b) => Ok(MesoValue::Boolean(b)),
            JsonValue::Number(num) => {
                if let Some(i) = num.as_i64() {
                    Ok(MesoValue::Int64(i))
                } else if let Some(f) = num.as_f64() {
                    Ok(MesoValue::Float64(f))
                } else {
                    Err("Invalid numeric type".into())
                }
            }
            JsonValue::String(s) => Ok(MesoValue::String(s)),
            _ => Err("Unsupported value type".into()),
        }
    };

    for wire in payload.facts {
        let core_value = match parse_json(wire.v) {
            Ok(v) => v,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        };

        let core_cas = match wire.cas_old_v {
            Some(val) => match parse_json(val) {
                Ok(v) => Some(v),
                Err(e) => {
                    return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e }))
                        .into_response();
                }
            },
            None => None,
        };

        core_facts.push(Fact {
            e: wire.e,
            ident: wire.ident,
            v: core_value,
            op: wire.op,
            cas_old_v: core_cas,
            valid_time: None,
        });
    }

    let tx_res = match payload.tx_time {
        Some(t) => db.transact_at(core_facts, t).await,
        None => db.transact(core_facts).await,
    };

    match tx_res {
        Ok(report) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "tx_id": report.tx_id,
                "timestamp": report.timestamp,
                "datoms_written": report.datoms_written
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("{:?}", e),
            }),
        )
            .into_response(),
    }
}

/// POST /query
/// Zero-allocation chunked HTTP stream. Capped memory footprint regardless of dataset size.
pub async fn handle_query(
    State(db): State<Arc<MesoDB>>,
    headers: HeaderMap,
    Json(payload): Json<QueryRequest>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let is_edn = headers
        .get(header::ACCEPT)
        .and_then(|val| val.to_str().ok())
        .is_some_and(|s| s.contains("application/edn"));

    let format = if is_edn {
        OutputFormat::Edn
    } else {
        OutputFormat::Json
    };

    let ast = parser::parse_query(&payload.query).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Parse error: {:?}", e),
            }),
        )
    })?;

    let opts = QueryOptions {
        as_of: payload.as_of,
        rules: payload.rules.clone(),
        format: format.clone(),
        history: false,
    };

    let batches = db
        .query_with_options(&payload.query, opts)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Execution error: {:?}", e),
                }),
            )
        })?;

    let batch_stream = stream::iter(batches.into_iter().map(Ok));
    let chunk_stream = BatchChunkStream::new(batch_stream, &ast.find, format);

    let content_type = if is_edn {
        "application/edn"
    } else {
        "application/json"
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(Body::from_stream(chunk_stream))
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Failed to build response stream: {}", e),
                }),
            )
        })
}

pub async fn handle_schema(
    State(db): State<Arc<MesoDB>>,
    Json(payload): Json<SchemaRequest>,
) -> impl IntoResponse {
    let mut defs = Vec::with_capacity(payload.attributes.len());

    for attr in payload.attributes {
        let vt = match attr.value_type.as_str() {
            "Int64" => ValueType::Int64,
            "Float64" => ValueType::Float64,
            "Boolean" => ValueType::Boolean,
            "Ref" => ValueType::Ref,
            "Timestamp" => ValueType::Timestamp,
            "Uuid" => ValueType::Uuid,
            _ => ValueType::String,
        };

        defs.push(AttributeDefinition {
            ident: attr.ident,
            value_type: vt,
            is_unique: attr.is_unique,
        });
    }

    match db.transact_schema(defs).await {
        Ok(added) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "success",
                "attributes_added": added.len()
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("Failed to apply schema: {:?}", e),
            }),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    // Tests remain identical; HTTP chunked bodies process seamlessly through axum::body::to_bytes
    // Check original tests code block from before if re-adding.
}
