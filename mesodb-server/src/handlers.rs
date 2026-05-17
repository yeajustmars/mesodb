// mesodb-server/src/handlers.rs

use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use mesodb_core::db::{MesoDB, OutputFormat, QueryOptions};
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value as MesoValue;

use crate::dto::{ErrorResponse, QueryRequest, TransactionRequest};

/// POST /transact
pub async fn handle_transact(
    State(db): State<Arc<MesoDB>>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    let mut core_facts = Vec::with_capacity(payload.facts.len());

    for wire in payload.facts {
        let core_value = match wire.v {
            JsonValue::Bool(b) => MesoValue::Boolean(b),
            JsonValue::Number(num) => {
                if let Some(i) = num.as_i64() {
                    MesoValue::Int64(i)
                } else if let Some(f) = num.as_f64() {
                    MesoValue::Float64(f)
                } else {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: "Invalid numeric type".into(),
                        }),
                    )
                        .into_response();
                }
            }
            JsonValue::String(s) => MesoValue::String(s),
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "Unsupported value type".into(),
                    }),
                )
                    .into_response();
            }
        };

        core_facts.push(Fact {
            e: wire.e,
            ident: wire.ident,
            v: core_value,
            op: wire.op,
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
pub async fn handle_query(
    State(db): State<Arc<MesoDB>>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    let output_format = match payload.format.as_deref() {
        Some("json") => OutputFormat::Json,
        Some("edn") => OutputFormat::Edn,
        _ => OutputFormat::Tabular,
    };

    let opts = QueryOptions {
        as_of: payload.as_of,
        format: output_format.clone(),
        rules: payload.rules,
    };

    match output_format {
        OutputFormat::Json => match db.query_json(&payload.query).await {
            Ok(json_str) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json_str,
            )
                .into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("{:?}", e),
                }),
            )
                .into_response(),
        },
        OutputFormat::Edn => match db.query_edn(&payload.query).await {
            Ok(edn_str) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/edn")],
                edn_str,
            )
                .into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("{:?}", e),
                }),
            )
                .into_response(),
        },
        OutputFormat::Tabular => match db.query_with_options(&payload.query, opts).await {
            Ok(batches) => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                (
                    StatusCode::OK,
                    Json(serde_json::json!({ "tabular_rows_returned": total_rows })),
                )
                    .into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("{:?}", e),
                }),
            )
                .into_response(),
        },
    }
}
