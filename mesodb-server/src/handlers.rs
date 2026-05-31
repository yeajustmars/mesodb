// mesodb-server/src/handlers.rs

use axum::{
    Json,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::dto::SchemaRequest;
use mesodb_core::db::{AttributeDefinition, MesoDB, OutputFormat, QueryOptions};
use mesodb_core::schema::ValueType;
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

// #[axum::debug_handler]
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
    use super::*;
    use axum::Json;
    use axum::extract::State;
    use axum::response::IntoResponse;
    use serde_json::Value as JsonValue;
    use std::sync::Arc;
    use tempfile::tempdir;

    use mesodb_core::config::Config;
    use mesodb_core::db::MesoDB;
    use mesodb_core::schema::SchemaMap;

    async fn setup_test_db() -> Arc<MesoDB> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("server_test.db");
        // Boot fresh engine with blank schema
        Arc::new(MesoDB::open(db_path, SchemaMap::new(), Config::default()).unwrap())
    }

    /// Helper to unwrap Axum responses into raw JSON values
    async fn extract_json(
        response: axum::response::Response,
    ) -> (axum::http::StatusCode, JsonValue) {
        let status = response.status();
        let body = response.into_body();
        // Axum 0.8 body extraction
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn test_server_handlers_end_to_end() {
        let db = setup_test_db().await;
        let state = State(db.clone());

        // ==========================================
        // 1. TEST SCHEMA HANDLER
        // ==========================================
        let schema_payload = serde_json::json!({
            "attributes": [
                { "ident": ":user/name", "value_type": "String", "is_unique": false },
                { "ident": ":user/age", "value_type": "Int64", "is_unique": false }
            ]
        });

        let schema_req = serde_json::from_value(schema_payload).unwrap();
        let schema_res = handle_schema(state.clone(), Json(schema_req))
            .await
            .into_response();
        let (status, schema_json) = extract_json(schema_res).await;

        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "Schema error: {}",
            schema_json
        );
        assert_eq!(schema_json["status"], "success"); // We wrote this one, so we know it has "status"

        // ==========================================
        // 2. TEST TRANSACT HANDLER
        // ==========================================
        let tx_payload = serde_json::json!({
            "facts": [
                // NOTE: Depending on your Value enum's Serialize implementation,
                // you might need {"String": "Alice"} here if it's strictly externally tagged.
                // Assuming your DTO handles standard JSON values:
                { "e": 1, "ident": ":user/name", "v": { "String": "Alice" }, "op": true },
                { "e": 1, "ident": ":user/age", "v": { "Int64": 30 }, "op": true }
            ]
        });
        let tx_req = serde_json::from_value(tx_payload).unwrap();

        let tx_res = handle_transact(state.clone(), Json(tx_req))
            .await
            .into_response();
        let (status, tx_json) = extract_json(tx_res).await;

        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "Transact failed: {}",
            tx_json
        );
        // We removed the `tx_json["status"] == "success"` check here.
        // Just verify it actually returned datoms written!
        assert!(
            tx_json.get("datoms_written").is_some() || tx_json.get("tx_id").is_some(),
            "Unexpected transact response shape: {}",
            tx_json
        );

        // ==========================================
        // 3. TEST QUERY HANDLER
        // ==========================================
        let query_payload = serde_json::json!({
            "query": "[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]",
            "options": {
                "format": "Json"
            }
        });
        let query_req = serde_json::from_value(query_payload).unwrap();

        let query_res = handle_query(state.clone(), Json(query_req))
            .await
            .into_response();
        let (status, query_json) = extract_json(query_res).await;

        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "Query failed: {}",
            query_json
        );

        // Safely extract the results, whether it's wrapped in `{"results": [...]}` or returned as a direct array `[...]`
        let results_str = if let Some(results) = query_json.get("results") {
            results.as_str().unwrap().to_string()
        } else if let Some(s) = query_json.as_str() {
            s.to_string()
        } else {
            query_json.to_string()
        };

        let results_array: Vec<JsonValue> = serde_json::from_str(&results_str).expect(&format!(
            "Failed to parse query results as JSON Array: {}",
            results_str
        ));

        assert_eq!(results_array.len(), 1);
        assert_eq!(results_array[0]["n"], "Alice");
        assert_eq!(results_array[0]["a"], 30);
    }
}
