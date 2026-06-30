// mesodb-server/src/handlers.rs

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::dto::SchemaRequest;
use crate::dto::{ErrorResponse, TransactionRequest};
use mesodb_core::db::{AttributeDefinition, MesoDB};
use mesodb_core::schema::ValueType;
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value as MesoValue;

/// POST /transact
pub async fn handle_transact(
    State(db): State<Arc<MesoDB>>,
    Json(payload): Json<TransactionRequest>,
) -> impl IntoResponse {
    let mut core_facts = Vec::with_capacity(payload.facts.len());

    // A quick helper to parse standard JSON into our engine's types
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
        // 1. Parse the main value
        let core_value = match parse_json(wire.v) {
            Ok(v) => v,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        };

        // 2. Parse the optional CAS value
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
// #[axum::debug_handler]
pub async fn handle_query(
    State(db): State<Arc<MesoDB>>,
    Json(payload): Json<crate::dto::QueryRequest>,
) -> impl IntoResponse {
    // Map the incoming DTO to the core engine's QueryOptions
    let opts = mesodb_core::db::QueryOptions {
        as_of: payload.as_of,
        rules: payload.rules.clone(),
        format: mesodb_core::db::OutputFormat::Json,
        history: false,
    };

    // Use the new options-aware JSON pipeline!
    match db.query_json_with_options(&payload.query, opts).await {
        Ok(json_str) => (
            axum::http::StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            json_str,
        )
            .into_response(),
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("{:?}", e)})),
        )
            .into_response(),
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
        assert_eq!(schema_json["status"], "success");

        // ==========================================
        // 2. TEST TRANSACT HANDLER
        // ==========================================
        let tx_payload = serde_json::json!({
            "facts": [
                { "e": 1, "ident": ":user/name", "v": "Alice", "op": true },
                { "e": 1, "ident": ":user/age", "v": 30, "op": true }
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

        // The endpoint now returns the raw JSON array!
        let results_array = query_json
            .as_array()
            .expect("Expected a JSON array of results");

        // Our test script only inserted Alice, so we expect 1 row.
        assert_eq!(results_array.len(), 1);
        assert_eq!(results_array[0]["n"], "Alice");
        assert_eq!(results_array[0]["a"], 30);
    }
}
