// mesodb-server/src/handlers.rs

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
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
pub async fn handle_query(
    State(db): State<Arc<MesoDB>>,
    headers: HeaderMap,
    Json(payload): Json<crate::dto::QueryRequest>,
) -> impl IntoResponse {
    // Determine target format via HTTP Accept header
    let is_edn = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|val| val.to_str().ok())
        .is_some_and(|s| s.contains("application/edn"));

    let opts = mesodb_core::db::QueryOptions {
        as_of: payload.as_of,
        rules: payload.rules.clone(),
        format: if is_edn {
            mesodb_core::db::OutputFormat::Edn
        } else {
            mesodb_core::db::OutputFormat::Json
        },
        history: false,
    };

    // Route directly to the highly-optimized zero-copy formatter
    let query_result = if is_edn {
        db.query_edn_with_options(&payload.query, opts).await
    } else {
        db.query_json_with_options(&payload.query, opts).await
    };

    match query_result {
        Ok(response_str) => {
            let content_type = if is_edn {
                "application/edn"
            } else {
                "application/json"
            };
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, content_type)],
                response_str,
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("{:?}", e)})),
        )
            .into_response(),
    }
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
    use super::*;
    use axum::Json;
    use axum::extract::State;
    use axum::http::HeaderMap;
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
        Arc::new(MesoDB::open(db_path, SchemaMap::new(), Config::default()).unwrap())
    }

    async fn extract_json(
        response: axum::response::Response,
    ) -> (axum::http::StatusCode, JsonValue) {
        let status = response.status();
        let body = response.into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let json: JsonValue = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn test_server_handlers_end_to_end() {
        let db = setup_test_db().await;
        let state = State(db.clone());

        // 1. TEST SCHEMA HANDLER
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

        assert_eq!(status, StatusCode::OK, "Schema error: {}", schema_json);
        assert_eq!(schema_json["status"], "success");

        // 2. TEST TRANSACT HANDLER
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

        assert_eq!(status, StatusCode::OK, "Transact failed: {}", tx_json);

        // 3. TEST QUERY HANDLER (JSON Negotiation)
        let query_payload = serde_json::json!({
            "query": "[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]",
        });
        let query_req = serde_json::from_value(query_payload.clone()).unwrap();

        let mut json_headers = HeaderMap::new();
        json_headers.insert("Accept", "application/json".parse().unwrap());

        let query_res = handle_query(state.clone(), json_headers, Json(query_req))
            .await
            .into_response();

        let (status, query_json) = extract_json(query_res).await;
        assert_eq!(status, StatusCode::OK);

        let results_array = query_json.as_array().expect("Expected JSON array");
        assert_eq!(results_array.len(), 1);
        assert_eq!(results_array[0]["n"], "Alice");

        // 4. TEST QUERY HANDLER (EDN Negotiation)
        let edn_req = serde_json::from_value(query_payload).unwrap();
        let mut edn_headers = HeaderMap::new();
        edn_headers.insert("Accept", "application/edn".parse().unwrap());

        let edn_res = handle_query(state.clone(), edn_headers, Json(edn_req))
            .await
            .into_response();

        assert_eq!(edn_res.status(), StatusCode::OK);
        assert_eq!(
            edn_res.headers().get("content-type").unwrap(),
            "application/edn"
        );

        let body = edn_res.into_body();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let edn_string = String::from_utf8_lossy(&bytes);

        // Ensure proper EDN keyword formatting
        assert!(edn_string.contains(r#":n "Alice""#));
        assert!(edn_string.contains(":a 30"));
    }
}
