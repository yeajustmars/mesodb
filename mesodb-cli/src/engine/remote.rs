// mesodb-cli/src/engine/remote.rs

use async_trait::async_trait;
use reqwest::{Client, header};
use serde_json::json;

use mesodb_core::edn::parse_transaction;
use mesodb_core::transactor::Fact;
use mesodb_core::types::Value;

use super::{
    CompactionReport, EngineError, MesoEngine, QueryOptions, QueryResult, ServerStatusReport,
    TxReport,
};

pub struct RemoteEngine {
    client: Client,
    endpoint: String,
}

impl RemoteEngine {
    pub fn new(endpoint: String) -> Self {
        Self {
            client: Client::new(),
            endpoint: endpoint.trim_end_matches('/').to_string(),
        }
    }

    fn value_to_json(v: &Value) -> serde_json::Value {
        match v {
            Value::Boolean(b) => json!(b),
            Value::Int64(i) => json!(i),
            Value::Float64(f) => json!(f),
            Value::String(s) => json!(s),
            Value::Ref(r) => json!(r),
            Value::Timestamp(t) => json!(t),
            Value::Uuid(_) => json!("uuid-not-supported-in-cli-json"),
        }
    }

    fn build_wire_fact(fact: &Fact) -> serde_json::Value {
        let cas_val = fact
            .cas_old_v
            .as_ref()
            .map(Self::value_to_json)
            .unwrap_or(serde_json::Value::Null);

        json!({
            "e": fact.e,
            "ident": fact.ident,
            "v": Self::value_to_json(&fact.v),
            "op": fact.op,
            "cas_old_v": cas_val
        })
    }
}

#[async_trait]
impl MesoEngine for RemoteEngine {
    async fn query(
        &self,
        datalog: &str,
        options: QueryOptions,
    ) -> Result<QueryResult, EngineError> {
        let url = format!("{}/query", self.endpoint);

        let payload = json!({
            "query": datalog,
            "as_of": options.as_of,
            "rules": None::<String>
        });

        let accept_header = match options.format.as_str() {
            "edn" => "application/edn",
            _ => "application/json",
        };

        let res = self
            .client
            .post(&url)
            .header(header::ACCEPT, accept_header)
            .json(&payload)
            .send()
            .await
            .map_err(|e| EngineError::Connection(e.to_string()))?;

        let status = res.status();
        let body_text = res
            .text()
            .await
            .map_err(|e| EngineError::Connection(e.to_string()))?;

        if status.is_success() {
            Ok(QueryResult {
                raw_output: body_text,
            })
        } else {
            Err(EngineError::Query(body_text))
        }
    }

    async fn transact(&self, edn_facts: &str) -> Result<TxReport, EngineError> {
        let facts =
            parse_transaction(edn_facts).map_err(|e| EngineError::Transaction(e.to_string()))?;

        let wire_facts: Vec<serde_json::Value> = facts.iter().map(Self::build_wire_fact).collect();
        let payload = json!({ "facts": wire_facts });

        let url = format!("{}/transact", self.endpoint);
        let res = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| EngineError::Connection(e.to_string()))?;

        let status = res.status();
        let body_text = res
            .text()
            .await
            .map_err(|e| EngineError::Connection(e.to_string()))?;

        if status.is_success() {
            let parsed: serde_json::Value = serde_json::from_str(&body_text)
                .map_err(|e| EngineError::Internal(format!("Failed to parse server ack: {}", e)))?;

            Ok(TxReport {
                tx_id: parsed["tx_id"].as_u64().unwrap_or(0),
                datoms_written: parsed["datoms_written"].as_u64().unwrap_or(0) as usize,
            })
        } else {
            Err(EngineError::Transaction(body_text))
        }
    }

    async fn server_status(&self) -> Result<ServerStatusReport, EngineError> {
        Err(EngineError::Internal(
            "Remote status endpoints not exposed by MesoDB Server.".to_string(),
        ))
    }

    async fn trigger_compaction(&self) -> Result<CompactionReport, EngineError> {
        Err(EngineError::Internal(
            "Remote compaction endpoints not exposed by MesoDB Server.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_fact_serialization() {
        let fact = Fact {
            e: 42,
            ident: ":user/name".to_string(),
            v: Value::String("Alice".to_string()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        };

        let json_val = RemoteEngine::build_wire_fact(&fact);
        assert_eq!(json_val["e"], 42);
        assert_eq!(json_val["ident"], ":user/name");
        assert_eq!(json_val["v"], "Alice");
        assert_eq!(json_val["op"], true);
        assert!(json_val["cas_old_v"].is_null());
    }

    #[tokio::test]
    async fn test_remote_engine_connection_error_mapping() {
        let engine = RemoteEngine::new("http://127.0.0.1:1".to_string()); // Port 1 guarantees a connection refusal

        let res = engine
            .query(
                "[:find ?e]",
                QueryOptions {
                    format: "json".to_string(),
                    as_of: None,
                },
            )
            .await;
        assert!(res.is_err());

        if let Err(EngineError::Connection(msg)) = res {
            assert!(
                !msg.is_empty(),
                "Connection error message should be populated"
            );
        } else {
            panic!("Expected EngineError::Connection");
        }
    }
}
