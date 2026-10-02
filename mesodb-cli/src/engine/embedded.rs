// mesodb-cli/src/engine/embedded.rs

use async_trait::async_trait;
use std::sync::Arc;

use mesodb_core::db::{MesoDB, OutputFormat as CoreOutputFormat, QueryOptions as CoreQueryOptions};
use mesodb_core::edn::parse_transaction;

use super::{
    CompactionReport, EngineError, MesoEngine, QueryOptions, QueryResult, ServerStatusReport,
    TxReport,
};

pub struct EmbeddedEngine {
    db: Arc<MesoDB>,
}

impl EmbeddedEngine {
    pub fn new(db: Arc<MesoDB>) -> Self {
        Self { db }
    }

    fn map_core_err(e: mesodb_core::error::MesoError) -> EngineError {
        EngineError::Internal(e.to_string())
    }
}

#[async_trait]
impl MesoEngine for EmbeddedEngine {
    async fn query(
        &self,
        datalog: &str,
        options: QueryOptions,
    ) -> Result<QueryResult, EngineError> {
        let core_format = match options.format.as_str() {
            "edn" => CoreOutputFormat::Edn,
            "json" => CoreOutputFormat::Json,
            _ => CoreOutputFormat::Tabular, // Defaults to tabular for terminal
        };

        let core_opts = CoreQueryOptions {
            as_of: options.as_of,
            format: core_format.clone(),
            history: false,
            rules: None,
        };

        let raw_output = if core_format == CoreOutputFormat::Edn {
            self.db
                .query_edn_with_options(datalog, core_opts)
                .await
                .map_err(Self::map_core_err)?
        } else {
            self.db
                .query_json_with_options(datalog, core_opts)
                .await
                .map_err(Self::map_core_err)?
        };

        Ok(QueryResult { raw_output })
    }

    async fn transact(&self, edn_facts: &str) -> Result<TxReport, EngineError> {
        let facts =
            parse_transaction(edn_facts).map_err(|e| EngineError::Transaction(e.to_string()))?;

        let report = self.db.transact(facts).await.map_err(Self::map_core_err)?;

        Ok(TxReport {
            tx_id: report.tx_id,
            datoms_written: report.datoms_written,
        })
    }

    async fn server_status(&self) -> Result<ServerStatusReport, EngineError> {
        // MesoDB core manages background states opaquely, so we return a static status for embedded
        Ok(ServerStatusReport {
            status: "Embedded engine online. Compaction runs automatically in background."
                .to_string(),
        })
    }

    async fn trigger_compaction(&self) -> Result<CompactionReport, EngineError> {
        Err(EngineError::Internal(
            "Manual compaction trigger is not supported in the current engine version.".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesodb_core::config::Config;
    use mesodb_core::schema::SchemaMap;

    #[tokio::test]
    async fn test_embedded_engine_transact_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDB::open(
                dir.path().join("test.db"),
                SchemaMap::new(),
                Config::default(),
            )
            .unwrap(),
        );
        let engine = EmbeddedEngine::new(db);

        // Test Transaction
        let tx_res = engine.transact(r#"[[:db/add 1 :user/name "Alice"]]"#).await;
        assert!(tx_res.is_ok());

        // Test Query
        let opts = QueryOptions {
            format: "json".to_string(),
            as_of: None,
        };
        let q_res = engine
            .query(r#"[:find ?n :where [1 :user/name ?n]]"#, opts)
            .await
            .unwrap();
        assert!(q_res.raw_output.contains("Alice"));
    }
}
