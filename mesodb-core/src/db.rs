// mesodb-core/src/db.rs
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable as DfMemTable;
use datafusion::prelude::*;
use std::path::Path;
use std::sync::Arc;

use crate::config::Config;
use crate::error::Result;
use crate::parser::parse_query;
use crate::planner::QueryPlanner;
use crate::schema::SchemaMap;
use crate::transactor::{Fact, Transactor, TxReceipt};

pub struct MesoDB {
    pub transactor: Transactor,
    pub ctx: SessionContext,
}

impl MesoDB {
    pub fn open<P: AsRef<Path>>(path: P, schema: SchemaMap) -> Result<Self> {
        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel(32);
        let data_dir = path.as_ref().parent().unwrap().to_path_buf();

        // Start the background persistence worker
        tokio::spawn(async move {
            let compactor = crate::storage::BackgroundCompactor::new(data_dir);
            let mut batch_count = 0;
            while let Some(batch) = flush_rx.recv().await {
                let _ = compactor.flush_to_parquet(batch, batch_count);
                batch_count += 1;
            }
        });

        let transactor = Transactor::new(path, schema, flush_tx)?;
        let ctx = SessionContext::new();

        Ok(Self { transactor, ctx })
    }

    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        schema: SchemaMap,
        config: Config,
    ) -> Result<Self> {
        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel(32);
        let data_dir = path.as_ref().parent().unwrap().to_path_buf();

        tokio::spawn(async move {
            let compactor = crate::storage::BackgroundCompactor::new(data_dir);
            let mut batch_count = 0;
            while let Some(batch) = flush_rx.recv().await {
                let _ = compactor.flush_to_parquet(batch, batch_count);
                batch_count += 1;
            }
        });

        // Initialize transactor with the config
        let mut transactor = Transactor::new(path, schema, flush_tx)?;
        transactor.config = config;

        let ctx = SessionContext::new();
        Ok(Self { transactor, ctx })
    }

    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReceipt> {
        self.transactor.transact(facts)
    }

    pub async fn query(&mut self, query_str: &str) -> Result<Vec<RecordBatch>> {
        let ast = parse_query(query_str)?;

        // 1. Snapshot everything currently in RAM [cite: 291]
        let mut ram_batches = Vec::new();
        {
            let history = self.transactor.frozen_history.read().unwrap();
            ram_batches.extend(history.clone());
        }

        if self.transactor.active_memtable.row_count() > 0 {
            let active_batch = self.transactor.active_memtable.finish()?;
            ram_batches.push(active_batch);
            // Re-initialize for future transactions [cite: 232, 234]
            self.transactor.active_memtable = crate::memtable::MemTable::new(
                self.transactor.config.storage.memtable_initial_capacity,
            );
        }

        // 2. Clear old registration to prepare for the Unified View
        if self.ctx.table_exist("datoms")? {
            self.ctx.deregister_table("datoms")?;
        }

        // 3. Register the Disk-based Parquet files
        // Use the data_dir we just implemented
        let data_dir = self.transactor.wal.data_dir();
        let table_path = data_dir.to_string_lossy().to_string();

        // Get the shared schema from the MemTable
        let schema = self.transactor.active_memtable.schema();

        // Seed the listing table with the explicit schema
        let options = datafusion::prelude::ParquetReadOptions::default().schema(&schema);

        // Ensure the directory exists so register_parquet doesn't complain
        std::fs::create_dir_all(&data_dir)?;

        // Registering a local directory as a Parquet table
        self.ctx
            .register_parquet("parquet_datoms", &table_path, options)
            .await?;

        // 4. Create the Unified logical table (Union of RAM + Disk)
        // If we have RAM data, we union it with the parquet table
        let df = if !ram_batches.is_empty() {
            let schema = ram_batches[0].schema();
            let ram_provider = DfMemTable::try_new(schema, vec![ram_batches])?;
            self.ctx
                .register_table("ram_datoms", Arc::new(ram_provider))?;

            // Union SQL: Combines disk and memory into one logical "datoms" view
            self.ctx
                .sql("SELECT * FROM parquet_datoms UNION ALL SELECT * FROM ram_datoms")
                .await?
        } else {
            self.ctx.table("parquet_datoms").await?
        };

        self.ctx.register_table("datoms", df.into_view())?;

        let planner = QueryPlanner::new(&self.ctx, &self.transactor.schema, "datoms");
        let final_df = planner.plan(&ast).await?;

        Ok(final_df.collect().await?)
    }

    pub fn schema(&self) -> &SchemaMap {
        &self.transactor.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ValueType;
    use crate::types::Value;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_end_to_end_query() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let mut db = MesoDB::open(temp_file.path(), schema).unwrap();
        db.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
            },
            Fact {
                e: 1,
                ident: ":user/age".into(),
                v: Value::Int64(30),
                op: true,
            },
        ])
        .unwrap();

        let query = r#"[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_high_pressure_persistence_and_unified_query() {
        use arrow::array::UInt64Array;
        use arrow::record_batch::RecordBatch;
        use std::collections::HashSet;
        use tempfile::tempdir;
        // Correcting imports based on your error logs:
        use crate::transactor::Fact;
        use crate::types::Value;

        let dir = tempdir().unwrap();
        let db_path = dir.path().join("stress_test.db");

        // 1. Setup a "Highly Volatile" configuration
        let mut config = crate::config::Config::default();
        config.storage.memtable_rotation_threshold = 3;
        config.storage.memtable_initial_capacity = 2;

        let mut schema = crate::schema::SchemaMap::new();
        schema.add_attribute(":test/id", crate::schema::ValueType::Int64, true);
        schema.add_attribute(":test/val", crate::schema::ValueType::String, false);

        // 2. Open DB and pour in data
        let mut db = MesoDB::open_with_config(&db_path, schema.clone(), config).unwrap();

        for i in 0..10 {
            db.transact(vec![Fact {
                e: i as u64,
                ident: ":test/id".into(),
                v: Value::Int64(i as i64),
                op: true,
            }])
            .unwrap();
        }

        // Give background worker time to flush
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // 3. Drop the DB to simulate a crash/restart
        drop(db);

        // 4. THE COLD BOOT RECOVERY
        let mut recovered_db = MesoDB::open(&db_path, schema).unwrap();
        let query_str = "[:find ?e :where [?e :test/id _]]";

        let results: Vec<RecordBatch> = recovered_db.query(query_str).await.unwrap();

        let mut unique_entities: HashSet<u64> = HashSet::new();

        for batch in results {
            let col_data = batch.column(0);
            if let Some(uint_col) = col_data.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..uint_col.len() {
                    unique_entities.insert(uint_col.value(i));
                }
            }
        }

        assert_eq!(
            unique_entities.len(),
            10,
            "Should see exactly 10 unique entities after recovery"
        );
    }
}
