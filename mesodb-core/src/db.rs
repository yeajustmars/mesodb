// mesodb-core/src/db.rs

use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable as DfMemTable;
use datafusion::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

use crate::config::Config;
use crate::error::MesoError;
use crate::parser::parse_query;
use crate::planner::QueryPlanner;
use crate::schema::SchemaMap;
use crate::transactor::{Fact, Transactor, TxReport};
use crate::types::Result;

/// A lightweight, lock-free snapshot of the database at a specific point in time.
#[derive(Clone)]
pub struct WorldView {
    /// A thread-safe pointer to all immutable memory batches.
    pub ram_batches: Arc<Vec<RecordBatch>>,
    /// The physical location of the disk storage (for Parquet later).
    pub data_dir: PathBuf,
    /// A snapshot of the schema at this moment in time.
    pub schema: Arc<SchemaMap>,
}

pub struct MesoDB {
    /// The transactor is placed behind a Tokio Mutex because it is the
    /// single writer. Multiple threads can queue up to write.
    transactor: Mutex<Transactor>,

    /// The WorldView is behind a standard RwLock. The lock is only held
    /// for nanoseconds to clone the Arc or swap the pointer.
    world_view: RwLock<Arc<WorldView>>,
}

impl MesoDB {
    pub fn open<P: AsRef<Path>>(path: P, schema: SchemaMap, config: Config) -> Result<Self> {
        let data_dir = path.as_ref().parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let transactor = Transactor::new(path, schema.clone(), config)?;

        // Seed the WorldView with an empty batch so the query engine always knows the schema
        let empty_batch = crate::memtable::MemTable::new(0).finish()?;

        let world_view = Arc::new(WorldView {
            ram_batches: Arc::new(vec![empty_batch]),
            data_dir,
            schema: Arc::new(schema),
        });

        Ok(Self {
            transactor: Mutex::new(transactor),
            world_view: RwLock::new(world_view),
        })
    }

    /// Takes an IMMUTABLE reference (&self). This means writes don't violate Rust's borrow
    /// rules when concurrent reads are happening.
    pub async fn transact(&self, facts: Vec<Fact>) -> Result<TxReport> {
        // 1. Acquire the write lock (only blocks other writers, NEVER readers)
        let mut tx = self.transactor.lock().await;

        // 2. Do the heavy lifting (Validation, WAL, MemTable building)
        let report = tx.transact(facts)?;

        // 3. Publish the new WorldView Instantly
        if report.datoms_written > 0 {
            // Read the current view
            let current_view = self.world_view.read().unwrap().as_ref().clone();

            // Clone the vector of pointers (cheap) and push the new batch
            let mut new_batches = current_view.ram_batches.as_ref().clone();
            new_batches.push(report.batch.clone());

            // Build the new snapshot
            let new_view = Arc::new(WorldView {
                ram_batches: Arc::new(new_batches),
                data_dir: current_view.data_dir,
                // The Transactor might have added JIT attributes, so grab its latest schema
                schema: Arc::new(tx.schema.clone()),
            });

            // Atomically swap the pointer. Takes nanoseconds.
            let mut writer = self.world_view.write().unwrap();
            *writer = new_view;
        }

        Ok(report)
    }

    /// Pure read-only query. Can be called by 10,000 threads simultaneously.
    pub async fn query(&self, query_str: &str) -> Result<Vec<RecordBatch>> {
        let ast = parse_query(query_str)?;

        // 1. Take a lock-free snapshot of the world (Nanosecond operation)
        let view = { self.world_view.read().unwrap().clone() };

        // 2. Setup DataFusion execution context for this specific query
        let ctx = SessionContext::new();

        // 3. Register the RAM batches to be queried
        // Because we seeded an empty batch in `open`, this is never empty and
        // DataFusion always gets the exact, correct Arrow Schema.
        let schema = view.ram_batches[0].schema();
        let ram_provider = DfMemTable::try_new(schema, vec![view.ram_batches.as_ref().clone()])
            .map_err(MesoError::DataFusion)?;

        ctx.register_table("datoms", Arc::new(ram_provider))
            .map_err(MesoError::DataFusion)?;

        // 4. Plan & Execute
        let planner = QueryPlanner::new(&ctx, view.schema.as_ref(), "datoms");
        let final_df = planner.plan(&ast).await?;

        Ok(final_df.collect().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ValueType;
    use crate::types::Value;
    use arrow::array::Array;

    #[tokio::test]
    async fn test_end_to_end_mvcc_query() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        // Notice db doesn't need to be `mut` anymore!
        let db = MesoDB::open(db_path, schema, Config::default()).unwrap();

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
        .await
        .unwrap();

        let query = r#"[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]"#;
        let results = db.query(query).await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "Should see exactly one result row for Alice");

        let name_found = results.iter().any(|batch| {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..col.len()).any(|i| col.value(i) == "Alice")
        });
        assert!(name_found);
    }

    #[tokio::test]
    async fn test_concurrent_reads_while_writing() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":sys/ping", ValueType::Int64, false);

        // Wrap DB in an Arc so we can share it across threads
        let db = Arc::new(MesoDB::open(db_path, schema, Config::default()).unwrap());

        // Spawn a background writer
        let db_writer = db.clone();
        let writer_handle = tokio::spawn(async move {
            for i in 1..=50 {
                db_writer
                    .transact(vec![Fact {
                        e: i,
                        ident: ":sys/ping".into(),
                        v: Value::Int64(i as i64),
                        op: true,
                    }])
                    .await
                    .unwrap();
                // Yield briefly to let readers slip in
                tokio::task::yield_now().await;
            }
        });

        // Spawn concurrent readers
        let mut reader_handles = Vec::new();
        for _ in 0..10 {
            let db_reader = db.clone();
            reader_handles.push(tokio::spawn(async move {
                let query = r#"[:find ?v :where [?e :sys/ping ?v]]"#;
                // Read a bunch of times while writing is happening
                for _ in 0..20 {
                    let results = db_reader.query(query).await.unwrap();
                    // Just verify the query executes successfully lock-free
                    let _total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
                    tokio::task::yield_now().await;
                }
            }));
        }

        writer_handle.await.unwrap();
        for handle in reader_handles {
            handle.await.unwrap();
        }

        // Final sanity check
        let final_results = db
            .query(r#"[:find ?v :where [?e :sys/ping ?v]]"#)
            .await
            .unwrap();
        let total_rows: usize = final_results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 50,
            "All 50 writes should be visible to final reader"
        );
    }
}
