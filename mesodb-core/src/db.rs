// mesodb-core/src/db.rs

use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable as DfMemTable;
use datafusion::prelude::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;
use crate::error::MesoError;
use crate::parser::parse_query;
use crate::planner::QueryPlanner;
use crate::schema::SchemaMap;
use crate::storage::BackgroundCompactor;
use crate::transactor::{Fact, Transactor, TxReport};
use crate::types::Result;

/// A lightweight, lock-free snapshot of the database at a specific point in time.
#[derive(Clone)]
pub struct WorldView {
    /// A thread-safe map of transactions currently sitting in RAM. Keyed by TxId.
    pub ram_batches: Arc<BTreeMap<u64, RecordBatch>>,
    /// The physical location of the disk storage (Parquet files).
    pub data_dir: PathBuf,
    /// A snapshot of the schema at this moment in time.
    pub schema: Arc<SchemaMap>,
}

pub struct MesoDB {
    /// The transactor is the single writer, guarded by a Tokio Mutex.
    transactor: Mutex<Transactor>,
    /// The WorldView is an atomic pointer. Swapping it takes nanoseconds.
    world_view: Arc<RwLock<Arc<WorldView>>>,
    /// The channel to send newly minted memory batches to the background compactor.
    flush_tx: mpsc::Sender<(u64, RecordBatch)>,
}

impl MesoDB {
    pub fn open<P: AsRef<Path>>(path: P, schema: SchemaMap, config: Config) -> Result<Self> {
        let data_dir = path.as_ref().parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let transactor = Transactor::new(path, schema.clone(), config.clone())?;

        // We always keep a "TxId 0" empty batch in RAM so DataFusion always knows the schema,
        // even if no data has been inserted or everything has been flushed to disk.
        let empty_batch = crate::memtable::MemTable::new(0).finish()?;
        let mut initial_ram = BTreeMap::new();
        initial_ram.insert(0, empty_batch);

        let world_view = Arc::new(RwLock::new(Arc::new(WorldView {
            ram_batches: Arc::new(initial_ram),
            data_dir: data_dir.clone(),
            schema: Arc::new(schema),
        })));

        // Spawn the asynchronous background compactor
        let (flush_tx, flush_rx) = mpsc::channel(config.compactor.backpressure_threshold);
        Self::spawn_background_worker(data_dir, flush_rx, world_view.clone());

        Ok(Self {
            transactor: Mutex::new(transactor),
            world_view,
            flush_tx,
        })
    }

    /// The Background Worker: Listens for RAM batches, writes to disk, and drops them from RAM.
    fn spawn_background_worker(
        data_dir: PathBuf,
        mut flush_rx: mpsc::Receiver<(u64, RecordBatch)>,
        world_view: Arc<RwLock<Arc<WorldView>>>,
    ) {
        tokio::spawn(async move {
            let compactor = BackgroundCompactor::new(data_dir);

            while let Some((tx_id, batch)) = flush_rx.recv().await {
                // 1. Write the batch to a Parquet file
                if let Err(e) = compactor.flush_to_parquet(batch, tx_id) {
                    eprintln!("Failed to flush Tx {} to Parquet: {:?}", tx_id, e);
                    continue; // Keep it in RAM if disk fails
                }

                // 2. Safely remove it from the RAM WorldView so we don't leak memory
                let current_view = world_view.read().unwrap().as_ref().clone();
                let mut new_ram = current_view.ram_batches.as_ref().clone();
                new_ram.remove(&tx_id);

                let new_view = Arc::new(WorldView {
                    ram_batches: Arc::new(new_ram),
                    data_dir: current_view.data_dir,
                    schema: current_view.schema,
                });

                // Atomically update the pointer
                let mut writer = world_view.write().unwrap();
                *writer = new_view;
            }
        });
    }

    /// Takes an IMMUTABLE reference (&self).
    pub async fn transact(&self, facts: Vec<Fact>) -> Result<TxReport> {
        let mut tx = self.transactor.lock().await;
        let report = tx.transact(facts)?;

        if report.datoms_written > 0 {
            // 1. Instantly publish the new batch to RAM for immediate reading
            let current_view = self.world_view.read().unwrap().as_ref().clone();
            let mut new_ram = current_view.ram_batches.as_ref().clone();
            new_ram.insert(report.tx_id, report.batch.clone());

            let new_view = Arc::new(WorldView {
                ram_batches: Arc::new(new_ram),
                data_dir: current_view.data_dir,
                schema: Arc::new(tx.schema.clone()),
            });

            {
                let mut writer = self.world_view.write().unwrap();
                *writer = new_view;
            }

            // 2. Queue the batch for background disk flushing
            // If the channel is full (disk is too slow), this will asynchronously wait,
            // naturally applying backpressure to the writer without crashing the system!
            if self
                .flush_tx
                .send((report.tx_id, report.batch.clone()))
                .await
                .is_err()
            {
                eprintln!("Warning: Background flusher disconnected.");
            }
        }

        Ok(report)
    }

    /// Pure read-only query. Completely lock-free.
    pub async fn query(&self, query_str: &str) -> Result<Vec<RecordBatch>> {
        let ast = parse_query(query_str)?;

        // 1. Take a nanosecond snapshot of the world
        let view = { self.world_view.read().unwrap().clone() };

        let ctx = SessionContext::new();

        // 2. Register Disk Data (Parquet)
        let schema = view.ram_batches.get(&0).unwrap().schema();
        let options = datafusion::prelude::ParquetReadOptions::default().schema(&schema);
        let table_path = view.data_dir.to_string_lossy().to_string();

        ctx.register_parquet("parquet_datoms", &table_path, options)
            .await
            .map_err(MesoError::DataFusion)?;

        // 3. Register RAM Data
        let ram_vec: Vec<RecordBatch> = view.ram_batches.values().cloned().collect();
        let ram_provider = DfMemTable::try_new(schema, vec![ram_vec])?;

        ctx.register_table("ram_datoms", Arc::new(ram_provider))
            .map_err(MesoError::DataFusion)?;

        // 4. Create the Unified View
        let df = ctx
            .sql("SELECT * FROM parquet_datoms UNION ALL SELECT * FROM ram_datoms")
            .await
            .map_err(MesoError::DataFusion)?;

        ctx.register_table("datoms", df.into_view())
            .map_err(MesoError::DataFusion)?;

        // 5. Plan & Execute
        let planner = QueryPlanner::new(&ctx, view.schema.as_ref(), "datoms");
        let final_df: datafusion::dataframe::DataFrame = planner.plan(&ast).await?;

        Ok(final_df.collect().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ValueType;
    use crate::types::Value;

    #[tokio::test]
    async fn test_end_to_end_mvcc_query_with_background_flush() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

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

        // 1. Query immediately while it's in RAM
        let query = r#"[:find ?n ?a :where [?e :user/name ?n] [?e :user/age ?a]]"#;
        let results = db.query(query).await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "Should see exactly one result row for Alice");

        // 2. Wait for the background worker to flush to Parquet and remove it from RAM
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // 3. Query again (it should seamlessly read from the new Parquet file!)
        let results_disk = db.query(query).await.unwrap();
        let total_rows_disk: usize = results_disk.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows_disk, 1,
            "Should STILL see Alice after background flush"
        );
    }
}
