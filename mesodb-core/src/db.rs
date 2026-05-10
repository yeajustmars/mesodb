// mesodb-core/src/db.rs

use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable as DfMemTable;
use datafusion::prelude::*;
use std::collections::BTreeMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;
use crate::parser::parse_query;
use crate::planner::QueryPlanner;
use crate::schema::SchemaMap;
use crate::storage::BackgroundCompactor;
use crate::transactor::{Fact, Transactor, TxReport};
use crate::types::Result;

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
        create_dir_all(&data_dir)?;

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
    /// Standard transaction using the current system time.
    pub async fn transact(&self, facts: Vec<Fact>) -> Result<TxReport> {
        self.execute(facts, None).await
    }

    /// Transaction that overrides the system time with a custom timestamp.
    /// Crucial for historical migrations, bitemporal testing, and syncing.
    pub async fn transact_at(&self, facts: Vec<Fact>, custom_now: i64) -> Result<TxReport> {
        self.execute(facts, Some(custom_now)).await
    }

    /// DRY helper that handles the lock, the transaction, and the lock-free WorldView pointer swap.
    async fn execute(&self, facts: Vec<Fact>, custom_now: Option<i64>) -> Result<TxReport> {
        let mut tx = self.transactor.lock().await;

        let report = match custom_now {
            Some(t) => tx.transact_at(facts, t)?,
            None => tx.transact(facts)?,
        };

        if report.datoms_written > 0 {
            // 1. Instantly publish the new batch to RAM for immediate reading
            let current_view = self.world_view.read().unwrap().as_ref().clone();
            let mut new_ram = current_view.ram_batches.as_ref().clone();
            new_ram.insert(report.tx_id, report.batch.clone());

            let new_view = Arc::new(WorldView {
                ram_batches: Arc::new(new_ram),
                data_dir: current_view.data_dir.clone(), // Add .clone() to be safe
                schema: Arc::new(tx.schema.clone()),
            });

            {
                let mut writer = self.world_view.write().unwrap();
                *writer = new_view;
            }

            // 2. Queue the batch for background disk flushing
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

    /// Pure read-only query using default options (current time). Completely lock-free.
    pub async fn query(&self, query_str: &str) -> Result<Vec<RecordBatch>> {
        self.query_with_options(query_str, QueryOptions::default())
            .await
    }

    /// Read-only query with global execution options (e.g., time travel).
    pub async fn query_with_options(
        &self,
        query_str: &str,
        options: QueryOptions,
    ) -> Result<Vec<RecordBatch>> {
        let ast = parse_query(query_str)?;

        // 1. Take a snapshot of the world
        let view = { self.world_view.read().unwrap().clone() };
        let ctx = SessionContext::new();

        // 2. Register Disk Data (Parquet)
        let schema = view.ram_batches.get(&0).unwrap().schema();
        let pq_options = ParquetReadOptions::default().schema(&schema);
        let table_path = view.data_dir.to_string_lossy().to_string();
        ctx.register_parquet("parquet_datoms", &table_path, pq_options)
            .await?;

        // 3. Register RAM Data
        let ram_vec: Vec<RecordBatch> = view.ram_batches.values().cloned().collect();
        let ram_provider = DfMemTable::try_new(schema, vec![ram_vec])?;
        ctx.register_table("ram_datoms", Arc::new(ram_provider))?;

        // 4. Create the Raw Unified View
        let df = ctx
            .sql("SELECT * FROM parquet_datoms UNION ALL SELECT * FROM ram_datoms")
            .await?;
        ctx.register_table("raw_datoms", df.into_view())?;

        // 5. THE BITEMPORAL VIEW
        // If an `as_of` option is provided, we physically ignore any facts asserted AFTER that time
        // before we even calculate the bitemporal overlap.
        let time_filter = match options.as_of {
            Some(t) => format!("WHERE CAST(valid_from AS BIGINT) <= {}", t),
            None => "".to_string(),
        };

        // We also filter the resolved view to ensure we only see facts that were valid AT the target time
        let resolved_time_filter = match options.as_of {
            Some(t) => format!(
                "AND CAST(valid_from AS BIGINT) <= {t} AND CAST(COALESCE(next_from, valid_to) AS BIGINT) > {t}"
            ),
            None => "".to_string(),
        };

        let resolved_sql = format!(
            r#"
            WITH raw_filtered AS (
                SELECT * FROM raw_datoms {}
            ),
            bounds AS (
                SELECT
                    *,
                    LEAD(valid_from) OVER (
                        PARTITION BY e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid
                        ORDER BY valid_from ASC, t ASC
                    ) as next_from
                FROM raw_filtered
            )
            SELECT
                e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from,
                COALESCE(next_from, valid_to) as valid_to
            FROM bounds
            WHERE op = true {}
        "#,
            time_filter, resolved_time_filter
        );

        let resolved_df = ctx.sql(&resolved_sql).await?;
        ctx.register_table("resolved_datoms", resolved_df.into_view())?;

        // 6. Plan & Execute against the perfectly resolved intervals
        let planner = QueryPlanner::new(&ctx, view.schema.as_ref(), "resolved_datoms");
        let final_df: DataFrame = planner.plan(&ast).await?;

        Ok(final_df.collect().await?)
    }
}

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

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Tabular,
    Json,
    Edn,
}

/// Global options applied to the entire query execution.
#[derive(Debug, Default, Clone)]
pub struct QueryOptions {
    /// If provided, rewinds the entire database to this timestamp before querying.
    pub as_of: Option<i64>,
    /// The serialization format for the output, strictly enforced.
    pub format: OutputFormat,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

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

    #[tokio::test]
    async fn test_engine_bitemporal_time_travel() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("time_travel.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        let db = MesoDB::open(db_path, schema, Config::default()).unwrap();

        // T = 100: Assert Alice
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
            }],
            100,
        )
        .await
        .unwrap();

        // T = 200: Overwrite with Alice-Revised
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice-Revised".into()),
                op: true,
            }],
            200,
        )
        .await
        .unwrap();

        // T = 300: Overwrite with Bob
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Bob".into()),
                op: true,
            }],
            300,
        )
        .await
        .unwrap();

        // --- QUERY 1: Travel back to T = 150 ---
        // At this point, "Alice" should be the only valid name
        let q_150 = r#"[:find ?n :where [1 :user/name ?n {:at 150}]]"#;
        let res_150 = db.query(q_150).await.unwrap();

        let col_150 = res_150[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col_150.len(), 1, "Should find exactly 1 record at T=150");
        assert_eq!(col_150.value(0), "Alice");

        // --- QUERY 2: Travel to T = 250 ---
        // At this point, "Alice-Revised" should be valid
        let q_250 = r#"[:find ?n :where [1 :user/name ?n {:at 250}]]"#;
        let res_250 = db.query(q_250).await.unwrap();

        let col_250 = res_250[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col_250.len(), 1, "Should find exactly 1 record at T=250");
        assert_eq!(col_250.value(0), "Alice-Revised");

        // --- QUERY 3: Query the Present (No time filter) ---
        // Should default to the newest assertion (Bob)
        let q_now = r#"[:find ?n :where [1 :user/name ?n]]"#;
        let res_now = db.query(q_now).await.unwrap();

        let col_now = res_now[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            col_now.len(),
            1,
            "Should find exactly 1 record for present time"
        );
        assert_eq!(col_now.value(0), "Bob");
    }

    #[tokio::test]
    async fn test_global_query_options_time_travel() {
        let dir = tempfile::tempdir().unwrap();
        let db = MesoDB::open(
            dir.path().join("global_time.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        // Setup schema
        db.transact(vec![Fact {
            e: 1,
            ident: ":sys/init".into(),
            v: Value::Boolean(true),
            op: true,
        }])
        .await
        .unwrap();

        // T = 100: Assert Alice
        db.transact_at(
            vec![Fact {
                e: 10,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
            }],
            100,
        )
        .await
        .unwrap();

        // T = 200: Overwrite with Alice-Revised
        db.transact_at(
            vec![Fact {
                e: 10,
                ident: ":user/name".into(),
                v: Value::String("Alice-Revised".into()),
                op: true,
            }],
            200,
        )
        .await
        .unwrap();

        // Standard Datalog Query (No inline options)
        let query = r#"[:find ?n :where [10 :user/name ?n]]"#;

        // 1. Query with global `as_of` = 150
        let opts_150 = QueryOptions {
            as_of: Some(150),
            format: OutputFormat::Tabular,
        };
        let res_150 = db.query_with_options(query, opts_150).await.unwrap();
        let col_150 = res_150[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            col_150.value(0),
            "Alice",
            "Global option should rewind to T=150"
        );

        // 2. Query with default options (Current Time)
        let res_now = db.query(query).await.unwrap();
        let col_now = res_now[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            col_now.value(0),
            "Alice-Revised",
            "Default should see latest state"
        );
    }

    #[tokio::test]
    async fn test_inline_datom_time_travel_superset() {
        let dir = tempfile::tempdir().unwrap();
        let db = MesoDB::open(
            dir.path().join("inline_time.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        // T = 100: Alice lives in New York
        db.transact_at(
            vec![
                Fact {
                    e: 1,
                    ident: ":user/name".into(),
                    v: Value::String("Alice".into()),
                    op: true,
                },
                Fact {
                    e: 1,
                    ident: ":user/city".into(),
                    v: Value::String("New York".into()),
                    op: true,
                },
            ],
            100,
        )
        .await
        .unwrap();

        // T = 200: Alice moves to London
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/city".into(),
                v: Value::String("London".into()),
                op: true,
            }],
            200,
        )
        .await
        .unwrap();

        // SUPERSET QUERY: What is Alice's CURRENT city, and what was her city AT T=150?
        // Datomic physically cannot do this in a single query.
        let query = r#"
            [:find ?current_city ?past_city
             :where
                [1 :user/city ?current_city]
                [1 :user/city ?past_city {:at 150}]
            ]
        "#;

        let results = db.query(query).await.unwrap();

        let current_city_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let past_city_col = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(
            current_city_col.value(0),
            "London",
            "Should find the present city"
        );
        assert_eq!(
            past_city_col.value(0),
            "New York",
            "Should find the historical city in the same query!"
        );
    }
}
