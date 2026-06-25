// mesodb-core/src/db.rs

use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::collections::BTreeMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;
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

        // Create the parquet sub-directory synchronously right here at startup
        create_dir_all(data_dir.join("parquet"))?;

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
            timeline: Arc::new(transactor.timeline.clone()),
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
                // Write the batch to a Parquet file
                if let Err(e) = compactor.flush_to_parquet(batch, tx_id) {
                    eprintln!("Failed to flush Tx {} to Parquet: {:?}", tx_id, e);
                    continue; // Keep it in RAM if disk fails
                }

                // Safely remove it from the RAM WorldView so we don't leak memory
                let current_view = world_view.read().unwrap().as_ref().clone();
                let mut new_ram = current_view.ram_batches.as_ref().clone();
                new_ram.remove(&tx_id);

                let new_view = Arc::new(WorldView {
                    ram_batches: Arc::new(new_ram),
                    data_dir: current_view.data_dir.clone(),
                    // FIX: Clone these safely from the current_view
                    schema: current_view.schema.clone(),
                    timeline: current_view.timeline.clone(),
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

    /// Exposes explicit schema definition to the outside world.
    /// Batches multiple attribute creations into a single locked transaction.
    pub async fn transact_schema(
        &self,
        attributes: Vec<AttributeDefinition>,
    ) -> Result<Vec<Arc<crate::schema::Attribute>>> {
        let mut tx = self.transactor.lock().await;
        let mut added_attrs = Vec::with_capacity(attributes.len());

        for attr in attributes {
            let added = tx.transact_schema(&attr.ident, attr.value_type, attr.is_unique)?;
            added_attrs.push(added);
        }

        // If we actually added anything, we must publish the new schema to RAM
        // so that read-queries can instantly recognize the new attributes.
        if !added_attrs.is_empty() {
            let current_view = self.world_view.read().unwrap().as_ref().clone();

            let new_view = Arc::new(WorldView {
                ram_batches: current_view.ram_batches.clone(), // Data is unchanged
                data_dir: current_view.data_dir.clone(),
                schema: Arc::new(tx.schema.clone()),
                timeline: Arc::new(tx.timeline.clone()),
            });

            {
                let mut writer = self.world_view.write().unwrap();
                *writer = new_view;
            }
        }

        Ok(added_attrs)
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
                data_dir: current_view.data_dir.clone(),
                schema: Arc::new(tx.schema.clone()),
                // FIX: Propagate the updated timeline safely
                timeline: Arc::new(tx.timeline.clone()),
            });
            {
                let mut writer = self.world_view.write().unwrap();
                *writer = new_view;
            }

            // --- THE COMPACTION COMPLIANCE THRESHOLD ---
            // Only queue a background disk flush when a memory batch size threshold is reached.
            // This stops high-frequency iterations from overwhelming the OS file system stack.
            // TODO: allow setting this value in config
            if report.batch.num_rows() >= 50_000
                && self
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

    // =====================================================================
    // STANDARD QUERY API PIPELINE
    // =====================================================================

    pub async fn query(&self, query_str: &str) -> Result<Vec<RecordBatch>> {
        self.query_with_options(query_str, QueryOptions::default())
            .await
    }

    pub async fn query_with_options(
        &self,
        query_str: &str,
        options: QueryOptions,
    ) -> Result<Vec<RecordBatch>> {
        let ast = crate::parser::parse_query(query_str)?;
        let view = { self.world_view.read().unwrap().clone() };
        let ctx = SessionContext::new();
        let ruleset = if let Some(r) = &options.rules {
            Some(crate::parser::parse_ruleset(r)?)
        } else {
            None
        };
        let arrow_schema = view.ram_batches.get(&0).unwrap().schema();
        let pq_options =
            datafusion::prelude::ParquetReadOptions::default().schema(arrow_schema.as_ref());
        let table_path = view.data_dir.to_string_lossy().to_string();

        let _ = ctx
            .register_parquet("parquet_datoms", &table_path, pq_options)
            .await;
        let mut ram_vec: Vec<RecordBatch> = view
            .ram_batches
            .values()
            .filter(|b| b.num_rows() > 0)
            .cloned()
            .collect();
        if ram_vec.is_empty() {
            ram_vec.push(arrow::record_batch::RecordBatch::new_empty(
                arrow_schema.clone(),
            ));
        }

        let ram_provider =
            datafusion::datasource::memory::MemTable::try_new(arrow_schema.clone(), vec![ram_vec])
                .unwrap();
        ctx.register_table("ram_datoms", Arc::new(ram_provider))?;

        let table_exists = ctx.table_exist("parquet_datoms").unwrap_or(false);
        if table_exists {
            let df = ctx
                .sql("SELECT * FROM parquet_datoms UNION ALL SELECT * FROM ram_datoms")
                .await?;
            ctx.register_table("raw_datoms", df.into_view())?;
        } else {
            let df = ctx.sql("SELECT * FROM ram_datoms").await?;
            ctx.register_table("raw_datoms", df.into_view())?;
        }

        let resolved_sql = if options.history {
            r#"SELECT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from, CAST(NULL AS BIGINT) as next_from, valid_to FROM raw_datoms"#.to_string()
        } else {
            let time_filter = match options.as_of {
                Some(t) => format!("WHERE CAST(valid_from AS BIGINT) <= {}", t),
                None => "".to_string(),
            };
            let resolved_time_filter = match options.as_of {
                Some(t) => format!(
                    "AND CAST(valid_from AS BIGINT) <= {t} AND CAST(COALESCE(next_from, valid_to) AS BIGINT) > {t}"
                ),
                None => "".to_string(),
            };
            format!(
                r#"
                WITH raw_filtered AS (SELECT * FROM raw_datoms {}),
                bounds AS (SELECT *, LEAD(valid_from) OVER (PARTITION BY e, a ORDER BY valid_from ASC, t ASC) as next_from FROM raw_filtered)
                SELECT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from, next_from, COALESCE(next_from, valid_to) as valid_to
                FROM bounds WHERE op = true {}
                "#,
                time_filter, resolved_time_filter
            )
        };

        let resolved_df = ctx
            .sql(&resolved_sql)
            .await
            .map_err(crate::error::MesoError::DataFusion)?;
        ctx.register_table("resolved_datoms", resolved_df.into_view())?;

        let planner = crate::planner::QueryPlanner::new(
            &ctx,
            view.timeline.as_ref(), // Updated to use timeline!
            "resolved_datoms",
            options.format.clone(),
            options.as_of,
            ruleset,
        );
        let final_df = planner.plan(&ast).await?;

        Ok(final_df.collect().await.unwrap_or_default())
    }

    pub async fn query_native(
        &self,
        query_str: &str,
    ) -> Result<Vec<std::collections::HashMap<String, crate::types::Value>>> {
        let batches = self.query(query_str).await?;
        crate::formatter::to_native(&batches)
    }

    pub async fn query_native_with_options(
        &self,
        query_str: &str,
        options: QueryOptions,
    ) -> Result<Vec<std::collections::HashMap<String, crate::types::Value>>> {
        let batches = self.query_with_options(query_str, options).await?;
        crate::formatter::to_native(&batches)
    }

    pub async fn query_json(&self, query_str: &str) -> Result<String> {
        let ast = crate::parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Json,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(crate::formatter::to_json_string(&batches, &ast.find))
    }

    pub async fn query_json_with_options(
        &self,
        query_str: &str,
        mut options: QueryOptions,
    ) -> Result<String> {
        let ast = crate::parser::parse_query(query_str)?;
        options.format = OutputFormat::Json; // Enforce JSON for this pipeline
        let batches = self.query_with_options(query_str, options).await?;
        Ok(crate::formatter::to_json_string(&batches, &ast.find))
    }

    pub async fn query_edn(&self, query_str: &str) -> Result<String> {
        let ast = crate::parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Edn,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(crate::formatter::to_edn_string(&batches, &ast.find))
    }

    // =====================================================================
    // HISTORY API PIPELINE
    // =====================================================================

    pub async fn history(&self, query_str: &str) -> Result<Vec<RecordBatch>> {
        self.query_with_options(
            query_str,
            QueryOptions {
                history: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn history_native(
        &self,
        query_str: &str,
    ) -> Result<Vec<std::collections::HashMap<String, crate::types::Value>>> {
        let batches = self.history(query_str).await?;
        crate::formatter::to_native(&batches)
    }

    pub async fn history_json(&self, query_str: &str) -> Result<String> {
        let ast = crate::parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Json,
            history: true,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(crate::formatter::to_json_string(&batches, &ast.find))
    }

    pub async fn history_edn(&self, query_str: &str) -> Result<String> {
        let ast = crate::parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Edn,
            history: true,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(crate::formatter::to_edn_string(&batches, &ast.find))
    }
}

#[derive(Clone)]
pub struct WorldView {
    pub ram_batches: Arc<BTreeMap<u64, RecordBatch>>,
    pub data_dir: PathBuf,
    pub schema: Arc<SchemaMap>,
    pub timeline: Arc<crate::schema::SchemaTimeline>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Tabular,
    Json,
    Edn,
}

#[derive(Debug, Default, Clone)]
pub struct QueryOptions {
    pub as_of: Option<i64>,
    pub format: OutputFormat,
    pub rules: Option<String>,
    pub history: bool,
}

#[derive(Debug, Clone)]
pub struct AttributeDefinition {
    pub ident: String,
    pub value_type: crate::schema::ValueType,
    pub is_unique: bool,
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
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/age".into(),
                v: Value::Int64(30),
                op: true,
                cas_old_v: None,
                valid_time: None,
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
                cas_old_v: None,
                valid_time: None,
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
                cas_old_v: None,
                valid_time: None,
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
                cas_old_v: None,
                valid_time: None,
            }],
            300,
        )
        .await
        .unwrap();

        // --- QUERY 1: Travel back to T = 150 ---
        // At this point, "Alice" should be the only valid name
        let q_flat = r#"[:find ?n :where [1 :user/name ?n]]"#;
        let res_150 = db
            .query_with_options(
                q_flat,
                QueryOptions {
                    as_of: Some(150),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let col_150 = res_150[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(col_150.len(), 1, "Should find exactly 1 record at T=150");
        assert_eq!(col_150.value(0), "Alice");

        // --- QUERY 2: Travel to T = 250 ---
        // At this point, "Alice-Revised" should be valid
        let res_250 = db
            .query_with_options(
                q_flat,
                QueryOptions {
                    as_of: Some(250),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

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
            cas_old_v: None,
            valid_time: None,
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
                cas_old_v: None,
                valid_time: None,
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
                cas_old_v: None,
                valid_time: None,
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
            rules: None,
            history: false,
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
    async fn test_pull_json_and_edn_formats() {
        let dir = tempfile::tempdir().unwrap();
        let db = MesoDB::open(
            dir.path().join("pull.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        db.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/address".into(),
                v: Value::Ref(2),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":address/city".into(),
                v: Value::String("New York".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        let query = r#"[:find (pull ?e [* {:user/address [:address/city]}]) :where [?e :user/name "Alice"]]"#;

        // 1. Test JSON Format
        let opts_json = QueryOptions {
            format: OutputFormat::Json,
            as_of: None,
            rules: None,
            history: false,
        };
        let res_json = db.query_with_options(query, opts_json).await.unwrap();
        let json_str = res_json[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0);

        // Keys should be stripped of colons
        assert!(json_str.contains(r#""user/name":"Alice""#));
        assert!(json_str.contains(r#""user/address":{"address/city":"New York"}"#));

        // 2. Test EDN Format
        let opts_edn = QueryOptions {
            format: OutputFormat::Edn,
            as_of: None,
            rules: None,
            history: false,
        };
        let res_edn = db.query_with_options(query, opts_edn).await.unwrap();
        let edn_str = res_edn[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0);

        // Keys should retain standard Datomic keyword syntax
        assert!(edn_str.contains(r#":user/name "Alice""#));
        assert!(edn_str.contains(r#":user/address {:address/city "New York"}"#));
    }

    #[tokio::test]
    async fn test_aggregations() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":order/user", ValueType::Ref, false);
        schema.add_attribute(":order/total", ValueType::Int64, false);

        let db = MesoDB::open(dir.path().join("aggr.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            // User 1 has two orders totaling 150
            Fact {
                e: 101,
                ident: ":order/user".into(),
                v: Value::Ref(1),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 101,
                ident: ":order/total".into(),
                v: Value::Int64(50),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 102,
                ident: ":order/user".into(),
                v: Value::Ref(1),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 102,
                ident: ":order/total".into(),
                v: Value::Int64(100),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            // User 2 has one order totaling 75
            Fact {
                e: 103,
                ident: ":order/user".into(),
                v: Value::Ref(2),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 103,
                ident: ":order/total".into(),
                v: Value::Int64(75),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // Query: For each user, what is their total order count, and the sum of their totals?
        let query = r#"[:find ?user (count ?order) (sum ?total)
                        :where [?order :order/user ?user]
                               [?order :order/total ?total]]"#;

        let results = db.query(query).await.unwrap();
        let batch = &results[0];

        // We expect two rows (one for User 1, one for User 2)
        assert_eq!(batch.num_rows(), 2);

        let user_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let sum_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        // Find the index for User 1
        let user_1_idx = if user_col.value(0) == 1 { 0 } else { 1 };
        assert_eq!(count_col.value(user_1_idx), 2);
        assert_eq!(sum_col.value(user_1_idx), 150);
    }

    #[tokio::test]
    async fn test_recursive_datalog_rules() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":person/parent", crate::schema::ValueType::Ref, false);
        schema.add_attribute(":person/name", crate::schema::ValueType::String, false);

        let db = MesoDB::open(dir.path().join("rules.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            // 1 (Alice) is parent of 2 (Bob)
            Fact {
                e: 1,
                ident: ":person/name".into(),
                v: Value::String("Alice (Grandparent)".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":person/parent".into(),
                v: Value::Ref(1),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":person/name".into(),
                v: Value::String("Bob (Parent)".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            // 2 (Bob) is parent of 3 (Charlie)
            Fact {
                e: 3,
                ident: ":person/parent".into(),
                v: Value::Ref(2),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":person/name".into(),
                v: Value::String("Charlie (Child)".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // The Ruleset: Defines both a direct parent and a recursive ancestor
        let rules = r#"
        [
            [(ancestor ?child ?parent)
             [?child :person/parent ?parent]]

            [(ancestor ?child ?ancestor)
             [?child :person/parent ?parent]
             (ancestor ?parent ?ancestor)]
        ]
        "#;

        // The Query: "Find the names of ALL ancestors for Charlie (Entity 3)"
        let query = r#"[:find ?ancestor_name
                        :where
                           (ancestor 3 ?a)
                           [?a :person/name ?ancestor_name]]"#;

        let opts = QueryOptions {
            rules: Some(rules.into()),
            ..Default::default()
        };
        let results = db.query_with_options(query, opts).await.unwrap();

        // FIX: DataFusion's UNION ALL emits results across multiple RecordBatches!
        // We must sum the rows across all returned batches to get the true total.
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        // It should recursively find BOTH Bob and Alice!
        assert_eq!(total_rows, 2);
    }

    // =====================================================================
    // SUITE 1: NATIVE RETURN TYPES & ZERO-COPY SERIALIZATION
    // =====================================================================

    async fn setup_api_db() -> MesoDB {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);
        schema.add_attribute(":user/active", ValueType::Boolean, false);
        schema.add_attribute(":user/address", ValueType::Ref, false);
        schema.add_attribute(":address/city", ValueType::String, false);

        let db = MesoDB::open(dir.path().join("api.db"), schema, Config::default()).unwrap();
        db.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/age".into(),
                v: Value::Int64(30),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/active".into(),
                v: Value::Boolean(true),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/address".into(),
                v: Value::Ref(2),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":address/city".into(),
                v: Value::String("New York".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":user/name".into(),
                v: Value::String("Bob".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":user/age".into(),
                v: Value::Int64(40),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn test_api_query_native_types() {
        let db = setup_api_db().await;
        let query =
            r#"[:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age] [(> ?age 35)]]"#;

        let results = db.query_native(query).await.unwrap();
        assert_eq!(results.len(), 1);

        // Ensure we get native Rust enum values back
        assert_eq!(results[0].get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(results[0].get("age"), Some(&Value::Int64(40)));
    }

    #[tokio::test]
    async fn test_api_query_json_flat() {
        let db = setup_api_db().await;
        let query =
            r#"[:find ?name ?active :where [?e :user/name ?name] [?e :user/active ?active]]"#;

        let json = db.query_json(query).await.unwrap();
        // Should be a perfectly formatted JSON array of objects
        assert!(json.contains(r#"{"name":"Alice","active":true}"#));
    }

    #[tokio::test]
    async fn test_api_query_edn_flat() {
        let db = setup_api_db().await;
        let query =
            r#"[:find ?name ?age :where [?e :user/name ?name] [?e :user/age ?age] [(< ?age 35)]]"#;

        let edn = db.query_edn(query).await.unwrap();
        // EDN string values are quoted, keys have colons
        assert!(edn.contains(r#"{:name "Alice" :age 30}"#));
    }

    #[tokio::test]
    async fn test_api_query_json_pull_zero_copy() {
        let db = setup_api_db().await;
        let query = r#"[:find (pull ?e [* {:user/address [:address/city]}]) :where [?e :user/name "Alice"]]"#;

        let json = db.query_json(query).await.unwrap();
        // Zero-copy should prevent double escaping! It shouldn't look like "\"user/name\""
        assert!(json.contains(r#""user/address":{"address/city":"New York"}"#));
    }

    #[tokio::test]
    async fn test_api_query_edn_pull_zero_copy() {
        let db = setup_api_db().await;
        let query = r#"[:find (pull ?e [* {:user/address [:address/city]}]) :where [?e :user/name "Alice"]]"#;

        let edn = db.query_edn(query).await.unwrap();
        // EDN pull should preserve the standard Datomic spacing and colons
        assert!(edn.contains(r#":user/address {:address/city "New York"}"#));
    }

    #[tokio::test]
    async fn test_api_json_aggregates() {
        let db = setup_api_db().await;
        let query = r#"[:find (count ?e) (sum ?age) :where [?e :user/age ?age]]"#;

        let json = db.query_json(query).await.unwrap();
        // DataFusion prefixes aggregates with the function name
        assert!(json.contains(r#""count_e":2"#));
        assert!(json.contains(r#""sum_age":70"#));
    }

    // =====================================================================
    // SUITE 2: DATALOG FUNCTIONS & PREDICATES
    // =====================================================================

    #[tokio::test]
    async fn test_datalog_math_operators() {
        let db = setup_api_db().await;
        // Test +, -, *, and / in a single binding block
        let query = r#"[:find ?next_year ?half_age
                        :where [?e :user/name "Bob"]
                               [?e :user/age ?age]
                               [(+ ?age 1) ?next_year]
                               [(/ ?age 2) ?half_age]]"#;

        let results = db.query_native(query).await.unwrap();
        assert_eq!(results[0].get("next_year"), Some(&Value::Int64(41)));
        assert_eq!(results[0].get("half_age"), Some(&Value::Int64(20)));
    }

    #[tokio::test]
    async fn test_datalog_comparison_predicates() {
        let db = setup_api_db().await;
        // Test != and >=
        let query = r#"[:find ?name
                        :where [?e :user/name ?name]
                               [?e :user/age ?age]
                               [(!= ?name "Bob")]
                               [(>= ?age 30)]]"#;

        let results = db.query_native(query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::String("Alice".into())));
    }

    #[tokio::test]
    async fn test_datalog_string_functions() {
        let db = setup_api_db().await;
        // Test `str` concatenation
        let query = r#"[:find ?greeting
                        :where [?e :user/name "Alice"]
                               [(str "Hello, " "Alice" "!") ?greeting]]"#;

        let results = db.query_native(query).await.unwrap();
        assert_eq!(
            results[0].get("greeting"),
            Some(&Value::String("Hello, Alice!".into()))
        );
    }

    #[tokio::test]
    async fn test_datalog_native_sql_fallback() {
        let db = setup_api_db().await;
        // If a function isn't natively mapped, our planner falls back to raw DataFusion SQL functions.
        // Let's test calling `UPPER` on a string.
        let query = r#"[:find ?yelling
                        :where [?e :user/name "Bob"]
                               [?e :user/name ?n]
                               [(upper ?n) ?yelling]]"#;

        let results = db.query_native(query).await.unwrap();
        assert_eq!(
            results[0].get("yelling"),
            Some(&Value::String("BOB".into()))
        );
    }

    #[tokio::test]
    async fn test_explicit_schema_transaction_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let db = MesoDB::open(
            dir.path().join("explicit_schema.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        // 1. Transact the explicit schema
        let attrs = vec![
            AttributeDefinition {
                ident: ":product/sku".into(),
                value_type: ValueType::String,
                is_unique: true,
            },
            AttributeDefinition {
                ident: ":product/price".into(),
                value_type: ValueType::Float64,
                is_unique: false,
            },
        ];

        let added = db.transact_schema(attrs).await.unwrap();
        assert_eq!(added.len(), 2);
        assert_eq!(added[0].ident, ":product/sku");
        assert_eq!(added[1].ident, ":product/price");

        // 2. Verify the lock-free WorldView pointer updated instantly
        {
            let view = db.world_view.read().unwrap().clone();
            assert!(view.schema.contains_ident(":product/sku"));
            assert!(view.schema.contains_ident(":product/price"));
        }

        // 3. Immediately transact data using the new schema!
        // If the pointer swap or timeline update failed, this or the query will panic.
        db.transact(vec![
            Fact {
                e: 100,
                ident: ":product/sku".into(),
                v: Value::String("XJ-900".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 100,
                ident: ":product/price".into(),
                v: Value::Float64(99.99),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // 4. Query the data to prove the QueryPlanner sees the new timeline
        let query =
            r#"[:find ?sku ?price :where [?e :product/sku ?sku] [?e :product/price ?price]]"#;
        let results = db.query_native(query).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("sku"), Some(&Value::String("XJ-900".into())));
        assert_eq!(results[0].get("price"), Some(&Value::Float64(99.99)));
    }

    #[tokio::test]
    async fn test_history_api_5_tuple_audit() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/status", crate::schema::ValueType::String, false);

        let db = MesoDB::open(dir.path().join("history.db"), schema, Config::default()).unwrap();

        // T = 100: User becomes "active"
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/status".into(),
                v: crate::types::Value::String("active".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            100,
        )
        .await
        .unwrap();

        // T = 200: User changes to "inactive"
        // Under the hood, this bitemporally closes "active" (retraction) and asserts "inactive".
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/status".into(),
                v: crate::types::Value::String("inactive".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            200,
        )
        .await
        .unwrap();

        // THE QUERY: Give me the raw audit trail of all values, transactions, and ops for Entity 1's status.
        let query = r#"[:find ?v ?tx ?op :where [1 :user/status ?v ?tx ?op]]"#;

        // Execute with the new history API enabled
        let opts = QueryOptions {
            history: true,
            format: OutputFormat::Tabular,
            ..Default::default()
        };

        let results = db.query_with_options(query, opts).await.unwrap();

        let batch = &results[0];

        // We expect EXACTLY 3 rows:
        // 1. The original "active" assertion (T=1)
        // 2. The "active" retraction (T=2)
        // 3. The new "inactive" assertion (T=2)
        assert_eq!(
            batch.num_rows(),
            3,
            "History view must return uncollapsed assertions and retractions."
        );

        let val_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let tx_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let op_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        // Row 0: Assert "active" at Tx 1
        assert_eq!(val_col.value(0), "active");
        assert_eq!(tx_col.value(0), 1);
        assert!(op_col.value(0));

        // Row 1: Retract "active" at Tx 2
        assert_eq!(val_col.value(1), "active");
        assert_eq!(tx_col.value(1), 2);
        assert!(!op_col.value(1));

        // Row 2: Assert "inactive" at Tx 2
        assert_eq!(val_col.value(2), "inactive");
        assert_eq!(tx_col.value(2), 2);
        assert!(op_col.value(2));
    }

    // =====================================================================
    // SUITE 3: POINT-IN-TIME (SNAPSHOT) QUERIES
    // =====================================================================

    #[tokio::test]
    async fn test_point_in_time_snapshot_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":item/price", crate::schema::ValueType::Int64, false);
        let db = MesoDB::open(dir.path().join("pit1.db"), schema, Config::default()).unwrap();

        // T=100: Assert Initial Price
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":item/price".into(),
                v: Value::Int64(50),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            100,
        )
        .await
        .unwrap();

        // T=200: Overwrite Price
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":item/price".into(),
                v: Value::Int64(75),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            200,
        )
        .await
        .unwrap();

        let query = r#"[:find ?price :where [1 :item/price ?price]]"#;

        // Snapshot at T=150 (Should see 50)
        let res_150 = db
            .query_with_options(
                query,
                QueryOptions {
                    as_of: Some(150),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let val_150 = res_150[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val_150, 50, "At T=150, the price should be 50");

        // Snapshot at T=250 (Should see 75)
        let res_250 = db
            .query_with_options(
                query,
                QueryOptions {
                    as_of: Some(250),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let val_250 = res_250[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(val_250, 75, "At T=250, the price should be 75");
    }

    #[tokio::test]
    async fn test_point_in_time_retraction_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/tag", crate::schema::ValueType::String, false);
        let db = MesoDB::open(dir.path().join("pit2.db"), schema, Config::default()).unwrap();

        // T=10: Assert Tag
        db.transact_at(
            vec![Fact {
                e: 2,
                ident: ":user/tag".into(),
                v: Value::String("beta".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            10,
        )
        .await
        .unwrap();

        // T=20: Retract Tag
        db.transact_at(
            vec![Fact {
                e: 2,
                ident: ":user/tag".into(),
                v: Value::String("beta".into()),
                op: false,
                cas_old_v: Some(Value::String("beta".into())),
                valid_time: None,
            }],
            20,
        )
        .await
        .unwrap();

        let query = r#"[:find ?tag :where [2 :user/tag ?tag]]"#;

        // At T=15, the tag is active
        let res_15 = db
            .query_with_options(
                query,
                QueryOptions {
                    as_of: Some(15),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            res_15[0].num_rows(),
            1,
            "Tag should exist before retraction"
        );

        // At T=25, the tag is gone
        let res_25 = db
            .query_with_options(
                query,
                QueryOptions {
                    as_of: Some(25),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            res_25.is_empty() || res_25[0].num_rows() == 0,
            "Tag should be completely invisible after retraction"
        );
    }

    // =====================================================================
    // SUITE 4: ACROSS-TIME (HISTORY) QUERIES
    // =====================================================================

    #[tokio::test]
    async fn test_across_time_entity_audit_trail() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":order/status", crate::schema::ValueType::String, false);
        let db = MesoDB::open(dir.path().join("at1.db"), schema, Config::default()).unwrap();

        db.transact_at(
            vec![Fact {
                e: 99,
                ident: ":order/status".into(),
                v: Value::String("pending".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            100,
        )
        .await
        .unwrap();
        db.transact_at(
            vec![Fact {
                e: 99,
                ident: ":order/status".into(),
                v: Value::String("shipped".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            200,
        )
        .await
        .unwrap();

        // Query: Show me EVERY assertion made to this order's status over time
        let query =
            r#"[:find ?status ?tx :where [99 :order/status ?status ?tx ?op] [(= ?op true)]]"#;
        let opts = QueryOptions {
            history: true,
            format: OutputFormat::Tabular,
            ..Default::default()
        };
        let res = db.query_with_options(query, opts).await.unwrap();

        let total_rows: usize = res.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "History view should yield both the historical 'pending' and current 'shipped' assertions"
        );
    }

    #[tokio::test]
    async fn test_across_time_temporal_self_join() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":device/state", crate::schema::ValueType::String, false);
        let db = MesoDB::open(dir.path().join("at2.db"), schema, Config::default()).unwrap();

        db.transact_at(
            vec![Fact {
                e: 42,
                ident: ":device/state".into(),
                v: Value::String("offline".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            10,
        )
        .await
        .unwrap();
        db.transact_at(
            vec![Fact {
                e: 42,
                ident: ":device/state".into(),
                v: Value::String("online".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }],
            20,
        )
        .await
        .unwrap();

        // Query: Self-join the history stream to find the exact state transition pair
        let query = r#"
            [:find ?past_state ?new_state
             :where
                [42 :device/state ?past_state ?tx1 ?op1]
                [42 :device/state ?new_state ?tx2 ?op2]
                [(= ?op1 true)]
                [(= ?op2 true)]
                [(< ?tx1 ?tx2)]
            ]
        "#;

        let opts = QueryOptions {
            history: true,
            format: OutputFormat::Tabular,
            ..Default::default()
        };
        let res = db.query_with_options(query, opts).await.unwrap();

        let batch = &res[0];
        assert_eq!(
            batch.num_rows(),
            1,
            "Should identify exactly one state transition"
        );

        let past_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let new_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(past_col.value(0), "offline");
        assert_eq!(new_col.value(0), "online");
    }

    // =====================================================================
    // SUITE 5: HISTORY API WRAPPERS
    // =====================================================================

    async fn setup_history_db(name: &str) -> MesoDB {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":doc/title", crate::schema::ValueType::String, false);

        let db = MesoDB::open(dir.path().join(name), schema, Config::default()).unwrap();

        // Tx 1: Assert "Draft"
        db.transact(vec![Fact {
            e: 100,
            ident: ":doc/title".into(),
            v: Value::String("Draft".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();
        // Tx 2: Overwrite with "Final" (Creates Retract "Draft" + Assert "Final")
        db.transact(vec![Fact {
            e: 100,
            ident: ":doc/title".into(),
            v: Value::String("Final".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        db
    }

    // --- 1. db.history() Tests ---

    #[tokio::test]
    async fn test_history_wrapper_returns_all_batches() {
        let db = setup_history_db("hist_wrapper_1.db").await;
        let query = r#"[:find ?v ?op :where [100 :doc/title ?v _ ?op]]"#;

        let batches = db.history(query).await.unwrap();
        assert!(!batches.is_empty());

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should return exactly 3 historical records");
    }

    #[tokio::test]
    async fn test_history_wrapper_arrow_downcasting() {
        let db = setup_history_db("hist_wrapper_2.db").await;
        let query = r#"[:find ?op :where [100 :doc/title "Draft" _ ?op]]"#;

        let batches = db.history(query).await.unwrap();
        let op_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        // "Draft" should have two entries: the initial assertion (true), and the subsequent retraction (false)
        assert_eq!(op_col.len(), 2);
        // We aren't guaranteeing sort order in this simple query, so we just check it contains both
        let ops: Vec<bool> = (0..op_col.len()).map(|i| op_col.value(i)).collect();
        assert!(ops.contains(&true));
        assert!(ops.contains(&false));
    }

    // --- 2. db.history_native() Tests ---

    #[tokio::test]
    async fn test_history_native_maps_correct_keys() {
        let db = setup_history_db("hist_native_1.db").await;
        let query = r#"[:find ?v ?op :where [100 :doc/title ?v _ ?op]]"#;

        let results = db.history_native(query).await.unwrap();
        assert_eq!(results.len(), 3);

        for row in results {
            assert!(
                row.contains_key("v"),
                "Native map should contain the 'v' key"
            );
            assert!(
                row.contains_key("op"),
                "Native map should contain the 'op' key"
            );
        }
    }

    #[tokio::test]
    async fn test_history_native_preserves_value_types() {
        let db = setup_history_db("hist_native_2.db").await;
        // Specifically look for the active "Final" state
        let query = r#"[:find ?v ?op :where [100 :doc/title ?v _ ?op] [(= ?v "Final")]]"#;

        let results = db.history_native(query).await.unwrap();
        assert_eq!(results.len(), 1);

        let row = &results[0];
        assert_eq!(row.get("v"), Some(&Value::String("Final".into())));
        assert_eq!(row.get("op"), Some(&Value::Boolean(true)));
    }

    // --- 3. db.history_json() Tests ---

    #[tokio::test]
    async fn test_history_json_array_format() {
        let db = setup_history_db("hist_json_1.db").await;
        let query = r#"[:find ?v :where [100 :doc/title ?v _ _]]"#;

        let json_str = db.history_json(query).await.unwrap();

        // Should be a valid JSON array of objects
        assert!(json_str.starts_with('['));
        assert!(json_str.ends_with(']'));

        // Both string states should exist in the raw JSON output
        assert!(json_str.contains(r#"{"v":"Draft"}"#));
        assert!(json_str.contains(r#"{"v":"Final"}"#));
    }

    #[tokio::test]
    async fn test_history_json_handles_booleans() {
        let db = setup_history_db("hist_json_2.db").await;
        let query = r#"[:find ?op :where [100 :doc/title "Draft" _ ?op]]"#;

        let json_str = db.history_json(query).await.unwrap();

        // "op" maps to native JSON booleans, not strings
        assert!(json_str.contains(r#"{"op":true}"#));
        assert!(json_str.contains(r#"{"op":false}"#));
    }

    // --- 4. db.history_edn() Tests ---

    #[tokio::test]
    async fn test_history_edn_keyword_formatting() {
        let db = setup_history_db("hist_edn_1.db").await;
        let query = r#"[:find ?v :where [100 :doc/title ?v _ _]]"#;

        let edn_str = db.history_edn(query).await.unwrap();

        // EDN format prepends keys with colons
        assert!(edn_str.contains(r#"{:v "Draft"}"#));
        assert!(edn_str.contains(r#"{:v "Final"}"#));
    }

    #[tokio::test]
    async fn test_history_edn_multiple_records() {
        let db = setup_history_db("hist_edn_2.db").await;
        let query = r#"[:find ?v ?op :where [100 :doc/title ?v _ ?op]]"#;

        let edn_str = db.history_edn(query).await.unwrap();

        // The output should be an EDN array [...] containing multiple maps
        assert!(edn_str.starts_with('['));
        assert!(edn_str.ends_with(']'));

        // Check for specific exact EDN state representations
        assert!(edn_str.contains(r#":v "Draft""#));
        assert!(edn_str.contains(r#":op false"#)); // The retraction
        assert!(edn_str.contains(r#":op true"#)); // The assertions
    }

    // =====================================================================
    // SUITE 6: LOGICAL OPERATORS (OR / NOT)
    // =====================================================================

    #[tokio::test]
    async fn test_datalog_or_clause() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":account/status", crate::schema::ValueType::String, false);
        schema.add_attribute(":account/type", crate::schema::ValueType::String, false);

        let db = MesoDB::open(dir.path().join("or_logic.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            // Account 1: Active Admin (Matches both)
            Fact {
                e: 1,
                ident: ":account/status".into(),
                v: Value::String("active".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":account/type".into(),
                v: Value::String("admin".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            // Account 2: Inactive Admin (Matches type)
            Fact {
                e: 2,
                ident: ":account/status".into(),
                v: Value::String("inactive".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":account/type".into(),
                v: Value::String("admin".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            // Account 3: Active User (Matches status)
            Fact {
                e: 3,
                ident: ":account/status".into(),
                v: Value::String("active".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":account/type".into(),
                v: Value::String("user".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            // Account 4: Inactive User (Matches NEITHER)
            Fact {
                e: 4,
                ident: ":account/status".into(),
                v: Value::String("inactive".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 4,
                ident: ":account/type".into(),
                v: Value::String("user".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // THE QUERY: Find any account that is EITHER 'active' OR an 'admin'
        let query = r#"
            [:find ?e
             :where
                (or [?e :account/status "active"]
                    [?e :account/type "admin"])]
        "#;

        let results = db.query(query).await.unwrap();

        // Sum the rows across all parallel Arrow partitions
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "OR clause should return the union of the matching branches"
        );
    }

    #[tokio::test]
    async fn test_datalog_not_clause() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":account/status", crate::schema::ValueType::String, false);
        schema.add_attribute(":account/type", crate::schema::ValueType::String, false);

        let db = MesoDB::open(dir.path().join("not_logic.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            // Account 1: Active Admin
            Fact {
                e: 1,
                ident: ":account/status".into(),
                v: Value::String("active".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":account/type".into(),
                v: Value::String("admin".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            // Account 2: Inactive Admin
            Fact {
                e: 2,
                ident: ":account/status".into(),
                v: Value::String("inactive".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":account/type".into(),
                v: Value::String("admin".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // THE QUERY: Find 'admins' who are NOT 'active'
        let query = r#"
            [:find ?e
             :where
                [?e :account/type "admin"]
                (not [?e :account/status "active"])]
        "#;

        let results = db.query(query).await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "NOT clause should filter out the active admin"
        );

        // Find the actual row regardless of which partition it landed in
        let mut found_e = 0;
        for batch in results {
            if batch.num_rows() > 0 {
                let e_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap();
                found_e = e_col.value(0);
                break;
            }
        }
        assert_eq!(found_e, 2);
    }

    // =====================================================================
    // SUITE 7: REIFIED TRANSACTIONS
    // =====================================================================

    #[tokio::test]
    async fn test_reified_transactions() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":tx/author", crate::schema::ValueType::String, false);
        schema.add_attribute(":user/name", crate::schema::ValueType::String, false);

        let db = MesoDB::open(dir.path().join("reified.db"), schema, Config::default()).unwrap();

        // Transact a user, AND attach metadata to the transaction itself using `e: 0`
        let report = db
            .transact(vec![
                Fact {
                    e: 100,
                    ident: ":user/name".into(),
                    v: Value::String("Alice".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: None,
                },
                Fact {
                    e: 0,
                    ident: ":tx/author".into(),
                    v: Value::String("SystemAdmin".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: None,
                },
            ])
            .await
            .unwrap();

        let actual_tx = report.tx_id;

        // QUERY 1: Did the transaction entity get written?
        let query_tx = format!("[:find ?author :where [{} :tx/author ?author]]", actual_tx);
        let results_tx = db.query_native(&query_tx).await.unwrap();

        assert_eq!(results_tx.len(), 1, "Transaction entity should exist");
        assert_eq!(
            results_tx[0].get("author"),
            Some(&Value::String("SystemAdmin".into()))
        );

        // QUERY 2: The History Audit Join!
        // "Find the user's name, and the author of the transaction that wrote it."
        let query_audit = r#"
            [:find ?name ?author
             :where
                [100 :user/name ?name ?tx true]
                [?tx :tx/author ?author]]
        "#;

        let opts = QueryOptions {
            history: true,
            format: OutputFormat::Tabular,
            ..Default::default()
        };
        let results_audit = db
            .query_native_with_options(query_audit, opts)
            .await
            .unwrap();

        assert_eq!(results_audit.len(), 1);
        assert_eq!(
            results_audit[0].get("name"),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(
            results_audit[0].get("author"),
            Some(&Value::String("SystemAdmin".into()))
        );
    }
}
