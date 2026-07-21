// mesodb-core/src/db.rs

use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use parking_lot::RwLock;
use std::{
    collections::BTreeMap,
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, mpsc};

use crate::{
    config::Config,
    error::MesoError,
    formatter,
    memtable::MemTable,
    parser,
    planner::QueryPlanner,
    schema::{Attribute, SchemaMap, SchemaTimeline, ValueType},
    storage::BackgroundCompactor,
    transactor::{Fact, Transactor, TxReport},
    types::{Result, Value},
};

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
        create_dir_all(data_dir.join("parquet"))?;

        // Initialize the Tier 1 B+Tree
        let now_idx_path = data_dir.join("now.idx");
        let now_index = Arc::new(RwLock::new(crate::btree::NowIndex::open(now_idx_path)?));

        // Pass the B+Tree into the transactor
        let (transactor, recovered_batch) =
            Transactor::new(path, schema.clone(), config.clone(), now_index.clone())?;

        let mut initial_ram = BTreeMap::new();
        let empty_batch = MemTable::new(0).finish()?;
        initial_ram.insert(0, empty_batch);

        if recovered_batch.num_rows() > 0 {
            initial_ram.insert(transactor.current_tx_id, recovered_batch);
        } else {
            let empty_batch = MemTable::new(0).finish()?;
            initial_ram.insert(0, empty_batch);
        }

        let world_view = Arc::new(RwLock::new(Arc::new(WorldView {
            ram_batches: Arc::new(initial_ram),
            data_dir: data_dir.clone(),
            schema: Arc::new(schema),
            timeline: Arc::new(transactor.timeline.clone()),
            now_index,
        })));

        let (flush_tx, flush_rx) = mpsc::channel(config.compactor.backpressure_threshold);

        // Pass config clone into the background worker
        Self::spawn_background_worker(data_dir, flush_rx, world_view.clone(), config.clone());

        Ok(Self {
            transactor: Mutex::new(transactor),
            world_view,
            flush_tx,
        })
    }

    /// The Background Worker: Listens for RAM batches, writes to disk, drops them from RAM,
    /// and dispatches detached compaction tasks to merge volatile files.
    fn spawn_background_worker(
        data_dir: PathBuf,
        mut flush_rx: mpsc::Receiver<(u64, RecordBatch)>,
        world_view: Arc<RwLock<Arc<WorldView>>>,
        config: Config,
    ) {
        tokio::spawn(async move {
            let compactor = BackgroundCompactor::new(data_dir);
            let mut uncompacted_files = Vec::new();

            while let Some((tx_id, batch)) = flush_rx.recv().await {
                // 1. Write the batch to a Volatile Parquet file
                let file_path = match compactor.flush_to_parquet(batch, tx_id) {
                    Ok(path) => path,
                    Err(e) => {
                        eprintln!("Failed to flush Tx {} to Parquet: {:?}", tx_id, e);
                        continue; // Keep it in RAM if disk fails
                    }
                };
                uncompacted_files.push(file_path);

                // 2. Safely remove from RAM using the lock-free pointer swap
                {
                    let mut writer = world_view.write();
                    let current_view = writer.as_ref().clone();

                    let mut new_ram = current_view.ram_batches.as_ref().clone();
                    new_ram.remove(&tx_id);

                    let new_view = Arc::new(WorldView {
                        ram_batches: Arc::new(new_ram),
                        data_dir: current_view.data_dir.clone(),
                        schema: current_view.schema.clone(),
                        timeline: current_view.timeline.clone(),
                        now_index: current_view.now_index.clone(),
                    });

                    *writer = new_view;
                }

                // 3. Enforce Config Limits & Trigger Compaction
                // If we reach our configured threshold of uncompacted volatile files, merge them.
                if uncompacted_files.len() >= config.compactor.backpressure_threshold {
                    // Take ownership of the current batch of files to send to a new thread
                    let files_to_compact = std::mem::take(&mut uncompacted_files);
                    let compactor_clone = compactor.clone();

                    // Offload the heavy DataFusion merge & mathematically pure bitemporal resolution
                    // to an entirely separate Tokio task.
                    tokio::spawn(async move {
                        if let Err(e) = compactor_clone.compact(&files_to_compact, tx_id).await {
                            eprintln!("Background compaction failed for Tx {}: {:?}", tx_id, e);
                        }
                    });
                }
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
    ) -> Result<Vec<Arc<Attribute>>> {
        let mut tx = self.transactor.lock().await;
        let mut added_attrs = Vec::with_capacity(attributes.len());

        for attr in attributes {
            let added = tx.transact_schema(&attr.ident, attr.value_type, attr.is_unique)?;
            added_attrs.push(added);
        }

        // If we actually added anything, we must publish the new schema to RAM
        // so that read-queries can instantly recognize the new attributes.
        if !added_attrs.is_empty() {
            let mut writer = self.world_view.write();
            let current_view = writer.as_ref().clone();

            let new_view = Arc::new(WorldView {
                ram_batches: current_view.ram_batches.clone(), // Data is unchanged
                data_dir: current_view.data_dir.clone(),
                schema: Arc::new(tx.schema.clone()),
                timeline: Arc::new(tx.timeline.clone()),
                now_index: current_view.now_index.clone(),
            });

            *writer = new_view;
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
            let batch_clone = report.batch.clone();
            let new_schema = Arc::new(tx.schema.clone());
            let new_timeline = Arc::new(tx.timeline.clone());

            // OPTION 1: IN-LOCK MUTATION
            {
                let mut writer = self.world_view.write();
                let current_view = writer.as_ref().clone();

                let mut new_ram = current_view.ram_batches.as_ref().clone();
                new_ram.insert(report.tx_id, batch_clone);

                let new_view = Arc::new(WorldView {
                    ram_batches: Arc::new(new_ram),
                    data_dir: current_view.data_dir.clone(),
                    schema: new_schema,
                    timeline: new_timeline,
                    now_index: current_view.now_index.clone(),
                });

                *writer = new_view;
            } // Write lock releases instantly here!

            // --- THE COMPACTION COMPLIANCE THRESHOLD ---
            if report.batch.num_rows() >= tx.config.storage.memtable_max_rows
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

    /// Evaluates if an AST qualifies for the Tier 1 zero-copy Fast-Path.
    /// If it does, it queries the memory-mapped B+Tree and short-circuits DataFusion entirely.
    fn try_fast_path(
        &self,
        ast: &crate::ast::Query,
        options: &QueryOptions,
        view: &WorldView,
    ) -> Result<Option<Vec<RecordBatch>>> {
        // Must be a "Now" query
        if options.history || options.as_of.is_some() || options.rules.is_some() {
            return Ok(None);
        }

        // AST must be a simple identity point-lookup: [:find ?v :where [42 :attr ?v]]
        if ast.find.len() != 1 || ast.where_clauses.len() != 1 {
            return Ok(None);
        }

        let find_var = match &ast.find[0] {
            crate::ast::FindSpec::Variable(v) => v,
            _ => return Ok(None),
        };

        let (e_id, a_ident, v_var) = match &ast.where_clauses[0] {
            crate::ast::WhereClause::DataPattern {
                e: crate::ast::Term::Integer(id),
                a: crate::ast::Term::Keyword(kw),
                v: crate::ast::Term::Variable(var),
                tx: None,
                op: None,
            } => (*id as u64, kw, var),
            _ => return Ok(None),
        };

        if v_var != find_var {
            return Ok(None);
        }

        let a_id = match view.schema.get_id(a_ident) {
            Some(id) => id,
            None => return Ok(Some(vec![])), // Valid empty response (Attribute doesn't exist)
        };

        // AST is verified. Hit the zero-copy B+Tree!
        let now_index = view.now_index.read();
        if let Some(val) = now_index.get(e_id, a_id)? {
            let clean_var = find_var.replace("?", "");

            // Reconstruct a single-row Arrow batch manually to bypass DataFusion
            let (field, array): (arrow::datatypes::Field, Arc<dyn arrow::array::Array>) = match val
            {
                Value::String(s) => (
                    arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ),
                    Arc::new(arrow::array::StringArray::from(vec![s])),
                ),
                Value::Int64(i) => (
                    arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                    Arc::new(arrow::array::Int64Array::from(vec![i])),
                ),
                Value::Float64(f) => (
                    arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::Float64,
                        true,
                    ),
                    Arc::new(arrow::array::Float64Array::from(vec![f])),
                ),
                Value::Boolean(b) => (
                    arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::Boolean,
                        true,
                    ),
                    Arc::new(arrow::array::BooleanArray::from(vec![b])),
                ),
                Value::Ref(r) => (
                    arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::UInt64,
                        true,
                    ),
                    Arc::new(arrow::array::UInt64Array::from(vec![r])),
                ),
                Value::Timestamp(t) => (
                    arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Microsecond,
                            None,
                        ),
                        true,
                    ),
                    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![t])),
                ),
                Value::Uuid(u) => {
                    let uuid_str = format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        u[0],
                        u[1],
                        u[2],
                        u[3],
                        u[4],
                        u[5],
                        u[6],
                        u[7],
                        u[8],
                        u[9],
                        u[10],
                        u[11],
                        u[12],
                        u[13],
                        u[14],
                        u[15]
                    );
                    (
                        arrow::datatypes::Field::new(
                            &clean_var,
                            arrow::datatypes::DataType::Utf8,
                            true,
                        ),
                        Arc::new(arrow::array::StringArray::from(vec![uuid_str])),
                    )
                }
            };

            let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
            let batch = RecordBatch::try_new(schema, vec![array]).map_err(MesoError::Arrow)?;
            return Ok(Some(vec![batch]));
        }

        Ok(Some(vec![])) // Entity not found, valid Fast-Path empty return
    }

    pub async fn query(&self, query_str: &str) -> Result<Vec<RecordBatch>> {
        self.query_with_options(query_str, QueryOptions::default())
            .await
    }

    pub async fn query_with_options(
        &self,
        query_str: &str,
        options: QueryOptions,
    ) -> Result<Vec<RecordBatch>> {
        let ast = parser::parse_query(query_str)?;
        let view = { self.world_view.read().clone() };

        // --- FAST PATH ROUTER ---
        if let Some(fast_result) = self.try_fast_path(&ast, &options, &view)? {
            return Ok(fast_result);
        }

        let ctx = SessionContext::new();
        let ruleset = if let Some(r) = &options.rules {
            Some(parser::parse_ruleset(r)?)
        } else {
            None
        };

        let arrow_schema = view.ram_batches.get(&0).unwrap().schema();
        let pq_options =
            datafusion::prelude::ParquetReadOptions::default().schema(arrow_schema.as_ref());

        // 1. Separate Physical Isolated Streams to Preserve Predicate Pushdown
        let mut compacted_paths = vec![];
        let mut volatile_paths = vec![];
        let pq_dir = view.data_dir.join("parquet");

        if let Ok(entries) = std::fs::read_dir(&pq_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let path = entry.path().to_string_lossy().to_string();
                if name.starts_with("compacted-") && name.ends_with(".parquet") {
                    compacted_paths.push(path);
                } else if name.starts_with("part-") && name.ends_with(".parquet") {
                    volatile_paths.push(path);
                }
            }
        }

        if !compacted_paths.is_empty() {
            let df = ctx
                .read_parquet(compacted_paths, pq_options.clone())
                .await?;
            ctx.register_table("compacted_datoms", df.into_view())?;
        } else {
            let provider = datafusion::datasource::memory::MemTable::try_new(
                arrow_schema.clone(),
                vec![vec![arrow::record_batch::RecordBatch::new_empty(
                    arrow_schema.clone(),
                )]],
            )
            .unwrap();
            ctx.register_table("compacted_datoms", Arc::new(provider))?;
        }

        if !volatile_paths.is_empty() {
            let df = ctx.read_parquet(volatile_paths, pq_options.clone()).await?;
            ctx.register_table("volatile_parquet", df.into_view())?;
        } else {
            let provider = datafusion::datasource::memory::MemTable::try_new(
                arrow_schema.clone(),
                vec![vec![arrow::record_batch::RecordBatch::new_empty(
                    arrow_schema.clone(),
                )]],
            )
            .unwrap();
            ctx.register_table("volatile_parquet", Arc::new(provider))?;
        }

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

        let df_vol = ctx
            .sql("SELECT * FROM volatile_parquet UNION ALL SELECT * FROM ram_datoms")
            .await?;
        ctx.register_table("volatile_datoms", df_vol.into_view())?;

        // 2. The Vectorized Option A SQL Router
        let resolved_sql = if options.history {
            // History queries bypass window groupings entirely to stream the raw, unadulterated audit trail.
            r#"
            WITH raw_combined AS (
                SELECT * FROM compacted_datoms UNION ALL SELECT * FROM volatile_datoms
            ),
            reconstructed_retractions AS (
                SELECT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, false as op, valid_to as valid_from, (CASE WHEN false THEN valid_to ELSE NULL END) as next_from, valid_to
                FROM compacted_datoms WHERE op = true AND CAST(valid_to AS BIGINT) < 9223372036854775807
            )
            SELECT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from, (CASE WHEN false THEN valid_from ELSE NULL END) as next_from, valid_to FROM raw_combined
            UNION ALL
            SELECT * FROM reconstructed_retractions
            "#.to_string()
        } else {
            // Dynamically construct time filters to safely evaluate 'current state' vs 'past state'
            let t = options.as_of.unwrap_or(i64::MAX);
            let (vol_filter, active_vol_filter, comp_filter) = match options.as_of {
                Some(_) => (
                    format!("WHERE CAST(valid_from AS BIGINT) <= {t}"),
                    format!(
                        "AND CAST(valid_from AS BIGINT) <= {t} AND CAST(COALESCE(next_from, valid_to) AS BIGINT) >= {t}"
                    ),
                    format!(
                        "AND CAST(c.valid_from AS BIGINT) <= {t} AND CAST(c.valid_to AS BIGINT) >= {t}"
                    ),
                ),
                None => (
                    "".to_string(),
                    "AND next_from IS NULL".to_string(),
                    "AND CAST(c.valid_to AS BIGINT) = 9223372036854775807".to_string(),
                ),
            };

            format!(
                r#"
                WITH
                volatile_filtered AS (
                    SELECT * FROM volatile_datoms {vol_filter}
                ),
                volatile_bounds AS (
                    SELECT *, LEAD(valid_from) OVER (PARTITION BY e, a ORDER BY valid_from ASC, t ASC, op ASC) as next_from
                    FROM volatile_filtered
                ),
                active_volatile AS (
                    SELECT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from, next_from, COALESCE(next_from, valid_to) as valid_to
                    FROM volatile_bounds
                    WHERE op = true {active_vol_filter}
                ),
                volatile_mask AS (
                    SELECT DISTINCT e, a FROM volatile_filtered
                ),
                surviving_compacted AS (
                    SELECT c.e, c.a, c.v_bool, c.v_int, c.v_float, c.v_str, c.v_ref, c.v_time, c.v_uuid, c.t, c.op, c.valid_from, (CASE WHEN false THEN c.valid_from ELSE NULL END) as next_from, c.valid_to
                    FROM compacted_datoms c
                    WHERE c.op = true {comp_filter}
                      AND NOT EXISTS (SELECT 1 FROM volatile_mask m WHERE m.e = c.e AND m.a = c.a)
                )
                SELECT * FROM surviving_compacted
                UNION ALL
                SELECT * FROM active_volatile
                "#
            )
        };

        let resolved_df = ctx
            .sql(&resolved_sql)
            .await
            .map_err(MesoError::DataFusion)?;
        ctx.register_table("resolved_datoms", resolved_df.into_view())?;

        let planner = QueryPlanner::new(
            &ctx,
            view.timeline.as_ref(),
            "resolved_datoms",
            options.format.clone(),
            options.as_of,
            ruleset,
        );

        let final_df = planner.plan(&ast).await?;

        // Explicit error propagation to prevent silent failures
        let batches = final_df.collect().await.map_err(MesoError::DataFusion)?;
        Ok(batches)
    }

    pub async fn query_native(
        &self,
        query_str: &str,
    ) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let batches = self.query(query_str).await?;
        formatter::to_native(&batches)
    }

    pub async fn query_native_with_options(
        &self,
        query_str: &str,
        options: QueryOptions,
    ) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let batches = self.query_with_options(query_str, options).await?;
        formatter::to_native(&batches)
    }

    pub async fn query_json(&self, query_str: &str) -> Result<String> {
        let ast = parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Json,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(formatter::to_json_string(&batches, &ast.find))
    }

    pub async fn query_json_with_options(
        &self,
        query_str: &str,
        mut options: QueryOptions,
    ) -> Result<String> {
        let ast = parser::parse_query(query_str)?;
        options.format = OutputFormat::Json; // Enforce JSON for this pipeline
        let batches = self.query_with_options(query_str, options).await?;
        Ok(formatter::to_json_string(&batches, &ast.find))
    }

    pub async fn query_edn(&self, query_str: &str) -> Result<String> {
        let ast = parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Edn,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(formatter::to_edn_string(&batches, &ast.find))
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
    ) -> Result<Vec<std::collections::HashMap<String, Value>>> {
        let batches = self.history(query_str).await?;
        formatter::to_native(&batches)
    }

    pub async fn history_json(&self, query_str: &str) -> Result<String> {
        let ast = parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Json,
            history: true,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(formatter::to_json_string(&batches, &ast.find))
    }

    pub async fn history_edn(&self, query_str: &str) -> Result<String> {
        let ast = parser::parse_query(query_str)?;
        let opts = QueryOptions {
            format: OutputFormat::Edn,
            history: true,
            ..Default::default()
        };
        let batches = self.query_with_options(query_str, opts).await?;
        Ok(formatter::to_edn_string(&batches, &ast.find))
    }
}

#[derive(Clone)]
pub struct WorldView {
    pub ram_batches: Arc<BTreeMap<u64, RecordBatch>>,
    pub data_dir: PathBuf,
    pub schema: Arc<SchemaMap>,
    pub timeline: Arc<SchemaTimeline>,
    pub now_index: Arc<RwLock<crate::btree::NowIndex>>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Arrow,
    Edn,
    Tabular,
    Json,
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
    pub value_type: ValueType,
    pub is_unique: bool,
}

// --- TESTS ---
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
        // Unify all partitions into a single RecordBatch before downcasting.
        let schema = results[0].schema();
        let batch =
            arrow::compute::concat_batches(&schema, &results).expect("Failed to concat batches");

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
        schema.add_attribute(":person/parent", ValueType::Ref, false);
        schema.add_attribute(":person/name", ValueType::String, false);

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
            let view = { db.world_view.read().clone() };
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
        schema.add_attribute(":user/status", ValueType::String, false);

        let db = MesoDB::open(dir.path().join("history.db"), schema, Config::default()).unwrap();

        // T = 100: User becomes "active"
        db.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/status".into(),
                v: Value::String("active".into()),
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
                v: Value::String("inactive".into()),
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
        schema.add_attribute(":item/price", ValueType::Int64, false);
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
        schema.add_attribute(":user/tag", ValueType::String, false);
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
        schema.add_attribute(":order/status", ValueType::String, false);
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
        schema.add_attribute(":device/state", ValueType::String, false);
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
        schema.add_attribute(":doc/title", ValueType::String, false);

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
        schema.add_attribute(":account/status", ValueType::String, false);
        schema.add_attribute(":account/type", ValueType::String, false);

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
        schema.add_attribute(":account/status", ValueType::String, false);
        schema.add_attribute(":account/type", ValueType::String, false);

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
        schema.add_attribute(":tx/author", ValueType::String, false);
        schema.add_attribute(":user/name", ValueType::String, false);

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

    // =====================================================================
    // SUITE 8: USER-SPACE LOCKING & CONCURRENCY EDGE CASES
    // =====================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_parking_lot_split_lock_avoidance() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDB::open(
                dir.path().join("pl_split.db"),
                SchemaMap::new(),
                Config::default(),
            )
            .unwrap(),
        );

        db.transact(vec![Fact {
            e: 1,
            ident: ":test/a".into(),
            v: Value::Int64(1),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();
        let initial_tx_id = db.transactor.lock().await.current_tx_id - 1;

        let db_clone = db.clone();

        // Emulate the background compactor removing a batch using the Option 1 In-Lock Mutation
        let worker_handle = tokio::spawn(async move {
            let mut writer = db_clone.world_view.write();
            tokio::task::block_in_place(|| {
                std::thread::sleep(std::time::Duration::from_millis(50))
            }); // Force a contention window inside the lock

            let current_view = writer.as_ref().clone();
            let mut new_ram = current_view.ram_batches.as_ref().clone();
            new_ram.remove(&initial_tx_id);

            *writer = Arc::new(WorldView {
                ram_batches: Arc::new(new_ram),
                data_dir: current_view.data_dir.clone(),
                schema: current_view.schema.clone(),
                timeline: current_view.timeline.clone(),
                now_index: current_view.now_index.clone(),
            });
        });

        // Concurrently push a new transaction. It MUST wait for the background worker's write lock to release.
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        db.transact(vec![Fact {
            e: 2,
            ident: ":test/b".into(),
            v: Value::Int64(2),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        worker_handle.await.unwrap();

        let final_view = db.world_view.read().clone();

        // VALIDATION: The split-lock bug is dead! The compactor removal succeeded, AND the new transaction exists.
        assert!(
            !final_view.ram_batches.contains_key(&initial_tx_id),
            "Background removal should succeed"
        );
        assert!(
            final_view.ram_batches.keys().any(|&k| k > initial_tx_id),
            "Concurrent transaction insertion should succeed"
        );
    }

    #[tokio::test]
    async fn test_snapshot_read_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDB::open(
                dir.path().join("pl_vis.db"),
                SchemaMap::new(),
                Config::default(),
            )
            .unwrap(),
        );

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

        // 1. Thread A loads a snapshot (Tx 0 Genesis + Tx 1)
        let view_snapshot_early = db.world_view.read().clone();

        // 2. Thread B mutates the database state while Thread A is "reading"
        db.transact(vec![Fact {
            e: 2,
            ident: ":sys/next".into(),
            v: Value::Boolean(true),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        // 3. Thread C loads a fresh snapshot
        let view_snapshot_late = db.world_view.read().clone();

        // VALIDATION: Thread A's isolated Arc pointer MUST NOT see Thread B's changes
        assert_eq!(
            view_snapshot_early.ram_batches.len(),
            2,
            "Early snapshot should only see the genesis and init batches"
        );
        assert_eq!(
            view_snapshot_late.ram_batches.len(),
            3,
            "Late snapshot should see all three batches"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parking_lot_high_contention_saturation() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDB::open(
                dir.path().join("pl_sat.db"),
                SchemaMap::new(),
                Config::default(),
            )
            .unwrap(),
        );

        let mut handles = vec![];

        // Spawn 100 furious reader threads pounding the user-space read lock
        for _ in 0..100 {
            let db_clone = db.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..50 {
                    let _view = db_clone.world_view.read().clone();
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Spawn a writer pushing updates concurrently
        let writer_db = db.clone();
        let writer_handle = tokio::spawn(async move {
            for i in 1..=10 {
                writer_db
                    .transact(vec![Fact {
                        e: i,
                        ident: ":sys/count".into(),
                        v: Value::Int64(i as i64),
                        op: true,
                        cas_old_v: None,
                        valid_time: None,
                    }])
                    .await
                    .unwrap();
            }
        });

        let _ = tokio::join!(writer_handle);
        for h in handles {
            let _ = h.await;
        }

        let final_view = db.world_view.read().clone();
        assert_eq!(
            final_view.ram_batches.len(),
            11,
            "All writes must commit without starvation despite 100 furious readers"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_schema_update_vs_compactor_in_lock_race() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDB::open(
                dir.path().join("pl_schema_race.db"),
                SchemaMap::new(),
                Config::default(),
            )
            .unwrap(),
        );

        db.transact(vec![Fact {
            e: 1,
            ident: ":test/a".into(),
            v: Value::Int64(1),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();
        let tx_id = db.transactor.lock().await.current_tx_id - 1;

        let db_clone = db.clone();

        let compactor_handle = tokio::spawn(async move {
            let mut writer = db_clone.world_view.write();
            tokio::task::block_in_place(|| {
                std::thread::sleep(std::time::Duration::from_millis(100))
            });

            let current_view = writer.as_ref().clone();
            let mut new_ram = current_view.ram_batches.as_ref().clone();
            new_ram.remove(&tx_id);

            *writer = Arc::new(WorldView {
                ram_batches: Arc::new(new_ram),
                data_dir: current_view.data_dir.clone(),
                schema: current_view.schema.clone(),
                timeline: current_view.timeline.clone(),
                now_index: current_view.now_index.clone(),
            });
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let attr_def = AttributeDefinition {
            ident: ":new/racing_attr".into(),
            value_type: ValueType::String,
            is_unique: false,
        };
        db.transact_schema(vec![attr_def]).await.unwrap();

        compactor_handle.await.unwrap();

        let final_view = db.world_view.read().clone();
        assert!(
            !final_view.ram_batches.contains_key(&tx_id),
            "The compactor must successfully remove the batch"
        );
        assert!(
            final_view.schema.contains_ident(":new/racing_attr"),
            "The schema update MUST perfectly stack on top of the compactor's removal"
        );
    }

    #[tokio::test]
    async fn test_idempotent_ghost_flush_handling() {
        let dir = tempfile::tempdir().unwrap();
        let db = MesoDB::open(
            dir.path().join("pl_ghost.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        // Emulate the background compactor trying to remove a batch that doesn't exist
        {
            let writer = db.world_view.write();
            let current_view = writer.as_ref().clone();
            let mut new_ram = current_view.ram_batches.as_ref().clone();

            if new_ram.remove(&9999).is_none() {
                // Do nothing. Drop the lock. The pointer remains entirely unmodified.
            } else {
                panic!("Should not execute");
            }
        }

        let final_view = db.world_view.read().clone();
        assert_eq!(
            final_view.ram_batches.len(),
            1,
            "Ghost flushes do not corrupt the RAM map"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_monotonic_time_with_safe_ids() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDB::open(
                dir.path().join("pl_mono.db"),
                SchemaMap::new(),
                Config::default(),
            )
            .unwrap(),
        );

        let mut read_handles = vec![];
        let mut write_handles = vec![];

        for w in 0..5 {
            let db_clone = db.clone();
            write_handles.push(tokio::spawn(async move {
                for i in 0..20 {
                    let safe_e = 1000 + (w * 100) + i; // Shifted ID to dodge reified Tx collision
                    db_clone
                        .transact(vec![Fact {
                            e: safe_e,
                            ident: ":sys/val".into(),
                            v: Value::Int64(1),
                            op: true,
                            cas_old_v: None,
                            valid_time: None,
                        }])
                        .await
                        .unwrap();
                }
            }));
        }

        for _ in 0..10 {
            let db_clone = db.clone();
            read_handles.push(tokio::spawn(async move {
                let mut max_seen_batches = 0;
                for _ in 0..100 {
                    let view = db_clone.world_view.read().clone();
                    let current_len = view.ram_batches.len();
                    assert!(
                        current_len >= max_seen_batches,
                        "CRITICAL: A reader's view of time moved backwards!"
                    );
                    max_seen_batches = current_len;
                    tokio::task::yield_now().await;
                }
            }));
        }

        for w in write_handles {
            w.await.unwrap();
        }
        for r in read_handles {
            r.await.unwrap();
        }

        let final_view = db.world_view.read().clone();
        assert_eq!(
            final_view.ram_batches.len(),
            101,
            "1 genesis + 100 safe transactions must fully commit"
        );
    }

    #[tokio::test]
    async fn test_generational_memory_pinning_via_arc() {
        let dir = tempfile::tempdir().unwrap();
        let db = MesoDB::open(
            dir.path().join("pl_pin.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        let pin_1 = db.world_view.read().clone();
        db.transact(vec![Fact {
            e: 1,
            ident: ":test/a".into(),
            v: Value::Int64(1),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        let pin_2 = db.world_view.read().clone();
        db.transact(vec![Fact {
            e: 2,
            ident: ":test/b".into(),
            v: Value::Int64(2),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        // VALIDATION: `parking_lot` + `Arc` preserves the exact memory graph for existing readers
        assert_eq!(pin_1.ram_batches.len(), 1);
        assert_eq!(pin_2.ram_batches.len(), 2);
        assert_eq!(db.world_view.read().ram_batches.len(), 3);
    }

    #[tokio::test]
    async fn test_background_compaction_trigger_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("compaction_test.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/score", ValueType::Int64, false);

        // Custom config to force fast flushes and compactions
        let mut config = Config::embedded();
        config.compactor.backpressure_threshold = 2; // Compact after 2 files
        config.storage.memtable_max_rows = 1; // Flush every single datom immediately

        let db = MesoDB::open(db_path, schema, config).unwrap();

        // 1. First transaction -> Fills the 1-row MemTable, flushes to part-000000000001.parquet
        db.transact(vec![Fact {
            e: 1,
            ident: ":user/score".into(),
            v: Value::Int64(100),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        // Give the async worker a moment to write the first file and clear RAM
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // 2. Second transaction -> Fills MemTable, flushes to part-000000000002.parquet
        // Since our threshold is 2, this TRIGGERS the detached compaction task!
        db.transact(vec![Fact {
            e: 2,
            ident: ":user/score".into(),
            v: Value::Int64(200),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        // Wait for the heavy background compaction task to finish merging and cleaning up
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // 3. Query the data - DataFusion should seamlessly route the query to the new compacted file
        let query = r#"[:find ?score :where [?e :user/score ?score]]"#;
        let results = db.query_native(query).await.unwrap();

        assert_eq!(results.len(), 2, "Should find both scores seamlessly");

        // 4. Verify the filesystem state physically matches our architecture
        let pq_dir = dir.path().join("parquet");
        let entries = std::fs::read_dir(pq_dir).unwrap();
        let mut compacted_count = 0;
        let mut volatile_count = 0;

        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("compacted-") {
                compacted_count += 1;
            } else if name.starts_with("part-") {
                volatile_count += 1;
            }
        }

        assert_eq!(
            volatile_count, 0,
            "Volatile parts should have been deleted by the compactor"
        );
        assert_eq!(
            compacted_count, 1,
            "There should be exactly one compacted file containing all data"
        );
    }
}
