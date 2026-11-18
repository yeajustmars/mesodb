// mesodb-core/src/db.rs

use arrow::{array::Array, record_batch::RecordBatch};
use datafusion::prelude::*;
use parking_lot::RwLock;
use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, mpsc};

use crate::{
    bitmap::BitmapStore,
    btree::NowIndex,
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

struct PendingFlush {
    batches: Vec<(u64, RecordBatch)>,
    rows: usize,
    bytes: usize,
}

struct FlushPayload {
    max_tx_id: u64,
    evict_tx_ids: Vec<u64>,
    batch: RecordBatch,
}

enum WorkerCommand {
    Flush {
        payload: Option<FlushPayload>,
        ack: Option<tokio::sync::oneshot::Sender<Result<()>>>,
    },
    Compact {
        ack: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Shutdown {
        ack: tokio::sync::oneshot::Sender<()>,
    },
}

pub struct MesoDb {
    /// The transactor is the single writer, guarded by a Tokio Mutex.
    transactor: Arc<Mutex<Transactor>>,
    /// The WorldView is an atomic pointer. Swapping it takes nanoseconds.
    world_view: Arc<RwLock<Arc<WorldView>>>,
    /// Accumulates un-flushed batches until row or byte thresholds are reached.
    pending_flush: parking_lot::Mutex<PendingFlush>,
    /// The channel to send commands and sealed MemTable batches to the background worker.
    flush_tx: mpsc::Sender<WorkerCommand>,
}

impl MesoDb {
    fn scan_parquet_manifest(pq_dir: &Path) -> (Vec<PathBuf>, Vec<PathBuf>, u64) {
        let mut compacted = Vec::new();
        let mut volatile = Vec::new();
        let mut max_tx_id = 0u64;

        if let Ok(entries) = std::fs::read_dir(pq_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(".tmp") {
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                if let Some(stem) = name
                    .strip_prefix("compacted-")
                    .and_then(|s| s.strip_suffix(".parquet"))
                {
                    if let Ok(tx_id) = stem.parse::<u64>() {
                        max_tx_id = max_tx_id.max(tx_id);
                    }
                    compacted.push(path);
                } else if let Some(stem) = name
                    .strip_prefix("part-")
                    .and_then(|s| s.strip_suffix(".parquet"))
                {
                    if let Ok(tx_id) = stem.parse::<u64>() {
                        max_tx_id = max_tx_id.max(tx_id);
                    }
                    volatile.push(path);
                }
            }
        }

        compacted.sort();
        volatile.sort();
        (compacted, volatile, max_tx_id)
    }

    fn record_dirty_batch(
        dirty_map: &mut ahash::AHashMap<
            crate::types::AttributeId,
            ahash::AHashSet<crate::types::EntityId>,
        >,
        batch: &RecordBatch,
    ) {
        if let (Some(e_col), Some(a_col)) = (
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>(),
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>(),
        ) {
            for i in 0..batch.num_rows() {
                dirty_map
                    .entry(a_col.value(i))
                    .or_default()
                    .insert(e_col.value(i));
            }
        }
    }

    pub fn open<P: AsRef<Path>>(path: P, schema: SchemaMap, config: Config) -> Result<Self> {
        let data_dir = path.as_ref().parent().unwrap().to_path_buf();
        let pq_dir = data_dir.join("parquet");
        create_dir_all(&data_dir)?;
        create_dir_all(&pq_dir)?;

        let (initial_compacted, initial_volatile, max_persisted_tx_id) =
            Self::scan_parquet_manifest(&pq_dir);

        // Initialize Tier 1 B+Trees (Owned by the background worker)
        let now_index = NowIndex::open(data_dir.join("now.idx"))?;
        let bitmap_store = BitmapStore::open(data_dir.join("bitmaps.idx"))?;
        let avt_index = crate::btree::AvtIndex::open(data_dir.join("avt.idx"))?;

        // Take initial snapshots for WorldView
        let now_snap = Arc::new(now_index.snapshot());
        let bitmap_snap = Arc::new(bitmap_store.snapshot());
        let avt_snap = Arc::new(avt_index.snapshot());

        let (transactor, recovered_batch) = Transactor::new_with_persisted_tx(
            path,
            schema.clone(),
            config.clone(),
            max_persisted_tx_id,
        )?;

        let mut initial_ram = Vec::new();
        let mut initial_dirty = ahash::AHashMap::new();
        let empty_batch = MemTable::new(0).finish()?;
        initial_ram.push((0, empty_batch));

        let mut initial_flush_payload = None;
        if recovered_batch.num_rows() > 0 {
            let recovery_tx_id = transactor.current_tx_id.saturating_sub(1);
            initial_ram.push((recovery_tx_id, recovered_batch.clone()));
            Self::record_dirty_batch(&mut initial_dirty, &recovered_batch);

            initial_flush_payload = Some(FlushPayload {
                max_tx_id: recovery_tx_id,
                evict_tx_ids: vec![recovery_tx_id],
                batch: recovered_batch,
            });
        }

        let transactor = Arc::new(Mutex::new(transactor));

        let world_view = Arc::new(RwLock::new(Arc::new(WorldView {
            ram_batches: Arc::new(initial_ram),
            compacted_files: Arc::new(initial_compacted),
            volatile_files: Arc::new(initial_volatile),
            data_dir: data_dir.clone(),
            schema: Arc::new(schema),
            timeline: Arc::new(
                transactor
                    .try_lock()
                    .map(|t| t.timeline.clone())
                    .unwrap_or_default(),
            ),
            now_index: now_snap,
            bitmap_index: bitmap_snap,
            avt_index: avt_snap,
            dirty_entities: Arc::new(initial_dirty),
        })));

        let channel_cap = config.compactor.backpressure_threshold.max(1);
        let (flush_tx, flush_rx) = mpsc::channel(channel_cap);

        if let Some(payload) = initial_flush_payload {
            let _ = flush_tx.try_send(WorkerCommand::Flush {
                payload: Some(payload),
                ack: None,
            });
        }

        Self::spawn_background_worker(
            data_dir,
            flush_rx,
            transactor.clone(),
            world_view.clone(),
            config,
            now_index,
            bitmap_store,
            avt_index,
        );

        Ok(Self {
            transactor,
            world_view,
            pending_flush: parking_lot::Mutex::new(PendingFlush {
                batches: Vec::new(),
                rows: 0,
                bytes: 0,
            }),
            flush_tx,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_background_worker(
        data_dir: PathBuf,
        mut flush_rx: mpsc::Receiver<WorkerCommand>,
        transactor: Arc<Mutex<Transactor>>,
        world_view: Arc<RwLock<Arc<WorldView>>>,
        config: Config,
        mut now_index: crate::btree::NowIndex,
        mut bitmap_store: crate::bitmap::BitmapStore,
        mut avt_index: crate::btree::AvtIndex,
    ) {
        tokio::spawn(async move {
            let compactor = BackgroundCompactor::new(data_dir);
            let mut last_flushed_tx_id = 0u64;
            let mut retired_files: Vec<(Arc<Vec<PathBuf>>, Vec<PathBuf>)> = Vec::new();

            let reap_retired = |retired: &mut Vec<(Arc<Vec<PathBuf>>, Vec<PathBuf>)>| {
                retired.retain(|(old_manifest, files)| {
                    if Arc::strong_count(old_manifest) == 1 {
                        for path in files {
                            let _ = std::fs::remove_file(path);
                        }
                        false
                    } else {
                        true
                    }
                });
            };

            let run_compaction = |compactor: BackgroundCompactor,
                                  world_view: Arc<RwLock<Arc<WorldView>>>,
                                  output_id: u64| async move {
                let (files_to_compact, many_attr_ids) = {
                    let view = world_view.read();
                    (
                        view.volatile_files.as_ref().clone(),
                        view.schema.many_attribute_ids(),
                    )
                };

                if files_to_compact.len() < 2 {
                    return Ok::<Option<(Arc<Vec<PathBuf>>, Vec<PathBuf>)>, MesoError>(None);
                }

                let compacted_path = compactor
                    .compact_retaining_sources(&files_to_compact, output_id, &many_attr_ids)
                    .await?;

                let old_manifest = {
                    let mut writer = world_view.write();
                    let world_view_mut = Arc::make_mut(&mut *writer);
                    let prev_volatile = world_view_mut.volatile_files.clone();

                    let mut next_volatile = prev_volatile.as_ref().clone();
                    next_volatile.retain(|p| !files_to_compact.contains(p));
                    world_view_mut.volatile_files = Arc::new(next_volatile);

                    let compacted_mut = Arc::make_mut(&mut world_view_mut.compacted_files);
                    compacted_mut.push(compacted_path);

                    prev_volatile
                };

                Ok(Some((old_manifest, files_to_compact)))
            };

            while let Some(cmd) = flush_rx.recv().await {
                reap_retired(&mut retired_files);

                match cmd {
                    WorkerCommand::Flush { payload, ack } => {
                        let Some(FlushPayload {
                            max_tx_id,
                            evict_tx_ids,
                            batch,
                        }) = payload
                        else {
                            if let Some(tx) = ack {
                                let _ = tx.send(Ok(()));
                            }
                            continue;
                        };

                        let schema_snap = world_view.read().schema.clone();
                        let step_res: Result<()> = (|| {
                            Self::update_indexes_from_batch(
                                &mut now_index,
                                &mut bitmap_store,
                                &mut avt_index,
                                &schema_snap,
                                &batch,
                            )?;

                            // 1. Persist Tier-1/2/3 indexes first so Parquet presence guarantees index durability
                            now_index.flush()?;
                            bitmap_store.flush()?;
                            avt_index.flush()?;

                            // 2. Atomically write Parquet part file
                            let file_path = compactor.flush_to_parquet(batch, max_tx_id)?;

                            let new_now_snap = Arc::new(now_index.snapshot());
                            let new_bitmap_snap = Arc::new(bitmap_store.snapshot());
                            let new_avt_snap = Arc::new(avt_index.snapshot());

                            // 3. Atomically swap RAM batches, Parquet manifest, and index snapshots
                            {
                                let mut writer = world_view.write();
                                let world_view_mut = Arc::make_mut(&mut *writer);
                                let ram_vec_mut = Arc::make_mut(&mut world_view_mut.ram_batches);

                                ram_vec_mut.retain(|(id, _)| !evict_tx_ids.contains(id));

                                let mut remaining_dirty = ahash::AHashMap::new();
                                for (_, b) in ram_vec_mut.iter() {
                                    Self::record_dirty_batch(&mut remaining_dirty, b);
                                }

                                let vol_mut = Arc::make_mut(&mut world_view_mut.volatile_files);
                                vol_mut.push(file_path);

                                world_view_mut.dirty_entities = Arc::new(remaining_dirty);
                                world_view_mut.now_index = new_now_snap;
                                world_view_mut.bitmap_index = new_bitmap_snap;
                                world_view_mut.avt_index = new_avt_snap;
                            }

                            Ok(())
                        })();

                        if step_res.is_ok() {
                            last_flushed_tx_id = last_flushed_tx_id.max(max_tx_id);

                            // 4. Prune WAL entries already persisted to Parquet + indexes
                            let mut tx_guard = transactor.lock().await;
                            if let Err(e) = tx_guard.wal.checkpoint(max_tx_id) {
                                eprintln!(
                                    "Warning: WAL checkpoint failed at Tx {}: {:?}",
                                    max_tx_id, e
                                );
                            }
                        } else if let Err(ref e) = step_res {
                            eprintln!(
                                "Error flushing Tx {}; retaining RAM batches: {:?}",
                                max_tx_id, e
                            );
                        }

                        if let Some(tx) = ack {
                            let _ = tx.send(step_res);
                        }

                        let volatile_count = world_view.read().volatile_files.len();
                        if volatile_count >= config.compactor.backpressure_threshold {
                            match run_compaction(
                                compactor.clone(),
                                world_view.clone(),
                                last_flushed_tx_id,
                            )
                            .await
                            {
                                Ok(Some(retired)) => {
                                    retired_files.push(retired);
                                    reap_retired(&mut retired_files);
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    eprintln!(
                                        "Background compaction failed for Tx {}: {:?}",
                                        last_flushed_tx_id, e
                                    );
                                }
                            }
                        }
                    }
                    WorkerCommand::Compact { ack } => {
                        let res = match run_compaction(
                            compactor.clone(),
                            world_view.clone(),
                            last_flushed_tx_id,
                        )
                        .await
                        {
                            Ok(Some(retired)) => {
                                retired_files.push(retired);
                                reap_retired(&mut retired_files);
                                Ok(())
                            }
                            Ok(None) => Ok(()),
                            Err(e) => Err(e),
                        };
                        let _ = ack.send(res);
                    }
                    WorkerCommand::Shutdown { ack } => {
                        reap_retired(&mut retired_files);
                        let _ = ack.send(());
                        break;
                    }
                }
            }
        });
    }

    fn update_indexes_from_batch(
        now_index: &mut crate::btree::NowIndex,
        bitmap_store: &mut crate::bitmap::BitmapStore,
        avt_index: &mut crate::btree::AvtIndex,
        schema: &SchemaMap,
        batch: &RecordBatch,
    ) -> Result<()> {
        let e_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| MesoError::Serialization("Missing e column".into()))?;
        let a_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .ok_or_else(|| MesoError::Serialization("Missing a column".into()))?;
        let v_bool = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| MesoError::Serialization("Missing v_bool".into()))?;
        let v_int = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| MesoError::Serialization("Missing v_int".into()))?;
        let v_float = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .ok_or_else(|| MesoError::Serialization("Missing v_float".into()))?;
        let v_str = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| MesoError::Serialization("Missing v_str".into()))?;
        let v_ref = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .ok_or_else(|| MesoError::Serialization("Missing v_ref".into()))?;
        let v_time = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .ok_or_else(|| MesoError::Serialization("Missing v_time".into()))?;
        let v_uuid = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .ok_or_else(|| MesoError::Serialization("Missing v_uuid".into()))?;
        let op_col = batch
            .column(10)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| MesoError::Serialization("Missing op".into()))?;

        for i in 0..batch.num_rows() {
            let e = e_col.value(i);
            let a = a_col.value(i);
            let op = op_col.value(i);

            let val = if v_bool.is_valid(i) {
                Value::Boolean(v_bool.value(i))
            } else if v_int.is_valid(i) {
                Value::Int64(v_int.value(i))
            } else if v_float.is_valid(i) {
                Value::Float64(v_float.value(i))
            } else if v_str.is_valid(i) {
                Value::String(v_str.value(i).to_string())
            } else if v_ref.is_valid(i) {
                Value::Ref(v_ref.value(i))
            } else if v_time.is_valid(i) {
                Value::Timestamp(v_time.value(i))
            } else if v_uuid.is_valid(i) {
                let bytes: [u8; 16] = v_uuid
                    .value(i)
                    .try_into()
                    .map_err(|_| MesoError::Serialization("Invalid 16-byte UUID".into()))?;
                Value::Uuid(bytes)
            } else {
                continue;
            };

            let is_many = schema
                .get_by_id(a)
                .is_some_and(|attr| attr.cardinality == crate::schema::Cardinality::Many);

            if op {
                if !is_many {
                    let _ = now_index.put(e, a, &val);
                }
                let _ = bitmap_store.put(a, &val, e, true);
                let _ = avt_index.put(a, &val, e, true);
            } else {
                if !is_many {
                    let _ = now_index.delete(e, a);
                }
                let _ = bitmap_store.put(a, &val, e, false);
                let _ = avt_index.put(a, &val, e, false);
            }
        }
        Ok(())
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
    pub async fn transact_schema(
        &self,
        attributes: Vec<AttributeDefinition>,
    ) -> Result<Vec<Arc<Attribute>>> {
        let (added_attrs, sync_rx) = {
            let mut tx = self.transactor.lock().await;
            let mut added_attrs = Vec::with_capacity(attributes.len());

            for attr in attributes {
                let added = tx.transact_schema_with_cardinality(
                    &attr.ident,
                    attr.value_type,
                    attr.is_unique,
                    attr.cardinality,
                )?;
                added_attrs.push(added);
            }

            if !added_attrs.is_empty() {
                let mut writer = self.world_view.write();
                let world_view_mut = Arc::make_mut(&mut *writer);
                world_view_mut.schema = Arc::new(tx.schema.clone());
                world_view_mut.timeline = Arc::new(tx.timeline.clone());
            }

            (added_attrs, tx.take_sync_rx())
        };

        if let Some(rx) = sync_rx {
            let _ = rx.await;
        }

        Ok(added_attrs)
    }

    /// DRY helper that handles the lock, the transaction, and the lock-free WorldView pointer swap.
    async fn execute(&self, facts: Vec<Fact>, custom_now: Option<i64>) -> Result<TxReport> {
        let (report, sync_rx, sealed_batches) = {
            let mut tx = self.transactor.lock().await;
            let report = match custom_now {
                Some(t) => tx.transact_at(facts, t)?,
                None => tx.transact(facts)?,
            };

            let mut sealed_batches = None;

            if report.datoms_written > 0 {
                let batch_clone = report.batch.clone();
                let new_schema = Arc::new(tx.schema.clone());
                let new_timeline = Arc::new(tx.timeline.clone());

                {
                    let mut writer = self.world_view.write();

                    let world_view_mut = Arc::make_mut(&mut *writer);
                    world_view_mut.schema = new_schema;
                    world_view_mut.timeline = new_timeline;

                    let ram_vec_mut = Arc::make_mut(&mut world_view_mut.ram_batches);
                    ram_vec_mut.push((report.tx_id, batch_clone.clone()));

                    // Insert dirty (entity, attribute) pairs into WorldView
                    let dirty_mut = Arc::make_mut(&mut world_view_mut.dirty_entities);
                    Self::record_dirty_batch(dirty_mut, &batch_clone);
                }

                let mut pending = self.pending_flush.lock();
                pending.rows += batch_clone.num_rows();
                pending.bytes += batch_clone.get_array_memory_size();
                pending.batches.push((report.tx_id, batch_clone));

                if pending.rows >= tx.config.storage.memtable_max_rows
                    || pending.bytes >= tx.config.storage.memtable_max_bytes
                {
                    pending.rows = 0;
                    pending.bytes = 0;
                    sealed_batches = Some(std::mem::take(&mut pending.batches));
                }
            }

            (report, tx.take_sync_rx(), sealed_batches)
        };

        // 1. Await the physical disk sync outside the Transactor lock
        if let Some(rx) = sync_rx {
            let _ = rx.await;
        }

        // 2. Coalesce and dispatch sealed batches outside the lock
        if let Some(batches) = sealed_batches {
            let max_tx_id = batches.last().map_or(report.tx_id, |(id, _)| *id);
            let evict_tx_ids: Vec<u64> = batches.iter().map(|(id, _)| *id).collect();
            let coalesced = if batches.len() == 1 {
                batches.into_iter().next().unwrap().1
            } else {
                let schema = batches[0].1.schema();
                let raw_batches: Vec<RecordBatch> = batches.into_iter().map(|(_, b)| b).collect();
                arrow::compute::concat_batches(&schema, &raw_batches).map_err(MesoError::Arrow)?
            };

            let payload = FlushPayload {
                max_tx_id,
                evict_tx_ids,
                batch: coalesced,
            };

            if self
                .flush_tx
                .send(WorkerCommand::Flush {
                    payload: Some(payload),
                    ack: None,
                })
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
        if options.history
            || options.as_of.is_some()
            || options.rules.is_some()
            || ast.in_vars.is_some()
            || !options.inputs.is_empty()
        {
            return Ok(None);
        }

        let is_card_many = |a_id: crate::types::AttributeId| {
            view.schema
                .get_by_id(a_id)
                .is_some_and(|attr| attr.cardinality == crate::schema::Cardinality::Many)
        };

        // --- PATTERN 1 & 2: Single or Multi-Attribute EAV Point Lookup ---
        // Match [:find ?v1 ... :where [101 :attr1 ?v1] [101 :attr2 ?v2]]
        let is_eav_candidate = !ast.find.is_empty()
            && ast
                .find
                .iter()
                .all(|f| matches!(f, crate::ast::FindSpec::Variable(_)))
            && !ast.where_clauses.is_empty()
            && ast.where_clauses.iter().all(|w| {
                matches!(
                    w,
                    crate::ast::WhereClause::DataPattern {
                        tx: None,
                        op: None,
                        ..
                    }
                )
            });

        if is_eav_candidate {
            let mut target_entity = None;
            let mut clause_bindings = Vec::new();

            for clause in &ast.where_clauses {
                if let crate::ast::WhereClause::DataPattern { e, a, v, .. } = clause {
                    let e_id = match e {
                        crate::ast::Term::Integer(id) => *id as u64,
                        _ => return Ok(None), // Must be a constant Entity ID
                    };

                    if let Some(existing_e) = target_entity {
                        if existing_e != e_id {
                            return Ok(None); // All clauses in fast-path must target the same entity
                        }
                    } else {
                        target_entity = Some(e_id);
                    }

                    let a_ident = match a {
                        crate::ast::Term::Keyword(kw) => kw,
                        _ => return Ok(None),
                    };

                    let v_var = match v {
                        crate::ast::Term::Variable(var) => var,
                        _ => return Ok(None),
                    };

                    clause_bindings.push((a_ident, v_var));
                }
            }

            let e_id = match target_entity {
                Some(id) => id,
                None => return Ok(None),
            };

            // Direct B+Tree lookup
            let now_index = &view.now_index;
            let mut fields = Vec::new();
            let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

            for find_spec in &ast.find {
                if let crate::ast::FindSpec::Variable(find_var) = find_spec {
                    let binding = clause_bindings.iter().find(|(_, v_var)| *v_var == find_var);
                    let a_ident = match binding {
                        Some((a_kw, _)) => a_kw,
                        None => return Ok(None),
                    };

                    let a_id = match view.schema.get_id(a_ident) {
                        Some(id) => id,
                        None => return Ok(Some(vec![])), // Attribute doesn't exist
                    };

                    if is_card_many(a_id) || view.is_entity_attr_dirty(e_id, a_id) {
                        return Ok(None); // Let DataFusion resolve Cardinality::Many or pending RAM state
                    }

                    let clean_var = find_var.replace("?", "");
                    if let Some(val) = now_index.get(e_id, a_id)? {
                        let (field, array) = Self::val_to_arrow_column(&clean_var, &val);
                        fields.push(field);
                        arrays.push(array);
                    } else {
                        return Ok(Some(vec![])); // Value missing for entity
                    }
                }
            }

            let schema = Arc::new(arrow::datatypes::Schema::new(fields));
            let batch = RecordBatch::try_new(schema, arrays).map_err(MesoError::Arrow)?;
            return Ok(Some(vec![batch]));
        }

        // --- PATTERN 3: Inverted AVE Lookup ---
        // Match [:find ?e :where [?e :attr "literal_value"]]
        if ast.find.len() == 1
            && ast.where_clauses.len() == 1
            && let crate::ast::FindSpec::Variable(find_var) = &ast.find[0]
            && let crate::ast::WhereClause::DataPattern {
                e,
                a,
                v,
                tx: None,
                op: None,
            } = &ast.where_clauses[0]
            && let (crate::ast::Term::Variable(e_var), crate::ast::Term::Keyword(a_ident)) = (e, a)
            && e_var == find_var
        {
            let literal_val = match v {
                crate::ast::Term::String(s) => Some(Value::String(s.clone())),
                crate::ast::Term::Integer(i) => Some(Value::Int64(*i)),
                crate::ast::Term::Boolean(b) => Some(Value::Boolean(*b)),
                crate::ast::Term::Float(f) => Some(Value::Float64(*f)),
                _ => None,
            };

            if let Some(val) = literal_val
                && let Some(a_id) = view.schema.get_id(a_ident)
            {
                if view.is_attr_dirty(a_id) {
                    return Ok(None);
                }

                let bitmap_index = &view.bitmap_index;
                if let Some(treemap) = bitmap_index.get(a_id, &val)? {
                    let clean_var = find_var.replace("?", "");
                    let field = arrow::datatypes::Field::new(
                        &clean_var,
                        arrow::datatypes::DataType::UInt64,
                        true,
                    );

                    // Massive Upgrade: Return all matching entities at once via RoaringTreemap IntoIterator!
                    let entities: Vec<u64> = treemap.into_iter().collect();

                    let array = Arc::new(arrow::array::UInt64Array::from(entities));
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
                    let batch =
                        RecordBatch::try_new(schema, vec![array]).map_err(MesoError::Arrow)?;

                    return Ok(Some(vec![batch]));
                } else {
                    return Ok(Some(vec![]));
                }
            }
        }

        // --- PATTERN 4: Constant Entity Pull ---
        // Match [:find (pull 101 [:user/name :user/age]) :where ...]
        if ast.find.len() == 1
            && ast.where_clauses.len() == 1
            && options.format != OutputFormat::Tabular
            && let crate::ast::FindSpec::Pull(var, pattern) = &ast.find[0]
            && pattern
                .0
                .iter()
                .all(|attr| matches!(attr, crate::ast::PullAttribute::Simple(_)))
            && let crate::ast::WhereClause::DataPattern {
                e,
                tx: None,
                op: None,
                ..
            } = &ast.where_clauses[0]
            && let crate::ast::Term::Integer(e_id) = e
        {
            let entity_id = *e_id as u64;
            for pull_attr in &pattern.0 {
                if let crate::ast::PullAttribute::Simple(a_ident) = pull_attr
                    && let Some(a_id) = view.schema.get_id(a_ident)
                    && (is_card_many(a_id) || view.is_entity_attr_dirty(entity_id, a_id))
                {
                    return Ok(None);
                }
            }

            let now_index = &view.now_index;
            let clean_var = var.replace("?", "");

            // Direct zero-copy JSON/EDN builder for constant entity pull
            let mut map_pairs = Vec::new();

            for pull_attr in &pattern.0 {
                if let crate::ast::PullAttribute::Simple(a_ident) = pull_attr
                    && let Some(a_id) = view.schema.get_id(a_ident)
                    && let Some(val) = now_index.get(entity_id, a_id)?
                {
                    let formatted_val = match options.format {
                        OutputFormat::Json => match val {
                            Value::String(s) => format!("\"{}\"", s),
                            Value::Int64(i) => i.to_string(),
                            Value::Float64(f) => f.to_string(),
                            Value::Boolean(b) => b.to_string(),
                            Value::Ref(r) => r.to_string(),
                            _ => "null".to_string(),
                        },
                        _ => match val {
                            Value::String(s) => format!("\"{}\"", s),
                            _ => val.to_string(),
                        },
                    };
                    let key_name = if options.format == OutputFormat::Edn {
                        a_ident.clone()
                    } else {
                        a_ident.trim_start_matches(':').to_string()
                    };
                    map_pairs.push((key_name, formatted_val));
                }
            }

            let pull_json = if options.format == OutputFormat::Edn {
                let entry_strs: Vec<String> = map_pairs
                    .iter()
                    .map(|(k, v)| format!("{} {}", k, v))
                    .collect();
                format!("{{{}}}", entry_strs.join(" "))
            } else {
                let entry_strs: Vec<String> = map_pairs
                    .iter()
                    .map(|(k, v)| format!("\"{}\":{}", k, v))
                    .collect();
                format!("{{{}}}", entry_strs.join(","))
            };

            let field =
                arrow::datatypes::Field::new(&clean_var, arrow::datatypes::DataType::Utf8, true);
            let array = Arc::new(arrow::array::StringArray::from(vec![pull_json]));
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
            let batch = RecordBatch::try_new(schema, vec![array]).map_err(MesoError::Arrow)?;
            return Ok(Some(vec![batch]));
        }

        // --- PATTERN 5: Bitmap Intersection (Simple Relational Join) ---
        // Match [:find ?v2 :where [?e :attr1 "literal"] [?e :attr2 ?v2]]
        if ast.find.len() == 1
            && ast.where_clauses.len() == 2
            && let crate::ast::FindSpec::Variable(find_var) = &ast.find[0]
        {
            let mut literal_clause = None;
            let mut proj_clause = None;

            for clause in &ast.where_clauses {
                if let crate::ast::WhereClause::DataPattern {
                    e,
                    a,
                    v,
                    tx: None,
                    op: None,
                } = clause
                    && let (crate::ast::Term::Variable(e_var), crate::ast::Term::Keyword(a_kw)) =
                        (e, a)
                {
                    match v {
                        crate::ast::Term::Variable(v_var) => {
                            proj_clause = Some((e_var, a_kw, v_var))
                        }
                        _ => literal_clause = Some((e_var, a_kw, v)),
                    }
                }
            }

            if let (Some((e_var1, a_kw1, v_lit)), Some((e_var2, a_kw2, v_var))) =
                (literal_clause, proj_clause)
                && e_var1 == e_var2
                && v_var == find_var
            {
                let literal_val = match v_lit {
                    crate::ast::Term::String(s) => Some(Value::String(s.clone())),
                    crate::ast::Term::Integer(i) => Some(Value::Int64(*i)),
                    crate::ast::Term::Boolean(b) => Some(Value::Boolean(*b)),
                    crate::ast::Term::Float(f) => Some(Value::Float64(*f)),
                    _ => None,
                };

                if let Some(val) = literal_val
                    && let Some(a_id1) = view.schema.get_id(a_kw1)
                    && let Some(a_id2) = view.schema.get_id(a_kw2)
                    && !is_card_many(a_id2)
                    && !view.is_attr_dirty(a_id1)
                    && !view.is_attr_dirty(a_id2)
                    && let Some(treemap) = view.bitmap_index.get(a_id1, &val)?
                {
                    let mut results = Vec::new();
                    for e_id in treemap {
                        if let Some(proj_val) = view.now_index.get(e_id, a_id2)? {
                            results.push(proj_val);
                        }
                    }

                    if results.is_empty() {
                        return Ok(Some(vec![]));
                    }

                    let clean_var = find_var.replace("?", "");
                    let (field, array) = Self::vals_to_arrow_column(&clean_var, &results);
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
                    let batch =
                        RecordBatch::try_new(schema, vec![array]).map_err(MesoError::Arrow)?;

                    return Ok(Some(vec![batch]));
                } else {
                    return Ok(Some(vec![]));
                }
            }
        }

        // --- PATTERN 6: Entity ID Range Scan ---
        // Match [:find ?v :where [?e :attr ?v] [(>= ?e 100)] [(<= ?e 200)]]
        if ast.find.len() == 1
            && !ast.where_clauses.is_empty()
            && let crate::ast::FindSpec::Variable(find_var) = &ast.find[0]
        {
            let mut dp_clause = None;
            let mut funcs = Vec::new();
            let mut is_valid = true;

            for clause in &ast.where_clauses {
                match clause {
                    crate::ast::WhereClause::DataPattern {
                        e,
                        a,
                        v,
                        tx: None,
                        op: None,
                    } => {
                        if dp_clause.is_none() {
                            dp_clause = Some((e, a, v));
                        } else {
                            is_valid = false;
                        }
                    }
                    crate::ast::WhereClause::Function {
                        fn_name,
                        args,
                        binding: None,
                    } => {
                        funcs.push((fn_name, args));
                    }
                    _ => is_valid = false,
                }
            }

            if is_valid
                && !funcs.is_empty()
                && let Some((
                    crate::ast::Term::Variable(e_var),
                    crate::ast::Term::Keyword(a_ident),
                    crate::ast::Term::Variable(v_var),
                )) = dp_clause
                && (find_var == v_var || find_var == e_var)
            {
                let mut e_min = 0u64;
                let mut e_max = u64::MAX;
                let mut bounds_applied = false;

                for (fn_name, args) in funcs {
                    if args.len() == 2
                        && let crate::ast::Term::Variable(arg_var) = &args[0]
                        && arg_var == e_var
                        && let crate::ast::Term::Integer(limit) = &args[1]
                    {
                        let lim = *limit as u64;
                        match fn_name.as_str() {
                            ">" => e_min = e_min.max(lim.saturating_add(1)),
                            ">=" => e_min = e_min.max(lim),
                            "<" => e_max = e_max.min(lim.saturating_sub(1)),
                            "<=" => e_max = e_max.min(lim),
                            _ => is_valid = false,
                        }
                        bounds_applied = true;
                    } else {
                        is_valid = false;
                    }
                }

                if is_valid
                    && bounds_applied
                    && let Some(a_id) = view.schema.get_id(a_ident)
                    && !view.is_attr_dirty(a_id)
                    && (find_var == e_var || !is_card_many(a_id))
                {
                    let kvs = view.now_index.range_scan(e_min, e_max, a_id)?;
                    if kvs.is_empty() {
                        return Ok(Some(vec![]));
                    }

                    let clean_var = find_var.replace("?", "");

                    let (field, array) = if find_var == e_var {
                        let e_vec: Vec<u64> = kvs.into_iter().map(|(e, _)| e).collect();
                        (
                            arrow::datatypes::Field::new(
                                &clean_var,
                                arrow::datatypes::DataType::UInt64,
                                true,
                            ),
                            Arc::new(arrow::array::UInt64Array::from(e_vec))
                                as Arc<dyn arrow::array::Array>,
                        )
                    } else {
                        let v_vec: Vec<Value> = kvs.into_iter().map(|(_, v)| v).collect();
                        Self::vals_to_arrow_column(&clean_var, &v_vec)
                    };

                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
                    let batch =
                        RecordBatch::try_new(schema, vec![array]).map_err(MesoError::Arrow)?;
                    return Ok(Some(vec![batch]));
                }
            }
        }

        // --- PATTERN 7: Value Range Scan (AvtIndex) ---
        // Match [:find ?e :where [?e :attr ?v] [(>= ?v 100)] [(<= ?v 200)]]
        if ast.find.len() == 1
            && !ast.where_clauses.is_empty()
            && let crate::ast::FindSpec::Variable(find_var) = &ast.find[0]
        {
            let mut dp_clause = None;
            let mut funcs = Vec::new();
            let mut is_valid = true;

            for clause in &ast.where_clauses {
                match clause {
                    crate::ast::WhereClause::DataPattern {
                        e,
                        a,
                        v,
                        tx: None,
                        op: None,
                    } => {
                        if dp_clause.is_none() {
                            dp_clause = Some((e, a, v));
                        } else {
                            is_valid = false;
                        }
                    }
                    crate::ast::WhereClause::Function {
                        fn_name,
                        args,
                        binding: None,
                    } => {
                        funcs.push((fn_name, args));
                    }
                    _ => is_valid = false,
                }
            }

            if is_valid
                && !funcs.is_empty()
                && let Some((
                    crate::ast::Term::Variable(e_var),
                    crate::ast::Term::Keyword(a_ident),
                    crate::ast::Term::Variable(v_var),
                )) = dp_clause
                && (find_var == v_var || find_var == e_var)
            {
                let mut v_min = 0u64;
                let mut v_max = u64::MAX;
                let mut bounds_applied = false;
                let mut has_numeric_bound = false;
                let mut str_bounds: Vec<(&str, &str)> = Vec::new();

                for (fn_name, args) in funcs {
                    if args.len() == 2
                        && let crate::ast::Term::Variable(arg_var) = &args[0]
                        && arg_var == v_var
                    {
                        match &args[1] {
                            crate::ast::Term::Integer(i) => {
                                let lim = crate::page::encode_i64(*i);
                                match fn_name.as_str() {
                                    ">" => v_min = v_min.max(lim.saturating_add(1)),
                                    ">=" => v_min = v_min.max(lim),
                                    "<" => v_max = v_max.min(lim.saturating_sub(1)),
                                    "<=" => v_max = v_max.min(lim),
                                    _ => is_valid = false,
                                }
                                has_numeric_bound = true;
                                bounds_applied = true;
                            }
                            crate::ast::Term::Float(f) => {
                                let lim = crate::page::encode_f64(*f);
                                match fn_name.as_str() {
                                    ">" => v_min = v_min.max(lim.saturating_add(1)),
                                    ">=" => v_min = v_min.max(lim),
                                    "<" => v_max = v_max.min(lim.saturating_sub(1)),
                                    "<=" => v_max = v_max.min(lim),
                                    _ => is_valid = false,
                                }
                                has_numeric_bound = true;
                                bounds_applied = true;
                            }
                            crate::ast::Term::String(s) => {
                                let lim = crate::page::encode_bytes_prefix(s.as_bytes());
                                match fn_name.as_str() {
                                    ">" | ">=" => v_min = v_min.max(lim),
                                    "<" | "<=" => v_max = v_max.min(lim),
                                    _ => is_valid = false,
                                }
                                str_bounds.push((fn_name.as_str(), s.as_str()));
                                bounds_applied = true;
                            }
                            _ => is_valid = false,
                        }
                    } else {
                        is_valid = false;
                    }
                }

                if has_numeric_bound && !str_bounds.is_empty() {
                    is_valid = false;
                }

                if is_valid
                    && bounds_applied
                    && let Some(a_id) = view.schema.get_id(a_ident)
                    && !view.is_attr_dirty(a_id)
                    && (find_var == e_var || !is_card_many(a_id))
                {
                    let candidate_ids = view.avt_index.range_scan(a_id, v_min, v_max)?;
                    if candidate_ids.is_empty() {
                        return Ok(Some(vec![]));
                    }

                    let clean_var = find_var.replace("?", "");

                    let (field, array) = if !str_bounds.is_empty() {
                        let mut matched_e = Vec::new();
                        let mut matched_v = Vec::new();

                        for e_id in candidate_ids {
                            if let Some(Value::String(val_str)) = view.now_index.get(e_id, a_id)? {
                                let passes = str_bounds.iter().all(|(op, bound)| match *op {
                                    ">" => val_str.as_str() > *bound,
                                    ">=" => val_str.as_str() >= *bound,
                                    "<" => val_str.as_str() < *bound,
                                    "<=" => val_str.as_str() <= *bound,
                                    _ => false,
                                });
                                if passes {
                                    matched_e.push(e_id);
                                    if find_var != e_var {
                                        matched_v.push(Value::String(val_str));
                                    }
                                }
                            }
                        }

                        if matched_e.is_empty() {
                            return Ok(Some(vec![]));
                        }

                        if find_var == e_var {
                            (
                                arrow::datatypes::Field::new(
                                    &clean_var,
                                    arrow::datatypes::DataType::UInt64,
                                    true,
                                ),
                                Arc::new(arrow::array::UInt64Array::from(matched_e))
                                    as Arc<dyn arrow::array::Array>,
                            )
                        } else {
                            Self::vals_to_arrow_column(&clean_var, &matched_v)
                        }
                    } else if find_var == e_var {
                        (
                            arrow::datatypes::Field::new(
                                &clean_var,
                                arrow::datatypes::DataType::UInt64,
                                true,
                            ),
                            Arc::new(arrow::array::UInt64Array::from(candidate_ids))
                                as Arc<dyn arrow::array::Array>,
                        )
                    } else {
                        let mut v_vec = Vec::with_capacity(candidate_ids.len());
                        for e_id in candidate_ids {
                            if let Some(val) = view.now_index.get(e_id, a_id)? {
                                v_vec.push(val);
                            }
                        }
                        Self::vals_to_arrow_column(&clean_var, &v_vec)
                    };

                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
                    let batch =
                        RecordBatch::try_new(schema, vec![array]).map_err(MesoError::Arrow)?;
                    return Ok(Some(vec![batch]));
                }
            }
        }

        Ok(None)
    }

    fn val_to_arrow_column(
        name: &str,
        val: &Value,
    ) -> (arrow::datatypes::Field, Arc<dyn arrow::array::Array>) {
        match val {
            Value::String(s) => (
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
                Arc::new(arrow::array::StringArray::from(vec![s.clone()])),
            ),
            Value::Int64(i) => (
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Int64, true),
                Arc::new(arrow::array::Int64Array::from(vec![*i])),
            ),
            Value::Float64(f) => (
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Float64, true),
                Arc::new(arrow::array::Float64Array::from(vec![*f])),
            ),
            Value::Boolean(b) => (
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Boolean, true),
                Arc::new(arrow::array::BooleanArray::from(vec![*b])),
            ),
            Value::Ref(r) => (
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::UInt64, true),
                Arc::new(arrow::array::UInt64Array::from(vec![*r])),
            ),
            Value::Timestamp(t) => (
                arrow::datatypes::Field::new(
                    name,
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        None,
                    ),
                    true,
                ),
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![*t])),
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
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
                    Arc::new(arrow::array::StringArray::from(vec![uuid_str])),
                )
            }
        }
    }

    fn vals_to_arrow_column(
        name: &str,
        vals: &[Value],
    ) -> (arrow::datatypes::Field, Arc<dyn arrow::array::Array>) {
        if vals.is_empty() {
            return (
                arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
                Arc::new(arrow::array::StringArray::from(Vec::<String>::new())),
            );
        }
        match &vals[0] {
            Value::String(_) => {
                let vec: Vec<String> = vals
                    .iter()
                    .map(|v| {
                        if let Value::String(s) = v {
                            s.clone()
                        } else {
                            "".to_string()
                        }
                    })
                    .collect();
                (
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
                    Arc::new(arrow::array::StringArray::from(vec)),
                )
            }
            Value::Int64(_) => {
                let vec: Vec<i64> = vals
                    .iter()
                    .map(|v| if let Value::Int64(i) = v { *i } else { 0 })
                    .collect();
                (
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Int64, true),
                    Arc::new(arrow::array::Int64Array::from(vec)),
                )
            }
            Value::Float64(_) => {
                let vec: Vec<f64> = vals
                    .iter()
                    .map(|v| if let Value::Float64(f) = v { *f } else { 0.0 })
                    .collect();
                (
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Float64, true),
                    Arc::new(arrow::array::Float64Array::from(vec)),
                )
            }
            Value::Boolean(_) => {
                let vec: Vec<bool> = vals
                    .iter()
                    .map(|v| if let Value::Boolean(b) = v { *b } else { false })
                    .collect();
                (
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Boolean, true),
                    Arc::new(arrow::array::BooleanArray::from(vec)),
                )
            }
            Value::Ref(_) => {
                let vec: Vec<u64> = vals
                    .iter()
                    .map(|v| if let Value::Ref(r) = v { *r } else { 0 })
                    .collect();
                (
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::UInt64, true),
                    Arc::new(arrow::array::UInt64Array::from(vec)),
                )
            }
            Value::Timestamp(_) => {
                let vec: Vec<i64> = vals
                    .iter()
                    .map(|v| if let Value::Timestamp(t) = v { *t } else { 0 })
                    .collect();
                (
                    arrow::datatypes::Field::new(
                        name,
                        arrow::datatypes::DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Microsecond,
                            None,
                        ),
                        true,
                    ),
                    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec)),
                )
            }
            Value::Uuid(_) => {
                let vec: Vec<String> = vals.iter().map(|v| if let Value::Uuid(u) = v { format!("{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}", u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]) } else { "".to_string() }).collect();
                (
                    arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
                    Arc::new(arrow::array::StringArray::from(vec)),
                )
            }
        }
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

        let arrow_schema = view.ram_batches.first().unwrap().1.schema();
        let pq_options =
            datafusion::prelude::ParquetReadOptions::default().schema(arrow_schema.as_ref());

        // 1. Read Physical Isolated Streams from the atomic WorldView manifest
        let compacted_paths: Vec<String> = view
            .compacted_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        let volatile_paths: Vec<String> = view
            .volatile_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

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
            .iter()
            .map(|(_, b)| b.clone())
            .filter(|b| b.num_rows() > 0)
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

            let many_attr_ids = view.schema.many_attribute_ids();
            if many_attr_ids.is_empty() {
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
            } else {
                let in_list = many_attr_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    r#"
                    WITH
                    volatile_filtered AS (
                        SELECT * FROM volatile_datoms {vol_filter}
                    ),
                    volatile_bounds AS (
                        SELECT *,
                            CASE WHEN a IN ({in_list}) THEN
                                LEAD(valid_from) OVER (
                                    PARTITION BY e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid
                                    ORDER BY valid_from ASC, t ASC, op ASC
                                )
                            ELSE
                                LEAD(valid_from) OVER (
                                    PARTITION BY e, a
                                    ORDER BY valid_from ASC, t ASC, op ASC
                                )
                            END as next_from
                        FROM volatile_filtered
                    ),
                    active_volatile AS (
                        SELECT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from, next_from, COALESCE(next_from, valid_to) as valid_to
                        FROM volatile_bounds
                        WHERE op = true {active_vol_filter}
                    ),
                    volatile_mask_one AS (
                        SELECT DISTINCT e, a FROM volatile_filtered WHERE a NOT IN ({in_list})
                    ),
                    volatile_mask_many AS (
                        SELECT DISTINCT e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid
                        FROM volatile_filtered WHERE a IN ({in_list})
                    ),
                    surviving_compacted AS (
                        SELECT c.e, c.a, c.v_bool, c.v_int, c.v_float, c.v_str, c.v_ref, c.v_time, c.v_uuid, c.t, c.op, c.valid_from, (CASE WHEN false THEN c.valid_from ELSE NULL END) as next_from, c.valid_to
                        FROM compacted_datoms c
                        WHERE c.op = true {comp_filter}
                          AND (
                              (c.a NOT IN ({in_list}) AND NOT EXISTS (SELECT 1 FROM volatile_mask_one m WHERE m.e = c.e AND m.a = c.a))
                              OR
                              (c.a IN ({in_list}) AND NOT EXISTS (
                                  SELECT 1 FROM volatile_mask_many m
                                  WHERE m.e = c.e AND m.a = c.a
                                    AND (m.v_bool = c.v_bool OR (m.v_bool IS NULL AND c.v_bool IS NULL))
                                    AND (m.v_int = c.v_int OR (m.v_int IS NULL AND c.v_int IS NULL))
                                    AND (m.v_float = c.v_float OR (m.v_float IS NULL AND c.v_float IS NULL))
                                    AND (m.v_str = c.v_str OR (m.v_str IS NULL AND c.v_str IS NULL))
                                    AND (m.v_ref = c.v_ref OR (m.v_ref IS NULL AND c.v_ref IS NULL))
                                    AND (m.v_time = c.v_time OR (m.v_time IS NULL AND c.v_time IS NULL))
                                    AND (m.v_uuid = c.v_uuid OR (m.v_uuid IS NULL AND c.v_uuid IS NULL))
                              ))
                          )
                    )
                    SELECT * FROM surviving_compacted
                    UNION ALL
                    SELECT * FROM active_volatile
                    "#
                )
            }
        };

        let resolved_df = ctx
            .sql(&resolved_sql)
            .await
            .map_err(MesoError::DataFusion)?;
        ctx.register_table("resolved_datoms", resolved_df.into_view())?;

        let planner = QueryPlanner::new(
            &ctx,
            &view,
            "resolved_datoms",
            options.format.clone(),
            options.as_of,
            ruleset,
            &options.inputs,
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

    pub async fn query_edn_with_options(
        &self,
        query_str: &str,
        mut options: QueryOptions,
    ) -> Result<String> {
        let ast = parser::parse_query(query_str)?;
        options.format = OutputFormat::Edn; // Enforce EDN for this pipeline
        let batches = self.query_with_options(query_str, options).await?;
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

    fn seal_pending_flush(&self) -> Result<Option<FlushPayload>> {
        let batches = {
            let mut pending = self.pending_flush.lock();
            if pending.batches.is_empty() {
                return Ok(None);
            }
            pending.rows = 0;
            pending.bytes = 0;
            std::mem::take(&mut pending.batches)
        };

        let max_tx_id = batches.last().map_or(0, |(id, _)| *id);
        let evict_tx_ids: Vec<u64> = batches.iter().map(|(id, _)| *id).collect();
        let coalesced = if batches.len() == 1 {
            batches.into_iter().next().unwrap().1
        } else {
            let schema = batches[0].1.schema();
            let raw_batches: Vec<RecordBatch> = batches.into_iter().map(|(_, b)| b).collect();
            arrow::compute::concat_batches(&schema, &raw_batches).map_err(MesoError::Arrow)?
        };

        Ok(Some(FlushPayload {
            max_tx_id,
            evict_tx_ids,
            batch: coalesced,
        }))
    }

    /// Forces all pending RAM batches to flush to disk and updates Tier-1/2/3 indexes.
    pub async fn flush(&self) -> Result<()> {
        let payload = self.seal_pending_flush()?;
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.flush_tx
            .send(WorkerCommand::Flush {
                payload,
                ack: Some(ack_tx),
            })
            .await
            .map_err(|_| MesoError::Serialization("Background worker disconnected".into()))?;

        ack_rx
            .await
            .map_err(|_| MesoError::Serialization("Background flush aborted".into()))?
    }

    /// Flushes any pending RAM batches and runs Option A bitemporal compaction on volatile Parquet files.
    pub async fn compact(&self) -> Result<()> {
        self.flush().await?;
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.flush_tx
            .send(WorkerCommand::Compact { ack: ack_tx })
            .await
            .map_err(|_| MesoError::Serialization("Background worker disconnected".into()))?;

        ack_rx
            .await
            .map_err(|_| MesoError::Serialization("Background compaction aborted".into()))?
    }

    /// Flushes all pending data and cleanly terminates the background worker.
    pub async fn shutdown(&self) -> Result<()> {
        self.flush().await?;
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        if self
            .flush_tx
            .send(WorkerCommand::Shutdown { ack: ack_tx })
            .await
            .is_ok()
        {
            let _ = ack_rx.await;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct WorldView {
    pub ram_batches: Arc<Vec<(u64, RecordBatch)>>,
    pub compacted_files: Arc<Vec<PathBuf>>,
    pub volatile_files: Arc<Vec<PathBuf>>,
    pub data_dir: PathBuf,
    pub schema: Arc<SchemaMap>,
    pub timeline: Arc<SchemaTimeline>,
    pub now_index: Arc<crate::btree::NowIndexSnapshot>,
    pub avt_index: Arc<crate::btree::AvtIndexSnapshot>,
    pub bitmap_index: Arc<crate::bitmap::BitmapSnapshot>,
    pub dirty_entities:
        Arc<ahash::AHashMap<crate::types::AttributeId, ahash::AHashSet<crate::types::EntityId>>>,
}

impl WorldView {
    #[inline(always)]
    pub fn is_entity_attr_dirty(
        &self,
        e: crate::types::EntityId,
        a: crate::types::AttributeId,
    ) -> bool {
        self.dirty_entities
            .get(&a)
            .is_some_and(|entities| entities.contains(&e))
    }

    #[inline(always)]
    pub fn is_attr_dirty(&self, a: crate::types::AttributeId) -> bool {
        self.dirty_entities
            .get(&a)
            .is_some_and(|entities| !entities.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Arrow,
    Edn,
    Tabular,
    Json,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryInput {
    Scalar(Value),
    Collection(Vec<Value>),
    Tuple(Vec<Value>),
    Relation(Vec<Vec<Value>>),
}

#[derive(Debug, Default, Clone)]
pub struct QueryOptions {
    pub as_of: Option<i64>,
    pub format: OutputFormat,
    pub rules: Option<String>,
    pub history: bool,
    pub inputs: Vec<QueryInput>,
}

#[derive(Debug, Clone)]
pub struct AttributeDefinition {
    pub ident: String,
    pub value_type: ValueType,
    pub is_unique: bool,
    pub cardinality: crate::schema::Cardinality,
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

        let db = MesoDb::open(db_path, schema, Config::default()).unwrap();

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

        let db = MesoDb::open(db_path, schema, Config::default()).unwrap();

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
        let db = MesoDb::open(
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
            inputs: Vec::new(),
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
        let db = MesoDb::open(
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
            inputs: Vec::new(),
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
            inputs: Vec::new(),
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

        let db = MesoDb::open(dir.path().join("aggr.db"), schema, Config::default()).unwrap();

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

        let db = MesoDb::open(dir.path().join("rules.db"), schema, Config::default()).unwrap();

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

    async fn setup_api_db() -> MesoDb {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);
        schema.add_attribute(":user/active", ValueType::Boolean, false);
        schema.add_attribute(":user/address", ValueType::Ref, false);
        schema.add_attribute(":address/city", ValueType::String, false);

        let db = MesoDb::open(dir.path().join("api.db"), schema, Config::default()).unwrap();
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
        let db = MesoDb::open(
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
                cardinality: crate::schema::Cardinality::One,
            },
            AttributeDefinition {
                ident: ":product/price".into(),
                value_type: ValueType::Float64,
                is_unique: false,
                cardinality: crate::schema::Cardinality::One,
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

        let db = MesoDb::open(dir.path().join("history.db"), schema, Config::default()).unwrap();

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
        let db = MesoDb::open(dir.path().join("pit1.db"), schema, Config::default()).unwrap();

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
        let db = MesoDb::open(dir.path().join("pit2.db"), schema, Config::default()).unwrap();

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
        let db = MesoDb::open(dir.path().join("at1.db"), schema, Config::default()).unwrap();

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
        let db = MesoDb::open(dir.path().join("at2.db"), schema, Config::default()).unwrap();

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

    async fn setup_history_db(name: &str) -> MesoDb {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":doc/title", ValueType::String, false);

        let db = MesoDb::open(dir.path().join(name), schema, Config::default()).unwrap();

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

        let db = MesoDb::open(dir.path().join("or_logic.db"), schema, Config::default()).unwrap();

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

        let db = MesoDb::open(dir.path().join("not_logic.db"), schema, Config::default()).unwrap();

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

        let db = MesoDb::open(dir.path().join("reified.db"), schema, Config::default()).unwrap();

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
            MesoDb::open(
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
            new_ram.retain(|(id, _)| *id != initial_tx_id);

            *writer = Arc::new(WorldView {
                ram_batches: Arc::new(new_ram),
                data_dir: current_view.data_dir.clone(),
                schema: current_view.schema.clone(),
                timeline: current_view.timeline.clone(),
                now_index: current_view.now_index.clone(),
                avt_index: current_view.avt_index.clone(),
                bitmap_index: current_view.bitmap_index.clone(),
                dirty_entities: current_view.dirty_entities.clone(),
                compacted_files: current_view.compacted_files.clone(),
                volatile_files: current_view.volatile_files.clone(),
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
            !final_view
                .ram_batches
                .iter()
                .any(|(id, _)| id == &initial_tx_id),
            "Background removal should succeed"
        );
        assert!(
            final_view
                .ram_batches
                .iter()
                .any(|(k, _)| *k > initial_tx_id),
            "Concurrent transaction insertion should succeed"
        );
    }

    #[tokio::test]
    async fn test_snapshot_read_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(
            MesoDb::open(
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
            MesoDb::open(
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
            MesoDb::open(
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
            new_ram.retain(|(id, _)| *id != tx_id);

            *writer = Arc::new(WorldView {
                ram_batches: Arc::new(new_ram),
                data_dir: current_view.data_dir.clone(),
                schema: current_view.schema.clone(),
                timeline: current_view.timeline.clone(),
                now_index: current_view.now_index.clone(),
                avt_index: current_view.avt_index.clone(),
                bitmap_index: current_view.bitmap_index.clone(),
                dirty_entities: current_view.dirty_entities.clone(),
                compacted_files: current_view.compacted_files.clone(),
                volatile_files: current_view.volatile_files.clone(),
            });
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let attr_def = AttributeDefinition {
            ident: ":new/racing_attr".into(),
            value_type: ValueType::String,
            is_unique: false,
            cardinality: crate::schema::Cardinality::One,
        };
        db.transact_schema(vec![attr_def]).await.unwrap();

        compactor_handle.await.unwrap();

        let final_view = db.world_view.read().clone();
        assert!(
            !final_view.ram_batches.iter().any(|(id, _)| id == &tx_id),
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
        let db = MesoDb::open(
            dir.path().join("pl_ghost.db"),
            SchemaMap::new(),
            Config::default(),
        )
        .unwrap();

        // Emulate the background compactor trying to remove a batch that doesn't exist
        {
            let writer = db.world_view.write();
            let current_view = writer.as_ref().clone();
            let new_ram = current_view.ram_batches.as_ref().clone();

            let pos = new_ram.iter().position(|(id, _)| *id == 9999);
            if pos.is_none() {
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
            MesoDb::open(
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
        let db = MesoDb::open(
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

        let db = MesoDb::open(db_path, schema, config).unwrap();

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

    // =====================================================================
    // PHASE 1: EXPANDED FAST-PATH ROUTER TESTS
    // =====================================================================

    #[tokio::test]
    async fn test_fast_path_multi_attribute_eav_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let db = MesoDb::open(dir.path().join("fp_multi.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            Fact {
                e: 101,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 101,
                ident: ":user/age".into(),
                v: Value::Int64(30),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // Wait for background worker to flush to index
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let query = r#"[:find ?n ?a :where [101 :user/name ?n] [101 :user/age ?a]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        let n_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let a_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(n_col.value(0), "Alice");
        assert_eq!(a_col.value(0), 30);
    }

    #[tokio::test]
    async fn test_fast_path_ave_inverted_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/email", ValueType::String, true);

        let db = MesoDb::open(dir.path().join("fp_ave.db"), schema, Config::default()).unwrap();

        db.transact(vec![Fact {
            e: 88,
            ident: ":user/email".into(),
            v: Value::String("target@example.com".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        // Wait for background worker to update AveIndex
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let query = r#"[:find ?e :where [?e :user/email "target@example.com"]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        let e_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(e_col.value(0), 88);
    }

    #[tokio::test]
    async fn test_fast_path_constant_entity_pull() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let mut config = Config::default();
        config.storage.memtable_max_rows = 1;
        config.compactor.backpressure_threshold = 1;

        let db = MesoDb::open(dir.path().join("fp_pull.db"), schema, config).unwrap();

        db.transact(vec![
            Fact {
                e: 42,
                ident: ":user/name".into(),
                v: Value::String("Bob".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 42,
                ident: ":user/age".into(),
                v: Value::Int64(45),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let query = r#"[:find (pull ?e [:user/name :user/age]) :where [42 :user/name _]]"#;
        let json_res = db.query_json(query).await.unwrap();

        assert!(json_res.contains(r#""user/name":"Bob""#));
        assert!(json_res.contains(r#""user/age":45"#));
    }

    #[tokio::test]
    async fn test_fast_path_ram_dirty_set_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/score", ValueType::Int64, false);

        let db = MesoDb::open(dir.path().join("fp_dirty.db"), schema, Config::default()).unwrap();

        // Fact in RAM (Not yet flushed to index)
        db.transact(vec![Fact {
            e: 777,
            ident: ":user/score".into(),
            v: Value::Int64(9000),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        // 1. Immediate query when entity is in dirty set -> Must fall back to DataFusion safely and find result
        let query = r#"[:find ?s :where [777 :user/score ?s]]"#;
        let results_ram = db.query(query).await.unwrap();
        let total_rows_ram: usize = results_ram.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows_ram, 1);

        // 2. Wait for flush -> Dirty set clears, fast-path executes from B+Tree directly
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let results_disk = db.query(query).await.unwrap();
        let total_rows_disk: usize = results_disk.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows_disk, 1);
    }

    #[tokio::test]
    async fn test_fast_path_bitmap_intersection_join() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/status", ValueType::String, false);
        schema.add_attribute(":user/name", ValueType::String, false);

        let db = MesoDb::open(dir.path().join("fp_join.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            Fact {
                e: 200,
                ident: ":user/status".into(),
                v: Value::String("active".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 200,
                ident: ":user/name".into(),
                v: Value::String("ActiveAlice".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 201,
                ident: ":user/status".into(),
                v: Value::String("inactive".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 201,
                ident: ":user/name".into(),
                v: Value::String("InactiveBob".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // Wait for background worker to flush RAM to the B-Tree / Bitmap indices
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let query = r#"[:find ?name :where [?e :user/status "active"] [?e :user/name ?name]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(
            batch.num_rows(),
            1,
            "Should bypass DataFusion and return exactly one row natively"
        );

        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        assert_eq!(name_col.value(0), "ActiveAlice");
    }

    #[tokio::test]
    async fn test_fast_path_entity_range_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":sys/log", ValueType::String, false);

        // FIX: Force immediate background flushes to clear RAM and enable the Fast-Path
        let mut config = Config::default();
        config.storage.memtable_max_rows = 1;
        config.compactor.backpressure_threshold = 1;

        let db = MesoDb::open(dir.path().join("fp_range.db"), schema, config).unwrap();

        let mut facts = Vec::new();
        for i in 100..=105 {
            facts.push(Fact {
                e: i,
                ident: ":sys/log".into(),
                v: Value::String(format!("Log {}", i)),
                op: true,
                cas_old_v: None,
                valid_time: None,
            });
        }
        db.transact(facts).await.unwrap();

        // Flush RAM state to the native B-Tree index
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let query = r#"[:find ?v :where [?e :sys/log ?v] [(>= ?e 102)] [(< ?e 105)]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(
            results.len(),
            1,
            "Should bypass DataFusion and return exactly one row natively"
        );
        let batch = &results[0];

        // Excludes 100, 101, and 105
        assert_eq!(
            batch.num_rows(),
            3,
            "Range scan should cleanly fetch 102, 103, and 104 natively"
        );

        let val_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(val_col.value(0), "Log 102");
        assert_eq!(val_col.value(2), "Log 104");
    }

    #[tokio::test]
    async fn test_fast_path_value_range_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":item/price", ValueType::Int64, false);

        // Force background syncs immediately
        let mut config = Config::default();
        config.storage.memtable_max_rows = 1;
        config.compactor.backpressure_threshold = 1;

        let db = MesoDb::open(dir.path().join("fp_val_range.db"), schema, config).unwrap();

        let mut facts = Vec::new();
        for i in 1..=5 {
            facts.push(Fact {
                e: i,
                ident: ":item/price".into(),
                v: Value::Int64(i as i64 * 10), // Prices: 10, 20, 30, 40, 50
                op: true,
                cas_old_v: None,
                valid_time: None,
            });
        }
        db.transact(facts).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let query =
            r#"[:find ?e :where [?e :item/price ?price] [(>= ?price 20)] [(<= ?price 40)]]"#;
        let results = db.query(query).await.unwrap();

        assert_eq!(results.len(), 1, "Should bypass DataFusion natively");
        let batch = &results[0];

        // Excludes 10 and 50
        assert_eq!(
            batch.num_rows(),
            3,
            "Range scan should fetch 20, 30, and 40"
        );

        let e_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();

        let mut e_ids = vec![e_col.value(0), e_col.value(1), e_col.value(2)];
        e_ids.sort();
        // Since price = e * 10, target entities are 2, 3, and 4
        assert_eq!(e_ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_cumulative_memtable_row_threshold_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":item/val", ValueType::Int64, false);

        let mut config = Config::default();
        config.storage.memtable_max_rows = 3;
        config.compactor.backpressure_threshold = 10;

        let db = MesoDb::open(dir.path().join("cum_rows.db"), schema, config).unwrap();

        // Three 1-row transactions: individually below threshold (3), cumulatively hit 3
        for i in 1..=3 {
            db.transact(vec![Fact {
                e: i,
                ident: ":item/val".into(),
                v: Value::Int64(i as i64 * 10),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }])
            .await
            .unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let view = db.world_view.read().clone();
        assert_eq!(
            view.ram_batches.len(),
            1,
            "All 3 cumulative batches should be evicted from RAM once threshold is reached"
        );
        assert!(
            view.dirty_entities.is_empty(),
            "Dirty entities should clear after cumulative flush"
        );

        let res = db
            .query_native(r#"[:find ?v :where [2 :item/val ?v]]"#)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].get("v"), Some(&Value::Int64(20)));
    }

    #[tokio::test]
    async fn test_cumulative_memtable_byte_threshold_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":doc/body", ValueType::String, false);

        let mut config = Config::default();
        config.storage.memtable_max_rows = 100_000;
        config.storage.memtable_max_bytes = 2048; // 2 KB byte cap
        config.compactor.backpressure_threshold = 10;

        let db = MesoDb::open(dir.path().join("cum_bytes.db"), schema, config).unwrap();

        let large_payload = "x".repeat(1500);
        for i in 1..=2 {
            db.transact(vec![Fact {
                e: i,
                ident: ":doc/body".into(),
                v: Value::String(large_payload.clone()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }])
            .await
            .unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let view = db.world_view.read().clone();
        assert_eq!(
            view.ram_batches.len(),
            1,
            "Byte threshold should trigger flush and RAM eviction even when row count is low"
        );
        assert!(view.dirty_entities.is_empty());
    }

    #[tokio::test]
    async fn test_startup_wal_recovery_populates_tier1_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("wal_idx_recovery.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        // Keep memtable large so data stays only in the WAL before drop
        let mut config = Config::default();
        config.storage.memtable_max_rows = 50_000;

        {
            let db = MesoDb::open(&db_path, schema.clone(), config.clone()).unwrap();
            db.transact(vec![Fact {
                e: 55,
                ident: ":user/name".into(),
                v: Value::String("RecoveredAlice".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }])
            .await
            .unwrap();
        }

        // Reopen and allow background worker to index the WAL-recovered batch
        let recovered_db = MesoDb::open(&db_path, schema, config).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let view = recovered_db.world_view.read().clone();
        assert!(
            view.dirty_entities.is_empty(),
            "Recovered batch should be indexed and evicted from dirty_entities"
        );
        assert_eq!(
            view.now_index.get(55, 100).unwrap(),
            Some(Value::String("RecoveredAlice".into())),
            "NowIndex must be populated from WAL recovery"
        );

        let res = recovered_db
            .query_native(r#"[:find ?n :where [55 :user/name ?n]]"#)
            .await
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(
            res[0].get("n"),
            Some(&Value::String("RecoveredAlice".into()))
        );
    }

    #[tokio::test]
    async fn test_wal_checkpoint_and_idempotent_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("wal_checkpoint.db");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        let config = Config::default();
        let initial_wal_len;

        {
            let db = MesoDb::open(&db_path, schema.clone(), config.clone()).unwrap();
            for i in 1..=20 {
                db.transact(vec![Fact {
                    e: i,
                    ident: ":user/name".into(),
                    v: Value::String(format!("User_{}", i)),
                    op: true,
                    cas_old_v: None,
                    valid_time: None,
                }])
                .await
                .unwrap();
            }

            let pre_flush_len = std::fs::metadata(&db_path).unwrap().len();
            assert!(pre_flush_len > 0);

            // Explicit flush persists Parquet + Tier-1 indexes and checkpoints the WAL
            db.flush().await.unwrap();
            initial_wal_len = std::fs::metadata(&db_path).unwrap().len();
            assert!(
                initial_wal_len < pre_flush_len,
                "WAL should shrink after checkpointing flushed transactions"
            );

            db.shutdown().await.unwrap();
        }

        // Reopen and verify zero duplicate rows in history or current queries
        let reopened = MesoDb::open(&db_path, schema, config).unwrap();
        let hist = reopened
            .history_native(r#"[:find ?v ?op :where [10 :user/name ?v _ ?op]]"#)
            .await
            .unwrap();
        assert_eq!(
            hist.len(),
            1,
            "Restart must not duplicate already-persisted Parquet datoms"
        );
        assert_eq!(hist[0].get("v"), Some(&Value::String("User_10".into())));
    }

    #[tokio::test]
    async fn test_explicit_compact_and_atomic_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":order/total", ValueType::Int64, false);

        let mut config = Config::default();
        config.compactor.backpressure_threshold = 100; // Prevent auto-compaction

        let db = MesoDb::open(dir.path().join("explicit_compact.db"), schema, config).unwrap();

        for i in 1..=3 {
            db.transact(vec![Fact {
                e: i,
                ident: ":order/total".into(),
                v: Value::Int64(i as i64 * 100),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }])
            .await
            .unwrap();
            db.flush().await.unwrap();
        }

        assert_eq!(db.world_view.read().volatile_files.len(), 3);
        assert_eq!(db.world_view.read().compacted_files.len(), 0);

        db.compact().await.unwrap();

        let view = db.world_view.read().clone();
        assert_eq!(view.volatile_files.len(), 0);
        assert_eq!(view.compacted_files.len(), 1);

        let res = db
            .query_native(r#"[:find (sum ?t) :where [?e :order/total ?t]]"#)
            .await
            .unwrap();
        assert_eq!(res[0].get("sum_t"), Some(&Value::Int64(600)));
    }

    #[tokio::test]
    async fn test_attribute_scoped_dirty_tracking_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/status", ValueType::String, false);

        // Large threshold so subsequent writes stay in RAM
        let mut config = Config::default();
        config.storage.memtable_max_rows = 100_000;

        let db = MesoDb::open(dir.path().join("scoped_dirty.db"), schema, config).unwrap();

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
                ident: ":user/status".into(),
                v: Value::String("active".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // Flush initial batch to Tier-1/2 indexes
        db.flush().await.unwrap();
        assert!(db.world_view.read().dirty_entities.is_empty());

        // Mutate ONLY :user/status in RAM without flushing
        db.transact(vec![Fact {
            e: 1,
            ident: ":user/status".into(),
            v: Value::String("paused".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .await
        .unwrap();

        let view = db.world_view.read().clone();
        let name_attr = view.schema.get_id(":user/name").unwrap();
        let status_attr = view.schema.get_id(":user/status").unwrap();

        // :user/name must remain completely clean for fast-path queries
        assert!(!view.is_attr_dirty(name_attr));
        assert!(!view.is_entity_attr_dirty(1, name_attr));
        assert!(view.is_attr_dirty(status_attr));
        assert!(view.is_entity_attr_dirty(1, status_attr));

        let fast_name = db
            .try_fast_path(
                &parser::parse_query(r#"[:find ?n :where [1 :user/name ?n]]"#).unwrap(),
                &QueryOptions::default(),
                &view,
            )
            .unwrap();
        assert!(
            fast_name.is_some(),
            "Fast path for :user/name must remain active when only :user/status is dirty"
        );

        let dirty_status = db
            .try_fast_path(
                &parser::parse_query(r#"[:find ?s :where [1 :user/status ?s]]"#).unwrap(),
                &QueryOptions::default(),
                &view,
            )
            .unwrap();
        assert!(
            dirty_status.is_none(),
            "Fast path for dirty :user/status must fall back to DataFusion"
        );

        let resolved = db
            .query_native(r#"[:find ?s :where [1 :user/status ?s]]"#)
            .await
            .unwrap();
        assert_eq!(resolved[0].get("s"), Some(&Value::String("paused".into())));
    }

    #[tokio::test]
    async fn test_fast_path_string_range_scan_and_uuid_indexing() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":item/code", ValueType::String, false);
        schema.add_attribute(":item/uuid", ValueType::Uuid, false);

        let db =
            MesoDb::open(dir.path().join("fp_str_uuid.db"), schema, Config::default()).unwrap();

        let u_target = [0xAB; 16];
        db.transact(vec![
            // Long strings sharing an identical 8-byte prefix ("product_")
            Fact {
                e: 1,
                ident: ":item/code".into(),
                v: Value::String("product_100".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":item/code".into(),
                v: Value::String("product_200".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":item/code".into(),
                v: Value::String("product_300".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":item/uuid".into(),
                v: Value::Uuid(u_target),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        db.flush().await.unwrap();

        // Verify strict > and < on strings sharing an 8-byte prefix via try_fast_path
        let view = db.world_view.read().clone();
        let ast = parser::parse_query(
            r#"[:find ?e :where [?e :item/code ?c] [(> ?c "product_100")] [(< ?c "product_300")]]"#,
        )
        .unwrap();
        let fast_res = db
            .try_fast_path(&ast, &QueryOptions::default(), &view)
            .unwrap()
            .expect("String range scan must hit fast path");
        assert_eq!(fast_res[0].num_rows(), 1);
        let e_col = fast_res[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(e_col.value(0), 2);

        // Verify UUID was indexed into NowIndex and BitmapStore
        let uuid_attr = view.schema.get_id(":item/uuid").unwrap();
        assert_eq!(
            view.now_index.get(2, uuid_attr).unwrap(),
            Some(Value::Uuid(u_target))
        );
        let bm = view
            .bitmap_index
            .get(uuid_attr, &Value::Uuid(u_target))
            .unwrap()
            .unwrap();
        assert!(bm.contains(2));
    }

    #[tokio::test]
    async fn test_sql_single_quote_escaping_and_polymorphic_attr_var() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/age", ValueType::Int64, false);

        let db = MesoDb::open(dir.path().join("quote_poly.db"), schema, Config::default()).unwrap();

        db.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("O'Reilly".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/age".into(),
                v: Value::Int64(42),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // String literal containing single quote must not break SQL planning
        let res_quote = db
            .query_native(r#"[:find ?e :where [?e :user/name "O'Reilly"]]"#)
            .await
            .unwrap();
        assert_eq!(res_quote.len(), 1);
        assert_eq!(res_quote[0].get("e"), Some(&Value::Ref(1)));

        // Unbound attribute variable [1 ?a ?v] must resolve both String and Int64 values
        let res_poly = db
            .query_native(r#"[:find ?a ?v :where [1 ?a ?v]]"#)
            .await
            .unwrap();
        assert_eq!(res_poly.len(), 2);
        let vals: Vec<Value> = res_poly
            .iter()
            .filter_map(|row| row.get("v").cloned())
            .collect();
        assert!(vals.contains(&Value::String("O'Reilly".into())));
        assert!(vals.contains(&Value::String("42".into())));
    }

    #[tokio::test]
    async fn test_or_join_not_join_and_constant_pull_wildcard_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/role", ValueType::String, false);
        schema.add_attribute(":user/suspended", ValueType::Boolean, false);

        let db = MesoDb::open(dir.path().join("joins_pull.db"), schema, Config::default()).unwrap();

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
                ident: ":user/role".into(),
                v: Value::String("admin".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":user/name".into(),
                v: Value::String("Bob".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":user/role".into(),
                v: Value::String("staff".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":user/suspended".into(),
                v: Value::Boolean(true),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        db.flush().await.unwrap();

        // or-join projects only [?e] across branches with differing internal variables
        let or_res = db
            .query_native(
                r#"[:find ?e
                    :where (or-join [?e]
                             [?e :user/role "admin"]
                             [?e :user/suspended true])]"#,
            )
            .await
            .unwrap();
        assert_eq!(or_res.len(), 2);

        // not-join correlates strictly on [?e], ignoring ?any_val
        let not_res = db
            .query_native(
                r#"[:find ?e
                    :where [?e :user/name ?any_val]
                           (not-join [?e] [?e :user/suspended ?any_val])]"#,
            )
            .await
            .unwrap();
        assert_eq!(not_res.len(), 1);
        assert_eq!(not_res[0].get("e"), Some(&Value::Ref(1)));

        // Pull with wildcard [*] must fall back to PullEngine and populate all attributes
        let json = db
            .query_json(r#"[:find (pull ?e [*]) :where [?e :user/name _] [(= ?e 1)]]"#)
            .await
            .unwrap();
        assert!(json.contains(r#""user/name":"Alice""#));
        assert!(json.contains(r#""user/role":"admin""#));
    }

    #[tokio::test]
    async fn test_parameterized_in_bindings_scalar_collection_tuple_relation() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/dept", ValueType::String, false);
        schema.add_attribute(":user/level", ValueType::Int64, false);

        let db =
            MesoDb::open(dir.path().join("in_bindings.db"), schema, Config::default()).unwrap();

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
                ident: ":user/dept".into(),
                v: Value::String("eng".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/level".into(),
                v: Value::Int64(5),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":user/name".into(),
                v: Value::String("Bob".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":user/dept".into(),
                v: Value::String("ops".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":user/level".into(),
                v: Value::Int64(4),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":user/name".into(),
                v: Value::String("Carol".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":user/dept".into(),
                v: Value::String("eng".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":user/level".into(),
                v: Value::Int64(3),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        // 1. Scalar :in binding
        let scalar_res = db
            .query_native_with_options(
                r#"[:find ?name :in $ ?target_dept :where [?e :user/dept ?target_dept] [?e :user/name ?name]]"#,
                QueryOptions {
                    inputs: vec![QueryInput::Scalar(Value::String("ops".into()))],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(scalar_res.len(), 1);
        assert_eq!(
            scalar_res[0].get("name"),
            Some(&Value::String("Bob".into()))
        );

        // 2. Collection :in [?e ...] binding
        let coll_res = db
            .query_native_with_options(
                r#"[:find ?name :in $ [?e ...] :where [?e :user/name ?name]]"#,
                QueryOptions {
                    inputs: vec![QueryInput::Collection(vec![
                        Value::Int64(1),
                        Value::Int64(3),
                    ])],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(coll_res.len(), 2);

        // 3. Tuple :in [?dept ?lvl] binding
        let tuple_res = db
            .query_native_with_options(
                r#"[:find ?name :in $ [?dept ?lvl] :where [?e :user/dept ?dept] [?e :user/level ?lvl] [?e :user/name ?name]]"#,
                QueryOptions {
                    inputs: vec![QueryInput::Tuple(vec![
                        Value::String("eng".into()),
                        Value::Int64(5),
                    ])],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(tuple_res.len(), 1);
        assert_eq!(
            tuple_res[0].get("name"),
            Some(&Value::String("Alice".into()))
        );

        // 4. Relation :in [[?dept ?lvl]] binding
        let rel_res = db
            .query_native_with_options(
                r#"[:find ?name :in $ [[?dept ?lvl]] :where [?e :user/dept ?dept] [?e :user/level ?lvl] [?e :user/name ?name]]"#,
                QueryOptions {
                    inputs: vec![QueryInput::Relation(vec![
                        vec![Value::String("ops".into()), Value::Int64(4)],
                        vec![Value::String("eng".into()), Value::Int64(3)],
                    ])],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(rel_res.len(), 2);
    }

    #[tokio::test]
    async fn test_cardinality_many_accumulation_retraction_and_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute_with_cardinality(
            ":user/tag",
            ValueType::String,
            false,
            crate::schema::Cardinality::Many,
        );

        let db = MesoDb::open(dir.path().join("card_many.db"), schema, Config::default()).unwrap();

        // Assert multiple tags on the same entity without overwriting prior values
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
                ident: ":user/tag".into(),
                v: Value::String("rust".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/tag".into(),
                v: Value::String("datalog".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        db.flush().await.unwrap();

        // Add a third tag and retract one specific tag ("rust")
        db.transact(vec![
            Fact {
                e: 1,
                ident: ":user/tag".into(),
                v: Value::String("systems".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 1,
                ident: ":user/tag".into(),
                v: Value::String("rust".into()),
                op: false,
                cas_old_v: None,
                valid_time: None,
            },
        ])
        .await
        .unwrap();

        let tags_res = db
            .query_native(r#"[:find ?tag :where [1 :user/tag ?tag]]"#)
            .await
            .unwrap();
        let mut tags: Vec<String> = tags_res
            .iter()
            .filter_map(|row| match row.get("tag") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        tags.sort();
        assert_eq!(tags, vec!["datalog".to_string(), "systems".to_string()]);

        // Verify Pull renders Cardinality::Many as an array
        let pull_json = db
            .query_json(
                r#"[:find (pull ?e [:user/name :user/tag]) :where [?e :user/name "Alice"]]"#,
            )
            .await
            .unwrap();
        assert!(pull_json.contains("\"user/tag\":["));

        // Verify Option A compaction preserves all surviving multi-valued members
        db.compact().await.unwrap();
        let post_compact = db
            .query_native(r#"[:find ?tag :where [1 :user/tag ?tag]]"#)
            .await
            .unwrap();
        assert_eq!(post_compact.len(), 2);
    }
}
