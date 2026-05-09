// mesodb-core/src/transactor.rs
use arrow::record_batch::RecordBatch;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Sender;

use crate::config::Config;
use crate::datom::Datom;
use crate::error::MesoError;
use crate::index::IndexManager;
use crate::memtable::MemTable;
use crate::schema::SchemaMap;
use crate::types::{EntityId, Result, TxId, Value};
use crate::wal::Wal;

#[derive(Debug, Clone)]
pub struct Fact {
    pub e: EntityId,
    pub ident: String,
    pub v: Value,
    pub op: bool,
}

#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub tx_id: TxId,
    pub timestamp: i64,
    pub datoms_written: usize,
}

pub struct Transactor {
    pub config: Config,
    pub wal: Wal,
    pub active_memtable: MemTable,
    pub schema: SchemaMap,
    pub indices: IndexManager,
    pub frozen_history: Arc<RwLock<Vec<RecordBatch>>>,
    pub current_tx_id: TxId,
    pub flush_tx: tokio::sync::mpsc::Sender<RecordBatch>,
}

impl Transactor {
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        schema: SchemaMap,
        flush_tx: Sender<RecordBatch>,
    ) -> Result<Self> {
        let config = Config::default(); // TODO: allow passing custom config
        let mut wal = Wal::open(wal_path)?;
        let mut active_memtable = MemTable::new(1024);
        let mut indices = IndexManager::default();
        let mut current_tx_id = 1;

        let recovered_datoms = wal.recover()?;
        for datom in recovered_datoms {
            if datom.t >= current_tx_id {
                current_tx_id = datom.t + 1;
            }

            if let Some(attr) = schema.get_by_id(datom.a)
                && attr.is_unique
            {
                if datom.op {
                    indices
                        .unique_index
                        .insert((datom.a, datom.v.clone()), datom.e);
                } else {
                    indices.unique_index.remove(&(datom.a, datom.v.clone()));
                }
            }
            indices.insert(datom.e, datom.a, datom.v.clone());
            active_memtable.append(datom);
        }

        Ok(Self {
            config,
            wal,
            active_memtable,
            schema,
            indices,
            frozen_history: Arc::new(RwLock::new(Vec::new())),
            current_tx_id,
            flush_tx,
        })
    }

    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReceipt> {
        // 1. Generate a single timestamp for the entire transaction
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let tx_id = now as u64;
        let datoms_count = facts.len();
        let mut pending_datoms = Vec::with_capacity(datoms_count);

        // 2. Validation Phase
        for fact in facts {
            let attr_id = self.schema.get_id(&fact.ident).ok_or_else(|| {
                MesoError::Serialization(format!("Unknown attribute: {}", fact.ident))
            })?;

            self.schema.validate_value(&fact.ident, &fact.v)?;

            pending_datoms.push(Datom {
                e: fact.e,
                a: attr_id,
                v: fact.v,
                t: tx_id,
                op: fact.op,
                valid_from: now,
                valid_to: i64::MAX, // Active datoms remain valid until retracted [cite: 475]
            });
        }

        // 3. Persistence Phase
        self.wal.append_batch(&pending_datoms)?;

        // 4. Memory Phase
        for datom in &pending_datoms {
            self.indices.insert(datom.e, datom.a, datom.v.clone()); // [cite: 282]
            self.active_memtable.append(datom.clone()); // [cite: 236]
        }

        // 5. Rotation Phase [cite: 120, 123]
        if self.active_memtable.row_count() >= self.config.storage.memtable_rotation_threshold {
            let frozen_batch = self.active_memtable.finish()?;
            let _ = self.flush_tx.try_send(frozen_batch); // [cite: 121]
            self.active_memtable = MemTable::new(self.config.storage.memtable_initial_capacity);
        }

        // 6. Return the full metadata package
        Ok(TxReceipt {
            tx_id,
            timestamp: now,
            datoms_written: datoms_count,
        })
    }

    /// Checks if the current memtable exceeds the configured threshold.
    pub fn check_rotation(&mut self) -> Result<()> {
        if self.active_memtable.row_count() >= self.config.storage.memtable_rotation_threshold {
            self.rotate_active_memtable()?;
        }
        Ok(())
    }

    /// Freezes the current memtable and initializes a fresh one.
    fn rotate_active_memtable(&mut self) -> Result<()> {
        // finish() converts builders into an immutable RecordBatch [cite: 241, 242]
        let batch = self.active_memtable.finish()?;

        // 1. Keep in RAM for immediate query access
        {
            let mut history = self.frozen_history.write().unwrap();
            history.push(batch.clone());
        }

        // 2. Send to background flusher (Non-blocking)
        // Using try_send ensures the transactor never stalls if the disk is slow.
        if let Err(e) = self.flush_tx.try_send(batch) {
            // In production, we log this but keep going because the batch is safe in history RAM.
            eprintln!(
                "Warning: Background flusher busy, batch queued in RAM: {}",
                e
            );
        }

        // 3. Reset Builders with pre-allocated capacity from config [cite: 231, 232]
        self.active_memtable = MemTable::new(self.config.storage.memtable_initial_capacity);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ValueType;
    use std::f64::consts::PI;
    use tempfile::NamedTempFile;

    fn setup_transactor() -> (Transactor, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();

        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/email", ValueType::String, true);
        schema.add_attribute(":user/age", ValueType::Int64, false);
        schema.add_attribute(":math/pi", ValueType::Float64, true);
        schema.add_attribute(":sys/uuid", ValueType::Uuid, true);
        schema.add_attribute(":user/friend", ValueType::Ref, false);
        schema.add_attribute(":flag/unique_bool", ValueType::Boolean, true);

        let (flush_tx, _flush_rx) = tokio::sync::mpsc::channel(1);
        let transactor = Transactor::new(temp_file.path(), schema, flush_tx).unwrap();

        (transactor, temp_file)
    }

    #[test]
    fn test_tx_01_successful_assertion() {
        let (mut t, _f) = setup_transactor();
        let receipt = t
            .transact(vec![Fact {
                e: 1,
                ident: ":user/name".to_string(),
                v: Value::String("Alice".to_string()),
                op: true,
            }])
            .unwrap();
        assert_eq!(receipt.tx_id, 1);
    }

    #[test]
    fn test_tx_02_multiple_assertions() {
        let (mut t, _f) = setup_transactor();
        let receipt = t
            .transact(vec![
                Fact {
                    e: 1,
                    ident: ":user/name".to_string(),
                    v: Value::String("Bob".to_string()),
                    op: true,
                },
                Fact {
                    e: 1,
                    ident: ":user/age".to_string(),
                    v: Value::Int64(30),
                    op: true,
                },
            ])
            .unwrap();
        assert_eq!(receipt.tx_id, 1);
    }

    #[test]
    fn test_tx_03_tx_id_increments_correctly() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".to_string(),
            v: Value::Int64(20),
            op: true,
        }])
        .unwrap();
        let r2 = t
            .transact(vec![Fact {
                e: 1,
                ident: ":user/age".to_string(),
                v: Value::Int64(21),
                op: true,
            }])
            .unwrap();
        assert_eq!(r2.tx_id, 2);
    }

    #[test]
    fn test_tx_04_empty_transaction_noop() {
        let (mut t, _f) = setup_transactor();
        let receipt = t.transact(vec![]).unwrap();
        assert_eq!(receipt.tx_id, 1);
        assert_eq!(t.current_tx_id, 1);
    }

    #[test]
    fn test_tx_05_missing_schema_ident_fails() {
        let (mut t, _f) = setup_transactor();
        let result = t.transact(vec![Fact {
            e: 1,
            ident: ":user/ghost".into(),
            v: Value::Int64(10),
            op: true,
        }]);
        assert!(result.is_err());
    }

    #[test]
    fn test_tx_06_type_mismatch_fails() {
        let (mut t, _f) = setup_transactor();
        let result = t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::String("X".into()),
            op: true,
        }]);
        assert!(result.is_err());
    }

    #[test]
    fn test_tx_07_uuid_type_handling() {
        let (mut t, _f) = setup_transactor();
        let uuid_bytes = [7u8; 16];
        assert!(
            t.transact(vec![Fact {
                e: 10,
                ident: ":sys/uuid".into(),
                v: Value::Uuid(uuid_bytes),
                op: true
            }])
            .is_ok()
        );
    }

    #[test]
    fn test_tx_08_ref_type_handling() {
        let (mut t, _f) = setup_transactor();
        assert!(
            t.transact(vec![Fact {
                e: 1,
                ident: ":user/friend".into(),
                v: Value::Ref(2),
                op: true
            }])
            .is_ok()
        );
    }

    #[test]
    fn test_tx_09_unique_constraint_violation() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
        }])
        .unwrap();
        let result = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
        }]);
        assert!(result.is_err());
    }

    #[test]
    fn test_tx_10_unique_constraint_same_entity_update() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
        }])
        .unwrap();
        assert!(
            t.transact(vec![Fact {
                e: 1,
                ident: ":user/email".into(),
                v: Value::String("a@b.com".into()),
                op: true
            }])
            .is_ok()
        );
    }

    #[test]
    fn test_tx_11_retraction_frees_unique_constraint() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("x@y.com".into()),
            op: true,
        }])
        .unwrap();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("x@y.com".into()),
            op: false,
        }])
        .unwrap();
        assert!(
            t.transact(vec![Fact {
                e: 2,
                ident: ":user/email".into(),
                v: Value::String("x@y.com".into()),
                op: true
            }])
            .is_ok()
        );
    }

    #[test]
    fn test_tx_12_intra_batch_unique_collision() {
        let (mut t, _f) = setup_transactor();
        let res = t.transact(vec![
            Fact {
                e: 10,
                ident: ":user/email".into(),
                v: Value::String("v@v.com".into()),
                op: true,
            },
            Fact {
                e: 11,
                ident: ":user/email".into(),
                v: Value::String("v@v.com".into()),
                op: true,
            },
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn test_tx_13_float_uniqueness_with_nan_protection() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":math/pi".into(),
            v: Value::Float64(PI),
            op: true,
        }])
        .unwrap();
        assert!(
            t.transact(vec![Fact {
                e: 2,
                ident: ":math/pi".into(),
                v: Value::Float64(PI),
                op: true
            }])
            .is_err()
        );
    }

    #[test]
    fn test_tx_14_boolean_unique_limit() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":flag/unique_bool".into(),
            v: Value::Boolean(true),
            op: true,
        }])
        .unwrap();
        t.transact(vec![Fact {
            e: 2,
            ident: ":flag/unique_bool".into(),
            v: Value::Boolean(false),
            op: true,
        }])
        .unwrap();
        assert!(
            t.transact(vec![Fact {
                e: 3,
                ident: ":flag/unique_bool".into(),
                v: Value::Boolean(true),
                op: true
            }])
            .is_err()
        );
    }

    #[test]
    fn test_tx_15_atomicity_failure_rolls_back() {
        let (mut t, _f) = setup_transactor();
        let _ = t.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("X".into()),
                op: true,
            },
            Fact {
                e: 1,
                ident: ":user/age".into(),
                v: Value::String("bad".into()),
                op: true,
            },
        ]);
        assert_eq!(t.current_tx_id, 1);
    }

    #[test]
    fn test_tx_16_wal_recovery_rebuilds_memory_and_indexes() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/email", ValueType::String, true);
        {
            let (tx, _) = tokio::sync::mpsc::channel(1);
            let mut t1 = Transactor::new(temp_file.path(), schema.clone(), tx).unwrap();
            t1.transact(vec![Fact {
                e: 10,
                ident: ":user/email".into(),
                v: Value::String("p@p.com".into()),
                op: true,
            }])
            .unwrap();
        }
        let (tx2, _) = tokio::sync::mpsc::channel(1);
        let mut t2 = Transactor::new(temp_file.path(), schema, tx2).unwrap();
        assert!(
            t2.transact(vec![Fact {
                e: 11,
                ident: ":user/email".into(),
                v: Value::String("p@p.com".into()),
                op: true
            }])
            .is_err()
        );
    }

    #[test]
    fn test_tx_17_large_batch_processing() {
        let (mut t, _f) = setup_transactor();
        let mut b = Vec::new();
        for i in 0..1000 {
            b.push(Fact {
                e: i,
                ident: ":user/age".into(),
                v: Value::Int64(i as i64),
                op: true,
            });
        }
        assert!(t.transact(b).is_ok());
    }

    #[test]
    fn test_tx_18_multiple_retractions_in_large_batch() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::Int64(30),
            op: true,
        }])
        .unwrap();
        let mut b = Vec::new();
        for i in 0..100 {
            b.push(Fact {
                e: i as u64,
                ident: ":user/age".into(),
                v: Value::Int64(30),
                op: false,
            });
        }
        assert!(t.transact(b).is_ok());
    }

    #[test]
    fn test_tx_19_retract_non_existent_unique() {
        let (mut t, _f) = setup_transactor();
        assert!(
            t.transact(vec![Fact {
                e: 99,
                ident: ":user/email".into(),
                v: Value::String("ghost".into()),
                op: false
            }])
            .is_ok()
        );
    }

    #[test]
    fn test_tx_20_assert_and_retract_same_batch_non_unique() {
        let (mut t, _f) = setup_transactor();
        assert!(
            t.transact(vec![
                Fact {
                    e: 1,
                    ident: ":user/name".into(),
                    v: Value::String("F".into()),
                    op: true
                },
                Fact {
                    e: 1,
                    ident: ":user/name".into(),
                    v: Value::String("F".into()),
                    op: false
                },
            ])
            .is_ok()
        );
    }
}
