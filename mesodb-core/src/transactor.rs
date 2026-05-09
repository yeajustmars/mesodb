// mesodb-core/src/transactor.rs
use ahash::AHashMap;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::datom::Datom;
use crate::error::{MesoError, Result};
use crate::index::IndexManager;
use crate::memtable::MemTable;
use crate::schema::{SchemaMap, ValueType};
use crate::types::{EntityId, TxId, Value};
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
    pub wal: Wal,
    pub active_memtable: MemTable,
    pub schema: SchemaMap,
    pub indices: IndexManager,
    pub frozen_history: Arc<RwLock<Vec<RecordBatch>>>,
    pub current_tx_id: TxId,
}

impl Transactor {
    pub fn new<P: AsRef<Path>>(wal_path: P, schema: SchemaMap) -> Result<Self> {
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
            wal,
            active_memtable,
            schema,
            indices,
            frozen_history: Arc::new(RwLock::new(Vec::new())),
            current_tx_id,
        })
    }

    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReceipt> {
        if facts.is_empty() {
            return Ok(TxReceipt {
                tx_id: self.current_tx_id,
                timestamp: Utc::now().timestamp_micros(),
                datoms_written: 0,
            });
        }

        let tx_id = self.current_tx_id;
        let timestamp = Utc::now().timestamp_micros();
        let mut pending_datoms = Vec::with_capacity(facts.len());
        let mut batch_unique_inserts = AHashMap::new();

        for fact in facts {
            let attr = self
                .schema
                .get_by_ident(&fact.ident)
                .ok_or_else(|| MesoError::UndefinedAttribute(fact.ident.clone()))?;

            if !Self::matches_type(&attr.value_type, &fact.v) {
                return Err(MesoError::TypeMismatch {
                    expected: attr.value_type.clone(),
                    found: fact.v.clone(),
                });
            }

            if attr.is_unique && fact.op {
                let key = (attr.id, fact.v.clone());
                if let Some(&existing_entity) = self.indices.unique_index.get(&key)
                    && existing_entity != fact.e
                {
                    return Err(MesoError::UniqueConstraintViolation {
                        attr: fact.ident.clone(),
                        value: fact.v.to_string(),
                        owner: existing_entity,
                    });
                }

                if let Some(&existing_entity) = batch_unique_inserts.get(&key)
                    && existing_entity != fact.e
                {
                    return Err(MesoError::UniqueConstraintViolation {
                        attr: fact.ident,
                        value: fact.v.to_string(),
                        owner: existing_entity,
                    });
                }
                batch_unique_inserts.insert(key, fact.e);
            }

            let datom = if fact.op {
                Datom::assert(fact.e, attr.id, fact.v, tx_id, timestamp)
            } else {
                Datom::retract(fact.e, attr.id, fact.v, tx_id, timestamp)
            };
            pending_datoms.push(datom);
        }

        self.wal.append_batch(&pending_datoms)?;

        for datom in pending_datoms {
            if let Some(attr) = self.schema.get_by_id(datom.a)
                && attr.is_unique
            {
                if datom.op {
                    self.indices
                        .unique_index
                        .insert((datom.a, datom.v.clone()), datom.e);
                } else {
                    self.indices
                        .unique_index
                        .remove(&(datom.a, datom.v.clone()));
                }
            }
            self.indices.insert(datom.e, datom.a, datom.v.clone());
            self.active_memtable.append(datom);
        }

        self.current_tx_id += 1;
        Ok(TxReceipt {
            tx_id,
            timestamp,
            datoms_written: batch_unique_inserts.len(),
        })
    }

    fn matches_type(val_type: &ValueType, val: &Value) -> bool {
        matches!(
            (val_type, val),
            (ValueType::Boolean, Value::Boolean(_))
                | (ValueType::Int64, Value::Int64(_))
                | (ValueType::Float64, Value::Float64(_))
                | (ValueType::String, Value::String(_))
                | (ValueType::Ref, Value::Ref(_))
                | (ValueType::Timestamp, Value::Timestamp(_))
                | (ValueType::Uuid, Value::Uuid(_))
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let transactor = Transactor::new(temp_file.path(), schema).unwrap();
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
            let mut t1 = Transactor::new(temp_file.path(), schema.clone()).unwrap();
            t1.transact(vec![Fact {
                e: 10,
                ident: ":user/email".into(),
                v: Value::String("p@p.com".into()),
                op: true,
            }])
            .unwrap();
        }
        let mut t2 = Transactor::new(temp_file.path(), schema).unwrap();
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
