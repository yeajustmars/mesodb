// mesodb-core/src/transactor.rs

use arrow::record_batch::RecordBatch;
use std::path::Path;

use crate::config::Config;
use crate::datom::Datom;
use crate::error::MesoError;
use crate::index::IndexManager;
use crate::memtable::MemTable;
use crate::schema::{SchemaMap, ValueType};
use crate::types::{EntityId, Result, TxId, Value};
use crate::wal::Wal;

#[derive(Debug, Clone)]
pub struct Fact {
    pub e: EntityId,
    pub ident: String,
    pub v: Value,
    pub op: bool,
}

#[derive(Debug)]
pub struct TxReport {
    pub tx_id: TxId,
    pub timestamp: i64,
    pub datoms_written: usize,
    /// The immutable Arrow batch for this specific transaction
    pub batch: RecordBatch,
}

pub struct Transactor {
    pub config: Config,
    pub wal: Wal,
    pub schema: SchemaMap,
    pub indices: IndexManager,
    pub current_tx_id: TxId,
}

impl Transactor {
    pub fn new<P: AsRef<Path>>(wal_path: P, schema: SchemaMap, config: Config) -> Result<Self> {
        let mut wal = Wal::open(wal_path)?;
        let mut indices = IndexManager::new();
        let mut current_tx_id = 1;

        // Rebuild memory indices from the WAL
        let recovered_datoms = wal.recover()?;
        for datom in recovered_datoms {
            if datom.t >= current_tx_id {
                current_tx_id = datom.t + 1;
            }
            if let Some(attr) = schema.get_by_id(datom.a) {
                if datom.op {
                    indices.insert(datom.e, datom.a, datom.v.clone(), attr.is_unique);
                } else {
                    indices.remove(datom.e, datom.a, &datom.v, attr.is_unique);
                }
            }
        }

        Ok(Self {
            config,
            wal,
            schema,
            indices,
            current_tx_id,
        })
    }

    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReport> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        self.execute(facts, now)
    }

    pub fn transact_at(&mut self, facts: Vec<Fact>, custom_now: i64) -> Result<TxReport> {
        self.execute(facts, custom_now)
    }

    fn execute(&mut self, facts: Vec<Fact>, now: i64) -> Result<TxReport> {
        if facts.is_empty() {
            let empty_batch = MemTable::new(0).finish()?;
            return Ok(TxReport {
                tx_id: self.current_tx_id,
                timestamp: now,
                datoms_written: 0,
                batch: empty_batch,
            });
        }

        let tx_id = self.current_tx_id;
        let mut pending_datoms = Vec::with_capacity(facts.len() * 2); // Room for retractions
        let mut batch_uniques = std::collections::HashSet::new();

        // --- PHASE 1: Validation & Bitemporal Resolution ---
        for fact in facts {
            let attr_id = if let Some(id) = self.schema.get_id(&fact.ident) {
                id
            } else if self.config.storage.allow_jit_schema {
                let inferred_type = match fact.v {
                    Value::Boolean(_) => ValueType::Boolean,
                    Value::Int64(_) => ValueType::Int64,
                    Value::Float64(_) => ValueType::Float64,
                    Value::Ref(_) => ValueType::Ref,
                    Value::Timestamp(_) => ValueType::Timestamp,
                    Value::Uuid(_) => ValueType::Uuid,
                    _ => ValueType::String,
                };
                self.schema
                    .add_attribute(&fact.ident, inferred_type, false)
                    .id
            } else {
                return Err(MesoError::Serialization(format!(
                    "Unknown attribute: {}",
                    fact.ident
                )));
            };

            self.schema.validate_value(&fact.ident, &fact.v)?;
            let is_unique = self.schema.get_by_id(attr_id).unwrap().is_unique;

            if fact.op {
                // Assertions
                if is_unique {
                    let key = (attr_id, fact.v.clone());
                    if let Some(owner) = self.indices.get_owner_of_unique(attr_id, &fact.v) {
                        if owner != fact.e {
                            return Err(MesoError::UniqueConstraintViolation {
                                attr: fact.ident,
                                value: fact.v.to_string(),
                                owner,
                            });
                        }
                    }
                    if !batch_uniques.insert(key) {
                        return Err(MesoError::Serialization(format!(
                            "Duplicate unique in batch: {}",
                            fact.ident
                        )));
                    }
                }

                // THE BITEMPORAL FIX: Retract existing value if it differs
                if let Some(existing_v) = self.indices.get_current_value(fact.e, attr_id) {
                    if existing_v != &fact.v {
                        pending_datoms.push(Datom::retract(
                            fact.e,
                            attr_id,
                            existing_v.clone(),
                            tx_id,
                            now,
                        ));
                    } else {
                        continue; // No-op: value is already exactly this
                    }
                }

                pending_datoms.push(Datom::assert(fact.e, attr_id, fact.v, tx_id, now));
            } else {
                // Retractions
                if let Some(existing_v) = self.indices.get_current_value(fact.e, attr_id) {
                    if existing_v == &fact.v {
                        pending_datoms.push(Datom::retract(fact.e, attr_id, fact.v, tx_id, now));
                    }
                }
            }
        }

        // --- PHASE 2: WAL Persistence (Crash Safety) ---
        self.wal.append_batch(&pending_datoms)?;

        // --- PHASE 3: Update RAM Indices & Build Arrow Batch ---
        let mut tx_memtable = MemTable::new(pending_datoms.len());
        for datom in &pending_datoms {
            let is_unique = self.schema.get_by_id(datom.a).unwrap().is_unique;
            if datom.op {
                self.indices
                    .insert(datom.e, datom.a, datom.v.clone(), is_unique);
            } else {
                self.indices.remove(datom.e, datom.a, &datom.v, is_unique);
            }
            tx_memtable.append(datom.clone());
        }

        let batch = tx_memtable.finish()?;
        self.current_tx_id += 1;

        Ok(TxReport {
            tx_id,
            timestamp: now,
            datoms_written: pending_datoms.len(),
            batch, // Ready to be consumed lock-free by readers!
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn setup_transactor() -> (Transactor, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/email", ValueType::String, true);
        let config = Config::default();
        let transactor = Transactor::new(temp_file.path(), schema, config).unwrap();
        (transactor, temp_file)
    }

    #[test]
    fn test_tx_bitemporal_interval_closing() {
        let (mut t, _f) = setup_transactor();

        // 1. Assert Alice
        let _ = t
            .transact_at(
                vec![Fact {
                    e: 1,
                    ident: ":user/name".into(),
                    v: Value::String("Alice".into()),
                    op: true,
                }],
                100,
            )
            .unwrap();

        // 2. Overwrite with Alice-Revised
        let report = t
            .transact_at(
                vec![Fact {
                    e: 1,
                    ident: ":user/name".into(),
                    v: Value::String("Alice-Revised".into()),
                    op: true,
                }],
                200,
            )
            .unwrap();

        // The second transaction should contain TWO datoms: a retraction for Alice, an assertion for Alice-Revised
        assert_eq!(report.datoms_written, 2);
        assert_eq!(report.batch.num_rows(), 2);
    }

    #[test]
    fn test_tx_unique_constraint_enforcement() {
        let (mut t, _f) = setup_transactor();

        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
        }])
        .unwrap();

        let err = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
        }]);

        assert!(err.is_err());
    }

    #[test]
    fn test_engine_raw_bitemporal_arrow_output() {
        use arrow::array::{BooleanArray, StringArray, TimestampMicrosecondArray};

        let (mut t, _f) = setup_transactor();

        // 1. Assert Alice at T=100
        t.transact_at(
            vec![Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Alice".into()),
                op: true,
            }],
            100,
        )
        .unwrap();

        // 2. Overwrite with Bob at T=200
        let report = t
            .transact_at(
                vec![Fact {
                    e: 1,
                    ident: ":user/name".into(),
                    v: Value::String("Bob".into()),
                    op: true,
                }],
                200,
            )
            .unwrap();

        let batch = report.batch;

        // Downcast the raw Arrow columns
        let op_col = batch
            .column(10)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let from_col = batch
            .column(11)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let to_col = batch
            .column(12)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let val_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(); // v_str

        // We expect EXACTLY 2 rows in this batch:
        // Row 0: The retraction of Alice
        // Row 1: The assertion of Bob
        assert_eq!(batch.num_rows(), 2);

        // Verify Row 0 (Retraction of Alice)
        assert_eq!(val_col.value(0), "Alice");
        assert_eq!(op_col.value(0), false); // op = false
        assert_eq!(from_col.value(0), 200);
        assert_eq!(to_col.value(0), 200); // Retractions close their own bounds immediately

        // Verify Row 1 (Assertion of Bob)
        assert_eq!(val_col.value(1), "Bob");
        assert_eq!(op_col.value(1), true); // op = true
        assert_eq!(from_col.value(1), 200);
        assert_eq!(to_col.value(1), i64::MAX); // New assertion is valid until the end of time
    }
}
