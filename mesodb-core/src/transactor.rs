// mesodb-core/src/transactor.rs

use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::datom::Datom;
use crate::error::MesoError;
use crate::index::IndexManager;
use crate::memtable::MemTable;
use crate::schema::{SchemaMap, SchemaTimeline, ValueType};
use crate::types::{EntityId, Result, TxId, Value};
use crate::wal::{Wal, WalEntry};

pub struct Transactor {
    pub config: Config,
    pub wal: Wal,
    pub schema: SchemaMap,
    pub timeline: SchemaTimeline,
    pub indices: IndexManager,
    pub current_tx_id: TxId,
}

impl Transactor {
    pub fn new<P: AsRef<Path>>(wal_path: P, mut schema: SchemaMap, config: Config) -> Result<Self> {
        let mut wal = Wal::open(wal_path)?;
        let mut indices = IndexManager::new();
        let mut current_tx_id = 1;
        let mut timeline = SchemaTimeline::new();

        // Baseline whatever schema was passed in
        timeline.append_version(0, 0, schema.clone());

        // Rebuild memory indices and timeline from the WAL
        let recovered_entries = wal.recover()?;
        for entry in recovered_entries {
            match entry {
                WalEntry::DataBatch(datoms) => {
                    for datom in datoms {
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
                }
                WalEntry::SchemaMutation(mutation) => match mutation {
                    crate::schema::SchemaMutation::AddAttribute {
                        tx_id,
                        timestamp,
                        attribute,
                    } => {
                        schema.ingest_attribute(attribute);
                        timeline.append_version(tx_id, timestamp, schema.clone());
                        if tx_id >= current_tx_id {
                            current_tx_id = tx_id + 1;
                        }
                    }
                },
            }
        }

        Ok(Self {
            config,
            wal,
            schema,
            timeline,
            indices,
            current_tx_id,
        })
    }

    pub fn transact_schema(
        &mut self,
        ident: &str,
        value_type: ValueType,
        is_unique: bool,
    ) -> Result<Arc<crate::schema::Attribute>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let tx_id = self.current_tx_id;
        self.current_tx_id += 1;

        let attr = self.schema.add_attribute(ident, value_type, is_unique);

        let mutation = crate::schema::SchemaMutation::AddAttribute {
            tx_id,
            timestamp: now,
            attribute: (*attr).clone(),
        };

        self.wal.append_entry(
            &WalEntry::SchemaMutation(mutation),
            &self.config.storage.wal_sync_mode,
        )?;
        self.timeline
            .append_version(tx_id, now, self.schema.clone());

        Ok(attr)
    }

    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReport> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
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
        let mut batch_uniques = HashSet::new();

        // --- PHASE 1: Validation & Bitemporal Resolution ---
        for mut fact in facts {
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

                // JIT Schema Bug Fix: Make JIT schema durable!
                let new_attr = self.schema.add_attribute(&fact.ident, inferred_type, false);

                let mutation = crate::schema::SchemaMutation::AddAttribute {
                    tx_id,
                    timestamp: now,
                    attribute: (*new_attr).clone(),
                };

                self.wal.append_entry(
                    &WalEntry::SchemaMutation(mutation),
                    &self.config.storage.wal_sync_mode,
                )?;
                self.timeline
                    .append_version(tx_id, now, self.schema.clone());

                new_attr.id
            } else {
                return Err(MesoError::Serialization(format!(
                    "Unknown attribute: {}",
                    fact.ident
                )));
            };

            // --- JIT TYPE COERCION ---
            // JSON numbers parse as Int64 by default. If the schema demands a Ref,
            // we safely cast it here before validation fails.
            if let Some(attr) = self.schema.get_by_id(attr_id)
                && attr.value_type == ValueType::Ref
                && let Value::Int64(i) = fact.v
            {
                fact.v = Value::Ref(i as u64);
            }

            self.schema.validate_value(&fact.ident, &fact.v)?;
            let is_unique = self.schema.get_by_id(attr_id).unwrap().is_unique;

            // --- CAS VALIDATION ---
            if let Some(expected_v) = &fact.cas_old_v {
                let current_v = self.indices.get_current_value(fact.e, attr_id);
                let current_matches = match current_v {
                    Some(v) => v == expected_v,
                    None => false, // Standard CAS requires the old value to exist
                };

                if !current_matches {
                    return Err(MesoError::Serialization(format!(
                        "CAS conflict for entity {} attribute '{}': expected {:?}, found {:?}",
                        fact.e, fact.ident, expected_v, current_v
                    )));
                }
            }

            if fact.op {
                // Assertions
                if is_unique {
                    let key = (attr_id, fact.v.clone());
                    if let Some(owner) = self.indices.get_owner_of_unique(attr_id, &fact.v)
                        && owner != fact.e
                    {
                        return Err(MesoError::UniqueConstraintViolation {
                            attr: fact.ident,
                            value: fact.v.to_string(),
                            owner,
                        });
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
                if let Some(existing_v) = self.indices.get_current_value(fact.e, attr_id)
                    && existing_v == &fact.v
                {
                    pending_datoms.push(Datom::retract(fact.e, attr_id, fact.v, tx_id, now));
                }
            }
        }

        // --- PHASE 2: WAL Persistence (Crash Safety) ---
        self.wal.append_entry(
            &WalEntry::DataBatch(pending_datoms.clone()),
            &self.config.storage.wal_sync_mode,
        )?;

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

#[derive(Debug, Clone)]
pub struct Fact {
    pub e: EntityId,
    pub ident: String,
    pub v: Value,
    pub op: bool,
    pub cas_old_v: Option<Value>, // Compare-And-Swap expectation
}

#[derive(Debug)]
pub struct TxReport {
    pub tx_id: TxId,
    pub timestamp: i64,
    pub datoms_written: usize,
    /// The immutable Arrow batch for this specific transaction
    pub batch: RecordBatch,
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
                    cas_old_v: None,
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
                    cas_old_v: None,
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
            cas_old_v: None,
        }])
        .unwrap();

        let err = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
            cas_old_v: None,
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
                cas_old_v: None,
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
                    cas_old_v: None,
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
        assert!(!op_col.value(0)); // op = false
        assert_eq!(from_col.value(0), 200);
        assert_eq!(to_col.value(0), 200); // Retractions close their own bounds immediately

        // Verify Row 1 (Assertion of Bob)
        assert_eq!(val_col.value(1), "Bob");
        assert!(op_col.value(1)); // op = true
        assert_eq!(from_col.value(1), 200);
        assert_eq!(to_col.value(1), i64::MAX); // New assertion is valid until the end of time
    }

    #[test]
    fn test_jit_schema_durability_and_recovery() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default();

        // 1. Boot fresh transactor and transact a totally unknown attribute
        {
            let mut t =
                Transactor::new(temp_file.path(), SchemaMap::new(), config.clone()).unwrap();
            t.transact(vec![Fact {
                e: 1,
                ident: ":new/jit_attr".into(), // Does not exist in the initial SchemaMap!
                v: Value::String("Test".into()),
                op: true,
                cas_old_v: None,
            }])
            .unwrap();

            // Verify the engine inferred it in RAM
            assert!(t.schema.contains_ident(":new/jit_attr"));
        } // `t` is dropped here. Server "crashes".

        // 2. Re-open from the exact same WAL file
        {
            let t_recovered = Transactor::new(temp_file.path(), SchemaMap::new(), config).unwrap();

            // If the SchemaMutation wasn't durable, this would fail!
            assert!(t_recovered.schema.contains_ident(":new/jit_attr"));

            let attr = t_recovered.schema.get_by_ident(":new/jit_attr").unwrap();
            assert_eq!(attr.value_type, ValueType::String);
        }
    }

    #[test]
    fn test_atomic_compare_and_swap() {
        let (mut t, _f) = setup_transactor();

        // 1. Initial State: Alice is 29
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(), // Will JIT create this attribute
            v: Value::Int64(29),
            op: true,
            cas_old_v: None,
        }])
        .unwrap();

        // 2. Failed CAS: Someone tries to update her to 31, but thinks she is 30.
        let bad_cas = t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::Int64(31),
            op: true,
            cas_old_v: Some(Value::Int64(30)), // Incorrect expectation!
        }]);

        // Transaction must abort!
        assert!(bad_cas.is_err());
        assert!(bad_cas.unwrap_err().to_string().contains("CAS conflict"));

        // 3. Successful CAS: We expect 29, and update to 30.
        let good_cas = t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::Int64(30),
            op: true,
            cas_old_v: Some(Value::Int64(29)), // Correct expectation!
        }]);

        assert!(good_cas.is_ok());
    }

    #[test]
    fn test_cas_transaction_atomicity() {
        let (mut t, _f) = setup_transactor();

        // 1. Setup Initial Bank Balances
        t.transact(vec![
            Fact {
                e: 10,
                ident: ":bank/balance".into(),
                v: Value::Int64(100),
                op: true,
                cas_old_v: None,
            },
            Fact {
                e: 20,
                ident: ":bank/balance".into(),
                v: Value::Int64(50),
                op: true,
                cas_old_v: None,
            },
        ])
        .unwrap();

        // 2. Attempt a transfer of 50 from Account 10 to Account 20.
        // We simulate a race condition where Account 20's balance is 50, but our client thought it was 40.
        let transfer_attempt = t.transact(vec![
            // Valid CAS (100 -> 50)
            Fact {
                e: 10,
                ident: ":bank/balance".into(),
                v: Value::Int64(50),
                op: true,
                cas_old_v: Some(Value::Int64(100)),
            },
            // INVALID CAS (Expected 40, but is actually 50)
            Fact {
                e: 20,
                ident: ":bank/balance".into(),
                v: Value::Int64(90),
                op: true,
                cas_old_v: Some(Value::Int64(40)),
            },
        ]);

        assert!(transfer_attempt.is_err());

        // 3. PROVE ATOMICITY: Account 10 MUST STILL BE 100.
        // The first valid fact should have been entirely rolled back.
        let attr_id = t.schema.get_id(":bank/balance").unwrap();
        let bal_10 = t.indices.get_current_value(10, attr_id).unwrap();
        assert_eq!(
            bal_10,
            &Value::Int64(100),
            "Atomicity failed! Account 10 was partially updated."
        );
    }

    #[test]
    fn test_cas_retractions_and_missing_values() {
        let (mut t, _f) = setup_transactor();

        t.transact(vec![Fact {
            e: 1,
            ident: ":user/status".into(),
            v: Value::String("active".into()),
            op: true,
            cas_old_v: None,
        }])
        .unwrap();

        // 1. Failed CAS on Missing Attribute (Entity 2 doesn't exist)
        let missing_err = t.transact(vec![Fact {
            e: 2,
            ident: ":user/status".into(),
            v: Value::String("active".into()),
            op: true,
            cas_old_v: Some(Value::String("inactive".into())),
        }]);
        assert!(
            missing_err.is_err(),
            "CAS should fail if the expected old value does not exist in the DB"
        );

        // 2. Successful CAS on Retraction
        // "Remove the status, but ONLY if it is currently 'active'"
        let good_retract = t.transact(vec![Fact {
            e: 1,
            ident: ":user/status".into(),
            v: Value::String("active".into()), // The value being retracted
            op: false,                         // Retract!
            cas_old_v: Some(Value::String("active".into())),
        }]);
        assert!(good_retract.is_ok());

        // Verify it's actually gone
        let attr_id = t.schema.get_id(":user/status").unwrap();
        assert!(t.indices.get_current_value(1, attr_id).is_none());
    }
}
