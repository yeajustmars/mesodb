// mesodb-core/src/transactor.rs

use arrow::record_batch::RecordBatch;
use std::{
    collections::HashSet,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config::Config,
    datom::Datom,
    error::MesoError,
    index::IndexManager,
    memtable::MemTable,
    schema::{Attribute, SchemaMutation},
    schema::{SchemaMap, SchemaTimeline, ValueType},
    types::{EntityId, Result, TxId, Value},
    wal::{Wal, WalEntry},
};

pub struct Transactor {
    pub config: Config,
    pub wal: Wal,
    pub schema: SchemaMap,
    pub timeline: SchemaTimeline,
    pub indices: IndexManager,
    pub current_tx_id: TxId,
}

impl Transactor {
    pub fn new<P: AsRef<Path>>(
        wal_path: P,
        mut schema: SchemaMap,
        config: Config,
    ) -> Result<(Self, RecordBatch)> {
        let mut wal = Wal::open(wal_path)?;
        let mut indices = IndexManager::new();
        let mut current_tx_id = 1;
        let mut timeline = SchemaTimeline::new();

        timeline.append_version(0, 0, schema.clone());

        // Initialize a MemTable to rebuild uncompacted RAM state
        let mut recovery_memtable = MemTable::new(10_000);

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
                                indices.insert(
                                    datom.e,
                                    datom.a,
                                    datom.v.clone(),
                                    attr.is_unique,
                                    datom.valid_from,
                                );
                            } else {
                                indices.remove(
                                    datom.e,
                                    datom.a,
                                    &datom.v,
                                    attr.is_unique,
                                    datom.valid_from,
                                );
                            }
                        }
                        // Push the recovered datom into RAM
                        recovery_memtable.append(datom);
                    }
                }
                WalEntry::SchemaMutation(mutation) => match mutation {
                    SchemaMutation::AddAttribute {
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

        // Seal the recovered batch
        let recovered_batch = recovery_memtable.finish()?;

        Ok((
            Self {
                config,
                wal,
                schema,
                timeline,
                indices,
                current_tx_id,
            },
            recovered_batch,
        ))
    }

    pub fn transact_schema(
        &mut self,
        ident: &str,
        value_type: ValueType,
        is_unique: bool,
    ) -> Result<Arc<Attribute>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let tx_id = self.current_tx_id;
        self.current_tx_id += 1;

        let attr = self.schema.add_attribute(ident, value_type, is_unique);

        let mutation = SchemaMutation::AddAttribute {
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
            let resolved_valid_time = fact.valid_time.unwrap_or(now);

            // Reified Transactions: Entity ID 0 points to current transaction
            if fact.e == 0 {
                fact.e = tx_id;
            }

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

                let new_attr = self.schema.add_attribute(&fact.ident, inferred_type, false);

                let mutation = SchemaMutation::AddAttribute {
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

            // JIT Type Coercion
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
                let current_v = self
                    .indices
                    .get_value_at(fact.e, attr_id, resolved_valid_time);
                let current_matches = match current_v {
                    Some(v) => v == expected_v,
                    None => false,
                };

                if !current_matches {
                    return Err(MesoError::Serialization(format!(
                        "CAS conflict for entity {} attribute '{}': expected {:?}, found {:?}",
                        fact.e, fact.ident, expected_v, current_v
                    )));
                }
            }

            if fact.op {
                // --- UNIQUE CONSTRAINT VALIDATION ---
                if is_unique {
                    let key = (attr_id, fact.v.clone());

                    if let Some(owner) =
                        self.indices
                            .get_owner_of_unique_at(attr_id, &fact.v, resolved_valid_time)
                        && owner != fact.e
                    {
                        return Err(MesoError::UniqueConstraintViolation {
                            attr: fact.ident,
                            value: fact.v.to_string(),
                            owner,
                        });
                    }
                    if let Some(future_owner) = self.indices.get_future_unique_conflict(
                        attr_id,
                        &fact.v,
                        fact.e,
                        resolved_valid_time,
                    ) {
                        return Err(MesoError::UniqueConstraintViolation {
                            attr: fact.ident,
                            value: fact.v.to_string(),
                            owner: future_owner,
                        });
                    }

                    if !batch_uniques.insert(key) {
                        return Err(MesoError::Serialization(format!(
                            "Duplicate unique in batch: {}",
                            fact.ident
                        )));
                    }
                }

                // Automatic Retraction Check (RESTORED unconditionally)
                if let Some(existing_v) =
                    self.indices
                        .get_value_at(fact.e, attr_id, resolved_valid_time)
                {
                    if existing_v != &fact.v {
                        pending_datoms.push(Datom::retract(
                            fact.e,
                            attr_id,
                            existing_v.clone(),
                            tx_id,
                            resolved_valid_time,
                        ));
                    } else {
                        continue; // No-op: value already set
                    }
                }

                pending_datoms.push(Datom::assert(
                    fact.e,
                    attr_id,
                    fact.v,
                    tx_id,
                    resolved_valid_time,
                ));
            } else {
                // Retraction (RESTORED verification)
                if let Some(existing_v) =
                    self.indices
                        .get_value_at(fact.e, attr_id, resolved_valid_time)
                    && existing_v == &fact.v
                {
                    pending_datoms.push(Datom::retract(
                        fact.e,
                        attr_id,
                        fact.v,
                        tx_id,
                        resolved_valid_time,
                    ));
                }
            }
        }

        // --- PHASE 2: WAL Persistence ---
        self.wal.append_entry(
            &WalEntry::DataBatch(pending_datoms.clone()),
            &self.config.storage.wal_sync_mode,
        )?;

        // --- PHASE 3: Build Arrow Batch & Maintain Validation Index ---
        let mut tx_memtable = MemTable::new(pending_datoms.len());

        for datom in &pending_datoms {
            let is_unique = self.schema.get_by_id(datom.a).unwrap().is_unique;

            // RESTORED unconditionally to maintain EAVT for history and CAS
            if datom.op {
                self.indices.insert(
                    datom.e,
                    datom.a,
                    datom.v.clone(),
                    is_unique,
                    datom.valid_from,
                );
            } else {
                self.indices
                    .remove(datom.e, datom.a, &datom.v, is_unique, datom.valid_from);
            }

            tx_memtable.append(datom.clone());
        }

        let batch = tx_memtable.finish()?;
        self.current_tx_id += 1;

        Ok(TxReport {
            tx_id,
            timestamp: now,
            datoms_written: pending_datoms.len(),
            batch,
        })
    }

    pub fn take_sync_rx(&mut self) -> Option<tokio::sync::oneshot::Receiver<()>> {
        self.wal.take_last_sync_rx()
    }
}

#[derive(Debug, Clone)]
pub struct Fact {
    pub e: EntityId,
    pub ident: String,
    pub v: Value,
    pub op: bool,
    pub cas_old_v: Option<Value>, // Compare-And-Swap expectation
    pub valid_time: Option<i64>,
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

    async fn setup_transactor() -> (Transactor, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/email", ValueType::String, true);

        let config = Config::default();
        let (transactor, _) = Transactor::new(wal_path, schema, config).unwrap();

        (transactor, dir)
    }

    #[tokio::test]
    async fn test_tx_bitemporal_interval_closing() {
        let (mut t, _f) = setup_transactor().await;
        // 1. Assert Alice
        let _ = t
            .transact_at(
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
                    valid_time: None,
                }],
                200,
            )
            .unwrap();

        // The second transaction should contain TWO datoms: a retraction for Alice, an assertion for Alice-Revised
        assert_eq!(report.datoms_written, 2);
        assert_eq!(report.batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_tx_unique_constraint_enforcement() {
        let (mut t, _f) = setup_transactor().await;
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .unwrap();

        let err = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".into(),
            v: Value::String("a@b.com".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }]);

        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_engine_raw_bitemporal_arrow_output() {
        use arrow::array::{BooleanArray, StringArray, TimestampMicrosecondArray};
        let (mut t, _f) = setup_transactor().await;

        // 1. Assert Alice at T=100
        t.transact_at(
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
                    valid_time: None,
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

    #[tokio::test]
    async fn test_jit_schema_durability_and_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        let config = Config::default();

        // 1. Boot fresh transactor and transact a totally unknown attribute
        {
            let (mut t, _) = Transactor::new(&wal_path, SchemaMap::new(), config.clone()).unwrap();

            t.transact(vec![Fact {
                e: 1,
                ident: ":new/jit_attr".into(), // Does not exist in the initial SchemaMap!
                v: Value::String("Test".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }])
            .unwrap();

            // Verify the engine inferred it in RAM
            assert!(t.schema.contains_ident(":new/jit_attr"));

            // NEW: Await the background group commit sync before dropping!
            if let Some(rx) = t.take_sync_rx() {
                let _ = rx.await;
            }
        } // `t` is dropped here. Server "crashes".

        // 2. Re-open from the exact same WAL file
        {
            let (t_recovered, _) = Transactor::new(&wal_path, SchemaMap::new(), config).unwrap();

            // If the SchemaMutation wasn't durable, this would fail!
            assert!(t_recovered.schema.contains_ident(":new/jit_attr"));

            let attr = t_recovered.schema.get_by_ident(":new/jit_attr").unwrap();
            assert_eq!(attr.value_type, ValueType::String);
        }
    }

    #[tokio::test]
    async fn test_atomic_compare_and_swap() {
        let (mut t, _f) = setup_transactor().await;

        // 1. Initial State: Alice is 29
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(), // Will JIT create this attribute
            v: Value::Int64(29),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .unwrap();

        // 2. Failed CAS: Someone tries to update her to 31, but thinks she is 30.
        let bad_cas = t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".into(),
            v: Value::Int64(31),
            op: true,
            cas_old_v: Some(Value::Int64(30)), // Incorrect expectation!
            valid_time: None,
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
            valid_time: None,
        }]);

        assert!(good_cas.is_ok());
    }

    #[tokio::test]
    async fn test_cas_transaction_atomicity() {
        let (mut t, _f) = setup_transactor().await;

        // 1. Setup Initial Bank Balances
        t.transact(vec![
            Fact {
                e: 10,
                ident: ":bank/balance".into(),
                v: Value::Int64(100),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 20,
                ident: ":bank/balance".into(),
                v: Value::Int64(50),
                op: true,
                cas_old_v: None,
                valid_time: None,
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
                valid_time: None,
            },
            // INVALID CAS (Expected 40, but is actually 50)
            Fact {
                e: 20,
                ident: ":bank/balance".into(),
                v: Value::Int64(90),
                op: true,
                cas_old_v: Some(Value::Int64(40)),
                valid_time: None,
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

    #[tokio::test]
    async fn test_cas_retractions_and_missing_values() {
        let (mut t, _f) = setup_transactor().await;

        t.transact(vec![Fact {
            e: 1,
            ident: ":user/status".into(),
            v: Value::String("active".into()),
            op: true,
            cas_old_v: None,
            valid_time: None,
        }])
        .unwrap();

        // 1. Failed CAS on Missing Attribute (Entity 2 doesn't exist)
        let missing_err = t.transact(vec![Fact {
            e: 2,
            ident: ":user/status".into(),
            v: Value::String("active".into()),
            op: true,
            cas_old_v: Some(Value::String("inactive".into())),
            valid_time: None,
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
            valid_time: None,
        }]);
        assert!(good_retract.is_ok());

        // Verify it's actually gone
        let attr_id = t.schema.get_id(":user/status").unwrap();
        assert!(t.indices.get_current_value(1, attr_id).is_none());
    }

    #[tokio::test]
    async fn test_explicit_valid_time_ingestion() {
        use arrow::array::TimestampMicrosecondArray;
        let (mut t, _f) = setup_transactor().await;

        let historical_time = 5000; // Deep in the past

        // Assert a fact with a specific valid_time
        let report = t
            .transact(vec![Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("TimeTraveler".into()),
                op: true,
                cas_old_v: None,
                valid_time: Some(historical_time),
            }])
            .unwrap();

        let batch = report.batch;
        let from_col = batch
            .column(11) // valid_from
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        // Verify the engine respected the user's valid_time instead of using the transaction's `now`
        assert_eq!(from_col.value(0), historical_time);
    }

    #[tokio::test]
    async fn test_bitemporal_past_valid_time() {
        use arrow::array::TimestampMicrosecondArray;
        let (mut t, _f) = setup_transactor().await;

        let past_time = 1500000000000; // Explicit past timestamp

        let report = t
            .transact(vec![Fact {
                e: 1,
                ident: ":user/name".into(),
                v: Value::String("Past User".into()),
                op: true,
                cas_old_v: None,
                valid_time: Some(past_time),
            }])
            .unwrap();

        let from_col = report
            .batch
            .column(11) // valid_from column index
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(
            from_col.value(0),
            past_time,
            "Transactor must respect explicit past time"
        );
    }

    #[tokio::test]
    async fn test_bitemporal_future_valid_time() {
        use arrow::array::TimestampMicrosecondArray;
        let (mut t, _f) = setup_transactor().await;

        let future_time = 2500000000000; // Explicit future timestamp

        let report = t
            .transact(vec![Fact {
                e: 2,
                ident: ":user/name".into(),
                v: Value::String("Future User".into()),
                op: true,
                cas_old_v: None,
                valid_time: Some(future_time),
            }])
            .unwrap();

        let from_col = report
            .batch
            .column(11) // valid_from column index
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(
            from_col.value(0),
            future_time,
            "Transactor must respect explicit future time"
        );
    }

    #[tokio::test]
    async fn test_bitemporal_mixed_valid_times_batch() {
        use arrow::array::TimestampMicrosecondArray;
        let (mut t, _f) = setup_transactor().await;

        let past_1 = 1000;
        let future_1 = 8_000_000_000_000_000; // Far future (Year ~2223)
        let past_2 = 2000;
        let future_2 = 9_000_000_000_000_000; // Farther future (Year ~2255)

        // Mix past, future, and implicit 'now' (None) in a single transaction batch
        let report = t
            .transact(vec![
                Fact {
                    e: 10,
                    ident: ":user/name".into(),
                    v: Value::String("A".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: Some(past_1),
                },
                Fact {
                    e: 11,
                    ident: ":user/name".into(),
                    v: Value::String("B".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: Some(future_1),
                },
                Fact {
                    e: 12,
                    ident: ":user/name".into(),
                    v: Value::String("C".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: None,
                }, // Falls back to internal SystemTime::now()
                Fact {
                    e: 13,
                    ident: ":user/name".into(),
                    v: Value::String("D".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: Some(past_2),
                },
                Fact {
                    e: 14,
                    ident: ":user/name".into(),
                    v: Value::String("E".into()),
                    op: true,
                    cas_old_v: None,
                    valid_time: Some(future_2),
                },
            ])
            .unwrap();

        let from_col = report
            .batch
            .column(11) // valid_from column index
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(report.batch.num_rows(), 5);

        // Assert explicit times were bound correctly
        assert_eq!(from_col.value(0), past_1);
        assert_eq!(from_col.value(1), future_1);
        assert_eq!(from_col.value(3), past_2);
        assert_eq!(from_col.value(4), future_2);

        // Assert the implicit 'None' fallback correctly generated a real transaction timestamp
        let implicit_now = from_col.value(2);
        assert!(
            implicit_now > past_2 && implicit_now < future_2,
            "Implicit fallback ({}) should reflect current system time and sit between {} and {}",
            implicit_now,
            past_2,
            future_2
        );
    }

    #[tokio::test]
    async fn test_bitemporal_contextual_uniqueness() {
        let (mut t, _f) = setup_transactor().await;

        // 1. Alice claims an email starting at T=100
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("context@test.com".into()),
            op: true,
            cas_old_v: None,
            valid_time: Some(100),
        }])
        .unwrap();

        // 2. Alice retracts the email at T=200
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".into(),
            v: Value::String("context@test.com".into()),
            op: false,
            cas_old_v: None,
            valid_time: Some(200),
        }])
        .unwrap();

        // 3. Bob attempts to claim the email retroactively at T=150 (OVERLAP! Alice owns it 100-200)
        let bad_overlap = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".into(),
            v: Value::String("context@test.com".into()),
            op: true,
            cas_old_v: None,
            valid_time: Some(150),
        }]);
        assert!(
            bad_overlap.is_err(),
            "Must prevent overlapping claims inside an active interval"
        );

        // 4. Bob claims the email at T=250 (NO OVERLAP! Alice freed it at 200)
        let valid_claim = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".into(),
            v: Value::String("context@test.com".into()),
            op: true,
            cas_old_v: None,
            valid_time: Some(250),
        }]);
        assert!(
            valid_claim.is_ok(),
            "Must allow claiming a unique value after it was freed in the timeline"
        );
    }
}
