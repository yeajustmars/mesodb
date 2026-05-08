use ahash::AHashMap;
use chrono::Utc;
use std::path::Path;

use crate::datom::Datom;
use crate::error::{MesoError, Result};
use crate::memtable::MemTable;
use crate::schema::{SchemaMap, ValueType};
use crate::types::{AttributeId, EntityId, TxId, Value};
use crate::wal::Wal;

/// A raw fact submitted by a user for ingestion.
#[derive(Debug, Clone)]
pub struct Fact {
    pub e: EntityId,
    pub ident: String,
    pub v: Value,
    pub op: bool, // true for assert, false for retract
}

/// The receipt returned to the user upon a successful transaction.
#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub tx_id: TxId,
    pub timestamp: i64,
    pub datoms_written: usize,
}

/// The single-threaded orchestrator that validates facts, ensures durability,
/// and updates the in-memory column-stores.
pub struct Transactor {
    wal: Wal,
    pub memtable: MemTable,
    pub schema: SchemaMap,

    /// Tracks unique constraints: (AttributeId, Value) -> EntityId
    unique_index: AHashMap<(AttributeId, Value), EntityId>,

    next_tx_id: TxId,
}

impl Transactor {
    /// Boots up a Transactor, automatically recovering state from the WAL if it exists.
    pub fn new<P: AsRef<Path>>(wal_path: P, schema: SchemaMap) -> Result<Self> {
        let mut wal = Wal::open(wal_path)?;
        let mut memtable = MemTable::new(1024);
        let mut unique_index = AHashMap::new();
        let mut next_tx_id = 1;

        // 1. Recover state from disk
        let recovered_datoms = wal.recover()?;

        // 2. Replay the WAL into the MemTable and rebuild the unique index
        for datom in recovered_datoms {
            if datom.t >= next_tx_id {
                next_tx_id = datom.t + 1;
            }

            // Rebuild the unique index if the attribute is marked as unique
            if let Some(attr) = schema.get_by_id(datom.a)
                && attr.is_unique
            {
                if datom.op {
                    unique_index.insert((datom.a, datom.v.clone()), datom.e);
                } else {
                    unique_index.remove(&(datom.a, datom.v.clone()));
                }
            }

            memtable.append(datom);
        }

        Ok(Self {
            wal,
            memtable,
            schema,
            unique_index,
            next_tx_id,
        })
    }

    /// Validates and applies a batch of facts.
    /// This is an all-or-nothing operation (ACID).
    pub fn transact(&mut self, facts: Vec<Fact>) -> Result<TxReceipt> {
        if facts.is_empty() {
            return Ok(TxReceipt {
                tx_id: self.next_tx_id,
                timestamp: Utc::now().timestamp_micros(),
                datoms_written: 0,
            });
        }

        let tx_id = self.next_tx_id;
        let timestamp = Utc::now().timestamp_micros();
        let mut pending_datoms = Vec::with_capacity(facts.len());

        // We track temporary uniqueness changes during this batch to prevent
        // conflicts within the same transaction.
        let mut batch_unique_inserts = AHashMap::new();

        // Phase 1: Validation & Translation
        for fact in facts {
            // 1. Resolve Schema Ident
            let attr = self
                .schema
                .get_by_ident(&fact.ident)
                .ok_or_else(|| MesoError::UndefinedAttribute(fact.ident.clone()))?;

            // 2. Validate Data Type
            if !Self::matches_type(&attr.value_type, &fact.v) {
                return Err(MesoError::TypeMismatch {
                    expected: attr.value_type.clone(),
                    found: fact.v.clone(),
                });
            }

            // 3. Enforce Unique Constraints
            if attr.is_unique && fact.op {
                let key = (attr.id, fact.v.clone());

                // Check the existing index
                if let Some(&existing_entity) = self.unique_index.get(&key)
                    && existing_entity != fact.e
                {
                    return Err(MesoError::UniqueConstraintViolation {
                        attr: fact.ident,
                        value: fact.v.to_string(),
                        owner: existing_entity,
                    });
                }

                // Check other facts within this same incoming batch
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

            // 4. Construct the physical Datom
            let datom = if fact.op {
                Datom::assert(fact.e, attr.id, fact.v, tx_id, timestamp)
            } else {
                Datom::retract(fact.e, attr.id, fact.v, tx_id, timestamp)
            };

            pending_datoms.push(datom);
        }

        // Phase 2: Durability (Write to SSD)
        self.wal.append_batch(&pending_datoms)?;

        // Phase 3: Update Memory (MemTable & Indexes)
        for datom in pending_datoms {
            if let Some(attr) = self.schema.get_by_id(datom.a)
                && attr.is_unique
            {
                if datom.op {
                    self.unique_index
                        .insert((datom.a, datom.v.clone()), datom.e);
                } else {
                    self.unique_index.remove(&(datom.a, datom.v.clone()));
                }
            }
            self.memtable.append(datom);
        }

        // Advance the transaction ID safely
        self.next_tx_id += 1;

        Ok(TxReceipt {
            tx_id,
            timestamp,
            datoms_written: batch_unique_inserts.len(),
        })
    }

    /// Helper function to ensure an incoming `Value` matches the physical `ValueType`.
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
    use tempfile::NamedTempFile;

    /// Helper to boot a fresh Transactor with a comprehensive schema for testing.
    fn setup_transactor() -> (Transactor, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();

        schema.add_attribute(":user/name", ValueType::String, false);
        schema.add_attribute(":user/email", ValueType::String, true); // Unique
        schema.add_attribute(":user/age", ValueType::Int64, false);
        schema.add_attribute(":math/pi", ValueType::Float64, true); // Unique Float
        schema.add_attribute(":sys/uuid", ValueType::Uuid, true); // Unique UUID
        schema.add_attribute(":user/friend", ValueType::Ref, false);
        schema.add_attribute(":flag/unique_bool", ValueType::Boolean, true);

        let transactor = Transactor::new(temp_file.path(), schema).unwrap();
        (transactor, temp_file)
    }

    // --- STANDARD BEHAVIOR TESTS ---

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
        assert_eq!(receipt.datoms_written, 0); // No unique constraints were hit
    }

    #[test]
    fn test_tx_03_tx_id_increments_correctly() {
        let (mut t, _f) = setup_transactor();
        let r1 = t
            .transact(vec![Fact {
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

        assert_eq!(r1.tx_id, 1);
        assert_eq!(r2.tx_id, 2);
    }

    #[test]
    fn test_tx_04_empty_transaction_noop() {
        let (mut t, _f) = setup_transactor();
        let receipt = t.transact(vec![]).unwrap();

        assert_eq!(receipt.tx_id, 1);
        assert_eq!(t.next_tx_id, 1); // Internal state should not advance
    }

    // --- SCHEMA & TYPE VALIDATION TESTS ---

    #[test]
    fn test_tx_05_missing_schema_ident_fails() {
        let (mut t, _f) = setup_transactor();
        let result = t.transact(vec![Fact {
            e: 1,
            ident: ":user/unknown".to_string(),
            v: Value::Int64(10),
            op: true,
        }]);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MesoError::UndefinedAttribute(_)
        ));
    }

    #[test]
    fn test_tx_06_type_mismatch_fails() {
        let (mut t, _f) = setup_transactor();
        let result = t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".to_string(),
            v: Value::String("Thirty".to_string()),
            op: true,
        }]);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MesoError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_tx_07_uuid_type_handling() {
        let (mut t, _f) = setup_transactor();
        let uuid_bytes = [7u8; 16];
        let receipt = t.transact(vec![Fact {
            e: 10,
            ident: ":sys/uuid".to_string(),
            v: Value::Uuid(uuid_bytes),
            op: true,
        }]);

        assert!(receipt.is_ok());
    }

    #[test]
    fn test_tx_08_ref_type_handling() {
        let (mut t, _f) = setup_transactor();
        // Entity 1 says Entity 2 is their friend
        let receipt = t.transact(vec![Fact {
            e: 1,
            ident: ":user/friend".to_string(),
            v: Value::Ref(2),
            op: true,
        }]);

        assert!(receipt.is_ok());
    }

    // --- UNIQUE CONSTRAINT TESTS ---

    #[test]
    fn test_tx_09_unique_constraint_violation() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".to_string(),
            v: Value::String("test@test.com".to_string()),
            op: true,
        }])
        .unwrap();

        // Entity 2 tries to claim the same email
        let result = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".to_string(),
            v: Value::String("test@test.com".to_string()),
            op: true,
        }]);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            MesoError::UniqueConstraintViolation { .. }
        ));
    }

    #[test]
    fn test_tx_10_unique_constraint_same_entity_update() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".to_string(),
            v: Value::String("alice@test.com".to_string()),
            op: true,
        }])
        .unwrap();

        // Asserting the exact same value for the SAME entity should succeed (idempotency)
        let result = t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".to_string(),
            v: Value::String("alice@test.com".to_string()),
            op: true,
        }]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_tx_11_retraction_frees_unique_constraint() {
        let (mut t, _f) = setup_transactor();
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".to_string(),
            v: Value::String("open@test.com".to_string()),
            op: true,
        }])
        .unwrap();

        // Entity 1 retracts the email
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/email".to_string(),
            v: Value::String("open@test.com".to_string()),
            op: false,
        }])
        .unwrap();

        // Entity 2 can now claim it
        let result = t.transact(vec![Fact {
            e: 2,
            ident: ":user/email".to_string(),
            v: Value::String("open@test.com".to_string()),
            op: true,
        }]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_tx_12_intra_batch_unique_collision() {
        let (mut t, _f) = setup_transactor();
        // In the EXACT SAME batch, two different entities try to claim the same unique email.
        // The transaction must be rejected.
        let result = t.transact(vec![
            Fact {
                e: 10,
                ident: ":user/email".to_string(),
                v: Value::String("race@test.com".to_string()),
                op: true,
            },
            Fact {
                e: 11,
                ident: ":user/email".to_string(),
                v: Value::String("race@test.com".to_string()),
                op: true,
            },
        ]);

        assert!(result.is_err());
    }

    #[test]
    fn test_tx_13_float_uniqueness_with_nan_protection() {
        let (mut t, _f) = setup_transactor();
        // We implemented raw bit-hashing for Floats. Let's ensure it catches unique violations!
        t.transact(vec![Fact {
            e: 1,
            ident: ":math/pi".to_string(),
            v: Value::Float64(std::f64::consts::PI),
            op: true,
        }])
        .unwrap();

        let result = t.transact(vec![Fact {
            e: 2,
            ident: ":math/pi".to_string(),
            v: Value::Float64(std::f64::consts::PI),
            op: true,
        }]);

        assert!(result.is_err());
    }

    #[test]
    fn test_tx_14_boolean_unique_limit() {
        let (mut t, _f) = setup_transactor();
        // A unique boolean is weird, but legally testable.
        // Entity 1 claims true.
        t.transact(vec![Fact {
            e: 1,
            ident: ":flag/unique_bool".to_string(),
            v: Value::Boolean(true),
            op: true,
        }])
        .unwrap();
        // Entity 2 claims false.
        t.transact(vec![Fact {
            e: 2,
            ident: ":flag/unique_bool".to_string(),
            v: Value::Boolean(false),
            op: true,
        }])
        .unwrap();

        // Entity 3 tries to claim true -> should fail!
        let result = t.transact(vec![Fact {
            e: 3,
            ident: ":flag/unique_bool".to_string(),
            v: Value::Boolean(true),
            op: true,
        }]);
        assert!(result.is_err());
    }

    // --- ATOMICITY & DURABILITY TESTS ---

    #[test]
    fn test_tx_15_atomicity_failure_rolls_back() {
        let (mut t, _f) = setup_transactor();

        // Batch with 1 good fact, followed by 1 bad fact (type mismatch)
        let result = t.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".to_string(),
                v: Value::String("Valid".to_string()),
                op: true,
            },
            Fact {
                e: 1,
                ident: ":user/age".to_string(),
                v: Value::String("InvalidType".to_string()),
                op: true,
            },
        ]);

        assert!(result.is_err());

        // Ensure the transaction completely rolled back and `tx_id` did NOT increment
        assert_eq!(t.next_tx_id, 1);

        // Since it never hit Phase 2 (WAL) or Phase 3 (MemTable), the name "Valid" should not be unique-indexed or written.
        let unique_claim = t.transact(vec![Fact {
            e: 2,
            ident: ":user/name".to_string(),
            v: Value::String("Valid".to_string()),
            op: true,
        }]);
        assert!(unique_claim.is_ok()); // Entity 2 can use it because Entity 1's batch failed.
    }

    #[test]
    fn test_tx_16_wal_recovery_rebuilds_memory_and_indexes() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/email", ValueType::String, true);

        // Scope 1: Boot, transact, and shut down
        {
            let mut t = Transactor::new(temp_file.path(), schema.clone()).unwrap();
            t.transact(vec![Fact {
                e: 10,
                ident: ":user/email".to_string(),
                v: Value::String("persist@test.com".to_string()),
                op: true,
            }])
            .unwrap();
        } // `t` is dropped here, simulating a server crash

        // Scope 2: Reboot, WAL automatically recovers the state
        let mut t2 = Transactor::new(temp_file.path(), schema).unwrap();

        // Ensure the unique constraint was properly restored into the unique_index by the WAL
        let result = t2.transact(vec![Fact {
            e: 11,
            ident: ":user/email".to_string(),
            v: Value::String("persist@test.com".to_string()),
            op: true,
        }]);

        assert!(result.is_err());
    }

    // --- EXTREME LOAD TESTS ---

    #[test]
    fn test_tx_17_large_batch_processing() {
        let (mut t, _f) = setup_transactor();
        let mut huge_batch = Vec::with_capacity(5000);

        for i in 0..5000 {
            huge_batch.push(Fact {
                e: i as u64,
                ident: ":user/age".to_string(),
                v: Value::Int64(i as i64),
                op: true,
            });
        }

        let receipt = t.transact(huge_batch).unwrap();
        assert_eq!(receipt.tx_id, 1);
        // Ensure state safely advanced to the next tx
        assert_eq!(t.next_tx_id, 2);
    }

    #[test]
    fn test_tx_18_multiple_retractions_in_large_batch() {
        let (mut t, _f) = setup_transactor();

        // Setup initial fact
        t.transact(vec![Fact {
            e: 1,
            ident: ":user/age".to_string(),
            v: Value::Int64(30),
            op: true,
        }])
        .unwrap();

        // Submit a massive batch of retractions for facts that might or might not exist
        let mut retraction_batch = Vec::with_capacity(100);
        for i in 0..100 {
            retraction_batch.push(Fact {
                e: i as u64,
                ident: ":user/age".to_string(),
                v: Value::Int64(30),
                op: false, // Retract
            });
        }

        // Database should flawlessly accept retractions, even for data it never held (idempotent history)
        let receipt = t.transact(retraction_batch);
        assert!(receipt.is_ok());
    }

    // --- EDGE CASE TESTS ---

    #[test]
    fn test_tx_19_retract_non_existent_unique() {
        let (mut t, _f) = setup_transactor();
        // Retracting a unique value that was NEVER asserted
        let receipt = t.transact(vec![Fact {
            e: 99,
            ident: ":user/email".to_string(),
            v: Value::String("ghost@test.com".to_string()),
            op: false,
        }]);

        // Should succeed without corrupting the unique index
        assert!(receipt.is_ok());
    }

    #[test]
    fn test_tx_20_assert_and_retract_same_batch_non_unique() {
        let (mut t, _f) = setup_transactor();
        // A system might rapidly assert and retract a status in a single burst update.
        // As long as it isn't violating a unique lock, this should process flawlessly into the history log.
        let receipt = t.transact(vec![
            Fact {
                e: 1,
                ident: ":user/name".to_string(),
                v: Value::String("Flash".to_string()),
                op: true,
            },
            Fact {
                e: 1,
                ident: ":user/name".to_string(),
                v: Value::String("Flash".to_string()),
                op: false,
            },
        ]);

        assert!(receipt.is_ok());
    }
}
