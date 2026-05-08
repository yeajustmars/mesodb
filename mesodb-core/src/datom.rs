// src/datom.rs
use crate::types::{AttributeId, EntityId, TimestampMicros, TxId, Value};

/// The fundamental, immutable bitemporal fact in MesoDB.
#[derive(Debug, Clone, PartialEq)]
pub struct Datom {
    /// E: The subject of the fact
    pub e: EntityId,
    /// A: The property being defined
    pub a: AttributeId,
    /// V: The object/value of the fact
    pub v: Value,
    /// T: System Time (Transaction ID)
    pub t: TxId,
    /// Op: True for assertion (add), False for retraction (remove)
    pub op: bool,
    /// Valid From: When this fact became true in the domain
    pub valid_from: TimestampMicros,
    /// Valid To: When this fact ceased to be true (defaults to MAX)
    pub valid_to: TimestampMicros,
}

impl Datom {
    /// Creates a new assertion Datom with infinite valid_to.
    pub fn assert(
        e: EntityId,
        a: AttributeId,
        v: Value,
        t: TxId,
        valid_from: TimestampMicros,
    ) -> Self {
        Self {
            e,
            a,
            v,
            t,
            op: true,
            valid_from,
            valid_to: i64::MAX, // Infinity by default
        }
    }

    /// Creates a retraction Datom to end a fact's validity.
    pub fn retract(
        e: EntityId,
        a: AttributeId,
        v: Value,
        t: TxId,
        valid_from: TimestampMicros, // The exact time it stopped being true
    ) -> Self {
        Self {
            e,
            a,
            v,
            t,
            op: false,
            valid_from,
            valid_to: valid_from, // A retraction represents the boundary
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_datom_assertion_creation() {
        let now = Utc::now().timestamp_micros();
        let datom = Datom::assert(
            1001,
            50, // Assuming 50 is mapped to :user/name
            Value::String("Alice".to_string()),
            1,
            now,
        );

        assert_eq!(datom.e, 1001);
        assert_eq!(datom.op, true);
        assert_eq!(datom.valid_to, i64::MAX); // Should be valid until the end of time
    }

    #[test]
    fn test_datom_retraction_creation() {
        let ret_time = Utc::now().timestamp_micros();
        let datom = Datom::retract(1001, 50, Value::String("Alice".to_string()), 2, ret_time);

        assert_eq!(datom.op, false);
        // Valid_to should reflect exactly when the retraction happened
        assert_eq!(datom.valid_to, ret_time);
    }
}
