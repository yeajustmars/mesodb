use crate::types::{AttributeId, EntityId, TimestampMicros, TxId, Value};
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub struct Datom {
    pub e: EntityId,
    pub a: AttributeId,
    pub v: Value,
    pub t: TxId,
    pub op: bool,
    pub valid_from: TimestampMicros,
    pub valid_to: TimestampMicros,
}

impl Datom {
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
            valid_to: i64::MAX, // Assertions are valid until the end of time (or until retracted)
        }
    }

    pub fn retract(
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
            op: false,
            valid_from,
            valid_to: valid_from, // Retractions immediately close their own interval
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_datom_assertion_creation() {
        let now = 1600000000;
        let datom = Datom::assert(1001, 50, Value::String("Alice".to_string()), 1, now);

        assert_eq!(datom.e, 1001);
        assert!(datom.op);
        assert_eq!(datom.valid_from, now);
        assert_eq!(datom.valid_to, i64::MAX);
    }

    #[test]
    fn test_datom_retraction_creation() {
        let ret_time = 1600000500;
        let datom = Datom::retract(1001, 50, Value::String("Alice".to_string()), 2, ret_time);

        assert!(!datom.op);
        assert_eq!(datom.valid_from, ret_time);
        assert_eq!(datom.valid_to, ret_time);
    }

    #[test]
    fn test_datom_equality() {
        let d1 = Datom::assert(1, 10, Value::Int64(100), 1, 5000);
        let d2 = Datom::assert(1, 10, Value::Int64(100), 1, 5000);
        let d3 = Datom::assert(1, 10, Value::Int64(101), 1, 5000); // Different value

        assert_eq!(d1, d2);
        assert_ne!(d1, d3);
    }
}
