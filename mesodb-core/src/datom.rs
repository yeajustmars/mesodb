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
            valid_to: i64::MAX,
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
            valid_to: valid_from,
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
        let datom = Datom::assert(1001, 50, Value::String("Alice".to_string()), 1, now);
        assert_eq!(datom.e, 1001);
        assert_eq!(datom.op, true);
        assert_eq!(datom.valid_to, i64::MAX);
    }

    #[test]
    fn test_datom_retraction_creation() {
        let ret_time = Utc::now().timestamp_micros();
        let datom = Datom::retract(1001, 50, Value::String("Alice".to_string()), 2, ret_time);
        assert_eq!(datom.op, false);
        assert_eq!(datom.valid_to, ret_time);
    }
}
