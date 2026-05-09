// mesodb-core/src/types.rs
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

use crate::error::MesoError;

pub type Result<T> = std::result::Result<T, MesoError>;

pub type EntityId = u64;
pub type AttributeId = u32;
pub type TxId = u64;
pub type TimestampMicros = i64;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Value {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Ref(EntityId),
    Timestamp(TimestampMicros),
    Uuid([u8; 16]),
}

impl Value {
    /// Helper to provide a stable ordering for variants in the total order.
    fn variant_order(&self) -> u8 {
        match self {
            Value::Boolean(_) => 0,
            Value::Int64(_) => 1,
            Value::Float64(_) => 2,
            Value::String(_) => 3,
            Value::Ref(_) => 4,
            Value::Timestamp(_) => 5,
            Value::Uuid(_) => 6,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            // Compare floats via bits to ensure NaN-safe equality
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Ref(a), Value::Ref(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            // Use f64::total_cmp to provide a robust total order for floats
            (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Ref(a), Value::Ref(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            // If variants differ, compare their defined order
            _ => self.variant_order().cmp(&other.variant_order()),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Boolean(b) => b.hash(state),
            Value::Int64(i) => i.hash(state),
            Value::Float64(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Ref(r) => r.hash(state),
            Value::Timestamp(t) => t.hash(state),
            Value::Uuid(u) => u.hash(state),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Int64(i) => write!(f, "{}", i),
            Value::Float64(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Ref(r) => write!(f, "Ref({})", r),
            Value::Timestamp(t) => write!(f, "Inst({})", t),
            Value::Uuid(u) => write!(f, "Uuid({:?})", u),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_display_formatting() {
        assert_eq!(Value::Boolean(true).to_string(), "true");
        assert_eq!(Value::Int64(42).to_string(), "42");
        assert_eq!(Value::Float64(4.14).to_string(), "4.14");
        assert_eq!(Value::String("hello".to_string()).to_string(), "\"hello\"");
        assert_eq!(Value::Ref(999).to_string(), "Ref(999)");
        assert_eq!(Value::Timestamp(1600000000).to_string(), "Inst(1600000000)");
    }

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Int64(100), Value::Int64(100));
        assert_ne!(Value::Int64(100), Value::Float64(100.0));
    }

    #[test]
    fn test_float_equality_and_hashing() {
        let f1 = Value::Float64(std::f64::consts::PI);
        let f2 = Value::Float64(std::f64::consts::PI);
        assert_eq!(f1, f2);
    }
}
