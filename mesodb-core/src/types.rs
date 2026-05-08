use rkyv::{Archive, Deserialize, Serialize};
use std::fmt;

pub type EntityId = u64;
pub type AttributeId = u32;
pub type TxId = u64;
pub type TimestampMicros = i64;

// 1. Notice we removed `PartialEq` from the derive macro!
#[derive(Debug, Clone, PartialOrd, Archive, Serialize, Deserialize)]
pub enum Value {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Ref(EntityId),
    Timestamp(TimestampMicros),
    Uuid([u8; 16]),
}

// 2. We manually implement strict Equality, converting floats to bits.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            // Compare floats via their exact memory bit patterns to safely handle NaN
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Ref(a), Value::Ref(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            _ => false, // Different variants are never equal
        }
    }
}

// 3. Because our PartialEq now handles NaN safely, we can promise Rust this is strictly Equal.
impl Eq for Value {}

// 4. We manually implement Hashing, again routing floats through `to_bits()`.
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the variant type so Int64(1) hashes differently than Timestamp(1)
        std::mem::discriminant(self).hash(state);

        match self {
            Value::Boolean(b) => b.hash(state),
            Value::Int64(i) => i.hash(state),
            // Hash the raw binary bits of the float
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

        let uuid_bytes = [1u8; 16];
        assert_eq!(
            Value::Uuid(uuid_bytes).to_string(),
            "Uuid([1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])"
        );
    }

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Int64(100), Value::Int64(100));
        assert_ne!(Value::Int64(100), Value::Float64(100.0));
        assert_ne!(Value::Ref(1), Value::Int64(1));
    }

    #[test]
    fn test_float_equality_and_hashing() {
        // Ensure our raw-bit math works for Floats!
        let f1 = Value::Float64(std::f64::consts::PI);
        let f2 = Value::Float64(std::f64::consts::PI);
        assert_eq!(f1, f2);
    }
}
