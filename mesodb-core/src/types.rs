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
            (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Ref(a), Value::Ref(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
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
