use rkyv::{Archive, Deserialize, Serialize};
use std::fmt;

pub type EntityId = u64;
pub type AttributeId = u32;
pub type TxId = u64;
pub type TimestampMicros = i64;

#[derive(Debug, Clone, PartialEq, PartialOrd, Archive, Serialize, Deserialize)]
pub enum Value {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Ref(EntityId),
    Timestamp(TimestampMicros),
    Uuid([u8; 16]),
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
