// src/types.rs
use std::fmt;

/// The permanent, unique identifier for an Entity in the graph.
pub type EntityId = u64;

/// The identifier for an Attribute (schema definition).
pub type AttributeId = u32;

/// The System Time identifier (Transaction ID).
pub type TxId = u64;

/// A timestamp representing Valid Time (microseconds since UNIX epoch).
pub type TimestampMicros = i64;

/// The strictly-typed Value variant for ingestion.
/// During ingestion, EDN/JSON maps to this Enum.
/// Later, this routes to the specific typed Arrow MemTable.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    /// A pointer to another EntityId. Crucial for graph traversals.
    Ref(EntityId),
    /// Temporal values
    Timestamp(TimestampMicros),
    /// A fixed 16-byte UUID for extremely fast scanning
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
