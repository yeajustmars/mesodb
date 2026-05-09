// mesodb-core/src/index.rs
use crate::types::Value;
use std::collections::{BTreeMap, HashSet};

pub struct IndexManager {
    /// AVET Index: Attribute -> Value -> Set of Entities
    pub avet: BTreeMap<u32, BTreeMap<Value, HashSet<u64>>>,
    /// EAVT Index: Entity -> Attribute -> Value
    pub eavt: BTreeMap<u64, BTreeMap<u32, Value>>,
    /// Tracks unique constraints: (AttributeId, Value) -> EntityId
    pub unique_index: BTreeMap<(u32, Value), u64>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            avet: BTreeMap::new(),
            eavt: BTreeMap::new(),
            unique_index: BTreeMap::new(),
        }
    }

    /// Primary entry point for updating all indices simultaneously.
    pub fn insert(&mut self, e: u64, a: u32, v: Value) {
        self.avet
            .entry(a)
            .or_default()
            .entry(v.clone())
            .or_default()
            .insert(e);

        self.eavt.entry(e).or_default().insert(a, v);
    }
}
