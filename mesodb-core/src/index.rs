// mesodb-core/src/index.rs

use crate::types::{AttributeId, EntityId, Value};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct IndexManager {
    /// EAVT Index: Entity -> Attribute -> Value
    /// Used by the Transactor to find the "current" value of an attribute
    /// so it can close its bitemporal interval when a new value is asserted.
    eavt: BTreeMap<EntityId, BTreeMap<AttributeId, Value>>,

    /// Tracks unique constraints: (AttributeId, Value) -> EntityId
    /// Used by the Transactor for O(1) collision detection.
    unique_index: BTreeMap<(AttributeId, Value), EntityId>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a new active datom in the writer's memory state.
    pub fn insert(&mut self, e: EntityId, a: AttributeId, v: Value, is_unique: bool) {
        self.eavt.entry(e).or_default().insert(a, v.clone());
        if is_unique {
            self.unique_index.insert((a, v), e);
        }
    }

    /// Removes a datom from the writer's memory state (e.g., during a retraction).
    pub fn remove(&mut self, e: EntityId, a: AttributeId, v: &Value, is_unique: bool) {
        if let Some(attrs) = self.eavt.get_mut(&e) {
            if let Some(existing_v) = attrs.get(&a) {
                if existing_v == v {
                    attrs.remove(&a);
                }
            }
            // Cleanup empty maps to prevent memory leaks
            if attrs.is_empty() {
                self.eavt.remove(&e);
            }
        }
        if is_unique {
            self.unique_index.remove(&(a, v.clone()));
        }
    }

    /// Fast lookup to see what value an entity currently holds for an attribute.
    pub fn get_current_value(&self, e: EntityId, a: AttributeId) -> Option<&Value> {
        self.eavt.get(&e).and_then(|attrs| attrs.get(&a))
    }

    /// Fast lookup to see who owns a unique value.
    pub fn get_owner_of_unique(&self, a: AttributeId, v: &Value) -> Option<EntityId> {
        self.unique_index.get(&(a, v.clone())).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_insert_and_get_current() {
        let mut idx = IndexManager::new();
        idx.insert(1, 10, Value::String("Alice".into()), false);
        idx.insert(1, 11, Value::Int64(30), false);

        assert_eq!(
            idx.get_current_value(1, 10),
            Some(&Value::String("Alice".into()))
        );
        assert_eq!(idx.get_current_value(1, 11), Some(&Value::Int64(30)));
        assert_eq!(idx.get_current_value(1, 99), None); // Non-existent attribute
    }

    #[test]
    fn test_index_uniqueness_tracking() {
        let mut idx = IndexManager::new();
        let email_attr = 50;
        let email_val = Value::String("alice@example.com".into());

        // Insert unique value
        idx.insert(1, email_attr, email_val.clone(), true);

        // Check owner
        assert_eq!(idx.get_owner_of_unique(email_attr, &email_val), Some(1));

        // Check a non-existent unique value
        assert_eq!(
            idx.get_owner_of_unique(email_attr, &Value::String("bob@example.com".into())),
            None
        );
    }

    #[test]
    fn test_index_removal_and_cleanup() {
        let mut idx = IndexManager::new();
        idx.insert(2, 20, Value::Int64(100), true);

        // Ensure it's there
        assert_eq!(idx.get_current_value(2, 20), Some(&Value::Int64(100)));
        assert_eq!(idx.get_owner_of_unique(20, &Value::Int64(100)), Some(2));

        // Remove it
        idx.remove(2, 20, &Value::Int64(100), true);

        // Ensure it's gone from both EAVT and Unique Index
        assert_eq!(idx.get_current_value(2, 20), None);
        assert_eq!(idx.get_owner_of_unique(20, &Value::Int64(100)), None);

        // Ensure the empty EAVT map for Entity 2 was cleaned up to save memory
        assert!(!idx.eavt.contains_key(&2));
    }
}
