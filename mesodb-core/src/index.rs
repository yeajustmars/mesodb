// mesodb-core/src/index.rs

use crate::types::{AttributeId, EntityId, Value};
use ahash::AHashMap;
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone)]
pub struct IndexManager {
    /// Flattened 4D EAVT Index: (EntityId, AttributeId) -> ValidTime -> Option<Value>
    /// A `None` value represents a retraction (closing the bitemporal interval).
    pub eavt: AHashMap<(EntityId, AttributeId), BTreeMap<i64, Option<Value>>>,

    /// Flattened 4D Unique Index: (AttributeId, Value) -> ValidTime -> Option<EntityId>
    pub unique_index: AHashMap<(AttributeId, Value), BTreeMap<i64, Option<EntityId>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        e: EntityId,
        a: AttributeId,
        v: Value,
        is_unique: bool,
        valid_time: i64,
    ) {
        self.eavt
            .entry((e, a))
            .or_default()
            .insert(valid_time, Some(v.clone()));
        if is_unique {
            self.unique_index
                .entry((a, v))
                .or_default()
                .insert(valid_time, Some(e));
        }
    }

    pub fn remove(
        &mut self,
        e: EntityId,
        a: AttributeId,
        v: &Value,
        is_unique: bool,
        valid_time: i64,
    ) {
        // We append `None` to the timeline to officially close the interval at `valid_time`.
        self.eavt
            .entry((e, a))
            .or_default()
            .insert(valid_time, None);
        if is_unique {
            self.unique_index
                .entry((a, v.clone()))
                .or_default()
                .insert(valid_time, None);
        }
    }

    pub fn get_value_at(&self, e: EntityId, a: AttributeId, valid_time: i64) -> Option<&Value> {
        self.eavt
            .get(&(e, a))
            .and_then(|timeline| timeline.range(..=valid_time).next_back())
            .and_then(|(_, opt_v)| opt_v.as_ref())
    }

    pub fn get_owner_of_unique_at(
        &self,
        a: AttributeId,
        v: &Value,
        valid_time: i64,
    ) -> Option<EntityId> {
        self.unique_index
            .get(&(a, v.clone()))
            .and_then(|timeline| timeline.range(..=valid_time).next_back())
            .and_then(|(_, opt_e)| opt_e.as_ref())
            .copied()
    }

    /// SQL:2011 WITHOUT OVERLAPS validation.
    pub fn get_future_unique_conflict(
        &self,
        a: AttributeId,
        v: &Value,
        requester: EntityId,
        valid_time: i64,
    ) -> Option<EntityId> {
        if let Some(timeline) = self.unique_index.get(&(a, v.clone())) {
            for (_, opt_owner) in timeline.range(valid_time..) {
                if let Some(owner) = opt_owner
                    && *owner != requester
                {
                    return Some(*owner);
                }
            }
        }
        None
    }

    /// Convenience method: gets the latest known value by querying at the end of time.
    pub fn get_current_value(&self, e: EntityId, a: AttributeId) -> Option<&Value> {
        self.get_value_at(e, a, i64::MAX)
    }

    /// Convenience method: gets the latest known owner by querying at the end of time.
    pub fn get_owner_of_unique(&self, a: AttributeId, v: &Value) -> Option<EntityId> {
        self.get_owner_of_unique_at(a, v, i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_insert_and_get_current() {
        let mut idx = IndexManager::new();
        idx.insert(1, 10, Value::String("Alice".into()), false, 100);
        idx.insert(1, 11, Value::Int64(30), false, 100);

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
        idx.insert(1, email_attr, email_val.clone(), true, 100);

        // Check owner
        assert_eq!(idx.get_owner_of_unique(email_attr, &email_val), Some(1));

        // Check a non-existent unique value
        assert_eq!(
            idx.get_owner_of_unique(email_attr, &Value::String("bob@example.com".into())),
            None
        );
    }

    #[test]
    fn test_index_removal_and_bitemporal_tombstones() {
        let mut idx = IndexManager::new();
        // Insert at T=100
        idx.insert(2, 20, Value::Int64(100), true, 100);

        // Ensure it's there presently
        assert_eq!(idx.get_current_value(2, 20), Some(&Value::Int64(100)));
        assert_eq!(idx.get_owner_of_unique(20, &Value::Int64(100)), Some(2));

        // Retract at T=200
        idx.remove(2, 20, &Value::Int64(100), true, 200);

        // Ensure it's gone from the PRESENT
        assert_eq!(idx.get_current_value(2, 20), None);
        assert_eq!(idx.get_owner_of_unique(20, &Value::Int64(100)), None);

        // Prove bitemporality: The entity map MUST still exist to preserve history
        assert!(
            idx.eavt.contains_key(&(2, 20)),
            "Timeline map must survive to preserve history"
        );

        // Prove time travel: The value was still active at T=150!
        assert_eq!(idx.get_value_at(2, 20, 150), Some(&Value::Int64(100)));
    }
}
