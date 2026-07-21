// mesodb-core/src/index.rs

use ahash::AHashMap;

use crate::types::{AttributeId, EntityId, Value};

#[derive(Debug, Default, Clone)]
pub struct IndexManager {
    /// Flattened EAVT Index: (EntityId, AttributeId) -> flat timeline vector
    pub eavt: AHashMap<(EntityId, AttributeId), Vec<(i64, Option<Value>)>>,

    /// Two-Level Unique Index: AttributeId -> Value -> flat timeline vector
    /// This structure allows us to query by `&Value` without cloning!
    pub unique_index: AHashMap<AttributeId, AHashMap<Value, Vec<(i64, Option<EntityId>)>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fast-path chronological insertion. If an out-of-order time-travel fact arrives,
    /// it correctly shifts it into place, but 99.9% of the time it is a raw O(1) push.
    #[inline(always)]
    fn insert_timeline<T>(timeline: &mut Vec<(i64, Option<T>)>, valid_time: i64, val: Option<T>) {
        if timeline
            .last()
            .map_or(true, |(last_t, _)| *last_t <= valid_time)
        {
            timeline.push((valid_time, val));
        } else {
            let pos = timeline
                .binary_search_by_key(&valid_time, |&(t, _)| t)
                .unwrap_or_else(|e| e);
            timeline.insert(pos, (valid_time, val));
        }
    }

    pub fn insert(
        &mut self,
        e: EntityId,
        a: AttributeId,
        v: Value,
        is_unique: bool,
        valid_time: i64,
    ) {
        let eavt_timeline = self.eavt.entry((e, a)).or_default();
        Self::insert_timeline(eavt_timeline, valid_time, Some(v.clone()));

        if is_unique {
            let unique_timeline = self
                .unique_index
                .entry(a)
                .or_default()
                .entry(v)
                .or_default();
            Self::insert_timeline(unique_timeline, valid_time, Some(e));
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
        let eavt_timeline = self.eavt.entry((e, a)).or_default();
        Self::insert_timeline(eavt_timeline, valid_time, None);

        if is_unique {
            if let Some(val_map) = self.unique_index.get_mut(&a) {
                let unique_timeline = val_map.entry(v.clone()).or_default();
                Self::insert_timeline(unique_timeline, valid_time, None);
            }
        }
    }

    pub fn get_value_at(&self, e: EntityId, a: AttributeId, valid_time: i64) -> Option<&Value> {
        self.eavt
            .get(&(e, a))
            .and_then(|timeline| {
                // Reverse search is instant because timelines are almost always 1-2 items long
                timeline.iter().rev().find(|(t, _)| *t <= valid_time)
            })
            .and_then(|(_, opt_v)| opt_v.as_ref())
    }

    pub fn get_owner_of_unique_at(
        &self,
        a: AttributeId,
        v: &Value,
        valid_time: i64,
    ) -> Option<EntityId> {
        self.unique_index
            .get(&a)
            .and_then(|val_map| val_map.get(v)) // ZERO CLONING!
            .and_then(|timeline| timeline.iter().rev().find(|(t, _)| *t <= valid_time))
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
        if let Some(timeline) = self.unique_index.get(&a).and_then(|m| m.get(v)) {
            for (t, opt_owner) in timeline.iter() {
                if *t >= valid_time {
                    if let Some(owner) = opt_owner {
                        if *owner != requester {
                            return Some(*owner);
                        }
                    }
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
