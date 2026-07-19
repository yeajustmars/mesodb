// mesodb-core/src/btree.rs

use std::path::Path;

use crate::{
    error::MesoError,
    page::{IndexKey, IndexValue, NUM_CELLS},
    pager::Pager,
    types::{AttributeId, EntityId, Value},
};

pub struct NowIndex {
    pager: Pager,
    pub root_page_id: u32,
}

impl NowIndex {
    /// Opens or creates the B+Tree index file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        // Start with 1 page for the root if the file is new
        let mut pager = Pager::open(path, 1)?;

        // Initialize the root page if it's completely empty
        let root = pager.get_node_mut(0)?;
        if root.header.num_cells == 0 && root.header.is_leaf == 0 {
            root.header.is_leaf = 1; // Root starts as a leaf
            pager.flush()?;
        }

        Ok(Self {
            pager,
            root_page_id: 0,
        })
    }

    /// Fast-Path Point Lookup: `O(log N)` binary search over the zero-copy page cache.
    pub fn get(&self, e: EntityId, a: AttributeId) -> Result<Option<Value>, MesoError> {
        let target_key = IndexKey { e, a, _pad: 0 };
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.pager.get_node(current_page_id)?;
            let num_cells = node.header.num_cells as usize;

            // Standard Binary Search on the fixed-width keys array
            let mut left = 0;
            let mut right = num_cells;

            while left < right {
                let mid = left + (right - left) / 2;
                if node.keys[mid] < target_key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            if node.header.is_leaf == 1 {
                // We reached the leaf. Check if the key matches exactly.
                if left < num_cells && node.keys[left] == target_key {
                    return Ok(Some(Self::decode_value(&node.values[left])));
                }
                return Ok(None);
            } else {
                // Internal node routing: follow the pointer to the child page
                // (Assuming internal nodes store child page IDs in the payload)
                if left < num_cells {
                    let child_bytes = node.values[left].payload;
                    current_page_id = u32::from_ne_bytes(child_bytes[0..4].try_into().unwrap());
                } else {
                    // Follow the rightmost sibling/edge pointer
                    current_page_id = node.header.right_sibling;
                }
            }
        }
    }

    /// Inserts or updates the current "Now" value for an Entity-Attribute pair.
    pub fn put(&mut self, e: EntityId, a: AttributeId, v: &Value) -> Result<(), MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        let val = Self::encode_value(v)?;

        // For this step, we will grab the root and insert directly.
        // Node splitting logic will be hooked up next.
        let node = self.pager.get_node_mut(self.root_page_id)?;
        let num_cells = node.header.num_cells as usize;

        let mut insert_idx = 0;
        while insert_idx < num_cells && node.keys[insert_idx] < key {
            insert_idx += 1;
        }

        if insert_idx < num_cells && node.keys[insert_idx] == key {
            // Update existing
            node.values[insert_idx] = val;
        } else {
            // Insert new (Shift cells right)
            if num_cells >= NUM_CELLS {
                // TODO: Trigger B+Tree Node Split
                return Err(MesoError::PlanError(
                    "B+Tree Page Full - Split Required".into(),
                ));
            }

            for i in (insert_idx..num_cells).rev() {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
            }

            node.keys[insert_idx] = key;
            node.values[insert_idx] = val;
            node.header.num_cells += 1;
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<(), MesoError> {
        self.pager.flush()
    }

    // --- Type Translation Helpers ---

    fn encode_value(v: &Value) -> Result<IndexValue, MesoError> {
        let mut payload = [0u8; 8];
        let type_tag = match v {
            Value::Boolean(b) => {
                payload[0] = *b as u8;
                0
            }
            Value::Int64(i) => {
                payload.copy_from_slice(&i.to_ne_bytes());
                1
            }
            Value::Float64(f) => {
                payload.copy_from_slice(&f.to_ne_bytes());
                2
            }
            Value::String(_) => {
                return Err(MesoError::Serialization(
                    "String indexing requires overflow pages (Pending)".into(),
                ));
            }
            Value::Ref(r) => {
                payload.copy_from_slice(&r.to_ne_bytes());
                4
            }
            Value::Timestamp(t) => {
                payload.copy_from_slice(&t.to_ne_bytes());
                5
            }
            Value::Uuid(_) => {
                return Err(MesoError::Serialization(
                    "UUID indexing requires overflow pages (Pending)".into(),
                ));
            }
        };

        Ok(IndexValue {
            type_tag,
            padding: [0; 7],
            payload,
        })
    }

    fn decode_value(iv: &IndexValue) -> Value {
        match iv.type_tag {
            0 => Value::Boolean(iv.payload[0] != 0),
            1 => Value::Int64(i64::from_ne_bytes(iv.payload)),
            2 => Value::Float64(f64::from_ne_bytes(iv.payload)),
            4 => Value::Ref(u64::from_ne_bytes(iv.payload)),
            5 => Value::Timestamp(i64::from_ne_bytes(iv.payload)),
            _ => Value::Boolean(false), // Fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn setup_index() -> (NowIndex, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let index = NowIndex::open(temp_file.path()).unwrap();
        (index, temp_file)
    }

    const PI: f64 = std::f64::consts::PI;

    #[test]
    fn test_now_index_basic_types_encoding() {
        let (mut index, _f) = setup_index();

        let e = 100;
        // Insert one of every supported type
        index.put(e, 1, &Value::Boolean(true)).unwrap();
        index.put(e, 2, &Value::Int64(-42)).unwrap();
        index.put(e, 3, &Value::Float64(PI)).unwrap();
        index.put(e, 4, &Value::Ref(9999)).unwrap();
        index.put(e, 5, &Value::Timestamp(1700000000)).unwrap();

        // Validate exact decoding
        assert_eq!(index.get(e, 1).unwrap(), Some(Value::Boolean(true)));
        assert_eq!(index.get(e, 2).unwrap(), Some(Value::Int64(-42)));
        assert_eq!(index.get(e, 3).unwrap(), Some(Value::Float64(PI)));
        assert_eq!(index.get(e, 4).unwrap(), Some(Value::Ref(9999)));
        assert_eq!(index.get(e, 5).unwrap(), Some(Value::Timestamp(1700000000)));
    }

    #[test]
    fn test_now_index_reverse_insertion_memory_shifting() {
        let (mut index, _f) = setup_index();

        // Insert in strictly descending order.
        // This forces the B+Tree to shift ALL existing memory cells to the right on every single insert.
        for i in (1..=50).rev() {
            index.put(i, 10, &Value::Int64(i as i64)).unwrap();
        }

        // Validate the binary search still resolves them correctly
        for i in 1..=50 {
            let val = index.get(i, 10).unwrap().unwrap();
            assert_eq!(val, Value::Int64(i as i64));
        }

        // Verify the internal page state
        let root = index.pager.get_node(index.root_page_id).unwrap();
        assert_eq!(root.header.num_cells, 50, "Should contain exactly 50 cells");
        assert_eq!(root.keys[0].e, 1, "First key must be the lowest Entity ID");
        assert_eq!(
            root.keys[49].e, 50,
            "Last key must be the highest Entity ID"
        );
    }

    #[test]
    fn test_now_index_update_overwrite() {
        let (mut index, _f) = setup_index();

        // 1. Initial Insert
        index.put(42, 99, &Value::Int64(100)).unwrap();

        let root_before = index.pager.get_node(index.root_page_id).unwrap();
        assert_eq!(root_before.header.num_cells, 1);

        // 2. Overwrite the exact same (Entity, Attribute) pair
        index.put(42, 99, &Value::Int64(200)).unwrap();

        let root_after = index.pager.get_node(index.root_page_id).unwrap();

        // The cell count MUST remain 1, proving we overwrote the value in place
        assert_eq!(
            root_after.header.num_cells, 1,
            "Updates must not increase cell count"
        );

        let val = index.get(42, 99).unwrap().unwrap();
        assert_eq!(
            val,
            Value::Int64(200),
            "Value must reflect the latest update"
        );
    }

    #[test]
    fn test_now_index_capacity_exhaustion_boundary() {
        let (mut index, _f) = setup_index();

        // Fill the page exactly to its maximum capacity (127 cells)
        for i in 0..NUM_CELLS {
            index.put(i as u64, 10, &Value::Int64(i as i64)).unwrap();
        }

        let root = index.pager.get_node(index.root_page_id).unwrap();
        assert_eq!(root.header.num_cells as usize, NUM_CELLS);

        // Attempting to insert the 128th item MUST trigger our specific split error
        let err = index.put(999, 10, &Value::Int64(999)).unwrap_err();

        assert!(
            err.to_string().contains("Split Required"),
            "Expected capacity exhaustion to trigger a split requirement, got: {}",
            err
        );
    }
}
