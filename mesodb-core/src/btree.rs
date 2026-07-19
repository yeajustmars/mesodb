// mesodb-core/src/btree.rs

use std::path::Path;

use crate::{
    error::MesoError,
    page::{IndexKey, IndexValue, NUM_CELLS},
    pager::Pager,
    types::{AttributeId, EntityId, Value},
};

pub struct NowIndex {
    pub pager: Pager,
    pub root_page_id: u32,
}

impl NowIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let mut pager = Pager::open(path, 1)?;

        let root = pager.get_node_mut(0)?;
        if root.header.num_cells == 0 && root.header.is_leaf == 0 {
            root.header.is_leaf = 1;
            pager.flush()?;
        }

        Ok(Self {
            pager,
            root_page_id: 0,
        })
    }

    pub fn get(&self, e: EntityId, a: AttributeId) -> Result<Option<Value>, MesoError> {
        let target_key = IndexKey { e, a, _pad: 0 };
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.pager.get_node(current_page_id)?;
            let num_cells = node.header.num_cells as usize;

            if num_cells == 0 {
                return Ok(None);
            }

            if node.header.is_leaf == 1 {
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

                if left < num_cells && node.keys[left] == target_key {
                    let iv = &node.values[left];
                    // 254 is our Tombstone tag for retracted values
                    if iv.type_tag == 254 {
                        return Ok(None);
                    }
                    return Ok(Some(Self::decode_value(iv)));
                }
                return Ok(None);
            } else {
                // Internal Node Routing
                let mut child_idx = 0;
                while child_idx < num_cells && target_key >= node.keys[child_idx] {
                    child_idx += 1;
                }
                if child_idx > 0 {
                    child_idx = child_idx.saturating_sub(1);
                }
                current_page_id = Self::child_page_id(&node.values[child_idx]);
            }
        }
    }

    /// Retracts a value from the current state index by inserting a Tombstone.
    pub fn delete(&mut self, e: EntityId, a: AttributeId) -> Result<(), MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        let tombstone = IndexValue {
            type_tag: 254,
            padding: [0; 7],
            payload: [0; 8],
        };

        if let Some((split_key, right_page_id)) =
            self.insert_into_node(self.root_page_id, key, tombstone)?
        {
            // Re-use the exact same root-split logic from `put`
            let new_left_id = self.pager.allocate_page()?;
            {
                let root_node = self.pager.get_node(self.root_page_id)?;
                let new_left_node = *root_node;
                let left_node_mut = self.pager.get_node_mut(new_left_id)?;
                *left_node_mut = new_left_node;
            }
            let left_min_key = self.pager.get_node(new_left_id)?.keys[0];
            let root_mut = self.pager.get_node_mut(self.root_page_id)?;

            root_mut.header.is_leaf = 0;
            root_mut.header.num_cells = 2;
            root_mut.keys[0] = left_min_key;
            root_mut.values[0] = Self::make_child_ptr(new_left_id);
            root_mut.keys[1] = split_key;
            root_mut.values[1] = Self::make_child_ptr(right_page_id);
        }
        Ok(())
    }

    pub fn put(&mut self, e: EntityId, a: AttributeId, v: &Value) -> Result<(), MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        let val = Self::encode_value(v)?;

        if let Some((split_key, right_page_id)) =
            self.insert_into_node(self.root_page_id, key, val)?
        {
            // The Root Split! We keep the root at Page 0, move the old root data to a new left page.
            let new_left_id = self.pager.allocate_page()?;

            {
                let root_node = self.pager.get_node(self.root_page_id)?;
                let new_left_node = *root_node;
                let left_node_mut = self.pager.get_node_mut(new_left_id)?;
                *left_node_mut = new_left_node;
            }

            let left_min_key = self.pager.get_node(new_left_id)?.keys[0];
            let root_mut = self.pager.get_node_mut(self.root_page_id)?;

            // Rewrite Root as an Internal Node
            root_mut.header.is_leaf = 0;
            root_mut.header.num_cells = 2;
            root_mut.keys[0] = left_min_key;
            root_mut.values[0] = Self::make_child_ptr(new_left_id);
            root_mut.keys[1] = split_key;
            root_mut.values[1] = Self::make_child_ptr(right_page_id);
        }

        Ok(())
    }

    /// Recursively inserts into the tree. Returns `Some((RoutingKey, NewRightPageId))` if it triggered a split.
    fn insert_into_node(
        &mut self,
        page_id: u32,
        target_key: IndexKey,
        target_val: IndexValue,
    ) -> Result<Option<(IndexKey, u32)>, MesoError> {
        let is_leaf;
        let num_cells;
        {
            let node = self.pager.get_node(page_id)?;
            is_leaf = node.header.is_leaf == 1;
            num_cells = node.header.num_cells as usize;
        }

        if is_leaf {
            let mut insert_idx = 0;
            {
                let node = self.pager.get_node(page_id)?;
                while insert_idx < num_cells && node.keys[insert_idx] < target_key {
                    insert_idx += 1;
                }
                if insert_idx < num_cells && node.keys[insert_idx] == target_key {
                    // Exact Match: Overwrite
                    let node_mut = self.pager.get_node_mut(page_id)?;
                    node_mut.values[insert_idx] = target_val;
                    return Ok(None);
                }
            }

            if num_cells < NUM_CELLS {
                let node_mut = self.pager.get_node_mut(page_id)?;
                for i in (insert_idx..num_cells).rev() {
                    node_mut.keys[i + 1] = node_mut.keys[i];
                    node_mut.values[i + 1] = node_mut.values[i];
                }
                node_mut.keys[insert_idx] = target_key;
                node_mut.values[insert_idx] = target_val;
                node_mut.header.num_cells += 1;
                Ok(None)
            } else {
                // SPLIT LEAF NODE
                let mut temp_keys = [IndexKey {
                    e: 0,
                    a: 0,
                    _pad: 0,
                }; NUM_CELLS + 1];
                let mut temp_vals = [IndexValue {
                    type_tag: 0,
                    padding: [0; 7],
                    payload: [0; 8],
                }; NUM_CELLS + 1];

                {
                    let node = self.pager.get_node(page_id)?;
                    temp_keys[..insert_idx].copy_from_slice(&node.keys[..insert_idx]);
                    temp_vals[..insert_idx].copy_from_slice(&node.values[..insert_idx]);
                    temp_keys[insert_idx] = target_key;
                    temp_vals[insert_idx] = target_val;
                    if insert_idx < NUM_CELLS {
                        temp_keys[insert_idx + 1..]
                            .copy_from_slice(&node.keys[insert_idx..NUM_CELLS]);
                        temp_vals[insert_idx + 1..]
                            .copy_from_slice(&node.values[insert_idx..NUM_CELLS]);
                    }
                }

                let right_page_id = self.pager.allocate_page()?;
                let split_idx = NUM_CELLS / 2;
                let left_count = split_idx;
                let right_count = (NUM_CELLS + 1) - split_idx;

                let right_sibling_of_left = {
                    let left_node = self.pager.get_node(page_id)?;
                    left_node.header.right_sibling
                };

                // Populate Right Page
                let right_node = self.pager.get_node_mut(right_page_id)?;
                right_node.header.is_leaf = 1;
                right_node.header.num_cells = right_count as u16;
                right_node.header.right_sibling = right_sibling_of_left;
                right_node.keys[..right_count].copy_from_slice(&temp_keys[left_count..]);
                right_node.values[..right_count].copy_from_slice(&temp_vals[left_count..]);

                // Update Left Page
                let left_node = self.pager.get_node_mut(page_id)?;
                left_node.header.num_cells = left_count as u16;
                left_node.header.right_sibling = right_page_id;
                left_node.keys[..left_count].copy_from_slice(&temp_keys[..left_count]);
                left_node.values[..left_count].copy_from_slice(&temp_vals[..left_count]);

                Ok(Some((temp_keys[left_count], right_page_id)))
            }
        } else {
            // INTERNAL NODE
            let mut child_idx = 0;
            let child_page_id;
            {
                let node = self.pager.get_node(page_id)?;
                while child_idx < num_cells && target_key >= node.keys[child_idx] {
                    child_idx += 1;
                }
                if child_idx > 0 {
                    child_idx = child_idx.saturating_sub(1);
                }
                child_page_id = Self::child_page_id(&node.values[child_idx]);
            }

            let split_res = self.insert_into_node(child_page_id, target_key, target_val)?;

            // If we inserted a new absolute minimum into the child, update our routing key
            {
                let node = self.pager.get_node(page_id)?;
                if target_key < node.keys[child_idx] {
                    let node_mut = self.pager.get_node_mut(page_id)?;
                    node_mut.keys[child_idx] = target_key;
                }
            }

            if let Some((split_key, right_page_id)) = split_res {
                let insert_idx = child_idx + 1;
                let target_val = Self::make_child_ptr(right_page_id);

                if num_cells < NUM_CELLS {
                    let node_mut = self.pager.get_node_mut(page_id)?;
                    for i in (insert_idx..num_cells).rev() {
                        node_mut.keys[i + 1] = node_mut.keys[i];
                        node_mut.values[i + 1] = node_mut.values[i];
                    }
                    node_mut.keys[insert_idx] = split_key;
                    node_mut.values[insert_idx] = target_val;
                    node_mut.header.num_cells += 1;
                    return Ok(None);
                } else {
                    // SPLIT INTERNAL NODE
                    let mut temp_keys = [IndexKey {
                        e: 0,
                        a: 0,
                        _pad: 0,
                    }; NUM_CELLS + 1];
                    let mut temp_vals = [IndexValue {
                        type_tag: 0,
                        padding: [0; 7],
                        payload: [0; 8],
                    }; NUM_CELLS + 1];

                    {
                        let node = self.pager.get_node(page_id)?;
                        temp_keys[..insert_idx].copy_from_slice(&node.keys[..insert_idx]);
                        temp_vals[..insert_idx].copy_from_slice(&node.values[..insert_idx]);
                        temp_keys[insert_idx] = split_key;
                        temp_vals[insert_idx] = target_val;
                        if insert_idx < NUM_CELLS {
                            temp_keys[insert_idx + 1..]
                                .copy_from_slice(&node.keys[insert_idx..NUM_CELLS]);
                            temp_vals[insert_idx + 1..]
                                .copy_from_slice(&node.values[insert_idx..NUM_CELLS]);
                        }
                    }

                    let right_internal_id = self.pager.allocate_page()?;
                    let split_idx = NUM_CELLS / 2;
                    let left_count = split_idx;
                    let right_count = (NUM_CELLS + 1) - split_idx;

                    let right_node = self.pager.get_node_mut(right_internal_id)?;
                    right_node.header.is_leaf = 0;
                    right_node.header.num_cells = right_count as u16;
                    right_node.keys[..right_count].copy_from_slice(&temp_keys[left_count..]);
                    right_node.values[..right_count].copy_from_slice(&temp_vals[left_count..]);

                    let left_node = self.pager.get_node_mut(page_id)?;
                    left_node.header.num_cells = left_count as u16;
                    left_node.keys[..left_count].copy_from_slice(&temp_keys[..left_count]);
                    left_node.values[..left_count].copy_from_slice(&temp_vals[..left_count]);

                    return Ok(Some((temp_keys[left_count], right_internal_id)));
                }
            }
            Ok(None)
        }
    }

    pub fn flush(&self) -> Result<(), MesoError> {
        self.pager.flush()
    }

    // --- Type Translation Helpers ---

    fn child_page_id(val: &IndexValue) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&val.payload[0..4]);
        u32::from_ne_bytes(bytes)
    }

    fn make_child_ptr(page_id: u32) -> IndexValue {
        let mut payload = [0u8; 8];
        payload[0..4].copy_from_slice(&page_id.to_ne_bytes());
        IndexValue {
            type_tag: 255,
            padding: [0; 7],
            payload,
        }
    }

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
                    "String indexing pending overflow pages".into(),
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
                    "UUID indexing pending overflow pages".into(),
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
            _ => Value::Boolean(false),
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

        // Insert the 128th item. This MUST trigger a split instead of an error.
        index
            .put(999, 10, &Value::Int64(999))
            .expect("Insert must succeed and trigger a split");

        // Validate the tree structurally expanded
        assert!(
            index.pager.num_pages >= 3,
            "A root split should allocate at least two new pages (left child, right child)"
        );

        // Validate the routing logic still finds the inserted item across the split
        let val = index.get(999, 10).unwrap().unwrap();
        assert_eq!(
            val,
            Value::Int64(999),
            "Data must remain retrievable after the split"
        );
    }

    #[test]
    fn test_b_tree_node_splitting_and_scaling() {
        let (mut index, _f) = setup_index();
        let target_records = 10_000; // Will force dozens of page splits

        // Insert massively out-of-order to heavily stress the node router
        for i in (0..target_records).rev() {
            index.put(i, 10, &Value::Int64(i as i64)).unwrap();
        }

        // Validate the point-lookup router correctly navigates the deep internal nodes
        for i in 0..target_records {
            let val = index.get(i, 10).unwrap().unwrap();
            assert_eq!(val, Value::Int64(i as i64));
        }

        // Validate structural B+Tree state
        let root = index.pager.get_node(index.root_page_id).unwrap();

        assert_eq!(
            root.header.is_leaf, 0,
            "Root must have transformed into an internal routing node"
        );
        assert!(
            index.pager.num_pages > 50,
            "The tree must have dynamically requested and formatted dozens of new pages from the OS"
        );
    }
}
