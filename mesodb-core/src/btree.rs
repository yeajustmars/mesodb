// mesodb-core/src/btree.rs

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::{
    error::MesoError,
    page::{IndexKey, IndexValue, NUM_CELLS, NodePage, OverflowPage},
    pager::{ReadPager, WritePager},
    types::{AttributeId, EntityId, Value},
};

// ==============================================================================
// CORE COW B+TREE ENGINE (Lock-Free Reads, Dirty-Cache Writes)
// ==============================================================================

pub struct CoreBTree {
    pub read_pager: ReadPager,
    pub write_pager: WritePager,
    pub root_page_id: u32,
    pub dirty_nodes: HashMap<u32, NodePage>,
    pub dirty_overflows: HashMap<u32, OverflowPage>,
}

impl CoreBTree {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let mut write_pager = WritePager::open(path)?;
        let file_len = write_pager.file().metadata().map_err(MesoError::Io)?.len();

        let root_page_id;

        if file_len == 0 {
            // Bootstrap New Database
            let mut empty_root = unsafe { std::mem::zeroed::<NodePage>() };
            empty_root.header.is_leaf = 1;

            // Page 1 is the genesis root
            write_pager.write_page_bytes(1, bytemuck::bytes_of(&empty_root))?;

            // Page 0 is the Metapage [root_page_id, next_page_id, padding...]
            let mut metapage = [0u8; 4096];
            metapage[0..4].copy_from_slice(&1u32.to_ne_bytes());
            metapage[4..8].copy_from_slice(&2u32.to_ne_bytes());
            write_pager.write_page_bytes(0, &metapage)?;
            write_pager.flush()?;

            root_page_id = 1;
            write_pager.next_page_id = 2;
        } else {
            // Recover from existing file
            let mut metapage = [0u8; 4096];
            write_pager
                .file
                .seek(SeekFrom::Start(0))
                .map_err(MesoError::Io)?;
            write_pager
                .file
                .read_exact(&mut metapage)
                .map_err(MesoError::Io)?;

            let mut r_bytes = [0u8; 4];
            r_bytes.copy_from_slice(&metapage[0..4]);
            root_page_id = u32::from_ne_bytes(r_bytes);

            let mut n_bytes = [0u8; 4];
            n_bytes.copy_from_slice(&metapage[4..8]);
            write_pager.next_page_id = u32::from_ne_bytes(n_bytes);
        }

        let read_pager = ReadPager::open(write_pager.file())?;

        Ok(Self {
            read_pager,
            write_pager,
            root_page_id,
            dirty_nodes: HashMap::new(),
            dirty_overflows: HashMap::new(),
        })
    }

    /// Pure lock-free reader. Resolves transparently against the dirty cache if un-flushed.
    #[inline(always)]
    pub fn get_node(&self, page_id: u32) -> Result<&NodePage, MesoError> {
        if let Some(node) = self.dirty_nodes.get(&page_id) {
            Ok(node)
        } else {
            self.read_pager.get_node(page_id)
        }
    }

    #[inline(always)]
    pub fn get_overflow(&self, page_id: u32) -> Result<&OverflowPage, MesoError> {
        if let Some(overflow) = self.dirty_overflows.get(&page_id) {
            Ok(overflow)
        } else {
            self.read_pager.get_overflow(page_id)
        }
    }

    pub fn allocate_page(&mut self) -> u32 {
        let id = self.write_pager.next_page_id;
        self.write_pager.next_page_id += 1;
        id
    }

    pub fn get_kv(&self, target_key: IndexKey) -> Result<Option<IndexValue>, MesoError> {
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.get_node(current_page_id)?;
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
                    let iv = node.values[left];
                    // 254 is Tombstone tag
                    if iv.type_tag == 254 {
                        return Ok(None);
                    }
                    return Ok(Some(iv));
                }
                return Ok(None);
            } else {
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

    pub fn put_kv(&mut self, key: IndexKey, val: IndexValue) -> Result<(), MesoError> {
        let (new_root_id, split_opt) = self.insert_into_node(self.root_page_id, key, val)?;

        if let Some((split_key, right_page_id)) = split_opt {
            // Root split! Create a new Super-Root.
            let super_root_id = self.allocate_page();
            let mut super_root = unsafe { std::mem::zeroed::<NodePage>() };
            super_root.header.is_leaf = 0;
            super_root.header.num_cells = 2;

            let left_min_key = self.get_node(new_root_id)?.keys[0];

            super_root.keys[0] = left_min_key;
            super_root.values[0] = Self::make_child_ptr(new_root_id);
            super_root.keys[1] = split_key;
            super_root.values[1] = Self::make_child_ptr(right_page_id);

            self.dirty_nodes.insert(super_root_id, super_root);
            self.root_page_id = super_root_id;
        } else {
            self.root_page_id = new_root_id;
        }

        Ok(())
    }

    /// Copy-On-Write Insert Logic. Returns the `new_page_id` and optionally split routing data.
    fn insert_into_node(
        &mut self,
        page_id: u32,
        target_key: IndexKey,
        target_val: IndexValue,
    ) -> Result<(u32, Option<(IndexKey, u32)>), MesoError> {
        let mut new_node = *self.get_node(page_id)?;

        // Optimization: If the page was allocated during THIS batch, mutate it in-place
        // inside the dirty cache to prevent runaway write-amplification.
        let new_page_id = if page_id >= self.read_pager.num_pages {
            page_id
        } else {
            self.allocate_page()
        };

        let is_leaf = new_node.header.is_leaf == 1;
        let num_cells = new_node.header.num_cells as usize;

        if is_leaf {
            let mut insert_idx = 0;
            while insert_idx < num_cells && new_node.keys[insert_idx] < target_key {
                insert_idx += 1;
            }

            if insert_idx < num_cells && new_node.keys[insert_idx] == target_key {
                new_node.values[insert_idx] = target_val;
                self.dirty_nodes.insert(new_page_id, new_node);
                return Ok((new_page_id, None));
            }

            if num_cells < NUM_CELLS {
                for i in (insert_idx..num_cells).rev() {
                    new_node.keys[i + 1] = new_node.keys[i];
                    new_node.values[i + 1] = new_node.values[i];
                }
                new_node.keys[insert_idx] = target_key;
                new_node.values[insert_idx] = target_val;
                new_node.header.num_cells += 1;
                self.dirty_nodes.insert(new_page_id, new_node);
                Ok((new_page_id, None))
            } else {
                // Split Leaf Node
                let mut temp_keys = [unsafe { std::mem::zeroed::<IndexKey>() }; NUM_CELLS + 1];
                let mut temp_vals = [unsafe { std::mem::zeroed::<IndexValue>() }; NUM_CELLS + 1];

                temp_keys[..insert_idx].copy_from_slice(&new_node.keys[..insert_idx]);
                temp_vals[..insert_idx].copy_from_slice(&new_node.values[..insert_idx]);
                temp_keys[insert_idx] = target_key;
                temp_vals[insert_idx] = target_val;
                if insert_idx < NUM_CELLS {
                    temp_keys[insert_idx + 1..]
                        .copy_from_slice(&new_node.keys[insert_idx..NUM_CELLS]);
                    temp_vals[insert_idx + 1..]
                        .copy_from_slice(&new_node.values[insert_idx..NUM_CELLS]);
                }

                let right_page_id = self.allocate_page();
                let split_idx = NUM_CELLS / 2;
                let left_count = split_idx;
                let right_count = (NUM_CELLS + 1) - split_idx;

                let right_sibling_of_left = new_node.header.right_sibling;

                let mut right_node = unsafe { std::mem::zeroed::<NodePage>() };
                right_node.header.is_leaf = 1;
                right_node.header.num_cells = right_count as u16;
                right_node.header.right_sibling = right_sibling_of_left;
                right_node.keys[..right_count].copy_from_slice(&temp_keys[left_count..]);
                right_node.values[..right_count].copy_from_slice(&temp_vals[left_count..]);

                new_node.header.num_cells = left_count as u16;
                new_node.header.right_sibling = right_page_id;
                new_node.keys[..left_count].copy_from_slice(&temp_keys[..left_count]);
                new_node.values[..left_count].copy_from_slice(&temp_vals[..left_count]);

                self.dirty_nodes.insert(new_page_id, new_node);
                self.dirty_nodes.insert(right_page_id, right_node);

                Ok((new_page_id, Some((temp_keys[left_count], right_page_id))))
            }
        } else {
            // Internal Node Routing
            let mut child_idx = 0;
            while child_idx < num_cells && target_key >= new_node.keys[child_idx] {
                child_idx += 1;
            }
            if child_idx > 0 {
                child_idx = child_idx.saturating_sub(1);
            }
            let child_page_id = Self::child_page_id(&new_node.values[child_idx]);

            // Recursively COW the child branch
            let (new_child_id, split_opt) =
                self.insert_into_node(child_page_id, target_key, target_val)?;

            new_node.values[child_idx] = Self::make_child_ptr(new_child_id);

            if target_key < new_node.keys[child_idx] {
                new_node.keys[child_idx] = target_key;
            }

            if let Some((split_key, right_child_id)) = split_opt {
                let insert_idx = child_idx + 1;
                let target_ptr = Self::make_child_ptr(right_child_id);

                if num_cells < NUM_CELLS {
                    for i in (insert_idx..num_cells).rev() {
                        new_node.keys[i + 1] = new_node.keys[i];
                        new_node.values[i + 1] = new_node.values[i];
                    }
                    new_node.keys[insert_idx] = split_key;
                    new_node.values[insert_idx] = target_ptr;
                    new_node.header.num_cells += 1;

                    self.dirty_nodes.insert(new_page_id, new_node);
                    Ok((new_page_id, None))
                } else {
                    // Split Internal Node
                    let mut temp_keys = [unsafe { std::mem::zeroed::<IndexKey>() }; NUM_CELLS + 1];
                    let mut temp_vals =
                        [unsafe { std::mem::zeroed::<IndexValue>() }; NUM_CELLS + 1];

                    temp_keys[..insert_idx].copy_from_slice(&new_node.keys[..insert_idx]);
                    temp_vals[..insert_idx].copy_from_slice(&new_node.values[..insert_idx]);
                    temp_keys[insert_idx] = split_key;
                    temp_vals[insert_idx] = target_ptr;
                    if insert_idx < NUM_CELLS {
                        temp_keys[insert_idx + 1..]
                            .copy_from_slice(&new_node.keys[insert_idx..NUM_CELLS]);
                        temp_vals[insert_idx + 1..]
                            .copy_from_slice(&new_node.values[insert_idx..NUM_CELLS]);
                    }

                    let right_internal_id = self.allocate_page();
                    let split_idx = NUM_CELLS / 2;
                    let left_count = split_idx;
                    let right_count = (NUM_CELLS + 1) - split_idx;

                    let mut right_node = unsafe { std::mem::zeroed::<NodePage>() };
                    right_node.header.is_leaf = 0;
                    right_node.header.num_cells = right_count as u16;
                    right_node.keys[..right_count].copy_from_slice(&temp_keys[left_count..]);
                    right_node.values[..right_count].copy_from_slice(&temp_vals[left_count..]);

                    new_node.header.num_cells = left_count as u16;
                    new_node.keys[..left_count].copy_from_slice(&temp_keys[..left_count]);
                    new_node.values[..left_count].copy_from_slice(&temp_vals[..left_count]);

                    self.dirty_nodes.insert(new_page_id, new_node);
                    self.dirty_nodes.insert(right_internal_id, right_node);

                    Ok((
                        new_page_id,
                        Some((temp_keys[left_count], right_internal_id)),
                    ))
                }
            } else {
                self.dirty_nodes.insert(new_page_id, new_node);
                Ok((new_page_id, None))
            }
        }
    }

    /// Atomically flushes all dirty COW pages to disk and hot-swaps the lock-free ReadPager.
    pub fn flush(&mut self) -> Result<(), MesoError> {
        for (page_id, node) in &self.dirty_nodes {
            self.write_pager
                .write_page_bytes(*page_id, bytemuck::bytes_of(node))?;
        }
        for (page_id, overflow) in &self.dirty_overflows {
            self.write_pager
                .write_page_bytes(*page_id, bytemuck::bytes_of(overflow))?;
        }

        // Write the Metapage (Page 0) to finalize the transaction
        let mut metapage = [0u8; 4096];
        metapage[0..4].copy_from_slice(&self.root_page_id.to_ne_bytes());
        metapage[4..8].copy_from_slice(&self.write_pager.next_page_id.to_ne_bytes());
        self.write_pager.write_page_bytes(0, &metapage)?;

        // Fsync storage
        self.write_pager.flush()?;

        // RCU Swap: Re-mmap the newly sized file for zero-copy readers
        self.read_pager = ReadPager::open(self.write_pager.file())?;

        // Drop the ephemeral dirty working set
        self.dirty_nodes.clear();
        self.dirty_overflows.clear();

        Ok(())
    }

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
}

// ==============================================================================
// NOW INDEX (E-A-V Lookups)
// ==============================================================================

pub struct NowIndex {
    pub core: CoreBTree,
}

impl NowIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        Ok(Self {
            core: CoreBTree::open(path)?,
        })
    }

    #[inline(always)]
    pub fn get(&self, e: EntityId, a: AttributeId) -> Result<Option<Value>, MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        if let Some(iv) = self.core.get_kv(key)? {
            Ok(Some(self.decode_value(&iv)?))
        } else {
            Ok(None)
        }
    }

    pub fn delete(&mut self, e: EntityId, a: AttributeId) -> Result<(), MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        let tombstone = IndexValue {
            type_tag: 254,
            padding: [0; 7],
            payload: [0; 8],
        };
        self.core.put_kv(key, tombstone)
    }

    pub fn put(&mut self, e: EntityId, a: AttributeId, v: &Value) -> Result<(), MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        let val = self.encode_value(v)?;
        self.core.put_kv(key, val)
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        self.core.flush()
    }

    // --- Type Translation Helpers ---

    fn encode_value(&mut self, v: &Value) -> Result<IndexValue, MesoError> {
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
            Value::String(s) => {
                let bytes = s.as_bytes();
                let mut first_page_id = 0;
                let mut prev_page_id = 0;

                for (i, chunk) in bytes.chunks(4088).enumerate() {
                    let page_id = self.core.allocate_page();
                    let mut overflow = unsafe { std::mem::zeroed::<OverflowPage>() };
                    overflow.length = chunk.len() as u32;
                    overflow.next_page_id = 0;
                    overflow.data[..chunk.len()].copy_from_slice(chunk);

                    if i == 0 {
                        first_page_id = page_id;
                    } else {
                        let mut prev = *self.core.get_overflow(prev_page_id)?;
                        prev.next_page_id = page_id;
                        self.core.dirty_overflows.insert(prev_page_id, prev);
                    }
                    self.core.dirty_overflows.insert(page_id, overflow);
                    prev_page_id = page_id;
                }

                if first_page_id == 0 {
                    let page_id = self.core.allocate_page();
                    let overflow = unsafe { std::mem::zeroed::<OverflowPage>() };
                    self.core.dirty_overflows.insert(page_id, overflow);
                    first_page_id = page_id;
                }

                payload[0..4].copy_from_slice(&first_page_id.to_ne_bytes());
                3
            }
            Value::Ref(r) => {
                payload.copy_from_slice(&r.to_ne_bytes());
                4
            }
            Value::Timestamp(t) => {
                payload.copy_from_slice(&t.to_ne_bytes());
                5
            }
            Value::Uuid(u) => {
                let page_id = self.core.allocate_page();
                let mut overflow = unsafe { std::mem::zeroed::<OverflowPage>() };
                overflow.length = 16;
                overflow.next_page_id = 0;
                overflow.data[..16].copy_from_slice(u);

                self.core.dirty_overflows.insert(page_id, overflow);
                payload[0..4].copy_from_slice(&page_id.to_ne_bytes());
                6
            }
        };

        Ok(IndexValue {
            type_tag,
            padding: [0; 7],
            payload,
        })
    }

    fn decode_value(&self, iv: &IndexValue) -> Result<Value, MesoError> {
        match iv.type_tag {
            0 => Ok(Value::Boolean(iv.payload[0] != 0)),
            1 => Ok(Value::Int64(i64::from_ne_bytes(iv.payload))),
            2 => Ok(Value::Float64(f64::from_ne_bytes(iv.payload))),
            3 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let mut current_page_id = u32::from_ne_bytes(bytes);
                let mut string_bytes = Vec::new();

                loop {
                    let overflow = self.core.get_overflow(current_page_id)?;
                    let len = overflow.length as usize;
                    string_bytes.extend_from_slice(&overflow.data[..len]);

                    if overflow.next_page_id == 0 {
                        break;
                    }
                    current_page_id = overflow.next_page_id;
                }

                let s = String::from_utf8(string_bytes).map_err(|_| {
                    MesoError::Serialization("Invalid UTF-8 in overflow chain".into())
                })?;
                Ok(Value::String(s))
            }
            4 => Ok(Value::Ref(u64::from_ne_bytes(iv.payload))),
            5 => Ok(Value::Timestamp(i64::from_ne_bytes(iv.payload))),
            6 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let page_id = u32::from_ne_bytes(bytes);
                let overflow = self.core.get_overflow(page_id)?;

                let mut u = [0u8; 16];
                u.copy_from_slice(&overflow.data[..16]);
                Ok(Value::Uuid(u))
            }
            _ => Ok(Value::Boolean(false)),
        }
    }
}

// ==============================================================================
// AVE INDEX (A-V-E Inverted Lookups)
// ==============================================================================

pub struct AveIndex {
    pub core: CoreBTree,
}

impl AveIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        Ok(Self {
            core: CoreBTree::open(path)?,
        })
    }

    fn hash_value(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        match value {
            Value::Int64(i) => i.hash(&mut hasher),
            Value::Float64(f) => hasher.write(&f.to_ne_bytes()),
            Value::Boolean(b) => b.hash(&mut hasher),
            Value::Timestamp(t) => t.hash(&mut hasher),
            Value::Ref(r) => r.hash(&mut hasher),
            Value::String(s) => s.hash(&mut hasher),
            Value::Uuid(u) => u.hash(&mut hasher),
        }
        hasher.finish()
    }

    fn make_key(a: AttributeId, val_hash: u64) -> IndexKey {
        IndexKey {
            e: val_hash,
            a,
            _pad: 0,
        }
    }

    pub fn put(&mut self, a: AttributeId, v: &Value, e: EntityId) -> Result<(), MesoError> {
        let key = Self::make_key(a, Self::hash_value(v));
        let mut payload = [0u8; 8];
        payload.copy_from_slice(&e.to_ne_bytes());

        let val = IndexValue {
            type_tag: 1, // EntityId payload tag
            padding: [0; 7],
            payload,
        };

        self.core.put_kv(key, val)
    }

    #[inline(always)]
    pub fn get(&self, a: AttributeId, v: &Value) -> Result<Option<EntityId>, MesoError> {
        let key = Self::make_key(a, Self::hash_value(v));

        if let Some(iv) = self.core.get_kv(key)? {
            if iv.type_tag == 254 {
                return Ok(None);
            }
            let mut e_bytes = [0u8; 8];
            e_bytes.copy_from_slice(&iv.payload);
            Ok(Some(EntityId::from_ne_bytes(e_bytes)))
        } else {
            Ok(None)
        }
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        self.core.flush()
    }
}

#[derive(Clone)]
pub struct BTreeSnapshot {
    pub pager: ReadPager,
    pub root_page_id: u32,
}

impl BTreeSnapshot {
    pub fn get_kv(&self, target_key: IndexKey) -> Result<Option<IndexValue>, MesoError> {
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
                    let iv = node.values[left];
                    if iv.type_tag == 254 {
                        return Ok(None);
                    }
                    return Ok(Some(iv));
                }
                return Ok(None);
            } else {
                let mut child_idx = 0;
                while child_idx < num_cells && target_key >= node.keys[child_idx] {
                    child_idx += 1;
                }
                if child_idx > 0 {
                    child_idx = child_idx.saturating_sub(1);
                }
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&node.values[child_idx].payload[0..4]);
                current_page_id = u32::from_ne_bytes(bytes);
            }
        }
    }
}

impl CoreBTree {
    pub fn snapshot(&self) -> BTreeSnapshot {
        BTreeSnapshot {
            pager: self.read_pager.clone(),
            root_page_id: self.root_page_id,
        }
    }
}

#[derive(Clone)]
pub struct NowIndexSnapshot {
    pub core: BTreeSnapshot,
}

impl NowIndex {
    pub fn snapshot(&self) -> NowIndexSnapshot {
        NowIndexSnapshot {
            core: self.core.snapshot(),
        }
    }
}

impl NowIndexSnapshot {
    pub fn get(&self, e: EntityId, a: AttributeId) -> Result<Option<Value>, MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        if let Some(iv) = self.core.get_kv(key)? {
            Ok(Some(self.decode_value(&iv)?))
        } else {
            Ok(None)
        }
    }

    fn decode_value(&self, iv: &IndexValue) -> Result<Value, MesoError> {
        match iv.type_tag {
            0 => Ok(Value::Boolean(iv.payload[0] != 0)),
            1 => Ok(Value::Int64(i64::from_ne_bytes(iv.payload))),
            2 => Ok(Value::Float64(f64::from_ne_bytes(iv.payload))),
            3 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let mut current_page_id = u32::from_ne_bytes(bytes);
                let mut string_bytes = Vec::new();
                loop {
                    let overflow = self.core.pager.get_overflow(current_page_id)?;
                    string_bytes.extend_from_slice(&overflow.data[..overflow.length as usize]);
                    if overflow.next_page_id == 0 {
                        break;
                    }
                    current_page_id = overflow.next_page_id;
                }
                Ok(Value::String(
                    String::from_utf8(string_bytes).unwrap_or_default(),
                ))
            }
            4 => Ok(Value::Ref(u64::from_ne_bytes(iv.payload))),
            5 => Ok(Value::Timestamp(i64::from_ne_bytes(iv.payload))),
            6 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let overflow = self.core.pager.get_overflow(u32::from_ne_bytes(bytes))?;
                let mut u = [0u8; 16];
                u.copy_from_slice(&overflow.data[..16]);
                Ok(Value::Uuid(u))
            }
            _ => Ok(Value::Boolean(false)),
        }
    }
}

#[derive(Clone)]
pub struct AveIndexSnapshot {
    pub core: BTreeSnapshot,
}

impl AveIndex {
    pub fn snapshot(&self) -> AveIndexSnapshot {
        AveIndexSnapshot {
            core: self.core.snapshot(),
        }
    }
}

impl AveIndexSnapshot {
    pub fn get(&self, a: AttributeId, v: &Value) -> Result<Option<EntityId>, MesoError> {
        let key = IndexKey {
            e: AveIndex::hash_value(v),
            a,
            _pad: 0,
        };
        if let Some(iv) = self.core.get_kv(key)? {
            if iv.type_tag == 254 {
                return Ok(None);
            }
            let mut e_bytes = [0u8; 8];
            e_bytes.copy_from_slice(&iv.payload);
            Ok(Some(EntityId::from_ne_bytes(e_bytes)))
        } else {
            Ok(None)
        }
    }
}

// ==============================================================================
// STRICT MILITARY-GRADE TESTS (DO NOT DELETE)
// ==============================================================================

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
        index.put(e, 1, &Value::Boolean(true)).unwrap();
        index.put(e, 2, &Value::Int64(-42)).unwrap();
        index.put(e, 3, &Value::Float64(PI)).unwrap();
        index.put(e, 4, &Value::Ref(9999)).unwrap();
        index.put(e, 5, &Value::Timestamp(1700000000)).unwrap();

        assert_eq!(index.get(e, 1).unwrap(), Some(Value::Boolean(true)));
        assert_eq!(index.get(e, 2).unwrap(), Some(Value::Int64(-42)));
        assert_eq!(index.get(e, 3).unwrap(), Some(Value::Float64(PI)));
        assert_eq!(index.get(e, 4).unwrap(), Some(Value::Ref(9999)));
        assert_eq!(index.get(e, 5).unwrap(), Some(Value::Timestamp(1700000000)));
    }

    #[test]
    fn test_now_index_reverse_insertion_memory_shifting() {
        let (mut index, _f) = setup_index();

        for i in (1..=50).rev() {
            index.put(i, 10, &Value::Int64(i as i64)).unwrap();
        }

        for i in 1..=50 {
            let val = index.get(i, 10).unwrap().unwrap();
            assert_eq!(val, Value::Int64(i as i64));
        }

        let root = index.core.get_node(index.core.root_page_id).unwrap();
        assert_eq!(root.header.num_cells, 50);
        assert_eq!(root.keys[0].e, 1);
        assert_eq!(root.keys[49].e, 50);
    }

    #[test]
    fn test_now_index_update_overwrite() {
        let (mut index, _f) = setup_index();

        index.put(42, 99, &Value::Int64(100)).unwrap();
        let root_before = index.core.get_node(index.core.root_page_id).unwrap();
        assert_eq!(root_before.header.num_cells, 1);

        index.put(42, 99, &Value::Int64(200)).unwrap();
        let root_after = index.core.get_node(index.core.root_page_id).unwrap();

        assert_eq!(root_after.header.num_cells, 1);

        let val = index.get(42, 99).unwrap().unwrap();
        assert_eq!(val, Value::Int64(200));
    }

    #[test]
    fn test_now_index_capacity_exhaustion_boundary() {
        let (mut index, _f) = setup_index();

        for i in 0..NUM_CELLS {
            index.put(i as u64, 10, &Value::Int64(i as i64)).unwrap();
        }

        let root = index.core.get_node(index.core.root_page_id).unwrap();
        assert_eq!(root.header.num_cells as usize, NUM_CELLS);

        index
            .put(999, 10, &Value::Int64(999))
            .expect("Insert must succeed and trigger a split");

        assert!(index.core.write_pager.next_page_id >= 3);

        let val = index.get(999, 10).unwrap().unwrap();
        assert_eq!(val, Value::Int64(999));
    }

    #[test]
    fn test_b_tree_node_splitting_and_scaling() {
        let (mut index, _f) = setup_index();
        let target_records = 10_000;

        for i in (0..target_records).rev() {
            index.put(i, 10, &Value::Int64(i as i64)).unwrap();
        }

        for i in 0..target_records {
            let val = index.get(i, 10).unwrap().unwrap();
            assert_eq!(val, Value::Int64(i as i64));
        }

        let root = index.core.get_node(index.core.root_page_id).unwrap();

        assert_eq!(root.header.is_leaf, 0);
        assert!(index.core.write_pager.next_page_id > 50);
    }

    #[test]
    fn test_now_index_string_overflow_basic() {
        let (mut index, _f) = setup_index();
        let s = "Hello, Overflow World!".to_string();

        index.put(1, 10, &Value::String(s.clone())).unwrap();
        let val = index.get(1, 10).unwrap().unwrap();

        assert_eq!(val, Value::String(s));
        assert!(index.core.write_pager.next_page_id >= 2);
    }

    #[test]
    fn test_now_index_uuid_overflow_basic() {
        let (mut index, _f) = setup_index();
        let u = [0xAB; 16];

        index.put(2, 20, &Value::Uuid(u)).unwrap();
        let val = index.get(2, 20).unwrap().unwrap();

        assert_eq!(val, Value::Uuid(u));
    }

    #[test]
    fn test_now_index_string_exact_limit() {
        let (mut index, _f) = setup_index();
        let s = "A".repeat(4088);

        index.put(3, 30, &Value::String(s.clone())).unwrap();
        let val = index.get(3, 30).unwrap().unwrap();

        assert_eq!(val, Value::String(s));
    }

    #[test]
    fn test_now_index_string_chaining() {
        let (mut index, _f) = setup_index();
        let s = "A".repeat(10_000);

        index.put(4, 40, &Value::String(s.clone())).unwrap();
        let val = index.get(4, 40).unwrap().unwrap();

        assert_eq!(val, Value::String(s));
        assert!(index.core.write_pager.next_page_id >= 4);
    }

    #[test]
    fn test_now_index_multiple_overflows_no_collision() {
        let (mut index, _f) = setup_index();
        let s1 = "First String".to_string();
        let u1 = [0x11; 16];
        let s2 = "Second String".to_string();

        index.put(5, 50, &Value::String(s1.clone())).unwrap();
        index.put(5, 51, &Value::Uuid(u1)).unwrap();
        index.put(5, 52, &Value::String(s2.clone())).unwrap();

        assert_eq!(index.get(5, 50).unwrap().unwrap(), Value::String(s1));
        assert_eq!(index.get(5, 51).unwrap().unwrap(), Value::Uuid(u1));
        assert_eq!(index.get(5, 52).unwrap().unwrap(), Value::String(s2));

        assert!(index.core.write_pager.next_page_id >= 4);
    }

    #[test]
    fn test_ave_index_basic_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ave_test.idx");
        let mut index = AveIndex::open(&path).unwrap();

        let attr_email = 100;
        let email_val = Value::String("alice@example.com".to_string());
        let target_entity = 849202;

        index.put(attr_email, &email_val, target_entity).unwrap();
        let resolved = index.get(attr_email, &email_val).unwrap();

        assert_eq!(resolved, Some(target_entity));
    }

    #[test]
    fn test_ave_index_different_types_hash_safely() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ave_types_test.idx");
        let mut index = AveIndex::open(&path).unwrap();

        let attr_age = 200;
        let age_val = Value::Int64(30);

        let attr_active = 201;
        let active_val = Value::Boolean(true);

        index.put(attr_age, &age_val, 111).unwrap();
        index.put(attr_active, &active_val, 222).unwrap();

        assert_eq!(index.get(attr_age, &age_val).unwrap(), Some(111));
        assert_eq!(index.get(attr_active, &active_val).unwrap(), Some(222));
        assert_eq!(index.get(attr_age, &Value::Int64(99)).unwrap(), None);
    }
}
