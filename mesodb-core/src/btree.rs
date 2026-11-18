// mesodb-core/src/btree.rs

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::{
    error::MesoError,
    page::{
        FREELIST_PAGE_CAP, FreeListPage, IndexKey, IndexValue, METAPAGE_INLINE_FREE_CAP, NUM_CELLS,
        NodePage, OverflowPage,
    },
    pager::{ReadPager, WritePager},
    types::{AttributeId, EntityId, Value},
};

// ==============================================================================
// CORE COW B+TREE ENGINE (Lock-Free Reads, Dirty-Cache Writes)
// ==============================================================================

type RetiredGeneration = (Arc<memmap2::Mmap>, Vec<u32>);

fn load_metapage_and_freelist(write_pager: &mut WritePager) -> Result<(u32, Vec<u32>), MesoError> {
    let mut metapage = [0u8; 4096];
    write_pager
        .file
        .seek(SeekFrom::Start(0))
        .map_err(MesoError::Io)?;
    write_pager
        .file
        .read_exact(&mut metapage)
        .map_err(MesoError::Io)?;

    let root_page_id = u32::from_le_bytes(metapage[0..4].try_into().unwrap());
    write_pager.next_page_id = u32::from_le_bytes(metapage[4..8].try_into().unwrap());

    let inline_count = (u32::from_le_bytes(metapage[8..12].try_into().unwrap()) as usize)
        .min(METAPAGE_INLINE_FREE_CAP);
    let mut chain_head = u32::from_le_bytes(metapage[12..16].try_into().unwrap());

    let mut free_pages = Vec::with_capacity(inline_count);
    for i in 0..inline_count {
        let off = 16 + i * 4;
        let pid = u32::from_le_bytes(metapage[off..off + 4].try_into().unwrap());
        if pid >= 2 {
            free_pages.push(pid);
        }
    }

    while chain_head != 0 {
        let offset = u64::from(chain_head) * 4096;
        let mut page_buf = [0u8; 4096];
        write_pager
            .file
            .seek(SeekFrom::Start(offset))
            .map_err(MesoError::Io)?;
        write_pager
            .file
            .read_exact(&mut page_buf)
            .map_err(MesoError::Io)?;

        let fl_page: &FreeListPage = bytemuck::from_bytes(&page_buf);
        let count = (fl_page.count as usize).min(FREELIST_PAGE_CAP);
        for &pid in &fl_page.pages[..count] {
            if pid >= 2 {
                free_pages.push(pid);
            }
        }
        let next = fl_page.next_page_id;
        // Reclaim the freelist chain page itself
        if chain_head >= 2 {
            free_pages.push(chain_head);
        }
        chain_head = next;
    }

    Ok((root_page_id, free_pages))
}

fn persist_metapage_and_freelist(
    write_pager: &mut WritePager,
    root_page_id: u32,
    live_free_pages: &mut Vec<u32>,
    retired_generations: &[RetiredGeneration],
) -> Result<(), MesoError> {
    let retired_count: usize = retired_generations.iter().map(|(_, p)| p.len()).sum();
    let mut chain_page_ids = Vec::new();

    // Pre-allocate freelist chain pages before building `all_free` so chain pages are never double-listed
    while live_free_pages.len() + retired_count
        > METAPAGE_INLINE_FREE_CAP + chain_page_ids.len() * FREELIST_PAGE_CAP
    {
        let page_id = if let Some(reused) = live_free_pages.pop() {
            reused
        } else {
            let id = write_pager.next_page_id;
            write_pager.next_page_id += 1;
            id
        };
        chain_page_ids.push(page_id);
    }

    let mut all_free = live_free_pages.clone();
    for (_, retired) in retired_generations {
        all_free.extend_from_slice(retired);
    }

    let mut chain_head = 0u32;
    if !chain_page_ids.is_empty() {
        let overflow_pages = all_free.split_off(METAPAGE_INLINE_FREE_CAP);
        // Write chain in reverse so each page knows the next_page_id
        for (chunk, &page_id) in overflow_pages
            .chunks(FREELIST_PAGE_CAP)
            .rev()
            .zip(chain_page_ids.iter().rev())
        {
            let mut fl_page = unsafe { std::mem::zeroed::<FreeListPage>() };
            fl_page.next_page_id = chain_head;
            fl_page.count = chunk.len() as u32;
            fl_page.pages[..chunk.len()].copy_from_slice(chunk);

            write_pager.write_page_bytes(page_id, bytemuck::bytes_of(&fl_page))?;
            chain_head = page_id;
        }
    }

    let final_inline = all_free.len().min(METAPAGE_INLINE_FREE_CAP);
    let mut metapage = [0u8; 4096];
    metapage[0..4].copy_from_slice(&root_page_id.to_le_bytes());
    metapage[4..8].copy_from_slice(&write_pager.next_page_id.to_le_bytes());
    metapage[8..12].copy_from_slice(&(final_inline as u32).to_le_bytes());
    metapage[12..16].copy_from_slice(&chain_head.to_le_bytes());

    for (i, &pid) in all_free[..final_inline].iter().enumerate() {
        let off = 16 + i * 4;
        metapage[off..off + 4].copy_from_slice(&pid.to_le_bytes());
    }

    write_pager.write_page_bytes(0, &metapage)
}

pub struct CoreBTree {
    pub read_pager: ReadPager,
    pub write_pager: WritePager,
    pub root_page_id: u32,
    pub dirty_nodes: HashMap<u32, NodePage>,
    pub dirty_overflows: HashMap<u32, OverflowPage>,
    pub free_pages: Vec<u32>,
    pub pending_free: Vec<u32>,
    retired_generations: Vec<RetiredGeneration>,
}

impl CoreBTree {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let mut write_pager = WritePager::open(path)?;
        let file_len = write_pager.file().metadata().map_err(MesoError::Io)?.len();

        let root_page_id;
        let free_pages;

        if file_len == 0 {
            // Bootstrap New Database
            let mut empty_root = unsafe { std::mem::zeroed::<NodePage>() };
            empty_root.header.is_leaf = 1;

            // Page 1 is the genesis root
            write_pager.write_page_bytes(1, bytemuck::bytes_of(&empty_root))?;

            // Page 0 is the Metapage [root_page_id, next_page_id, inline_free_count, chain_head, ...]
            let mut metapage = [0u8; 4096];
            metapage[0..4].copy_from_slice(&1u32.to_le_bytes());
            metapage[4..8].copy_from_slice(&2u32.to_le_bytes());
            write_pager.write_page_bytes(0, &metapage)?;
            write_pager.flush()?;

            root_page_id = 1;
            write_pager.next_page_id = 2;
            free_pages = Vec::new();
        } else {
            let (recovered_root, recovered_free) = load_metapage_and_freelist(&mut write_pager)?;
            root_page_id = recovered_root;
            free_pages = recovered_free;
        }

        let read_pager = ReadPager::open(write_pager.file())?;

        Ok(Self {
            read_pager,
            write_pager,
            root_page_id,
            dirty_nodes: HashMap::new(),
            dirty_overflows: HashMap::new(),
            free_pages,
            pending_free: Vec::new(),
            retired_generations: Vec::new(),
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

    fn reclaim_retired(&mut self) {
        self.retired_generations.retain_mut(|(old_mmap, pages)| {
            if Arc::strong_count(old_mmap) == 1 {
                self.free_pages.append(pages);
                false
            } else {
                true
            }
        });
    }

    pub fn allocate_page(&mut self) -> u32 {
        self.reclaim_retired();
        if let Some(id) = self.free_pages.pop() {
            return id;
        }
        let id = self.write_pager.next_page_id;
        self.write_pager.next_page_id += 1;
        id
    }

    pub fn free_overflow_page(&mut self, page_id: u32) {
        if page_id < 2 {
            return;
        }
        if self.dirty_overflows.remove(&page_id).is_some() {
            self.free_pages.push(page_id);
        } else {
            self.pending_free.push(page_id);
        }
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

        // Optimization: If the page was already allocated/COW'd during THIS batch,
        // mutate it in-place inside the dirty cache to prevent runaway write-amplification.
        let new_page_id = if self.dirty_nodes.contains_key(&page_id) {
            page_id
        } else {
            let allocated = self.allocate_page();
            if page_id >= 2 {
                self.pending_free.push(page_id);
            }
            allocated
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

    pub fn flush(&mut self) -> Result<(), MesoError> {
        for (page_id, node) in &self.dirty_nodes {
            self.write_pager
                .write_page_bytes(*page_id, bytemuck::bytes_of(node))?;
        }
        for (page_id, overflow) in &self.dirty_overflows {
            self.write_pager
                .write_page_bytes(*page_id, bytemuck::bytes_of(overflow))?;
        }

        let old_mmap = self.read_pager.mmap.clone();
        let newly_retired = std::mem::take(&mut self.pending_free);
        if !newly_retired.is_empty() {
            self.retired_generations.push((old_mmap, newly_retired));
        }

        // Open new ReadPager BEFORE reclaiming so old_mmap drops its active reader reference
        self.write_pager.flush()?;
        self.read_pager = ReadPager::open(self.write_pager.file())?;
        self.reclaim_retired();

        // Persist Metapage (Page 0) with updated root, next_page_id, and free list
        persist_metapage_and_freelist(
            &mut self.write_pager,
            self.root_page_id,
            &mut self.free_pages,
            &self.retired_generations,
        )?;
        self.write_pager.flush()?;
        self.read_pager = ReadPager::open(self.write_pager.file())?;

        // Drop the ephemeral dirty working set
        self.dirty_nodes.clear();
        self.dirty_overflows.clear();

        Ok(())
    }

    fn child_page_id(val: &IndexValue) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&val.payload[0..4]);
        u32::from_le_bytes(bytes)
    }

    fn make_child_ptr(page_id: u32) -> IndexValue {
        let mut payload = [0u8; 8];
        payload[0..4].copy_from_slice(&page_id.to_le_bytes());
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
        self.reclaim_existing_overflow(key)?;
        let tombstone = IndexValue {
            type_tag: 254,
            padding: [0; 7],
            payload: [0; 8],
        };
        self.core.put_kv(key, tombstone)
    }

    pub fn put(&mut self, e: EntityId, a: AttributeId, v: &Value) -> Result<(), MesoError> {
        let key = IndexKey { e, a, _pad: 0 };
        self.reclaim_existing_overflow(key)?;
        let val = self.encode_value(v)?;
        self.core.put_kv(key, val)
    }

    fn reclaim_existing_overflow(&mut self, key: IndexKey) -> Result<(), MesoError> {
        if let Some(existing) = self.core.get_kv(key)? {
            match existing.type_tag {
                3 => {
                    let mut bytes = [0u8; 4];
                    bytes.copy_from_slice(&existing.payload[0..4]);
                    let mut current_page_id = u32::from_le_bytes(bytes);
                    while current_page_id != 0 {
                        let next_page_id = self.core.get_overflow(current_page_id)?.next_page_id;
                        self.core.free_overflow_page(current_page_id);
                        current_page_id = next_page_id;
                    }
                }
                6 => {
                    let mut bytes = [0u8; 4];
                    bytes.copy_from_slice(&existing.payload[0..4]);
                    let page_id = u32::from_le_bytes(bytes);
                    if page_id != 0 {
                        self.core.free_overflow_page(page_id);
                    }
                }
                _ => {}
            }
        }
        Ok(())
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
                payload.copy_from_slice(&i.to_le_bytes());
                1
            }
            Value::Float64(f) => {
                payload.copy_from_slice(&f.to_le_bytes());
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

                payload[0..4].copy_from_slice(&first_page_id.to_le_bytes());
                3
            }
            Value::Ref(r) => {
                payload.copy_from_slice(&r.to_le_bytes());
                4
            }
            Value::Timestamp(t) => {
                payload.copy_from_slice(&t.to_le_bytes());
                5
            }
            Value::Uuid(u) => {
                let page_id = self.core.allocate_page();
                let mut overflow = unsafe { std::mem::zeroed::<OverflowPage>() };
                overflow.length = 16;
                overflow.next_page_id = 0;
                overflow.data[..16].copy_from_slice(u);

                self.core.dirty_overflows.insert(page_id, overflow);
                payload[0..4].copy_from_slice(&page_id.to_le_bytes());
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
            1 => Ok(Value::Int64(i64::from_le_bytes(iv.payload))),
            2 => Ok(Value::Float64(f64::from_le_bytes(iv.payload))),
            3 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let mut current_page_id = u32::from_le_bytes(bytes);
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
            4 => Ok(Value::Ref(u64::from_le_bytes(iv.payload))),
            5 => Ok(Value::Timestamp(i64::from_le_bytes(iv.payload))),
            6 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let page_id = u32::from_le_bytes(bytes);
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

// ==============================================================================
// AVE INDEX (A-V-E Inverted Lookups - 128-Bit Collision-Safe)
// ==============================================================================

pub struct AveIndex {
    pub core: AvtCoreBTree,
}

impl AveIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        Ok(Self {
            core: AvtCoreBTree::open(path)?,
        })
    }

    fn make_key(a: AttributeId, v: &Value) -> AvIndexKey {
        let hash = crate::bitmap::hash_value(v);
        AvIndexKey {
            a,
            _pad: 0,
            v_sort: (hash >> 64) as u64,
            e: hash as u64,
        }
    }

    pub fn put(&mut self, a: AttributeId, v: &Value, e: EntityId) -> Result<(), MesoError> {
        let key = Self::make_key(a, v);
        let mut payload = [0u8; 8];
        payload.copy_from_slice(&e.to_le_bytes());

        let val = IndexValue {
            type_tag: 1, // EntityId payload tag
            padding: [0; 7],
            payload,
        };

        self.core.put_kv(key, val)
    }

    pub fn delete(&mut self, a: AttributeId, v: &Value) -> Result<(), MesoError> {
        let key = Self::make_key(a, v);
        let tombstone = IndexValue {
            type_tag: 254,
            padding: [0; 7],
            payload: [0; 8],
        };
        self.core.put_kv(key, tombstone)
    }

    #[inline(always)]
    pub fn get(&self, a: AttributeId, v: &Value) -> Result<Option<EntityId>, MesoError> {
        let target_key = Self::make_key(a, v);
        let mut current_page_id = self.core.root_page_id;

        loop {
            let node = self.core.get_node(current_page_id)?;
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
                    let mut e_bytes = [0u8; 8];
                    e_bytes.copy_from_slice(&iv.payload);
                    return Ok(Some(EntityId::from_le_bytes(e_bytes)));
                }
                return Ok(None);
            } else {
                let mut child_idx = 0;
                while child_idx < num_cells && target_key >= node.keys[child_idx] {
                    child_idx += 1;
                }
                child_idx = child_idx.saturating_sub(1);
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&node.values[child_idx].payload[0..4]);
                current_page_id = u32::from_le_bytes(bytes);
            }
        }
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        self.core.flush()
    }
}

#[derive(Clone)]
pub struct AveIndexSnapshot {
    pub pager: ReadPager,
    pub root_page_id: u32,
}

impl AveIndex {
    pub fn snapshot(&self) -> AveIndexSnapshot {
        AveIndexSnapshot {
            pager: self.core.read_pager.clone(),
            root_page_id: self.core.root_page_id,
        }
    }
}

impl AveIndexSnapshot {
    pub fn get(&self, a: AttributeId, v: &Value) -> Result<Option<EntityId>, MesoError> {
        let target_key = AveIndex::make_key(a, v);
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.pager.get_av_node(current_page_id)?;
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
                    let mut e_bytes = [0u8; 8];
                    e_bytes.copy_from_slice(&iv.payload);
                    return Ok(Some(EntityId::from_le_bytes(e_bytes)));
                }
                return Ok(None);
            } else {
                let mut child_idx = 0;
                while child_idx < num_cells && target_key >= node.keys[child_idx] {
                    child_idx += 1;
                }
                child_idx = child_idx.saturating_sub(1);
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&node.values[child_idx].payload[0..4]);
                current_page_id = u32::from_le_bytes(bytes);
            }
        }
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
                current_page_id = u32::from_le_bytes(bytes);
            }
        }
    }

    /// O(log N + K) Sequential Leaf-Chain Scan
    pub fn range_scan(
        &self,
        start_key: IndexKey,
        end_key: IndexKey,
    ) -> Result<Vec<(IndexKey, IndexValue)>, MesoError> {
        let mut current_page_id = self.root_page_id;

        // 1. Traverse down to the first leaf node containing start_key (or the next highest)
        loop {
            let node = self.pager.get_node(current_page_id)?;
            let num_cells = node.header.num_cells as usize;
            if num_cells == 0 {
                return Ok(Vec::new());
            }

            if node.header.is_leaf == 1 {
                break;
            } else {
                let mut child_idx = 0;
                while child_idx < num_cells && start_key >= node.keys[child_idx] {
                    child_idx += 1;
                }
                if child_idx > 0 {
                    child_idx = child_idx.saturating_sub(1);
                }
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&node.values[child_idx].payload[0..4]);
                current_page_id = u32::from_le_bytes(bytes);
            }
        }

        // 2. Walk the right_sibling leaf chain horizontally
        let mut results = Vec::new();
        loop {
            let node = self.pager.get_node(current_page_id)?;
            let num_cells = node.header.num_cells as usize;

            for i in 0..num_cells {
                let key = node.keys[i];
                if key > end_key {
                    return Ok(results); // Safely exit once we breach the upper bound
                }
                if key >= start_key {
                    let iv = node.values[i];
                    if iv.type_tag != 254 {
                        // Ignore tombstones
                        results.push((key, iv));
                    }
                }
            }

            if node.header.right_sibling == 0 {
                break;
            }
            current_page_id = node.header.right_sibling;
        }

        Ok(results)
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

    pub fn range_scan(
        &self,
        start_e: EntityId,
        end_e: EntityId,
        a: AttributeId,
    ) -> Result<Vec<(EntityId, Value)>, MesoError> {
        let start_key = IndexKey {
            e: start_e,
            a,
            _pad: 0,
        };
        let end_key = IndexKey {
            e: end_e,
            a,
            _pad: 0,
        };

        let raw_kvs = self.core.range_scan(start_key, end_key)?;
        let mut results = Vec::with_capacity(raw_kvs.len());

        for (k, iv) in raw_kvs {
            // Filter strictly by the target attribute, as the raw scan will pick up
            // secondary attributes belonging to entities within the e-range.
            if k.a == a {
                results.push((k.e, self.decode_value(&iv)?));
            }
        }
        Ok(results)
    }

    fn decode_value(&self, iv: &IndexValue) -> Result<Value, MesoError> {
        match iv.type_tag {
            0 => Ok(Value::Boolean(iv.payload[0] != 0)),
            1 => Ok(Value::Int64(i64::from_le_bytes(iv.payload))),
            2 => Ok(Value::Float64(f64::from_le_bytes(iv.payload))),
            3 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let mut current_page_id = u32::from_le_bytes(bytes);
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
            4 => Ok(Value::Ref(u64::from_le_bytes(iv.payload))),
            5 => Ok(Value::Timestamp(i64::from_le_bytes(iv.payload))),
            6 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&iv.payload[0..4]);
                let overflow = self.core.pager.get_overflow(u32::from_le_bytes(bytes))?;
                let mut u = [0u8; 16];
                u.copy_from_slice(&overflow.data[..16]);
                Ok(Value::Uuid(u))
            }
            _ => Ok(Value::Boolean(false)),
        }
    }
}

// ==============================================================================
// AVT INDEX (Value-Based Range Scans)
// ==============================================================================

use crate::page::{AV_NUM_CELLS, AvIndexKey, AvNodePage};

pub struct AvtCoreBTree {
    pub read_pager: ReadPager,
    pub write_pager: WritePager,
    pub root_page_id: u32,
    pub dirty_nodes: HashMap<u32, AvNodePage>,
    pub free_pages: Vec<u32>,
    pub pending_free: Vec<u32>,
    retired_generations: Vec<RetiredGeneration>,
}

impl AvtCoreBTree {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let mut write_pager = WritePager::open(path)?;
        let file_len = write_pager.file().metadata().map_err(MesoError::Io)?.len();

        let root_page_id;
        let free_pages;

        if file_len == 0 {
            let mut empty_root = unsafe { std::mem::zeroed::<AvNodePage>() };
            empty_root.header.is_leaf = 1;
            write_pager.write_page_bytes(1, bytemuck::bytes_of(&empty_root))?;

            let mut metapage = [0u8; 4096];
            metapage[0..4].copy_from_slice(&1u32.to_le_bytes());
            metapage[4..8].copy_from_slice(&2u32.to_le_bytes());
            write_pager.write_page_bytes(0, &metapage)?;
            write_pager.flush()?;

            root_page_id = 1;
            write_pager.next_page_id = 2;
            free_pages = Vec::new();
        } else {
            let (recovered_root, recovered_free) = load_metapage_and_freelist(&mut write_pager)?;
            root_page_id = recovered_root;
            free_pages = recovered_free;
        }

        let read_pager = ReadPager::open(write_pager.file())?;
        Ok(Self {
            read_pager,
            write_pager,
            root_page_id,
            dirty_nodes: HashMap::new(),
            free_pages,
            pending_free: Vec::new(),
            retired_generations: Vec::new(),
        })
    }

    fn reclaim_retired(&mut self) {
        self.retired_generations.retain_mut(|(old_mmap, pages)| {
            if Arc::strong_count(old_mmap) == 1 {
                self.free_pages.append(pages);
                false
            } else {
                true
            }
        });
    }

    pub fn allocate_page(&mut self) -> u32 {
        self.reclaim_retired();
        if let Some(id) = self.free_pages.pop() {
            return id;
        }
        let id = self.write_pager.next_page_id;
        self.write_pager.next_page_id += 1;
        id
    }

    #[inline(always)]
    pub fn get_node(&self, page_id: u32) -> Result<&AvNodePage, MesoError> {
        if let Some(node) = self.dirty_nodes.get(&page_id) {
            Ok(node)
        } else {
            self.read_pager.get_av_node(page_id)
        }
    }

    pub fn put_kv(&mut self, key: AvIndexKey, val: IndexValue) -> Result<(), MesoError> {
        let (new_root_id, split_opt) = self.insert_into_node(self.root_page_id, key, val)?;
        if let Some((split_key, right_page_id)) = split_opt {
            let super_root_id = self.allocate_page();
            let mut super_root = unsafe { std::mem::zeroed::<AvNodePage>() };
            super_root.header.num_cells = 2;
            super_root.keys[0] = self.get_node(new_root_id)?.keys[0];
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

    fn insert_into_node(
        &mut self,
        page_id: u32,
        target_key: AvIndexKey,
        target_val: IndexValue,
    ) -> Result<(u32, Option<(AvIndexKey, u32)>), MesoError> {
        let mut new_node = *self.get_node(page_id)?;
        let new_page_id = if self.dirty_nodes.contains_key(&page_id) {
            page_id
        } else {
            let allocated = self.allocate_page();
            if page_id >= 1 {
                self.pending_free.push(page_id);
            }
            allocated
        };
        let num_cells = new_node.header.num_cells as usize;

        if new_node.header.is_leaf == 1 {
            let mut insert_idx = 0;
            while insert_idx < num_cells && new_node.keys[insert_idx] < target_key {
                insert_idx += 1;
            }
            if insert_idx < num_cells && new_node.keys[insert_idx] == target_key {
                new_node.values[insert_idx] = target_val;
                self.dirty_nodes.insert(new_page_id, new_node);
                return Ok((new_page_id, None));
            }
            if num_cells < AV_NUM_CELLS {
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
                let mut temp_keys = [unsafe { std::mem::zeroed::<AvIndexKey>() }; AV_NUM_CELLS + 1];
                let mut temp_vals = [unsafe { std::mem::zeroed::<IndexValue>() }; AV_NUM_CELLS + 1];
                temp_keys[..insert_idx].copy_from_slice(&new_node.keys[..insert_idx]);
                temp_vals[..insert_idx].copy_from_slice(&new_node.values[..insert_idx]);
                temp_keys[insert_idx] = target_key;
                temp_vals[insert_idx] = target_val;
                if insert_idx < AV_NUM_CELLS {
                    temp_keys[insert_idx + 1..]
                        .copy_from_slice(&new_node.keys[insert_idx..AV_NUM_CELLS]);
                    temp_vals[insert_idx + 1..]
                        .copy_from_slice(&new_node.values[insert_idx..AV_NUM_CELLS]);
                }
                let right_page_id = self.allocate_page();
                let split_idx = AV_NUM_CELLS / 2;
                let mut right_node = unsafe { std::mem::zeroed::<AvNodePage>() };
                right_node.header.is_leaf = 1;
                right_node.header.num_cells = ((AV_NUM_CELLS + 1) - split_idx) as u16;
                right_node.header.right_sibling = new_node.header.right_sibling;
                right_node.keys[..right_node.header.num_cells as usize]
                    .copy_from_slice(&temp_keys[split_idx..]);
                right_node.values[..right_node.header.num_cells as usize]
                    .copy_from_slice(&temp_vals[split_idx..]);
                new_node.header.num_cells = split_idx as u16;
                new_node.header.right_sibling = right_page_id;
                new_node.keys[..split_idx].copy_from_slice(&temp_keys[..split_idx]);
                new_node.values[..split_idx].copy_from_slice(&temp_vals[..split_idx]);
                self.dirty_nodes.insert(new_page_id, new_node);
                self.dirty_nodes.insert(right_page_id, right_node);
                Ok((new_page_id, Some((temp_keys[split_idx], right_page_id))))
            }
        } else {
            let mut child_idx = 0;
            while child_idx < num_cells && target_key >= new_node.keys[child_idx] {
                child_idx += 1;
            }
            child_idx = child_idx.saturating_sub(1);
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&new_node.values[child_idx].payload[0..4]);
            let child_page_id = u32::from_le_bytes(bytes);
            let (new_child_id, split_opt) =
                self.insert_into_node(child_page_id, target_key, target_val)?;
            new_node.values[child_idx] = Self::make_child_ptr(new_child_id);
            if target_key < new_node.keys[child_idx] {
                new_node.keys[child_idx] = target_key;
            }
            if let Some((split_key, right_child_id)) = split_opt {
                let insert_idx = child_idx + 1;
                let target_ptr = Self::make_child_ptr(right_child_id);
                if num_cells < AV_NUM_CELLS {
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
                    let mut temp_keys =
                        [unsafe { std::mem::zeroed::<AvIndexKey>() }; AV_NUM_CELLS + 1];
                    let mut temp_vals =
                        [unsafe { std::mem::zeroed::<IndexValue>() }; AV_NUM_CELLS + 1];
                    temp_keys[..insert_idx].copy_from_slice(&new_node.keys[..insert_idx]);
                    temp_vals[..insert_idx].copy_from_slice(&new_node.values[..insert_idx]);
                    temp_keys[insert_idx] = split_key;
                    temp_vals[insert_idx] = target_ptr;
                    if insert_idx < AV_NUM_CELLS {
                        temp_keys[insert_idx + 1..]
                            .copy_from_slice(&new_node.keys[insert_idx..AV_NUM_CELLS]);
                        temp_vals[insert_idx + 1..]
                            .copy_from_slice(&new_node.values[insert_idx..AV_NUM_CELLS]);
                    }
                    let right_internal_id = self.allocate_page();
                    let split_idx = AV_NUM_CELLS / 2;
                    let mut right_node = unsafe { std::mem::zeroed::<AvNodePage>() };
                    right_node.header.num_cells = ((AV_NUM_CELLS + 1) - split_idx) as u16;
                    right_node.keys[..right_node.header.num_cells as usize]
                        .copy_from_slice(&temp_keys[split_idx..]);
                    right_node.values[..right_node.header.num_cells as usize]
                        .copy_from_slice(&temp_vals[split_idx..]);
                    new_node.header.num_cells = split_idx as u16;
                    new_node.keys[..split_idx].copy_from_slice(&temp_keys[..split_idx]);
                    new_node.values[..split_idx].copy_from_slice(&temp_vals[..split_idx]);
                    self.dirty_nodes.insert(new_page_id, new_node);
                    self.dirty_nodes.insert(right_internal_id, right_node);
                    Ok((new_page_id, Some((temp_keys[split_idx], right_internal_id))))
                }
            } else {
                self.dirty_nodes.insert(new_page_id, new_node);
                Ok((new_page_id, None))
            }
        }
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        for (page_id, node) in &self.dirty_nodes {
            self.write_pager
                .write_page_bytes(*page_id, bytemuck::bytes_of(node))?;
        }

        let old_mmap = self.read_pager.mmap.clone();
        let newly_retired = std::mem::take(&mut self.pending_free);
        if !newly_retired.is_empty() {
            self.retired_generations.push((old_mmap, newly_retired));
        }

        self.write_pager.flush()?;
        self.read_pager = ReadPager::open(self.write_pager.file())?;
        self.reclaim_retired();

        persist_metapage_and_freelist(
            &mut self.write_pager,
            self.root_page_id,
            &mut self.free_pages,
            &self.retired_generations,
        )?;
        self.write_pager.flush()?;
        self.read_pager = ReadPager::open(self.write_pager.file())?;
        self.dirty_nodes.clear();
        Ok(())
    }

    fn make_child_ptr(page_id: u32) -> IndexValue {
        let mut payload = [0u8; 8];
        payload[0..4].copy_from_slice(&page_id.to_le_bytes());
        IndexValue {
            type_tag: 255,
            padding: [0; 7],
            payload,
        }
    }
}

pub struct AvtIndex {
    pub core: AvtCoreBTree,
}
impl AvtIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        Ok(Self {
            core: AvtCoreBTree::open(path)?,
        })
    }

    pub fn put(
        &mut self,
        a: AttributeId,
        v: &Value,
        e: EntityId,
        is_add: bool,
    ) -> Result<(), MesoError> {
        let v_sort = match v {
            Value::Int64(i) => crate::page::encode_i64(*i),
            Value::Float64(f) => crate::page::encode_f64(*f),
            Value::Timestamp(t) => crate::page::encode_i64(*t),
            Value::Boolean(b) => u64::from(*b),
            Value::Ref(r) => *r,
            Value::String(s) => crate::page::encode_bytes_prefix(s.as_bytes()),
            Value::Uuid(u) => crate::page::encode_bytes_prefix(u),
        };
        let key = AvIndexKey {
            a,
            _pad: 0,
            v_sort,
            e,
        };
        let val = IndexValue {
            type_tag: if is_add { 1 } else { 254 },
            padding: [0; 7],
            payload: [0; 8],
        };
        self.core.put_kv(key, val)
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        self.core.flush()
    }

    pub fn snapshot(&self) -> AvtIndexSnapshot {
        AvtIndexSnapshot {
            pager: self.core.read_pager.clone(),
            root_page_id: self.core.root_page_id,
        }
    }
}

#[derive(Clone)]
pub struct AvtIndexSnapshot {
    pub pager: ReadPager,
    pub root_page_id: u32,
}
impl AvtIndexSnapshot {
    pub fn range_scan(
        &self,
        a: AttributeId,
        v_min: u64,
        v_max: u64,
    ) -> Result<Vec<EntityId>, MesoError> {
        let start_key = AvIndexKey {
            a,
            _pad: 0,
            v_sort: v_min,
            e: 0,
        };
        let end_key = AvIndexKey {
            a,
            _pad: 0,
            v_sort: v_max,
            e: u64::MAX,
        };
        let mut current_page_id = self.root_page_id;

        loop {
            let node = self.pager.get_av_node(current_page_id)?;
            let num_cells = node.header.num_cells as usize;
            if num_cells == 0 {
                return Ok(Vec::new());
            }
            if node.header.is_leaf == 1 {
                break;
            }
            let mut child_idx = 0;
            while child_idx < num_cells && start_key >= node.keys[child_idx] {
                child_idx += 1;
            }
            child_idx = child_idx.saturating_sub(1);
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&node.values[child_idx].payload[0..4]);
            current_page_id = u32::from_le_bytes(bytes);
        }

        let mut results = Vec::new();
        loop {
            let node = self.pager.get_av_node(current_page_id)?;
            let num_cells = node.header.num_cells as usize;
            for i in 0..num_cells {
                let key = node.keys[i];
                if key > end_key {
                    return Ok(results);
                }
                if key >= start_key && node.values[i].type_tag != 254 {
                    results.push(key.e);
                }
            }
            if node.header.right_sibling == 0 {
                break;
            }
            current_page_id = node.header.right_sibling;
        }
        Ok(results)
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

    #[test]
    fn test_avt_index_range_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = AvtIndex::open(dir.path().join("avt.idx")).unwrap();

        let attr = 50;
        // Insert values 10, 20, 30, 40
        index.put(attr, &Value::Int64(10), 1, true).unwrap();
        index.put(attr, &Value::Int64(20), 2, true).unwrap();
        index.put(attr, &Value::Int64(30), 3, true).unwrap();
        index.put(attr, &Value::Int64(40), 4, true).unwrap();

        index.flush().unwrap();
        let snap = index.snapshot();

        let v_min = crate::page::encode_i64(15);
        let v_max = crate::page::encode_i64(35);

        let results = snap.range_scan(attr, v_min, v_max).unwrap();

        // Should fetch entities 2 and 3 natively via the B+Tree leaf chain
        assert_eq!(results, vec![2, 3]);
    }

    #[test]
    fn test_ave_index_type_separation_and_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ave_128_test.idx");

        {
            let mut index = AveIndex::open(&path).unwrap();
            index.put(10, &Value::Int64(42), 1001).unwrap();
            index.put(10, &Value::Ref(42), 1002).unwrap();
            index.put(10, &Value::Timestamp(42), 1003).unwrap();
            index.flush().unwrap();
        }

        let reopened = AveIndex::open(&path).unwrap();
        let snap = reopened.snapshot();
        assert_eq!(snap.get(10, &Value::Int64(42)).unwrap(), Some(1001));
        assert_eq!(snap.get(10, &Value::Ref(42)).unwrap(), Some(1002));
        assert_eq!(snap.get(10, &Value::Timestamp(42)).unwrap(), Some(1003));
    }

    #[test]
    fn test_cow_node_and_overflow_page_reclamation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reclaim_now.idx");

        let mut index = NowIndex::open(&path).unwrap();
        let large_str = "X".repeat(9000); // Spans 3 overflow pages per write

        // Seed initial keys + large strings and flush
        for i in 1..=20 {
            index.put(i, 10, &Value::String(large_str.clone())).unwrap();
        }
        index.flush().unwrap();
        let baseline_pages = index.core.write_pager.next_page_id;

        // Round 0 allocates the shadow COW generation before retiring the initial generation
        for i in 1..=20 {
            let updated = format!("0_{}", large_str);
            index.put(i, 10, &Value::String(updated)).unwrap();
        }
        index.flush().unwrap();
        let steady_state_pages = index.core.write_pager.next_page_id;
        assert!(steady_state_pages <= baseline_pages * 2);

        // Perform 24 more full overwrite + flush cycles; page allocation must remain completely flat
        for round in 1..25 {
            for i in 1..=20 {
                let updated = format!("{}_{}", round, large_str);
                index.put(i, 10, &Value::String(updated)).unwrap();
            }
            index.flush().unwrap();
        }

        let after_cycles_pages = index.core.write_pager.next_page_id;
        assert_eq!(
            after_cycles_pages, steady_state_pages,
            "COW node and overflow pages must reach exact steady state with zero leaks"
        );

        // Verify persistence across reopen
        drop(index);
        let reopened = NowIndex::open(&path).unwrap();
        assert!(!reopened.core.free_pages.is_empty());
        let val = reopened.get(15, 10).unwrap().unwrap();
        assert_eq!(val, Value::String(format!("24_{}", large_str)));
    }

    #[test]
    fn test_snapshot_isolation_defers_page_reuse_until_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap_isolation.idx");

        let mut index = NowIndex::open(&path).unwrap();
        index
            .put(1, 10, &Value::String("OriginalValue".into()))
            .unwrap();
        index.flush().unwrap();

        // Pin a snapshot before overwriting
        let pinned_snap = index.snapshot();

        // Overwrite and flush while snapshot is still alive
        index
            .put(1, 10, &Value::String("UpdatedValue".into()))
            .unwrap();
        index.flush().unwrap();

        // Allocate additional keys; pinned_snap's old pages must NOT be overwritten yet
        for i in 2..=30 {
            index.put(i, 10, &Value::String("Filler".into())).unwrap();
        }
        index.flush().unwrap();

        assert_eq!(
            pinned_snap.get(1, 10).unwrap(),
            Some(Value::String("OriginalValue".into())),
            "Pinned snapshot must never see overwritten pages"
        );

        // Dropping the snapshot allows retired pages to be reclaimed on subsequent allocations
        drop(pinned_snap);
        let _ = index.core.allocate_page();
    }

    #[test]
    fn test_avt_index_string_and_uuid_range_scans() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = AvtIndex::open(dir.path().join("avt_str_uuid.idx")).unwrap();

        let str_attr = 60;
        index
            .put(str_attr, &Value::String("alpha_0001".into()), 1, true)
            .unwrap();
        index
            .put(str_attr, &Value::String("beta_0001".into()), 2, true)
            .unwrap();
        index
            .put(str_attr, &Value::String("beta_0002".into()), 3, true)
            .unwrap();
        index
            .put(str_attr, &Value::String("gamma_0001".into()), 4, true)
            .unwrap();

        let uuid_attr = 61;
        let u1 = [0x10; 16];
        let u2 = [0x20; 16];
        let u3 = [0x30; 16];
        index.put(uuid_attr, &Value::Uuid(u1), 10, true).unwrap();
        index.put(uuid_attr, &Value::Uuid(u2), 20, true).unwrap();
        index.put(uuid_attr, &Value::Uuid(u3), 30, true).unwrap();

        index.flush().unwrap();
        let snap = index.snapshot();

        let s_min = crate::page::encode_bytes_prefix(b"beta_0000");
        let s_max = crate::page::encode_bytes_prefix(b"beta_9999");
        let str_hits = snap.range_scan(str_attr, s_min, s_max).unwrap();
        assert_eq!(str_hits, vec![2, 3]);

        let u_min = crate::page::encode_bytes_prefix(&[0x15; 16]);
        let u_max = crate::page::encode_bytes_prefix(&[0x30; 16]);
        let uuid_hits = snap.range_scan(uuid_attr, u_min, u_max).unwrap();
        assert_eq!(uuid_hits, vec![20, 30]);
    }
}
