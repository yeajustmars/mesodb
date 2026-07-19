// mesodb-core/src/pager.rs

use memmap2::{MmapMut, MmapOptions};
use std::{fs::OpenOptions, path::Path};

use crate::error::MesoError;
use crate::page::{NodePage, PAGE_SIZE};

pub struct Pager {
    file: std::fs::File,
    mmap: MmapMut,
    pub num_pages: u32,
}

impl Pager {
    pub fn open<P: AsRef<Path>>(path: P, initial_pages: u32) -> Result<Self, MesoError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let target_len = (initial_pages as u64) * (PAGE_SIZE as u64);

        // Pre-allocate the physical file space if it's too small
        if file_len < target_len {
            file.set_len(target_len)?;
        }

        // Map the entire file directly into virtual memory
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let num_pages = (mmap.len() / PAGE_SIZE) as u32;

        Ok(Self {
            file,
            mmap,
            num_pages,
        })
    }

    /// Fetches a mutable reference to a B+Tree Node, zero-copy.
    pub fn get_node_mut(&mut self, page_id: u32) -> Result<&mut NodePage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "Page out of bounds: {}",
                page_id
            )));
        }

        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &mut self.mmap[offset..(offset + PAGE_SIZE)];

        // The cast boundary: Bytes to struct instantly
        let node: &mut NodePage = bytemuck::from_bytes_mut(page_bytes);
        Ok(node)
    }

    /// Fetches a read-only reference to a B+Tree Node, zero-copy.
    pub fn get_node(&self, page_id: u32) -> Result<&NodePage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "Page out of bounds: {}",
                page_id
            )));
        }

        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &self.mmap[offset..(offset + PAGE_SIZE)];

        let node: &NodePage = bytemuck::from_bytes(page_bytes);
        Ok(node)
    }

    pub fn flush(&self) -> Result<(), MesoError> {
        self.mmap.flush().map_err(MesoError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{IndexKey, IndexValue};
    use tempfile::NamedTempFile;

    #[test]
    fn test_zero_copy_persistence() {
        let temp_file = NamedTempFile::new().unwrap();

        // 1. Initialize Pager and allocate 2 memory-mapped pages
        let mut pager = Pager::open(temp_file.path(), 2).expect("Failed to open pager");
        assert_eq!(pager.num_pages, 2);

        // 2. Mutate the raw memory boundary (Write Alice's Age = 35)
        {
            let node = pager.get_node_mut(1).expect("Failed to get page 1");
            node.header.is_leaf = 1;
            node.header.num_cells = 1;

            node.keys[0] = IndexKey {
                e: 42,
                a: 99,
                _pad: 0,
            };

            let mut payload = [0u8; 8];
            payload[0..8].copy_from_slice(&35i64.to_ne_bytes());

            node.values[0] = IndexValue {
                type_tag: 1, // Simulated ValueType::Int64 variant order
                padding: [0; 7],
                payload,
            };
        }

        pager.flush().expect("Failed to flush mmap to disk");
        drop(pager); // Drop the lock and OS mapping

        // 3. Re-open to prove true disk persistence WITHOUT SerDe overhead
        let recovering_pager = Pager::open(temp_file.path(), 2).unwrap();

        // 4. Validate the zero-copy read
        let recovered_node = recovering_pager.get_node(1).unwrap();

        assert_eq!(recovered_node.header.is_leaf, 1);
        assert_eq!(recovered_node.header.num_cells, 1);
        assert_eq!(recovered_node.keys[0].e, 42);
        assert_eq!(recovered_node.keys[0].a, 99);

        let val_bytes = recovered_node.values[0].payload;
        let age = i64::from_ne_bytes(val_bytes);
        assert_eq!(age, 35);
    }
}
