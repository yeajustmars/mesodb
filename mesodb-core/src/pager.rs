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

        if file_len < target_len {
            file.set_len(target_len)?;
        }

        let mmap = unsafe { MmapOptions::new().map_mut(&file).map_err(MesoError::Io)? };
        let num_pages = (mmap.len() / PAGE_SIZE) as u32;

        Ok(Self {
            file,
            mmap,
            num_pages,
        })
    }

    /// Dynamically grows the memory-mapped file, allocating a new zeroed page.
    pub fn allocate_page(&mut self) -> Result<u32, MesoError> {
        let page_id = self.num_pages;
        let needed_len = ((page_id + 1) as u64) * (PAGE_SIZE as u64);

        // If we exceed our virtual map, double the file size and remap
        if needed_len > self.mmap.len() as u64 {
            self.flush()?;
            let new_len = std::cmp::max(needed_len, (self.mmap.len() * 2) as u64);
            self.file.set_len(new_len).map_err(MesoError::Io)?;
            self.mmap = unsafe {
                MmapOptions::new()
                    .map_mut(&self.file)
                    .map_err(MesoError::Io)?
            };
        }

        self.num_pages += 1;

        // Guarantee the new page is zero-initialized for safety
        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &mut self.mmap[offset..(offset + PAGE_SIZE)];
        page_bytes.fill(0);

        Ok(page_id)
    }

    pub fn get_node_mut(&mut self, page_id: u32) -> Result<&mut NodePage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "Page out of bounds: {}",
                page_id
            )));
        }
        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &mut self.mmap[offset..(offset + PAGE_SIZE)];
        Ok(bytemuck::from_bytes_mut(page_bytes))
    }

    pub fn get_node(&self, page_id: u32) -> Result<&NodePage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "Page out of bounds: {}",
                page_id
            )));
        }
        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &self.mmap[offset..(offset + PAGE_SIZE)];
        Ok(bytemuck::from_bytes(page_bytes))
    }

    pub fn get_overflow_mut(
        &mut self,
        page_id: u32,
    ) -> Result<&mut crate::page::OverflowPage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "Page out of bounds: {}",
                page_id
            )));
        }
        let offset = (page_id as usize) * crate::page::PAGE_SIZE;
        let page_bytes = &mut self.mmap[offset..(offset + crate::page::PAGE_SIZE)];
        Ok(bytemuck::from_bytes_mut(page_bytes))
    }

    pub fn get_overflow(&self, page_id: u32) -> Result<&crate::page::OverflowPage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "Page out of bounds: {}",
                page_id
            )));
        }
        let offset = (page_id as usize) * crate::page::PAGE_SIZE;
        let page_bytes = &self.mmap[offset..(offset + crate::page::PAGE_SIZE)];
        Ok(bytemuck::from_bytes(page_bytes))
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
