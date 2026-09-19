// mesodb-core/src/pager.rs

use memmap2::{Mmap, MmapOptions};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

use crate::{
    error::MesoError,
    page::{NodePage, OverflowPage, PAGE_SIZE},
};

/// 100% Lock-Free, Zero-Copy Read Engine.
/// Readers hold an immutable Arc to the memory map. They can never be blocked.
#[derive(Clone)]
pub struct ReadPager {
    mmap: Arc<Mmap>,
    pub num_pages: u32,
}

impl ReadPager {
    pub fn open(file: &File) -> Result<Self, MesoError> {
        let file_len = file.metadata().map_err(MesoError::Io)?.len();
        if file_len == 0 {
            return Err(MesoError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Cannot mmap empty file",
            )));
        }
        // Safety: We never mutate the file in-place where readers are looking.
        // The background writer exclusively appends new pages to the end.
        let mmap = unsafe { MmapOptions::new().map(file).map_err(MesoError::Io)? };
        let num_pages = (mmap.len() / PAGE_SIZE) as u32;

        Ok(Self {
            mmap: Arc::new(mmap),
            num_pages,
        })
    }

    #[inline(always)]
    pub fn get_node(&self, page_id: u32) -> Result<&NodePage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "ReadPager out of bounds: {}",
                page_id
            )));
        }
        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &self.mmap[offset..(offset + PAGE_SIZE)];
        Ok(bytemuck::from_bytes(page_bytes))
    }

    #[inline(always)]
    pub fn get_overflow(&self, page_id: u32) -> Result<&OverflowPage, MesoError> {
        if page_id >= self.num_pages {
            return Err(MesoError::Serialization(format!(
                "ReadPager out of bounds: {}",
                page_id
            )));
        }
        let offset = (page_id as usize) * PAGE_SIZE;
        let page_bytes = &self.mmap[offset..(offset + PAGE_SIZE)];
        Ok(bytemuck::from_bytes(page_bytes))
    }
}

/// The Background Writer Engine.
/// Appends COW pages safely without interfering with active mmaps.
pub struct WritePager {
    pub file: File,
    pub next_page_id: u32,
}

impl WritePager {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Explicitly declare we are preserving existing data
            .open(path)
            .map_err(MesoError::Io)?;

        Ok(Self {
            file,
            next_page_id: 0, // Bootstrapped by the CoreBTree Metapage logic
        })
    }

    pub fn write_page_bytes(&mut self, page_id: u32, bytes: &[u8]) -> Result<(), MesoError> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(MesoError::Io)?;
        self.file.write_all(bytes).map_err(MesoError::Io)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        self.file.sync_data().map_err(MesoError::Io)
    }

    pub fn file(&self) -> &File {
        &self.file
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

        // 1. Initialize Write Pager and allocate 2 pages mechanically
        let mut wp = WritePager::open(temp_file.path()).expect("Failed to open write pager");
        wp.next_page_id = 2;

        // 2. Build the memory boundary (Write Alice's Age = 35)
        let mut node = unsafe { std::mem::zeroed::<NodePage>() };
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
            type_tag: 1, // Simulated ValueType::Int64
            padding: [0; 7],
            payload,
        };

        let bytes = bytemuck::bytes_of(&node);
        wp.write_page_bytes(1, bytes).unwrap();
        wp.flush().expect("Failed to flush to disk");

        // 3. Mount the lock-free ReadPager to prove true disk persistence WITHOUT SerDe
        let rp = ReadPager::open(wp.file()).unwrap();

        // 4. Validate the zero-copy read
        let recovered_node = rp.get_node(1).unwrap();

        assert_eq!(recovered_node.header.is_leaf, 1);
        assert_eq!(recovered_node.header.num_cells, 1);
        assert_eq!(recovered_node.keys[0].e, 42);
        assert_eq!(recovered_node.keys[0].a, 99);

        let val_bytes = recovered_node.values[0].payload;
        let age = i64::from_ne_bytes(val_bytes);
        assert_eq!(age, 35);
    }
}
