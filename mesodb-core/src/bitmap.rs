// mesodb-core/src/bitmap.rs

use ahash::AHashMap;
use memmap2::{Mmap, MmapOptions};
use roaring::RoaringTreemap;
use std::{
    collections::hash_map::DefaultHasher,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    error::MesoError,
    types::{AttributeId, EntityId, Value},
};

pub fn hash_value(value: &Value) -> u64 {
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

/// 100% Lock-Free Mmap Snapshot of the active bitmaps.
#[derive(Clone)]
pub struct BitmapSnapshot {
    mmap: Arc<Mmap>,
    pointers: Arc<AHashMap<(AttributeId, u64), (u64, u32)>>,
}

impl BitmapSnapshot {
    #[inline(always)]
    pub fn get(&self, a: AttributeId, v: &Value) -> Result<Option<RoaringTreemap>, MesoError> {
        let val_hash = hash_value(v);
        if let Some(&(offset, len)) = self.pointers.get(&(a, val_hash)) {
            let start = offset as usize;
            let end = start + (len as usize);
            let slice = &self.mmap[start..end];
            let treemap = RoaringTreemap::deserialize_from(slice)
                .map_err(|e| MesoError::Serialization(e.to_string()))?;
            Ok(Some(treemap))
        } else {
            Ok(None)
        }
    }
}

/// The append-only Log-Structured Bitmap Writer.
pub struct BitmapStore {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
    current_offset: u64,
    pointers: Arc<AHashMap<(AttributeId, u64), (u64, u32)>>,
    dirty_bitmaps: AHashMap<(AttributeId, u64), RoaringTreemap>,
    read_mmap: Arc<Mmap>,
}

impl BitmapStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(MesoError::Io)?;

        let mut current_offset = file.metadata().map_err(MesoError::Io)?.len();
        let mut pointers = AHashMap::new();

        // Safety: memmap2 cannot map a 0-byte file. Initialize with a 1-byte pad.
        if current_offset == 0 {
            file.write_all(&[0]).map_err(MesoError::Io)?;
            file.sync_data().map_err(MesoError::Io)?;
            current_offset = 1;
        } else if current_offset > 1 {
            // Rebuild pointer map from disk sequentially
            file.seek(SeekFrom::Start(1)).map_err(MesoError::Io)?;
            let mut pos = 1;
            while pos < current_offset {
                let mut head = [0u8; 16];
                if file.read_exact(&mut head).is_err() {
                    break;
                }
                let a_id = u32::from_ne_bytes(head[0..4].try_into().unwrap());
                let val_hash = u64::from_ne_bytes(head[4..12].try_into().unwrap());
                let len = u32::from_ne_bytes(head[12..16].try_into().unwrap());

                pointers.insert((a_id, val_hash), (pos + 16, len));
                pos += 16 + (len as u64);
                file.seek(SeekFrom::Start(pos)).map_err(MesoError::Io)?;
            }
        }

        let mmap = unsafe { MmapOptions::new().map(&file).map_err(MesoError::Io)? };

        Ok(Self {
            file,
            path,
            current_offset,
            pointers: Arc::new(pointers),
            dirty_bitmaps: AHashMap::new(),
            read_mmap: Arc::new(mmap),
        })
    }

    pub fn snapshot(&self) -> BitmapSnapshot {
        BitmapSnapshot {
            mmap: self.read_mmap.clone(),
            pointers: self.pointers.clone(),
        }
    }

    pub fn put(
        &mut self,
        a: AttributeId,
        v: &Value,
        e: EntityId,
        is_add: bool,
    ) -> Result<(), MesoError> {
        let val_hash = hash_value(v);
        let key = (a, val_hash);

        if !self.dirty_bitmaps.contains_key(&key) {
            // Load existing from disk if it exists, or start fresh
            if let Some(&(offset, len)) = self.pointers.get(&key) {
                let start = offset as usize;
                let end = start + (len as usize);
                let slice = &self.read_mmap[start..end];
                let mut treemap = RoaringTreemap::deserialize_from(slice)
                    .map_err(|e| MesoError::Serialization(e.to_string()))?;

                if is_add {
                    treemap.insert(e);
                } else {
                    treemap.remove(e);
                }
                self.dirty_bitmaps.insert(key, treemap);
            } else {
                let mut treemap = RoaringTreemap::new();
                if is_add {
                    treemap.insert(e);
                }
                self.dirty_bitmaps.insert(key, treemap);
            }
        } else {
            let treemap = self.dirty_bitmaps.get_mut(&key).unwrap();
            if is_add {
                treemap.insert(e);
            } else {
                treemap.remove(e);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), MesoError> {
        if self.dirty_bitmaps.is_empty() {
            return Ok(());
        }

        self.file.seek(SeekFrom::End(0)).map_err(MesoError::Io)?;
        let mut new_pointers = (*self.pointers).clone();

        for (&(a_id, val_hash), treemap) in &self.dirty_bitmaps {
            let mut buf = Vec::new();
            treemap
                .serialize_into(&mut buf)
                .map_err(|e| MesoError::Serialization(e.to_string()))?;

            let len = buf.len() as u32;
            let mut head = [0u8; 16];
            head[0..4].copy_from_slice(&a_id.to_ne_bytes());
            head[4..12].copy_from_slice(&val_hash.to_ne_bytes());
            head[12..16].copy_from_slice(&len.to_ne_bytes());

            self.file.write_all(&head).map_err(MesoError::Io)?;
            self.file.write_all(&buf).map_err(MesoError::Io)?;

            new_pointers.insert((a_id, val_hash), (self.current_offset + 16, len));
            self.current_offset += 16 + (len as u64);
        }

        self.file.sync_data().map_err(MesoError::Io)?;
        self.pointers = Arc::new(new_pointers);
        self.dirty_bitmaps.clear();

        // RCU Swap for active mmap readers
        self.read_mmap =
            Arc::new(unsafe { MmapOptions::new().map(&self.file).map_err(MesoError::Io)? });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_log_structured_bitmap_store() {
        let dir = tempdir().unwrap();
        let mut store = BitmapStore::open(dir.path().join("bitmaps.idx")).unwrap();

        // Entity 1 & 2 share "active" status
        store
            .put(100, &Value::String("active".into()), 1, true)
            .unwrap();
        store
            .put(100, &Value::String("active".into()), 2, true)
            .unwrap();

        // Entity 3 has "inactive" status
        store
            .put(100, &Value::String("inactive".into()), 3, true)
            .unwrap();

        store.flush().unwrap();

        let snap = store.snapshot();

        let active_map = snap
            .get(100, &Value::String("active".into()))
            .unwrap()
            .unwrap();
        assert_eq!(active_map.len(), 2);
        assert!(active_map.contains(1));
        assert!(active_map.contains(2));

        let inactive_map = snap
            .get(100, &Value::String("inactive".into()))
            .unwrap()
            .unwrap();
        assert_eq!(inactive_map.len(), 1);
        assert!(inactive_map.contains(3));
    }
}
