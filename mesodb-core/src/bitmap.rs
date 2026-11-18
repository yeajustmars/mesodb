// mesodb-core/src/bitmap.rs

use ahash::AHashMap;
use memmap2::{Mmap, MmapOptions};
use roaring::RoaringTreemap;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    error::MesoError,
    types::{AttributeId, EntityId, Value},
};

const BITMAP_HEADER_LEN: usize = 24;

#[inline(always)]
fn fnv1a_update(mut state: u64, prime: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        state ^= u64::from(b);
        state = state.wrapping_mul(prime);
    }
    state
}

#[inline(always)]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Deterministic, platform-independent 128-bit hash with explicit `Value` variant separation.
pub fn hash_value(value: &Value) -> u128 {
    const OFFSET_LO: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME_LO: u64 = 0x0000_0100_0000_01b3;
    const OFFSET_HI: u64 = 0x8422_2325_cbf2_9ce4;
    const PRIME_HI: u64 = 0x0000_01b3_0000_0100;

    let (tag, bytes_storage): (u8, [u8; 8]) = match value {
        Value::Boolean(b) => (0, [u8::from(*b), 0, 0, 0, 0, 0, 0, 0]),
        Value::Int64(i) => (1, i.to_le_bytes()),
        Value::Float64(f) => {
            let canonical_bits = if *f == 0.0 { 0u64 } else { f.to_bits() };
            (2, canonical_bits.to_le_bytes())
        }
        Value::String(_) => (3, [0; 8]),
        Value::Ref(r) => (4, r.to_le_bytes()),
        Value::Timestamp(t) => (5, t.to_le_bytes()),
        Value::Uuid(_) => (6, [0; 8]),
    };

    let mut lo = fnv1a_update(OFFSET_LO, PRIME_LO, &[tag]);
    let mut hi = fnv1a_update(OFFSET_HI, PRIME_HI, &[tag]);

    match value {
        Value::Boolean(_) => {
            lo = fnv1a_update(lo, PRIME_LO, &bytes_storage[..1]);
            hi = fnv1a_update(hi, PRIME_HI, &bytes_storage[..1]);
        }
        Value::Int64(_) | Value::Float64(_) | Value::Ref(_) | Value::Timestamp(_) => {
            lo = fnv1a_update(lo, PRIME_LO, &bytes_storage);
            hi = fnv1a_update(hi, PRIME_HI, &bytes_storage);
        }
        Value::String(s) => {
            let len_bytes = (s.len() as u64).to_le_bytes();
            lo = fnv1a_update(lo, PRIME_LO, &len_bytes);
            hi = fnv1a_update(hi, PRIME_HI, &len_bytes);
            lo = fnv1a_update(lo, PRIME_LO, s.as_bytes());
            hi = fnv1a_update(hi, PRIME_HI, s.as_bytes());
        }
        Value::Uuid(u) => {
            lo = fnv1a_update(lo, PRIME_LO, u);
            hi = fnv1a_update(hi, PRIME_HI, u);
        }
    }

    let final_lo = mix64(lo ^ hi.rotate_left(17));
    let final_hi = mix64(hi ^ lo.rotate_right(23));
    (u128::from(final_hi) << 64) | u128::from(final_lo)
}

/// 100% Lock-Free Mmap Snapshot of the active bitmaps.
#[derive(Clone)]
pub struct BitmapSnapshot {
    mmap: Arc<Mmap>,
    pointers: Arc<AHashMap<(AttributeId, u128), (u64, u32)>>,
}

impl BitmapSnapshot {
    #[inline(always)]
    pub fn get(&self, a: AttributeId, v: &Value) -> Result<Option<RoaringTreemap>, MesoError> {
        let val_hash = hash_value(v);
        if let Some(&(offset, len)) = self.pointers.get(&(a, val_hash)) {
            let start = offset as usize;
            let end = start
                .checked_add(len as usize)
                .ok_or_else(|| MesoError::Serialization("Bitmap slice overflow".into()))?;
            let slice = self
                .mmap
                .get(start..end)
                .ok_or_else(|| MesoError::Serialization("Bitmap slice out of bounds".into()))?;
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
    pointers: Arc<AHashMap<(AttributeId, u128), (u64, u32)>>,
    dirty_bitmaps: AHashMap<(AttributeId, u128), RoaringTreemap>,
    read_mmap: Arc<Mmap>,
}

impl BitmapStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MesoError> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Explicitly declare we are preserving existing data
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
            let mut pos = 1u64;
            while pos + (BITMAP_HEADER_LEN as u64) <= current_offset {
                let mut head = [0u8; BITMAP_HEADER_LEN];
                if file.read_exact(&mut head).is_err() {
                    break;
                }
                let a_id = u32::from_le_bytes(head[0..4].try_into().unwrap());
                let val_hash = u128::from_le_bytes(head[4..20].try_into().unwrap());
                let len = u32::from_le_bytes(head[20..24].try_into().unwrap());

                let next_pos = pos + (BITMAP_HEADER_LEN as u64) + u64::from(len);
                if next_pos > current_offset {
                    break;
                }

                pointers.insert((a_id, val_hash), (pos + (BITMAP_HEADER_LEN as u64), len));
                pos = next_pos;
                file.seek(SeekFrom::Start(pos)).map_err(MesoError::Io)?;
            }
            current_offset = pos;
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

        self.file
            .seek(SeekFrom::Start(self.current_offset))
            .map_err(MesoError::Io)?;
        let mut new_pointers = (*self.pointers).clone();

        for (&(a_id, val_hash), treemap) in &self.dirty_bitmaps {
            let mut buf = Vec::new();
            treemap
                .serialize_into(&mut buf)
                .map_err(|e| MesoError::Serialization(e.to_string()))?;

            let len = buf.len() as u32;
            let mut head = [0u8; BITMAP_HEADER_LEN];
            head[0..4].copy_from_slice(&a_id.to_le_bytes());
            head[4..20].copy_from_slice(&val_hash.to_le_bytes());
            head[20..24].copy_from_slice(&len.to_le_bytes());

            self.file.write_all(&head).map_err(MesoError::Io)?;
            self.file.write_all(&buf).map_err(MesoError::Io)?;

            new_pointers.insert(
                (a_id, val_hash),
                (self.current_offset + (BITMAP_HEADER_LEN as u64), len),
            );
            self.current_offset += (BITMAP_HEADER_LEN as u64) + u64::from(len);
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

    #[test]
    fn test_deterministic_128bit_hash_type_separation() {
        let h_int = hash_value(&Value::Int64(1));
        let h_ref = hash_value(&Value::Ref(1));
        let h_ts = hash_value(&Value::Timestamp(1));
        let h_bool = hash_value(&Value::Boolean(true));
        let h_float = hash_value(&Value::Float64(1.0));
        let h_str = hash_value(&Value::String("1".into()));

        let all = [h_int, h_ref, h_ts, h_bool, h_float, h_str];
        for i in 0..all.len() {
            for j in (i + 1)..all.len() {
                assert_ne!(all[i], all[j], "Distinct Value variants must not collide");
            }
        }

        // Canonical float zero normalization
        assert_eq!(
            hash_value(&Value::Float64(0.0)),
            hash_value(&Value::Float64(-0.0))
        );
    }

    #[test]
    fn test_bitmap_store_reopen_and_truncated_tail_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bitmaps_recover.idx");

        {
            let mut store = BitmapStore::open(&path).unwrap();
            store.put(10, &Value::Int64(42), 100, true).unwrap();
            store.put(10, &Value::Ref(42), 200, true).unwrap();
            store.flush().unwrap();
        }

        // Append a torn/partial header to simulate a crash mid-write
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xAA; 10]).unwrap();
            f.sync_all().unwrap();
        }

        let reopened = BitmapStore::open(&path).unwrap();
        let snap = reopened.snapshot();

        let int_map = snap.get(10, &Value::Int64(42)).unwrap().unwrap();
        assert_eq!(int_map.len(), 1);
        assert!(int_map.contains(100));

        let ref_map = snap.get(10, &Value::Ref(42)).unwrap().unwrap();
        assert_eq!(ref_map.len(), 1);
        assert!(ref_map.contains(200));
    }
}
