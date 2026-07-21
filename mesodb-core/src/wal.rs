use rkyv::{Archive, Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    config::WalSyncMode, datom::Datom, error::MesoError, schema::SchemaMutation, types::Result,
};

#[derive(Debug, Archive, Serialize, Deserialize)]
pub enum WalEntry {
    DataBatch(Vec<Datom>),
    SchemaMutation(SchemaMutation),
}

pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    #[allow(clippy::suspicious_open_options)]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // We intentionally avoid truncate/append here to allow manual seeking for recovery
            // Do NOT remove suspicious_open_options above and follow Clippy's advise.
            .open(&p)?;

        Ok(Self { file, path: p })
    }

    pub fn append_entry(&mut self, entry: &WalEntry, sync_mode: &WalSyncMode) -> Result<()> {
        self.file.seek(SeekFrom::End(0))?;

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(entry)
            .map_err(|e| MesoError::Serialization(e.to_string()))?;

        let len = bytes.len() as u64;
        if len == 0 {
            return Err(MesoError::Serialization(
                "Serialized entry was empty".into(),
            ));
        }

        self.file.write_all(&len.to_ne_bytes())?;
        self.file.write_all(&bytes)?;

        if *sync_mode == WalSyncMode::Strict {
            self.file.sync_all()?;
        }

        Ok(())
    }

    pub fn recover(&mut self) -> Result<Vec<WalEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut all_entries = Vec::new();
        let file_len = self.file.metadata()?.len();

        while self.file.stream_position()? < file_len {
            let mut len_bytes = [0u8; 8];
            if self.file.read_exact(&mut len_bytes).is_err() {
                break;
            }
            let len = u64::from_ne_bytes(len_bytes);

            let current_pos = self.file.stream_position()?;
            if len > (file_len - current_pos) {
                eprintln!(
                    "WAL Recovery: Found corrupted entry length {}, stopping.",
                    len
                );
                break;
            }

            let mut buffer = vec![0u8; len as usize];
            if self.file.read_exact(&mut buffer).is_err() {
                break;
            }

            match rkyv::from_bytes::<WalEntry, rkyv::rancor::Error>(&buffer) {
                Ok(entry) => all_entries.push(entry),
                Err(_) => {
                    eprintln!("WAL Recovery: Failed to deserialize entry, stopping.");
                    break;
                }
            };
        }

        self.file.seek(SeekFrom::End(0))?;
        Ok(all_entries)
    }

    /// Checkpoints the WAL by rewriting it.
    /// Drops any DataBatch with a transaction ID <= `safe_tx_id`.
    /// Retains all SchemaMutations and newer DataBatches.
    pub fn checkpoint(&mut self, safe_tx_id: u64) -> Result<()> {
        let entries = self.recover()?;
        let mut retained = Vec::new();

        for entry in entries {
            match &entry {
                WalEntry::DataBatch(datoms) => {
                    // Only keep batches that contain datoms newer than our safe checkpoint
                    let max_t = datoms.iter().map(|d| d.t).max().unwrap_or(0);
                    if max_t > safe_tx_id {
                        retained.push(entry);
                    }
                }
                WalEntry::SchemaMutation(_) => {
                    // Schema mutations define our timeline and must be preserved
                    // until we build a dedicated Parquet schema catalog.
                    retained.push(entry);
                }
            }
        }

        // Write to a temporary file first to guarantee crash safety during the checkpoint
        let tmp_path = self.path.with_extension("tmp");
        let mut tmp_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true) // Ensure we start fresh
            .open(&tmp_path)?;

        for entry in &retained {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(entry)
                .map_err(|e| MesoError::Serialization(e.to_string()))?;
            let len = bytes.len() as u64;

            tmp_file.write_all(&len.to_ne_bytes())?;
            tmp_file.write_all(&bytes)?;
        }
        tmp_file.sync_all()?;

        // Atomic file swap
        std::fs::rename(&tmp_path, &self.path)?;

        // Re-open the main file handle and seek to the end for future appends
        self.file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Satisfy Clippy: explicitly declare we are not truncating
            .open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    pub fn data_dir(&self) -> PathBuf {
        self.path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_append_and_recover() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp_file.path()).unwrap();

        let batch1 = vec![Datom::assert(1, 10, Value::Int64(100), 1, 1000)];
        let batch2 = vec![Datom::assert(2, 10, Value::Int64(200), 2, 2000)];

        // Wrap the raw datoms in the new WalEntry enum
        wal.append_entry(&WalEntry::DataBatch(batch1), &WalSyncMode::Strict)
            .unwrap();
        wal.append_entry(&WalEntry::DataBatch(batch2), &WalSyncMode::Strict)
            .unwrap();

        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().unwrap();

        // We expect two ENVELOPES back
        assert_eq!(recovered.len(), 2);

        // Extract the datoms from Envelope 1
        if let WalEntry::DataBatch(datoms) = &recovered[0] {
            assert_eq!(datoms[0].e, 1);
        } else {
            panic!("Expected Envelope 1 to be a DataBatch");
        }

        // Extract the datoms from Envelope 2
        if let WalEntry::DataBatch(datoms) = &recovered[1] {
            assert_eq!(datoms[0].e, 2);
        } else {
            panic!("Expected Envelope 2 to be a DataBatch");
        }
    }

    #[test]
    fn test_wal_corruption_resistance() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp_file.path()).unwrap();

        let valid_batch = vec![Datom::assert(1, 10, Value::Int64(100), 1, 1000)];

        // Wrap the raw datoms
        wal.append_entry(&WalEntry::DataBatch(valid_batch), &WalSyncMode::Strict)
            .unwrap();

        // Simulate disk corruption by writing garbage
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(temp_file.path())
            .unwrap();
        file.write_all(&9u32.to_le_bytes()).unwrap();
        file.write_all(b"BAD_BYTES").unwrap();

        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().unwrap();

        // Only the valid envelope survives
        assert_eq!(recovered.len(), 1);
    }

    #[test]
    fn test_wal_checkpoint_pruning() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp_file.path()).unwrap();

        // 1. DataBatch (Tx 1) - Should be pruned
        wal.append_entry(
            &WalEntry::DataBatch(vec![Datom::assert(1, 10, Value::Int64(100), 1, 1000)]),
            &WalSyncMode::Strict,
        )
        .unwrap();

        // 2. SchemaMutation (Tx 2) - MUST be retained
        let schema_mutation = crate::schema::SchemaMutation::AddAttribute {
            tx_id: 2,
            timestamp: 1500,
            attribute: crate::schema::Attribute {
                id: 11,
                ident: ":test/new".into(),
                value_type: crate::schema::ValueType::String,
                is_unique: false,
            },
        };
        wal.append_entry(
            &WalEntry::SchemaMutation(schema_mutation),
            &WalSyncMode::Strict,
        )
        .unwrap();

        // 3. DataBatch (Tx 3) - MUST be retained (newer than safe_tx_id)
        wal.append_entry(
            &WalEntry::DataBatch(vec![Datom::assert(
                2,
                11,
                Value::String("keep".into()),
                3,
                2000,
            )]),
            &WalSyncMode::Strict,
        )
        .unwrap();

        // EXECUTE CHECKPOINT: Declare everything up to Tx 2 is safely in Parquet.
        wal.checkpoint(2).unwrap();

        // RECOVER AND VERIFY
        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().unwrap();

        // We expect exactly 2 envelopes: The Schema (Tx 2) and the Data (Tx 3)
        assert_eq!(
            recovered.len(),
            2,
            "WAL should have pruned exactly 1 DataBatch"
        );

        match &recovered[0] {
            WalEntry::SchemaMutation(m) => match m {
                crate::schema::SchemaMutation::AddAttribute { tx_id, .. } => assert_eq!(*tx_id, 2),
            },
            _ => panic!("Expected first retained entry to be a SchemaMutation"),
        }

        match &recovered[1] {
            WalEntry::DataBatch(datoms) => assert_eq!(datoms[0].t, 3),
            _ => panic!("Expected second retained entry to be the newer DataBatch"),
        }
    }
}
