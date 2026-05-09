use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::datom::Datom;
use crate::error::{MesoError, Result};

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
            // WARNING: Do not listen to Clippy here. We intentionally do NOT want to truncate or
            // append. Again, Do NOT add truncate(true) or append(true) here!
            // read + write + manual seek is the way to handle WALs
            // that need to both recover (read) and persist (write).
            .open(&p)?;

        Ok(Self { file, path: p })
    }

    pub fn append_batch(&mut self, datoms: &Vec<Datom>) -> Result<()> {
        if datoms.is_empty() {
            return Ok(());
        }

        // 1. Position at the very end to ensure we aren't overwriting
        self.file.seek(SeekFrom::End(0))?;

        // 2. Serialize the batch
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(datoms)
            .map_err(|e| MesoError::Serialization(e.to_string()))?;

        // 3. Safety Check: Ensure we aren't writing something suspiciously large
        // (Though unlikely during write, it catches logic errors early)
        let len = bytes.len() as u64;
        if len == 0 {
            return Err(MesoError::Serialization(
                "Serialized batch was empty".into(),
            ));
        }

        // 4. Atomic-style write: Length header followed by data
        // We use Native Endian (ne) to match the recovery logic
        self.file.write_all(&len.to_ne_bytes())?;
        self.file.write_all(&bytes)?;

        // 5. Force the OS to flush buffers to physical disk
        self.file.sync_all()?;

        Ok(())
    }

    pub fn recover(&mut self) -> Result<Vec<Datom>> {
        use std::io::{Read, Seek, SeekFrom};
        self.file.seek(SeekFrom::Start(0))?;

        let mut all_datoms = Vec::new();
        let file_len = self.file.metadata()?.len();

        while self.file.stream_position()? < file_len {
            let mut len_bytes = [0u8; 8];
            // If we can't read 8 bytes, we've hit the end or a partial write
            if self.file.read_exact(&mut len_bytes).is_err() {
                break;
            }
            let len = u64::from_ne_bytes(len_bytes);

            // --- PRO-LEVEL SAFETY CHECK ---
            // If the encoded length is greater than the remaining file size,
            // it's corrupted or we're reading junk. Stop here instead of crashing.
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

            // Try to deserialize. If junk, break the loop.
            let batch: Vec<Datom> =
                match rkyv::from_bytes::<Vec<Datom>, rkyv::rancor::Error>(&buffer) {
                    Ok(b) => b,
                    Err(_) => {
                        eprintln!("WAL Recovery: Failed to deserialize batch, stopping.");
                        break;
                    }
                };
            all_datoms.extend(batch);
        }

        self.file.seek(SeekFrom::End(0))?;
        Ok(all_datoms)
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
        let mut wal = Wal::open(temp_file.path()).expect("Failed to open WAL");

        let batch1 = vec![
            Datom::assert(1, 10, Value::Int64(100), 1, 1000),
            Datom::assert(1, 11, Value::String("Alice".to_string()), 1, 1000),
        ];

        let batch2 = vec![Datom::assert(2, 10, Value::Int64(200), 2, 2000)];

        wal.append_batch(&batch1).unwrap();
        wal.append_batch(&batch2).unwrap();

        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().expect("Failed to recover WAL");

        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[0].e, 1);
        assert_eq!(recovered[1].v, Value::String("Alice".to_string()));
        assert_eq!(recovered[2].e, 2);
    }

    #[test]
    fn test_wal_empty_batch() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp_file.path()).unwrap();

        // Appending an empty vector should instantly return Ok(()) without writing bytes
        assert!(wal.append_batch(&vec![]).is_ok());

        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_wal_corruption_resistance() {
        use std::fs::OpenOptions;
        use std::io::Write;

        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp_file.path()).unwrap();

        // 1. Write a valid batch
        let valid_batch = vec![Datom::assert(1, 10, Value::Int64(100), 1, 1000)];
        wal.append_batch(&valid_batch).unwrap();

        // 2. Simulate disk corruption by writing garbage directly to the file
        let mut file = OpenOptions::new()
            .append(true)
            .open(temp_file.path())
            .unwrap();
        // Write a fake length prefix (4 bytes) then some garbage
        file.write_all(&9u32.to_le_bytes()).unwrap();
        file.write_all(b"BAD_BYTES").unwrap();

        // 3. Attempt recovery
        let _recovering_wal = Wal::open(temp_file.path()).unwrap();
        let result = wal.recover();

        assert!(result.is_ok()); // Change from is_err()
        //
        let recovered = result.unwrap();
        assert_eq!(recovered.len(), 1); // Should only have the first valid datom
    }
}
