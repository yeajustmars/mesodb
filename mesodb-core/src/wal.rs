use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::datom::Datom;
use crate::error::MesoError;
use crate::types::Result;

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

    pub fn append_batch(&mut self, datoms: &Vec<Datom>) -> Result<()> {
        if datoms.is_empty() {
            return Ok(());
        }

        self.file.seek(SeekFrom::End(0))?;

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(datoms)
            .map_err(|e| MesoError::Serialization(e.to_string()))?;

        let len = bytes.len() as u64;
        if len == 0 {
            return Err(MesoError::Serialization(
                "Serialized batch was empty".into(),
            ));
        }

        self.file.write_all(&len.to_ne_bytes())?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;

        Ok(())
    }

    pub fn recover(&mut self) -> Result<Vec<Datom>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut all_datoms = Vec::new();
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
        let mut wal = Wal::open(temp_file.path()).unwrap();

        let batch1 = vec![Datom::assert(1, 10, Value::Int64(100), 1, 1000)];
        let batch2 = vec![Datom::assert(2, 10, Value::Int64(200), 2, 2000)];

        wal.append_batch(&batch1).unwrap();
        wal.append_batch(&batch2).unwrap();

        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().unwrap();

        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].e, 1);
        assert_eq!(recovered[1].e, 2);
    }

    #[test]
    fn test_wal_corruption_resistance() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut wal = Wal::open(temp_file.path()).unwrap();

        let valid_batch = vec![Datom::assert(1, 10, Value::Int64(100), 1, 1000)];
        wal.append_batch(&valid_batch).unwrap();

        // Simulate disk corruption by writing garbage
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(temp_file.path())
            .unwrap();
        file.write_all(&9u32.to_le_bytes()).unwrap();
        file.write_all(b"BAD_BYTES").unwrap();

        let mut recovering_wal = Wal::open(temp_file.path()).unwrap();
        let recovered = recovering_wal.recover().unwrap();

        assert_eq!(recovered.len(), 1); // Only the valid datom survives
    }
}
