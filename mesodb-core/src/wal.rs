use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

use crate::datom::Datom;
use rkyv::{from_bytes, rancor::Error, to_bytes};

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        Ok(Self { file })
    }

    // Notice we changed `&[Datom]` to `&Vec<Datom>` to provide a Sized type
    pub fn append_batch(&mut self, batch: &Vec<Datom>) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let bytes = to_bytes::<Error>(batch).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL serialization failed: {}", e),
            )
        })?;

        let len = bytes.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;

        Ok(())
    }

    pub fn recover(&mut self) -> io::Result<Vec<Datom>> {
        use std::io::{Seek, SeekFrom};
        self.file.seek(SeekFrom::Start(0))?;

        let mut recovered = Vec::new();
        let mut length_buf = [0u8; 4];

        loop {
            match self.file.read_exact(&mut length_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let len = u32::from_le_bytes(length_buf) as usize;
            let mut payload = vec![0u8; len];
            self.file.read_exact(&mut payload)?;

            let batch: Vec<Datom> = from_bytes::<Vec<Datom>, Error>(&payload).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("WAL corruption detected: {}", e),
                )
            })?;

            recovered.extend(batch);
        }

        Ok(recovered)
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
}
