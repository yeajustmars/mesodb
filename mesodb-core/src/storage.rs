// mesodb-core/src/storage.rs

use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{File, create_dir_all};
use std::path::PathBuf;

use crate::error::MesoError;
use crate::types::Result;

pub struct BackgroundCompactor {
    data_dir: PathBuf,
}

impl BackgroundCompactor {
    pub fn new(data_dir: PathBuf) -> Self {
        create_dir_all(&data_dir).expect("Failed to create data directory");
        Self { data_dir }
    }

    /// Writes a single RecordBatch to a unique Parquet file.
    pub fn flush_to_parquet(&self, batch: RecordBatch, tx_id: u64) -> Result<PathBuf> {
        // 1. Ensure the exact path and any parent folders are fully created
        let file_path = self.data_dir.join(format!("tx_{}.parquet", tx_id));
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 2. Now initialize the Parquet Writer safely...
        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        // Map parquet errors to our Serialization error if you haven't added a Parquet variant yet
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
            .map_err(|e| MesoError::Serialization(format!("Parquet error: {}", e)))?;

        writer
            .write(&batch)
            .map_err(|e| MesoError::Serialization(format!("Parquet write error: {}", e)))?;

        writer
            .close()
            .map_err(|e| MesoError::Serialization(format!("Parquet close error: {}", e)))?;

        Ok(file_path)
    }

    /// Merges multiple parquet files into a single optimized file using DataFusion.
    pub async fn compact(&self, file_paths: &[PathBuf], output_id: u64) -> Result<PathBuf> {
        let ctx = SessionContext::new();
        let output_path = self
            .data_dir
            .join(format!("compacted-{:012}.parquet", output_id));

        let paths = file_paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<String>>();

        let df = ctx.read_parquet(paths, Default::default()).await?;

        // Write out to a single file to prevent folder fragmentation
        let write_options = DataFrameWriteOptions::default().with_single_file_output(true);
        df.write_parquet(output_path.to_str().unwrap(), write_options, None)
            .await?;

        // Cleanup the old, fragmented files
        for path in file_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(output_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datom::Datom;
    use crate::memtable::MemTable;
    use crate::types::Value;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_flush_and_compact() {
        let dir = tempdir().unwrap();
        let compactor = BackgroundCompactor::new(dir.path().to_path_buf());

        // 1. Create two separate memory batches
        let mut mt1 = MemTable::new(10);
        mt1.append(Datom::assert(1, 10, Value::Int64(100), 1, 1000));
        let batch1 = mt1.finish().unwrap();

        let mut mt2 = MemTable::new(10);
        mt2.append(Datom::assert(2, 10, Value::Int64(200), 2, 2000));
        let batch2 = mt2.finish().unwrap();

        // 2. Flush them to disk independently
        let path1 = compactor.flush_to_parquet(batch1, 1).unwrap();
        let path2 = compactor.flush_to_parquet(batch2, 2).unwrap();

        assert!(path1.exists());
        assert!(path2.exists());

        // 3. Compact them into a single file
        let compacted_path = compactor
            .compact(&[path1.clone(), path2.clone()], 3)
            .await
            .unwrap();

        assert!(compacted_path.exists());
        assert!(!path1.exists(), "Original file 1 should be deleted");
        assert!(!path2.exists(), "Original file 2 should be deleted");

        // 4. Verify the compacted file has both rows
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "compacted",
            compacted_path.to_str().unwrap(),
            Default::default(),
        )
        .await
        .unwrap();

        let df = ctx.sql("SELECT * FROM compacted").await.unwrap();
        let results = df.collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
