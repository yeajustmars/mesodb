use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;

use crate::types::Result;

pub struct BackgroundCompactor {
    data_dir: PathBuf,
}

impl BackgroundCompactor {
    pub fn new(data_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");
        Self { data_dir }
    }

    /// Writes a RecordBatch to a unique Parquet file.
    pub fn flush_to_parquet(&self, batch: RecordBatch, batch_id: u64) -> Result<PathBuf> {
        let file_path = self.data_dir.join(format!("part-{:012}.parquet", batch_id));
        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(file_path)
    }

    /// Merges multiple parquet files into a single optimized file.
    pub async fn compact(&self, file_paths: &[PathBuf], output_id: u64) -> Result<PathBuf> {
        let ctx = SessionContext::new();
        let output_path = self
            .data_dir
            .join(format!("compacted-{:012}.parquet", output_id));

        let paths: Vec<String> = file_paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        let df = ctx.read_parquet(paths, Default::default()).await?;

        // 1. Configure the write options
        // .with_single_file_output(true) ensures we don't get a folder of part files
        let write_options = DataFrameWriteOptions::default().with_single_file_output(true);

        // 2. Execute the write
        // The API is now: path, options, and optional TableParquetOptions
        df.write_parquet(output_path.to_str().unwrap(), write_options, None)
            .await?;

        // 3. Cleanup old files
        for path in file_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(output_path)
    }
}
