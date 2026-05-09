use crate::error::Result;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;

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
}
