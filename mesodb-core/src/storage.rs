use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;

use crate::error::MesoError;
use crate::types::Result;

pub struct BackgroundCompactor {
    data_dir: PathBuf,
}

impl BackgroundCompactor {
    pub fn new(data_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&data_dir).expect("Failed to create base data directory");
        std::fs::create_dir_all(data_dir.join("parquet"))
            .expect("Failed to create parquet subdirectory");

        Self { data_dir }
    }

    pub fn flush_to_parquet(&self, batch: RecordBatch, tx_id: u64) -> Result<PathBuf> {
        let file_path = self
            .data_dir
            .join("parquet")
            .join(format!("part-{:012}.parquet", tx_id));
        let file = File::create(&file_path)?;

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build();

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

    /// OPTION A BITEMPORAL COMPACTION
    /// Merges multiple parquet files, mathematically resolves the bitemporal valid_to bounds,
    /// and strips the op=false rows to maximize Point-in-Time read throughput.
    pub async fn compact(&self, file_paths: &[PathBuf], output_id: u64) -> Result<PathBuf> {
        let ctx = SessionContext::new();
        let output_path = self
            .data_dir
            .join(format!("compacted-{:012}.parquet", output_id));

        let paths = file_paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<String>>();

        // 1. Read all the fragmented files into a single DataFrame
        let df = ctx.read_parquet(paths, Default::default()).await?;
        ctx.register_table("raw_fragments", df.into_view())?;

        // 2. The Iceberg Resolution (Option A)
        let sql = r#"
            WITH bounds AS (
                SELECT *,
                       LEAD(valid_from) OVER (PARTITION BY e, a ORDER BY valid_from ASC, t ASC, op ASC) as next_from
                FROM raw_fragments
            )
            SELECT
                e, a, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid, t, op, valid_from,
                COALESCE(next_from, valid_to) as valid_to
            FROM bounds
            WHERE op = true
        "#;

        // Execute the transformation
        let compacted_df = ctx.sql(sql).await.map_err(|e| MesoError::DataFusion(e))?;

        // 3. Write the mathematically pure result out to a single file
        let write_options = DataFrameWriteOptions::default().with_single_file_output(true);
        compacted_df
            .write_parquet(output_path.to_str().unwrap(), write_options, None)
            .await?;

        // 4. Cleanup the old, fragmented files
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
        let path1 = compactor.flush_to_parquet(batch1.clone(), 1).unwrap();
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

        // 4. Verify the Compaction Results
        let ctx = SessionContext::new();

        // Explicitly load the schema from the MemTable batch to prevent Arrow downcast panics
        let schema = batch1.schema();
        let pq_options = datafusion::prelude::ParquetReadOptions::default().schema(schema.as_ref());

        ctx.register_parquet("compacted", compacted_path.to_str().unwrap(), pq_options)
            .await
            .unwrap();

        let df = ctx.sql("SELECT * FROM compacted").await.unwrap();
        let results = df.collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_option_a_bitemporal_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let compactor = BackgroundCompactor::new(dir.path().to_path_buf());

        // 1. T=100: Assert Alice
        let mut mt1 = MemTable::new(10);
        mt1.append(Datom::assert(1, 10, Value::String("Alice".into()), 1, 100));
        let batch1 = mt1.finish().unwrap();

        // 2. T=200: Overwrite with Alice-Revised (Emits a Retract + Assert)
        let mut mt2 = MemTable::new(10);
        mt2.append(Datom::retract(1, 10, Value::String("Alice".into()), 2, 200));
        mt2.append(Datom::assert(
            1,
            10,
            Value::String("Alice-Revised".into()),
            2,
            200,
        ));
        let batch2 = mt2.finish().unwrap();

        let path1 = compactor.flush_to_parquet(batch1, 1).unwrap();
        let path2 = compactor.flush_to_parquet(batch2, 2).unwrap();

        // 3. Execute the Option A Compaction
        let compacted_path = compactor.compact(&[path1, path2], 3).await.unwrap();

        // 4. Verify the Compaction Results
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "compacted",
            compacted_path.to_str().unwrap(),
            Default::default(),
        )
        .await
        .unwrap();

        let df = ctx
            .sql("SELECT valid_from, valid_to, v_str FROM compacted ORDER BY valid_from ASC")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();

        let batch = &results[0];

        // Use arrow compute casts to force DataFusion's optimized physical types back to our expected Rust types
        let from_cast =
            arrow::compute::cast(batch.column(0), &arrow::datatypes::DataType::Int64).unwrap();
        let to_cast =
            arrow::compute::cast(batch.column(1), &arrow::datatypes::DataType::Int64).unwrap();
        let val_cast =
            arrow::compute::cast(batch.column(2), &arrow::datatypes::DataType::Utf8).unwrap();

        let from_col = from_cast
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let to_col = to_cast
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let val_col = val_cast
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        // Under Option A, the Retraction row MUST be stripped, leaving exactly 2 perfect intervals
        assert_eq!(batch.num_rows(), 2);

        // Interval 1: Alice (Active from 100 -> 200)
        assert_eq!(val_col.value(0), "Alice");
        assert_eq!(from_col.value(0), 100);
        assert_eq!(to_col.value(0), 200);

        // Interval 2: Alice-Revised (Active from 200 -> End of Time)
        assert_eq!(val_col.value(1), "Alice-Revised");
        assert_eq!(from_col.value(1), 200);
        assert_eq!(to_col.value(1), i64::MAX);
    }
}
