use std::sync::Arc;

use arrow::array::{
    BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::datom::Datom;
use crate::types::Value;

/// The in-memory Arrow builder for MesoDB's bitemporal data.
/// It converts row-based `Datom` structs into a strictly typed,
/// columnar `RecordBatch` optimized for zero-copy Parquet flushes.
pub struct MemTable {
    // Core Datom Identity
    e: UInt64Builder,
    a: UInt32Builder,

    // The "Wide Column" Value fields (Sparse arrays)
    v_bool: BooleanBuilder,
    v_int: Int64Builder,
    v_float: Float64Builder,
    v_str: StringBuilder,
    v_ref: UInt64Builder,
    v_time: TimestampMicrosecondBuilder,
    v_uuid: FixedSizeBinaryBuilder,

    // Bitemporal and System metadata
    t: UInt64Builder,
    op: BooleanBuilder,
    valid_from: TimestampMicrosecondBuilder,
    valid_to: TimestampMicrosecondBuilder,
}

impl MemTable {
    /// Initializes a new MemTable with pre-allocated memory capacity.
    /// Pre-allocating prevents expensive memory re-allocations during fast ingestion.
    pub fn new(capacity: usize) -> Self {
        Self {
            e: UInt64Builder::with_capacity(capacity),
            a: UInt32Builder::with_capacity(capacity),

            v_bool: BooleanBuilder::with_capacity(capacity),
            v_int: Int64Builder::with_capacity(capacity),
            v_float: Float64Builder::with_capacity(capacity),
            v_str: StringBuilder::with_capacity(capacity, capacity * 16), // Guessing 16 bytes per string
            v_ref: UInt64Builder::with_capacity(capacity),
            v_time: TimestampMicrosecondBuilder::with_capacity(capacity),
            v_uuid: FixedSizeBinaryBuilder::with_capacity(capacity, 16), // UUID is strictly 16 bytes

            t: UInt64Builder::with_capacity(capacity),
            op: BooleanBuilder::with_capacity(capacity),
            valid_from: TimestampMicrosecondBuilder::with_capacity(capacity),
            valid_to: TimestampMicrosecondBuilder::with_capacity(capacity),
        }
    }

    /// Appends a single Datom to the columnar arrays.
    pub fn append(&mut self, datom: Datom) {
        // 1. Append the dense, guaranteed columns
        self.e.append_value(datom.e);
        self.a.append_value(datom.a);
        self.t.append_value(datom.t);
        self.op.append_value(datom.op);
        self.valid_from.append_value(datom.valid_from);
        self.valid_to.append_value(datom.valid_to);

        // 2. Append the sparse Value columns
        // We match the enum and insert a null into every other type column.
        match datom.v {
            Value::Boolean(b) => self.v_bool.append_value(b),
            _ => self.v_bool.append_null(),
        }

        match datom.v {
            Value::Int64(i) => self.v_int.append_value(i),
            _ => self.v_int.append_null(),
        }

        match datom.v {
            Value::Float64(f) => self.v_float.append_value(f),
            _ => self.v_float.append_null(),
        }

        match &datom.v {
            Value::String(s) => self.v_str.append_value(s),
            _ => self.v_str.append_null(),
        }

        match datom.v {
            Value::Ref(r) => self.v_ref.append_value(r),
            _ => self.v_ref.append_null(),
        }

        match datom.v {
            Value::Timestamp(t) => self.v_time.append_value(t),
            _ => self.v_time.append_null(),
        }

        match &datom.v {
            // Unwrap the Result because we guarantee `u` is exactly [u8; 16]
            Value::Uuid(u) => self.v_uuid.append_value(u).expect("UUID length mismatch"),
            _ => self.v_uuid.append_null(),
        }
    }

    /// Freezes the MemTable and converts it into a zero-copy Apache Arrow RecordBatch.
    /// This RecordBatch can be instantly queried by DataFusion or flushed to Parquet.
    pub fn finish(&mut self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("e", DataType::UInt64, false),
            Field::new("a", DataType::UInt32, false),
            Field::new("v_bool", DataType::Boolean, true),
            Field::new("v_int", DataType::Int64, true),
            Field::new("v_float", DataType::Float64, true),
            Field::new("v_str", DataType::Utf8, true),
            Field::new("v_ref", DataType::UInt64, true),
            Field::new(
                "v_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("v_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("t", DataType::UInt64, false),
            Field::new("op", DataType::Boolean, false),
            Field::new(
                "valid_from",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "valid_to",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.e.finish()),
                Arc::new(self.a.finish()),
                Arc::new(self.v_bool.finish()),
                Arc::new(self.v_int.finish()),
                Arc::new(self.v_float.finish()),
                Arc::new(self.v_str.finish()),
                Arc::new(self.v_ref.finish()),
                Arc::new(self.v_time.finish()),
                Arc::new(self.v_uuid.finish()),
                Arc::new(self.t.finish()),
                Arc::new(self.op.finish()),
                Arc::new(self.valid_from.finish()),
                Arc::new(self.valid_to.finish()),
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_memtable_append_and_finish() {
        let mut memtable = MemTable::new(100);
        let now = Utc::now().timestamp_micros();

        // 1. Insert a String Datom
        let str_datom = Datom::assert(1001, 50, Value::String("Alice".to_string()), 1, now);
        memtable.append(str_datom);

        // 2. Insert a Ref (EntityId) Datom
        let ref_datom = Datom::assert(
            1001,
            51, // e.g., :user/best_friend
            Value::Ref(2002),
            1,
            now,
        );
        memtable.append(ref_datom);

        // 3. Insert an Integer Datom
        let int_datom = Datom::assert(
            1001,
            52, // e.g., :user/age
            Value::Int64(35),
            2,
            now,
        );
        memtable.append(int_datom);

        // Build the RecordBatch
        let batch = memtable.finish().expect("Failed to build RecordBatch");

        // Assertions
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 13);

        // Verify the schema maps perfectly
        assert_eq!(
            batch.schema().field_with_name("v_str").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            batch.schema().field_with_name("v_ref").unwrap().data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("v_uuid")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeBinary(16)
        );
    }
}
