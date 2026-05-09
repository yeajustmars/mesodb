// mesodb-core/src/memtable.rs
use crate::datom::Datom;
use crate::error::Result;
use crate::types::Value;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    e: UInt64Builder,
    a: UInt32Builder,
    v_bool: BooleanBuilder,
    v_int: Int64Builder,
    v_float: Float64Builder,
    v_str: StringBuilder,
    v_ref: UInt64Builder,
    v_time: TimestampMicrosecondBuilder,
    v_uuid: FixedSizeBinaryBuilder,
    t: UInt64Builder,
    op: BooleanBuilder,
    valid_from: TimestampMicrosecondBuilder,
    valid_to: TimestampMicrosecondBuilder,
    row_count: usize,
}

impl MemTable {
    pub fn new(capacity: usize) -> Self {
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

        Self {
            schema,
            e: UInt64Builder::with_capacity(capacity),
            a: UInt32Builder::with_capacity(capacity),
            v_bool: BooleanBuilder::with_capacity(capacity),
            v_int: Int64Builder::with_capacity(capacity),
            v_float: Float64Builder::with_capacity(capacity),
            v_str: StringBuilder::with_capacity(capacity, capacity * 16),
            v_ref: UInt64Builder::with_capacity(capacity),
            v_time: TimestampMicrosecondBuilder::with_capacity(capacity),
            v_uuid: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            t: UInt64Builder::with_capacity(capacity),
            op: BooleanBuilder::with_capacity(capacity),
            valid_from: TimestampMicrosecondBuilder::with_capacity(capacity),
            valid_to: TimestampMicrosecondBuilder::with_capacity(capacity),
            row_count: 0,
        }
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn append(&mut self, datom: Datom) {
        self.e.append_value(datom.e);
        self.a.append_value(datom.a);
        self.t.append_value(datom.t);
        self.op.append_value(datom.op);
        self.valid_from.append_value(datom.valid_from);
        self.valid_to.append_value(datom.valid_to);

        match datom.v {
            Value::Boolean(b) => {
                self.v_bool.append_value(b);
                self.null_all_except(0);
            }
            Value::Int64(i) => {
                self.v_int.append_value(i);
                self.null_all_except(1);
            }
            Value::Float64(f) => {
                self.v_float.append_value(f);
                self.null_all_except(2);
            }
            Value::String(s) => {
                self.v_str.append_value(s);
                self.null_all_except(3);
            }
            Value::Ref(r) => {
                self.v_ref.append_value(r);
                self.null_all_except(4);
            }
            Value::Timestamp(t) => {
                self.v_time.append_value(t);
                self.null_all_except(5);
            }
            Value::Uuid(u) => {
                self.v_uuid.append_value(u).unwrap();
                self.null_all_except(6);
            }
        }

        self.row_count += 1;
    }

    fn null_all_except(&mut self, idx: usize) {
        if idx != 0 {
            self.v_bool.append_null();
        }
        if idx != 1 {
            self.v_int.append_null();
        }
        if idx != 2 {
            self.v_float.append_null();
        }
        if idx != 3 {
            self.v_str.append_null();
        }
        if idx != 4 {
            self.v_ref.append_null();
        }
        if idx != 5 {
            self.v_time.append_null();
        }
        if idx != 6 {
            self.v_uuid.append_null();
        }
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let batch = RecordBatch::try_new(
            self.schema.clone(),
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
        )?;
        Ok(batch)
    }

    /// Returns the exact Arrow schema used by this MemTable's builders.
    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        let mut temp = Self::new(1);
        // This ensures the schema exactly matches what DataFusion sees in 'finish()'
        temp.finish().expect("Schema generation failed").schema()
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
        memtable.append(Datom::assert(
            1001,
            50,
            Value::String("Alice".to_string()),
            1,
            now,
        ));
        memtable.append(Datom::assert(1001, 51, Value::Ref(2002), 1, now));
        memtable.append(Datom::assert(1001, 52, Value::Int64(35), 2, now));

        let batch = memtable.finish().expect("Failed to build RecordBatch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 13);
    }

    #[test]
    fn test_memtable_alignment_all_types() {
        let mut memtable = MemTable::new(10);
        let now = Utc::now().timestamp_micros();
        memtable.append(Datom::assert(1, 1, Value::Boolean(true), 1, now));
        memtable.append(Datom::assert(1, 2, Value::Int64(42), 1, now));
        memtable.append(Datom::assert(1, 3, Value::Float64(4.14), 1, now));
        memtable.append(Datom::assert(1, 4, Value::String("test".into()), 1, now));
        memtable.append(Datom::assert(1, 5, Value::Ref(99), 1, now));
        memtable.append(Datom::assert(1, 6, Value::Timestamp(now), 1, now));
        memtable.append(Datom::assert(1, 7, Value::Uuid([2u8; 16]), 1, now));

        let batch = memtable.finish().expect("Failed to build batch");
        assert_eq!(batch.num_rows(), 7);
    }

    #[test]
    fn test_memtable_capacity_growth() {
        let mut memtable = MemTable::new(2);
        for i in 0..100 {
            memtable.append(Datom::assert(i, 10, Value::Int64(i as i64), 1, 0));
        }
        let batch = memtable.finish().unwrap();
        assert_eq!(batch.num_rows(), 100);
    }
}
