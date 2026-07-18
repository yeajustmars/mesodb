// mesodb-core/src/wire.rs

use arrow::{
    array::*,
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;

use crate::{
    error::MesoError,
    transactor::Fact,
    types::{Result, Value},
};

/// The WireBatchBuilder acts as the columnar packer for outbound network payloads.
pub struct WireBatchBuilder {
    schema: SchemaRef,
    e: UInt64Builder,
    ident: StringBuilder,
    op: BooleanBuilder,
    valid_time: TimestampMicrosecondBuilder,

    // Primary Value Columns
    v_bool: BooleanBuilder,
    v_int: Int64Builder,
    v_float: Float64Builder,
    v_str: StringBuilder,
    v_ref: UInt64Builder,
    v_time: TimestampMicrosecondBuilder,
    v_uuid: FixedSizeBinaryBuilder,

    // Compare-And-Swap (CAS) Columns
    cas_bool: BooleanBuilder,
    cas_int: Int64Builder,
    cas_float: Float64Builder,
    cas_str: StringBuilder,
    cas_ref: UInt64Builder,
    cas_time: TimestampMicrosecondBuilder,
    cas_uuid: FixedSizeBinaryBuilder,

    row_count: usize,
}

impl WireBatchBuilder {
    pub fn new(capacity: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("e", DataType::UInt64, false),
            Field::new("ident", DataType::Utf8, false),
            Field::new("op", DataType::Boolean, false),
            Field::new(
                "valid_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
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
            Field::new("cas_bool", DataType::Boolean, true),
            Field::new("cas_int", DataType::Int64, true),
            Field::new("cas_float", DataType::Float64, true),
            Field::new("cas_str", DataType::Utf8, true),
            Field::new("cas_ref", DataType::UInt64, true),
            Field::new(
                "cas_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("cas_uuid", DataType::FixedSizeBinary(16), true),
        ]));

        Self {
            schema,
            e: UInt64Builder::with_capacity(capacity),
            ident: StringBuilder::with_capacity(capacity, capacity * 16),
            op: BooleanBuilder::with_capacity(capacity),
            valid_time: TimestampMicrosecondBuilder::with_capacity(capacity),
            v_bool: BooleanBuilder::with_capacity(capacity),
            v_int: Int64Builder::with_capacity(capacity),
            v_float: Float64Builder::with_capacity(capacity),
            v_str: StringBuilder::with_capacity(capacity, capacity * 16),
            v_ref: UInt64Builder::with_capacity(capacity),
            v_time: TimestampMicrosecondBuilder::with_capacity(capacity),
            v_uuid: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            cas_bool: BooleanBuilder::with_capacity(capacity),
            cas_int: Int64Builder::with_capacity(capacity),
            cas_float: Float64Builder::with_capacity(capacity),
            cas_str: StringBuilder::with_capacity(capacity, capacity * 16),
            cas_ref: UInt64Builder::with_capacity(capacity),
            cas_time: TimestampMicrosecondBuilder::with_capacity(capacity),
            cas_uuid: FixedSizeBinaryBuilder::with_capacity(capacity, 16),
            row_count: 0,
        }
    }

    pub fn append(&mut self, fact: &Fact) -> Result<()> {
        self.e.append_value(fact.e);
        self.ident.append_value(&fact.ident);
        self.op.append_value(fact.op);

        if let Some(vt) = fact.valid_time {
            self.valid_time.append_value(vt);
        } else {
            self.valid_time.append_null();
        }

        Self::append_value_variant(
            &fact.v,
            &mut self.v_bool,
            &mut self.v_int,
            &mut self.v_float,
            &mut self.v_str,
            &mut self.v_ref,
            &mut self.v_time,
            &mut self.v_uuid,
        )?;

        if let Some(cas_v) = &fact.cas_old_v {
            Self::append_value_variant(
                cas_v,
                &mut self.cas_bool,
                &mut self.cas_int,
                &mut self.cas_float,
                &mut self.cas_str,
                &mut self.cas_ref,
                &mut self.cas_time,
                &mut self.cas_uuid,
            )?;
        } else {
            self.cas_bool.append_null();
            self.cas_int.append_null();
            self.cas_float.append_null();
            self.cas_str.append_null();
            self.cas_ref.append_null();
            self.cas_time.append_null();
            self.cas_uuid.append_null();
        }

        self.row_count += 1;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn append_value_variant(
        v: &Value,
        b_bool: &mut BooleanBuilder,
        b_int: &mut Int64Builder,
        b_float: &mut Float64Builder,
        b_str: &mut StringBuilder,
        b_ref: &mut UInt64Builder,
        b_time: &mut TimestampMicrosecondBuilder,
        b_uuid: &mut FixedSizeBinaryBuilder,
    ) -> Result<()> {
        // Append the active value and return its structural index
        let active_idx = match v {
            Value::Boolean(val) => {
                b_bool.append_value(*val);
                0
            }
            Value::Int64(val) => {
                b_int.append_value(*val);
                1
            }
            Value::Float64(val) => {
                b_float.append_value(*val);
                2
            }
            Value::String(val) => {
                b_str.append_value(val);
                3
            }
            Value::Ref(val) => {
                b_ref.append_value(*val);
                4
            }
            Value::Timestamp(val) => {
                b_time.append_value(*val);
                5
            }
            Value::Uuid(val) => {
                b_uuid.append_value(val).map_err(MesoError::Arrow)?;
                6
            }
        };

        // Fill the parallel columns with nulls to maintain strict row alignment
        if active_idx != 0 {
            b_bool.append_null();
        }
        if active_idx != 1 {
            b_int.append_null();
        }
        if active_idx != 2 {
            b_float.append_null();
        }
        if active_idx != 3 {
            b_str.append_null();
        }
        if active_idx != 4 {
            b_ref.append_null();
        }
        if active_idx != 5 {
            b_time.append_null();
        }
        if active_idx != 6 {
            b_uuid.append_null();
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.e.finish()),
                Arc::new(self.ident.finish()),
                Arc::new(self.op.finish()),
                Arc::new(self.valid_time.finish()),
                Arc::new(self.v_bool.finish()),
                Arc::new(self.v_int.finish()),
                Arc::new(self.v_float.finish()),
                Arc::new(self.v_str.finish()),
                Arc::new(self.v_ref.finish()),
                Arc::new(self.v_time.finish()),
                Arc::new(self.v_uuid.finish()),
                Arc::new(self.cas_bool.finish()),
                Arc::new(self.cas_int.finish()),
                Arc::new(self.cas_float.finish()),
                Arc::new(self.cas_str.finish()),
                Arc::new(self.cas_ref.finish()),
                Arc::new(self.cas_time.finish()),
                Arc::new(self.cas_uuid.finish()),
            ],
        )
        .map_err(MesoError::Arrow)
    }
}

/// Decodes an inbound Arrow RecordBatch from the wire back into discrete Fact structs for the Transactor.
pub fn parse_wire_batch(batch: &RecordBatch) -> Result<Vec<Fact>> {
    let num_rows = batch.num_rows();
    let mut facts = Vec::with_capacity(num_rows);

    let e_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let ident_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let op_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let valid_time_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    // Value Columns
    let v_bool = batch
        .column(4)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let v_int = batch
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let v_float = batch
        .column(6)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let v_str = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let v_ref = batch
        .column(8)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let v_time = batch
        .column(9)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let v_uuid = batch
        .column(10)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    // CAS Columns
    let c_bool = batch
        .column(11)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let c_int = batch
        .column(12)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c_float = batch
        .column(13)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let c_str = batch
        .column(14)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let c_ref = batch
        .column(15)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let c_time = batch
        .column(16)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let c_uuid = batch
        .column(17)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    for row in 0..num_rows {
        let v = extract_value(row, v_bool, v_int, v_float, v_str, v_ref, v_time, v_uuid)
            .ok_or_else(|| MesoError::Serialization("Wire batch missing primary value".into()))?;

        let cas_old_v = extract_value(row, c_bool, c_int, c_float, c_str, c_ref, c_time, c_uuid);

        let valid_time = if valid_time_col.is_null(row) {
            None
        } else {
            Some(valid_time_col.value(row))
        };

        facts.push(Fact {
            e: e_col.value(row),
            ident: ident_col.value(row).to_string(),
            v,
            op: op_col.value(row),
            cas_old_v,
            valid_time,
        });
    }

    Ok(facts)
}

#[allow(clippy::too_many_arguments)]
fn extract_value(
    row: usize,
    b_bool: &BooleanArray,
    b_int: &Int64Array,
    b_float: &Float64Array,
    b_str: &StringArray,
    b_ref: &UInt64Array,
    b_time: &TimestampMicrosecondArray,
    b_uuid: &FixedSizeBinaryArray,
) -> Option<Value> {
    if !b_bool.is_null(row) {
        Some(Value::Boolean(b_bool.value(row)))
    } else if !b_int.is_null(row) {
        Some(Value::Int64(b_int.value(row)))
    } else if !b_float.is_null(row) {
        Some(Value::Float64(b_float.value(row)))
    } else if !b_str.is_null(row) {
        Some(Value::String(b_str.value(row).to_string()))
    } else if !b_ref.is_null(row) {
        Some(Value::Ref(b_ref.value(row)))
    } else if !b_time.is_null(row) {
        Some(Value::Timestamp(b_time.value(row)))
    } else if !b_uuid.is_null(row) {
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(b_uuid.value(row));
        Some(Value::Uuid(uuid))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_batch_round_trip() {
        let mut builder = WireBatchBuilder::new(10);

        let fact_1 = Fact {
            e: 42,
            ident: ":user/name".into(),
            v: Value::String("Alice".into()),
            op: true,
            cas_old_v: Some(Value::String("Alicia".into())),
            valid_time: None,
        };

        let fact_2 = Fact {
            e: 99,
            ident: ":user/age".into(),
            v: Value::Int64(30),
            op: false,
            cas_old_v: None,
            valid_time: Some(1500000000),
        };

        builder.append(&fact_1).unwrap();
        builder.append(&fact_2).unwrap();

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let decoded_facts = parse_wire_batch(&batch).unwrap();
        assert_eq!(decoded_facts.len(), 2);

        // Verify Fact 1 Mapping
        assert_eq!(decoded_facts[0].e, 42);
        assert_eq!(decoded_facts[0].ident, ":user/name");
        assert_eq!(decoded_facts[0].v, Value::String("Alice".into()));
        assert!(decoded_facts[0].op);
        assert_eq!(
            decoded_facts[0].cas_old_v,
            Some(Value::String("Alicia".into()))
        );
        assert_eq!(decoded_facts[0].valid_time, None);

        // Verify Fact 2 Mapping
        assert_eq!(decoded_facts[1].e, 99);
        assert_eq!(decoded_facts[1].ident, ":user/age");
        assert_eq!(decoded_facts[1].v, Value::Int64(30));
        assert!(!decoded_facts[1].op);
        assert_eq!(decoded_facts[1].cas_old_v, None);
        assert_eq!(decoded_facts[1].valid_time, Some(1500000000));
    }

    #[test]
    fn test_wire_batch_all_value_variants() {
        let mut builder = WireBatchBuilder::new(10);
        let uuid_bytes = [8u8; 16];

        let facts = vec![
            Fact {
                e: 1,
                ident: ":type/bool".into(),
                v: Value::Boolean(true),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":type/int".into(),
                v: Value::Int64(-42),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 3,
                ident: ":type/float".into(),
                v: Value::Float64(9.14159),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 4,
                ident: ":type/str".into(),
                v: Value::String("Data".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 5,
                ident: ":type/ref".into(),
                v: Value::Ref(999),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 6,
                ident: ":type/inst".into(),
                v: Value::Timestamp(1700000000),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
            Fact {
                e: 7,
                ident: ":type/uuid".into(),
                v: Value::Uuid(uuid_bytes),
                op: true,
                cas_old_v: None,
                valid_time: None,
            },
        ];

        for fact in &facts {
            builder.append(fact).unwrap();
        }

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), 7);

        let decoded = parse_wire_batch(&batch).unwrap();
        assert_eq!(decoded.len(), 7);

        // Verify exact round-trip fidelity for every single native Value type
        for (i, original) in facts.iter().enumerate() {
            assert_eq!(decoded[i].e, original.e);
            assert_eq!(decoded[i].ident, original.ident);
            assert_eq!(
                decoded[i].v, original.v,
                "Mismatch on value variant index {}",
                i
            );
        }
    }

    #[test]
    fn test_wire_batch_empty_payload() {
        // Guarantee that creating a batch with 0 facts doesn't panic on finish() or parse()
        let mut builder = WireBatchBuilder::new(10);

        let batch = builder.finish().unwrap();
        assert_eq!(
            batch.num_rows(),
            0,
            "Empty builder must yield a 0-row RecordBatch"
        );

        let decoded = parse_wire_batch(&batch).unwrap();
        assert!(
            decoded.is_empty(),
            "Parsing a 0-row batch must yield an empty Vec<Fact>"
        );
    }

    #[test]
    fn test_wire_batch_mixed_cas_alignment() {
        // This is the crucible for our manual `null_all_except` logic.
        // If we fail to append nulls to inactive columns, the Arrow arrays will desynchronize,
        // causing panics or shifting data to the wrong row.
        let mut builder = WireBatchBuilder::new(10);

        let facts = vec![
            Fact {
                e: 1,
                ident: ":test/a".into(),
                v: Value::Int64(1),
                op: true,
                cas_old_v: Some(Value::Int64(0)),
                valid_time: None,
            },
            Fact {
                e: 2,
                ident: ":test/b".into(),
                v: Value::String("B".into()),
                op: true,
                cas_old_v: None,
                valid_time: None,
            }, // CAS is entirely absent
            Fact {
                e: 3,
                ident: ":test/c".into(),
                v: Value::Ref(100),
                op: true,
                cas_old_v: Some(Value::Ref(99)),
                valid_time: None,
            },
        ];

        for fact in &facts {
            builder.append(fact).unwrap();
        }

        let batch = builder.finish().unwrap();
        let decoded = parse_wire_batch(&batch).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].cas_old_v, Some(Value::Int64(0)));
        assert_eq!(
            decoded[1].cas_old_v, None,
            "Row 2 CAS must safely resolve to None without shifting Row 3's data"
        );
        assert_eq!(decoded[2].cas_old_v, Some(Value::Ref(99)));
    }

    #[test]
    fn test_wire_batch_auto_expansion() {
        // Force the Arrow builders to exceed their initial capacity allocation.
        // This ensures our logic plays nicely with Arrow's internal memory re-allocation.
        let initial_capacity = 2;
        let target_rows = 100;
        let mut builder = WireBatchBuilder::new(initial_capacity);

        for i in 0..target_rows {
            let fact = Fact {
                e: i as u64,
                ident: ":sys/scale".into(),
                v: Value::Int64(i as i64),
                op: true,
                cas_old_v: None,
                valid_time: None,
            };
            builder.append(&fact).unwrap();
        }

        let batch = builder.finish().unwrap();
        assert_eq!(batch.num_rows(), target_rows as usize);

        let decoded = parse_wire_batch(&batch).unwrap();
        assert_eq!(decoded.len(), target_rows as usize);

        // Spot check the boundaries
        assert_eq!(decoded[0].e, 0);
        assert_eq!(decoded[99].e, 99);
        assert_eq!(decoded[99].v, Value::Int64(99));
    }
}
