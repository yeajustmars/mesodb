// mesodb-core/src/formatter.rs

use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use std::collections::HashMap;

use crate::{
    ast::FindSpec,
    types::{Result, Value},
};

/// Formats a series of RecordBatches directly into a JSON string with zero intermediate structs.
pub fn to_json_string(batches: &[RecordBatch], finds: &[FindSpec]) -> String {
    // Determine which columns are pre-formatted `pull` queries to prevent double-escaping
    let pull_cols: Vec<bool> = finds
        .iter()
        .map(|f| matches!(f, FindSpec::Pull(_, _)))
        .collect();

    // Pre-allocate buffer: Roughly estimate 128 bytes per row to minimize re-allocations
    let est_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut out = String::with_capacity(est_rows * 128 + 2);

    out.push('[');
    let mut first_row = true;

    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            if !first_row {
                out.push(',');
            }
            first_row = false;

            out.push('{');
            let mut first_col = true;

            for (col, (column, &is_pull)) in
                batch.columns().iter().zip(pull_cols.iter()).enumerate()
            {
                if column.is_null(row) {
                    continue;
                }

                if !first_col {
                    out.push(',');
                }
                first_col = false;

                // Write the JSON key
                out.push('"');
                out.push_str(schema.field(col).name());
                out.push_str("\":");

                // Write the JSON value directly from Arrow
                if is_pull {
                    // ZERO-COPY PULL: It is already formatted JSON, just blast it into the buffer!
                    let str_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    out.push_str(str_array.value(row));
                } else {
                    match column.data_type() {
                        DataType::Boolean => {
                            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                            out.push_str(if arr.value(row) { "true" } else { "false" });
                        }
                        DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _) => {
                            // Integers and Dates format directly
                            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                                out.push_str(&arr.value(row).to_string());
                            } else if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>()
                            {
                                out.push_str(&arr.value(row).to_string());
                            }
                        }
                        DataType::Float64 => {
                            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                            out.push_str(&arr.value(row).to_string());
                        }
                        DataType::Utf8 => {
                            // Standard strings MUST be escaped
                            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                            out.push('"');
                            // Simple escape for quotes to keep it fast
                            out.push_str(&arr.value(row).replace('"', "\\\""));
                            out.push('"');
                        }
                        _ => out.push_str("null"),
                    }
                }
            }
            out.push('}');
        }
    }
    out.push(']');
    out
}

/// Formats a series of RecordBatches directly into an EDN string.
pub fn to_edn_string(batches: &[RecordBatch], finds: &[FindSpec]) -> String {
    let pull_cols: Vec<bool> = finds
        .iter()
        .map(|f| matches!(f, FindSpec::Pull(_, _)))
        .collect();

    let est_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut out = String::with_capacity(est_rows * 128 + 2);

    out.push('[');
    let mut first_row = true;

    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            if !first_row {
                out.push(' ');
            }
            first_row = false;

            out.push('{');
            let mut first_col = true;

            for (col, (column, &is_pull)) in
                batch.columns().iter().zip(pull_cols.iter()).enumerate()
            {
                if column.is_null(row) {
                    continue;
                }

                if !first_col {
                    out.push(' ');
                }
                first_col = false;

                // Write EDN key (Everything gets a colon prefix in EDN)
                let field_name = schema.field(col).name();
                out.push(':');
                out.push_str(field_name);
                out.push(' ');

                if is_pull {
                    let str_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    out.push_str(str_array.value(row));
                } else {
                    match column.data_type() {
                        DataType::Boolean => {
                            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                            out.push_str(if arr.value(row) { "true" } else { "false" });
                        }
                        DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _) => {
                            if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
                                out.push_str(&arr.value(row).to_string());
                            } else if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>()
                            {
                                out.push_str(&arr.value(row).to_string());
                            }
                        }
                        DataType::Float64 => {
                            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                            out.push_str(&arr.value(row).to_string());
                        }
                        DataType::Utf8 => {
                            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                            out.push('"');
                            out.push_str(&arr.value(row).replace('"', "\\\""));
                            out.push('"');
                        }
                        _ => out.push_str("nil"),
                    }
                }
            }
            out.push('}');
        }
    }
    out.push(']');
    out
}

/// Formats directly into standard Rust types for embedded applications.
pub fn to_native(batches: &[RecordBatch]) -> Result<Vec<HashMap<String, Value>>> {
    let mut results = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut map = HashMap::new();

            for (col, column) in batch.columns().iter().enumerate() {
                if column.is_null(row) {
                    continue;
                }

                let key = schema.field(col).name().clone();
                let val = match column.data_type() {
                    DataType::Boolean => {
                        let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        Value::Boolean(arr.value(row))
                    }
                    DataType::Int64 => {
                        let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        Value::Int64(arr.value(row))
                    }
                    DataType::UInt64 => {
                        let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                        Value::Ref(arr.value(row))
                    }
                    DataType::Float64 => {
                        let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        Value::Float64(arr.value(row))
                    }
                    DataType::Utf8 => {
                        let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                        Value::String(arr.value(row).to_string())
                    }
                    DataType::Timestamp(_, _) => {
                        let arr = column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        Value::Timestamp(arr.value(row))
                    }
                    _ => continue,
                };
                map.insert(key, val);
            }
            results.push(map);
        }
    }
    Ok(results)
}
