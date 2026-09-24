// mesodb-core/src/formatter.rs

use arrow::{array::*, datatypes::DataType, record_batch::RecordBatch};
use bytes::{Bytes, BytesMut};
use futures::stream::Stream;
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    ast::FindSpec,
    db::OutputFormat,
    types::{Result, Value},
};

pub fn to_json_string(batches: &[RecordBatch], finds: &[FindSpec]) -> String {
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

                out.push('"');
                out.push_str(schema.field(col).name());
                out.push_str("\":");

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

enum StreamState {
    Init,
    Active,
    Exhausted,
}

pub struct BatchChunkStream<S> {
    inner: S,
    format: OutputFormat,
    pull_cols: Vec<bool>,
    state: StreamState,
}

impl<S> BatchChunkStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin + Send + 'static,
{
    pub fn new(inner: S, finds: &[FindSpec], format: OutputFormat) -> Self {
        let pull_cols = finds
            .iter()
            .map(|f| matches!(f, FindSpec::Pull(_, _)))
            .collect();

        Self {
            inner,
            format,
            pull_cols,
            state: StreamState::Init,
        }
    }

    fn encode_chunk(&self, batch: &RecordBatch, is_first: bool) -> Bytes {
        let mut buf = BytesMut::with_capacity(batch.num_rows() * 128 + 16);

        if is_first {
            buf.extend_from_slice(b"[");
        }

        let schema = batch.schema();
        let mut first_row = is_first;

        for row in 0..batch.num_rows() {
            if !first_row {
                match self.format {
                    OutputFormat::Json => buf.extend_from_slice(b","),
                    OutputFormat::Edn => buf.extend_from_slice(b" "),
                    _ => buf.extend_from_slice(b","),
                }
            }
            first_row = false;

            buf.extend_from_slice(b"{");
            let mut first_col = true;

            for (col, (column, &is_pull)) in batch
                .columns()
                .iter()
                .zip(self.pull_cols.iter())
                .enumerate()
            {
                if column.is_null(row) {
                    continue;
                }

                if !first_col {
                    match self.format {
                        OutputFormat::Json => buf.extend_from_slice(b","),
                        OutputFormat::Edn => buf.extend_from_slice(b" "),
                        _ => buf.extend_from_slice(b","),
                    }
                }
                first_col = false;

                let field_name = schema.field(col).name();

                match self.format {
                    OutputFormat::Edn => {
                        buf.extend_from_slice(b":");
                        buf.extend_from_slice(field_name.as_bytes());
                        buf.extend_from_slice(b" ");
                    }
                    _ => {
                        buf.extend_from_slice(b"\"");
                        buf.extend_from_slice(field_name.as_bytes());
                        buf.extend_from_slice(b"\":");
                    }
                }

                if is_pull {
                    let str_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    buf.extend_from_slice(str_array.value(row).as_bytes());
                } else {
                    Self::encode_scalar(&mut buf, column, row);
                }
            }
            buf.extend_from_slice(b"}");
        }

        buf.freeze()
    }

    #[inline(always)]
    fn encode_scalar(buf: &mut BytesMut, column: &std::sync::Arc<dyn Array>, row: usize) {
        match column.data_type() {
            DataType::Boolean => {
                let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                buf.extend_from_slice(if arr.value(row) { b"true" } else { b"false" });
            }
            DataType::Int64 => {
                let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
                buf.extend_from_slice(arr.value(row).to_string().as_bytes());
            }
            DataType::UInt64 => {
                let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                buf.extend_from_slice(arr.value(row).to_string().as_bytes());
            }
            DataType::Float64 => {
                let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
                buf.extend_from_slice(arr.value(row).to_string().as_bytes());
            }
            DataType::Utf8 => {
                let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
                buf.extend_from_slice(b"\"");
                let escaped = arr.value(row).replace('"', "\\\"");
                buf.extend_from_slice(escaped.as_bytes());
                buf.extend_from_slice(b"\"");
            }
            DataType::Timestamp(_, _) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                buf.extend_from_slice(arr.value(row).to_string().as_bytes());
            }
            DataType::FixedSizeBinary(_) => {
                let arr = column
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                buf.extend_from_slice(b"\"");
                let u = arr.value(row);
                let uuid_str = format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    u[0],
                    u[1],
                    u[2],
                    u[3],
                    u[4],
                    u[5],
                    u[6],
                    u[7],
                    u[8],
                    u[9],
                    u[10],
                    u[11],
                    u[12],
                    u[13],
                    u[14],
                    u[15]
                );
                buf.extend_from_slice(uuid_str.as_bytes());
                buf.extend_from_slice(b"\"");
            }
            _ => buf.extend_from_slice(b"null"),
        }
    }
}

impl<S> Stream for BatchChunkStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin + Send + 'static,
{
    type Item = std::result::Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.state {
            StreamState::Init => {
                self.state = StreamState::Active;
                match Pin::new(&mut self.inner).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        let chunk = self.encode_chunk(&batch, true);
                        Poll::Ready(Some(Ok(chunk)))
                    }
                    Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(std::io::Error::other(e)))),
                    Poll::Ready(None) => {
                        self.state = StreamState::Exhausted;
                        Poll::Ready(Some(Ok(Bytes::from_static(b"[]"))))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            StreamState::Active => match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let chunk = self.encode_chunk(&batch, false);
                    Poll::Ready(Some(Ok(chunk)))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(std::io::Error::other(e)))),
                Poll::Ready(None) => {
                    self.state = StreamState::Exhausted;
                    Poll::Ready(Some(Ok(Bytes::from_static(b"]"))))
                }
                Poll::Pending => Poll::Pending,
            },
            StreamState::Exhausted => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use futures::stream::{self, StreamExt};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_batch_chunk_stream_json() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(Int64Array::from(vec![30, 40])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Charlie"])),
                Arc::new(Int64Array::from(vec![50])),
            ],
        )
        .unwrap();

        let mock_stream = stream::iter(vec![Ok(batch1), Ok(batch2)]);
        let finds = vec![
            FindSpec::Variable("?name".into()),
            FindSpec::Variable("?age".into()),
        ];

        let mut chunk_stream = BatchChunkStream::new(mock_stream, &finds, OutputFormat::Json);
        let mut out = Vec::new();

        while let Some(chunk) = chunk_stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }

        let json_str = String::from_utf8(out).unwrap();
        assert_eq!(
            json_str,
            r#"[{"name":"Alice","age":30},{"name":"Bob","age":40},{"name":"Charlie","age":50}]"#
        );
    }

    #[tokio::test]
    async fn test_batch_chunk_stream_edn() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice"])),
                Arc::new(Int64Array::from(vec![30])),
            ],
        )
        .unwrap();

        let mock_stream = stream::iter(vec![Ok(batch)]);
        let finds = vec![
            FindSpec::Variable("?name".into()),
            FindSpec::Variable("?age".into()),
        ];

        let mut chunk_stream = BatchChunkStream::new(mock_stream, &finds, OutputFormat::Edn);
        let mut out = Vec::new();

        while let Some(chunk) = chunk_stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }

        let edn_str = String::from_utf8(out).unwrap();
        assert_eq!(edn_str, r#"[{:name "Alice" :age 30}]"#);
    }

    #[tokio::test]
    async fn test_batch_chunk_stream_empty() {
        let mock_stream = stream::empty::<Result<RecordBatch>>();
        let finds = vec![];
        let mut chunk_stream = BatchChunkStream::new(mock_stream, &finds, OutputFormat::Json);
        let mut out = Vec::new();

        while let Some(chunk) = chunk_stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }

        let json_str = String::from_utf8(out).unwrap();
        assert_eq!(json_str, r#"[]"#);
    }
}
