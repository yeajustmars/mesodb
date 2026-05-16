// mesodb-core/src/pull.rs
use arrow::array::Array;
use arrow::array::StringArray;
use datafusion::prelude::*;
use serde_json::{Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};

use crate::ast::{PullAttribute, PullPattern};
use crate::db::OutputFormat;
use crate::error::MesoError;
use crate::schema::SchemaMap;

pub struct PullEngine<'a> {
    ctx: &'a SessionContext,
    schema: &'a SchemaMap,
    format: &'a OutputFormat,
    as_of: Option<i64>,
}

impl<'a> PullEngine<'a> {
    pub fn new(
        ctx: &'a SessionContext,
        schema: &'a SchemaMap,
        format: &'a OutputFormat,
        as_of: Option<i64>,
    ) -> Self {
        Self {
            ctx,
            schema,
            format,
            as_of,
        }
    }

    pub async fn execute_pull(
        &self,
        entity_ids: &[u64],
        pattern: &PullPattern,
    ) -> Result<StringArray, MesoError> {
        let mut results = Vec::with_capacity(entity_ids.len());

        for &e in entity_ids {
            let doc = self.pull_entity(e, pattern).await?;

            let serialized = match self.format {
                OutputFormat::Json => {
                    // For pure JSON, we strip the leading ':' from Datomic keywords
                    let json_doc = Self::strip_colons(&JsonValue::Object(doc));
                    serde_json::to_string(&json_doc).unwrap()
                }
                OutputFormat::Edn => Self::to_edn(&JsonValue::Object(doc)),
                _ => unreachable!(),
            };
            results.push(serialized);
        }

        Ok(StringArray::from(results))
    }

    // The Recursive Graph Walker
    fn pull_entity<'b>(
        &'b self,
        e: u64,
        pattern: &'b PullPattern,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Map<String, JsonValue>, MesoError>> + 'b>,
    > {
        Box::pin(async move {
            let time_clause = match self.as_of {
                Some(t) => {
                    format!("CAST(valid_from AS BIGINT) <= {t} AND CAST(valid_to AS BIGINT) > {t}")
                }
                None => "next_from IS NULL".to_string(),
            };

            let sql = format!(
                "SELECT a, v_bool, v_int, v_float, v_str, v_ref FROM resolved_datoms WHERE e = {} AND {}",
                e, time_clause
            );

            let df = self.ctx.sql(&sql).await.map_err(MesoError::DataFusion)?;
            let batches = df.collect().await.map_err(MesoError::DataFusion)?;

            let mut doc = Map::new();
            let mut requested_wildcard = false;
            let mut requested_simples = HashSet::new();
            let mut requested_maps = HashMap::new();

            for attr in &pattern.0 {
                match attr {
                    PullAttribute::Wildcard => requested_wildcard = true,
                    PullAttribute::Simple(ident) => {
                        requested_simples.insert(ident.clone());
                    }
                    PullAttribute::Map(ident, sub) => {
                        requested_maps.insert(ident.clone(), sub.clone());
                    }
                }
            }

            for batch in batches {
                let a_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .unwrap();
                let bool_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                let int_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let float_col = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                let str_col = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let ref_col = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let attr_id = a_col.value(i);
                    let attr_meta = self.schema.get_by_id(attr_id);
                    if attr_meta.is_none() {
                        continue;
                    }
                    let attr_meta = attr_meta.unwrap();
                    let ident = &attr_meta.ident;

                    let should_include = requested_wildcard
                        || requested_simples.contains(ident)
                        || requested_maps.contains_key(ident);

                    if !should_include {
                        continue;
                    }

                    let json_val = match attr_meta.value_type {
                        crate::schema::ValueType::Boolean => {
                            if bool_col.is_valid(i) {
                                JsonValue::Bool(bool_col.value(i))
                            } else {
                                JsonValue::Null
                            }
                        }
                        crate::schema::ValueType::Int64 => {
                            if int_col.is_valid(i) {
                                JsonValue::Number(serde_json::Number::from(int_col.value(i)))
                            } else {
                                JsonValue::Null
                            }
                        }
                        crate::schema::ValueType::Float64 => {
                            if float_col.is_valid(i) {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(float_col.value(i)).unwrap(),
                                )
                            } else {
                                JsonValue::Null
                            }
                        }
                        crate::schema::ValueType::String => {
                            if str_col.is_valid(i) {
                                JsonValue::String(str_col.value(i).to_string())
                            } else {
                                JsonValue::Null
                            }
                        }
                        crate::schema::ValueType::Ref => {
                            if ref_col.is_valid(i) {
                                let target_e = ref_col.value(i);
                                if let Some(sub_pattern) = requested_maps.get(ident) {
                                    // Graph traversal trigger!
                                    let sub_doc = self.pull_entity(target_e, sub_pattern).await?;
                                    JsonValue::Object(sub_doc)
                                } else {
                                    JsonValue::Number(serde_json::Number::from(target_e))
                                }
                            } else {
                                JsonValue::Null
                            }
                        }
                        _ => JsonValue::Null,
                    };

                    if json_val != JsonValue::Null {
                        doc.insert(ident.clone(), json_val);
                    }
                }
            }
            Ok(doc)
        })
    }

    fn strip_colons(val: &JsonValue) -> JsonValue {
        match val {
            JsonValue::Object(map) => {
                let mut new_map = Map::new();
                for (k, v) in map {
                    let new_k = if k.starts_with(':') { &k[1..] } else { k };
                    new_map.insert(new_k.to_string(), Self::strip_colons(v));
                }
                JsonValue::Object(new_map)
            }
            JsonValue::Array(arr) => JsonValue::Array(arr.iter().map(Self::strip_colons).collect()),
            _ => val.clone(),
        }
    }

    fn to_edn(val: &JsonValue) -> String {
        match val {
            JsonValue::Null => "nil".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => format!("\"{}\"", s),
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::to_edn).collect();
                format!("[{}]", items.join(" "))
            }
            JsonValue::Object(map) => {
                let items: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{} {}", k, Self::to_edn(v)))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
}
