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
        // 1. Execute the batched fetch for all entities
        let mut doc_map = self.pull_entities(entity_ids, pattern).await?;

        // 2. Format them back in the exact order they were requested
        let mut results = Vec::with_capacity(entity_ids.len());

        for &e in entity_ids {
            let doc = doc_map.remove(&e).unwrap_or_else(Map::new);

            let serialized = match self.format {
                OutputFormat::Json => {
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

    // The Recursive, BATCHED Graph Walker
    fn pull_entities<'b>(
        &'b self,
        e_ids: &'b [u64],
        pattern: &'b PullPattern,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<HashMap<u64, Map<String, JsonValue>>, MesoError>,
                > + Send
                + 'b,
        >,
    > {
        Box::pin(async move {
            if e_ids.is_empty() {
                return Ok(HashMap::new());
            }

            let time_clause = match self.as_of {
                Some(t) => {
                    format!("CAST(valid_from AS BIGINT) <= {t} AND CAST(valid_to AS BIGINT) > {t}")
                }
                None => "next_from IS NULL".to_string(),
            };

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

            let mut all_batches = Vec::new();

            // CHUNKING: Protect the SQL parser by executing in chunks of 1000 IDs
            for chunk in e_ids.chunks(1000) {
                let in_list = chunk
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "SELECT e, a, v_bool, v_int, v_float, v_str, v_ref FROM resolved_datoms WHERE e IN ({}) AND {}",
                    in_list, time_clause
                );

                let df = self.ctx.sql(&sql).await.map_err(MesoError::DataFusion)?;
                let batches = df.collect().await.map_err(MesoError::DataFusion)?;
                all_batches.extend(batches);
            }

            // Initialize the output map with empty docs so requested IDs always exist
            let mut docs: HashMap<u64, Map<String, JsonValue>> =
                HashMap::with_capacity(e_ids.len());
            for &e in e_ids {
                docs.insert(e, Map::new());
            }

            // Track target IDs for recursive sub-queries
            let mut sub_fetches: HashMap<String, HashSet<u64>> = HashMap::new();

            for batch in all_batches {
                let e_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap();
                let a_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .unwrap();
                let bool_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                let int_col = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let float_col = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                let str_col = batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let ref_col = batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let attr_id = a_col.value(i);
                    let attr_meta = match self.schema.get_by_id(attr_id) {
                        Some(meta) => meta,
                        None => continue,
                    };

                    let ident = &attr_meta.ident;

                    if !requested_wildcard
                        && !requested_simples.contains(ident)
                        && !requested_maps.contains_key(ident)
                    {
                        continue;
                    }

                    let json_val = match attr_meta.value_type {
                        crate::schema::ValueType::Boolean if bool_col.is_valid(i) => {
                            JsonValue::Bool(bool_col.value(i))
                        }
                        crate::schema::ValueType::Int64 if int_col.is_valid(i) => {
                            JsonValue::Number(serde_json::Number::from(int_col.value(i)))
                        }
                        crate::schema::ValueType::Float64 if float_col.is_valid(i) => {
                            JsonValue::Number(
                                serde_json::Number::from_f64(float_col.value(i)).unwrap(),
                            )
                        }
                        crate::schema::ValueType::String if str_col.is_valid(i) => {
                            JsonValue::String(str_col.value(i).to_string())
                        }
                        crate::schema::ValueType::Ref if ref_col.is_valid(i) => {
                            let target_e = ref_col.value(i);
                            if requested_maps.contains_key(ident) {
                                // Add to our batched fetch list and place a temporary placeholder!
                                sub_fetches
                                    .entry(ident.clone())
                                    .or_default()
                                    .insert(target_e);
                            }
                            JsonValue::Number(serde_json::Number::from(target_e))
                        }
                        _ => JsonValue::Null,
                    };

                    if json_val != JsonValue::Null {
                        let e = e_col.value(i);
                        if let Some(doc) = docs.get_mut(&e) {
                            doc.insert(ident.clone(), json_val);
                        }
                    }
                }
            }

            // --- THE RECURSIVE BATCH RESOLUTION ---
            for (ident, target_set) in sub_fetches {
                let target_vec: Vec<u64> = target_set.into_iter().collect();
                let sub_pattern = requested_maps.get(&ident).unwrap();

                // Fetch ALL sub-documents in one massive call
                let sub_results = self.pull_entities(&target_vec, sub_pattern).await?;

                // Patch the parent documents with the retrieved JSON objects
                for doc in docs.values_mut() {
                    if let Some(val) = doc.get_mut(&ident) {
                        if let Some(target_e_num) = val.as_u64() {
                            if let Some(sub_doc) = sub_results.get(&target_e_num) {
                                *val = JsonValue::Object(sub_doc.clone());
                            } else {
                                *val = JsonValue::Object(Map::new()); // Empty object if target doesn't exist
                            }
                        }
                    }
                }
            }

            Ok(docs)
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
