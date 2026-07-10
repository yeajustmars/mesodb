use ahash::AHashMap;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::MesoError;
use crate::types::{AttributeId, Result, Value};

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub enum ValueType {
    Boolean,
    Int64,
    Float64,
    String,
    Ref,
    Timestamp,
    Uuid,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct Attribute {
    pub id: AttributeId,
    pub ident: String,
    pub value_type: ValueType,
    pub is_unique: bool,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum SchemaMutation {
    AddAttribute {
        tx_id: u64,
        timestamp: i64,
        attribute: Attribute,
    },
}

#[derive(Debug, Clone)]
pub struct SchemaTimeline {
    versions: BTreeMap<u64, Arc<SchemaMap>>,
    time_index: BTreeMap<i64, u64>, // NEW: Maps Timestamp -> TxId
    latest_tx: u64,
}

impl Default for SchemaTimeline {
    fn default() -> Self {
        let mut versions = BTreeMap::new();
        versions.insert(0, Arc::new(SchemaMap::new()));

        let mut time_index = BTreeMap::new();
        time_index.insert(0, 0); // Genesis time

        Self {
            versions,
            time_index,
            latest_tx: 0,
        }
    }
}

impl SchemaTimeline {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append_version(&mut self, tx_id: u64, timestamp: i64, new_schema: SchemaMap) {
        self.versions.insert(tx_id, Arc::new(new_schema));
        self.time_index.insert(timestamp, tx_id);
        self.latest_tx = tx_id;
    }

    /// Converts a wall-clock microsecond timestamp into the active TxId
    pub fn tx_for_timestamp(&self, timestamp: i64) -> u64 {
        self.time_index
            .range(..=timestamp)
            .next_back()
            .map(|(_, &tx)| tx)
            .unwrap_or(0)
    }

    pub fn get_schema_at(&self, tx_id: u64) -> Arc<SchemaMap> {
        let (_, schema) = self.versions.range(..=tx_id).next_back().unwrap();
        Arc::clone(schema)
    }

    pub fn get_versions_in_range(&self, start_tx: u64, end_tx: u64) -> Vec<(u64, Arc<SchemaMap>)> {
        let mut active_versions = Vec::new();
        let (&base_tx, base_schema) = self.versions.range(..=start_tx).next_back().unwrap();
        active_versions.push((base_tx, Arc::clone(base_schema)));

        for (&tx, schema) in self.versions.range((start_tx + 1)..=end_tx) {
            active_versions.push((tx, Arc::clone(schema)));
        }
        active_versions
    }

    pub fn latest_tx(&self) -> u64 {
        self.latest_tx
    }
}

#[derive(Debug, Default, Clone)]
pub struct SchemaMap {
    by_ident: AHashMap<String, Arc<Attribute>>,
    by_id: AHashMap<AttributeId, Arc<Attribute>>,
    next_id: AttributeId,
}

impl SchemaMap {
    pub fn new() -> Self {
        Self {
            by_ident: AHashMap::new(),
            by_id: AHashMap::new(),
            next_id: 100, // Reserve 1-99 for internal system attributes
        }
    }

    pub fn add_attribute(
        &mut self,
        ident: &str,
        value_type: ValueType,
        is_unique: bool,
    ) -> Arc<Attribute> {
        let id = self.next_id;
        self.next_id += 1;

        let attr = Arc::new(Attribute {
            id,
            ident: ident.to_string(),
            value_type,
            is_unique,
        });

        self.by_ident.insert(ident.to_string(), attr.clone());
        self.by_id.insert(id, attr.clone());

        attr
    }

    pub fn get_by_ident(&self, ident: &str) -> Option<Arc<Attribute>> {
        self.by_ident.get(ident).cloned()
    }

    pub fn get_by_id(&self, id: AttributeId) -> Option<Arc<Attribute>> {
        self.by_id.get(&id).cloned()
    }

    pub fn get_id(&self, ident: &str) -> Option<u32> {
        self.by_ident.get(ident).map(|a| a.id)
    }

    pub fn contains_ident(&self, ident: &str) -> bool {
        self.by_ident.contains_key(ident)
    }

    pub fn validate_value(&self, ident: &str, value: &Value) -> Result<()> {
        let attr = self
            .by_ident
            .get(ident)
            .ok_or_else(|| MesoError::Serialization(format!("Attribute {} not found", ident)))?;

        match (&attr.value_type, value) {
            (ValueType::Int64, Value::Int64(_)) => Ok(()),
            (ValueType::String, Value::String(_)) => Ok(()),
            (ValueType::Boolean, Value::Boolean(_)) => Ok(()),
            (ValueType::Float64, Value::Float64(_)) => Ok(()),
            (ValueType::Uuid, Value::Uuid(_)) => Ok(()),
            (ValueType::Ref, Value::Ref(_)) => Ok(()),
            (ValueType::Timestamp, Value::Timestamp(_)) => Ok(()),
            (expected, found) => Err(MesoError::Serialization(format!(
                "Type mismatch for {}: expected {:?}, found {:?}",
                ident, expected, found
            ))),
        }
    }

    /// Safely ingests an attribute recovered from the WAL
    pub fn ingest_attribute(&mut self, attr: Attribute) {
        if attr.id >= self.next_id {
            self.next_id = attr.id + 1;
        }
        let arc_attr = Arc::new(attr);
        self.by_ident
            .insert(arc_attr.ident.clone(), arc_attr.clone());
        self.by_id.insert(arc_attr.id, arc_attr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_map_creation_and_lookup() {
        let mut schema = SchemaMap::new();

        let email_attr = schema.add_attribute(":user/email", ValueType::String, true);
        assert_eq!(email_attr.id, 100);
        assert_eq!(email_attr.ident, ":user/email");
        assert!(email_attr.is_unique);

        let age_attr = schema.add_attribute(":user/age", ValueType::Int64, false);
        assert_eq!(age_attr.id, 101);

        let lookup_by_ident = schema.get_by_ident(":user/email").unwrap();
        assert_eq!(lookup_by_ident.id, 100);

        let lookup_by_id = schema.get_by_id(101).unwrap();
        assert_eq!(lookup_by_id.ident, ":user/age");
        assert_eq!(lookup_by_id.value_type, ValueType::Int64);
    }

    #[test]
    fn test_schema_validation_success_and_failure() {
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/age", ValueType::Int64, false);

        // Valid
        assert!(
            schema
                .validate_value(":user/age", &Value::Int64(30))
                .is_ok()
        );

        // Invalid Type
        assert!(
            schema
                .validate_value(":user/age", &Value::String("30".into()))
                .is_err()
        );

        // Missing Attribute
        assert!(
            schema
                .validate_value(":user/ghost", &Value::Int64(30))
                .is_err()
        );
    }

    #[test]
    fn test_schema_auto_increment() {
        let mut schema = SchemaMap::new();
        let a1 = schema.add_attribute(":sys/a", ValueType::Boolean, false);
        let a2 = schema.add_attribute(":sys/b", ValueType::Boolean, false);

        assert_eq!(a1.id, 100);
        assert_eq!(a2.id, 101);
    }

    #[test]
    fn test_schema_timeline_time_travel() {
        let mut timeline = SchemaTimeline::new(); // Implicitly creates Tx 0 at T 0

        // Schema V1 at T = 1000
        let mut schema1 = SchemaMap::new();
        schema1.add_attribute(":v1/attr", ValueType::String, false);
        timeline.append_version(1, 1000, schema1);

        // Schema V2 at T = 2000
        let mut schema2 = SchemaMap::new();
        schema2.add_attribute(":v2/attr", ValueType::Int64, false);
        timeline.append_version(2, 2000, schema2);

        // 1. Exact matches
        assert_eq!(timeline.tx_for_timestamp(1000), 1);
        assert_eq!(timeline.tx_for_timestamp(2000), 2);

        // 2. In-between times (Should floor to the highest Tx <= Timestamp)
        assert_eq!(timeline.tx_for_timestamp(1500), 1);
        assert_eq!(timeline.tx_for_timestamp(2999), 2);

        // 3. Before genesis
        assert_eq!(timeline.tx_for_timestamp(500), 0);
    }
}
