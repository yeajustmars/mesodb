use ahash::AHashMap;
use rkyv::{Archive, Deserialize, Serialize};
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
    AddAttribute(Attribute),
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
}
