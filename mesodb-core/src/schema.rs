use ahash::AHashMap;
use std::sync::Arc;

use crate::types::AttributeId;

/// Represents the physical data types that MesoDB supports.
/// This maps directly to our `Value` enum and Arrow MemTable arrays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueType {
    Boolean,
    Int64,
    Float64,
    String,
    Ref,
    Timestamp,
    Uuid,
}

/// The definition of a single Attribute (property) in the database.
#[derive(Debug, Clone)]
pub struct Attribute {
    /// The internal integer ID used in Datoms and MemTables
    pub id: AttributeId,
    /// The string identifier used by clients (e.g., ":user/email")
    pub ident: String,
    /// The physical data type of this attribute
    pub value_type: ValueType,
    /// Whether this attribute must be strictly unique across all entities
    pub is_unique: bool,
}

/// The fast, in-memory catalog of all attributes in the database.
#[derive(Debug, Default, Clone)]
pub struct SchemaMap {
    // We maintain two maps for O(1) lookups in both directions
    by_ident: AHashMap<String, Arc<Attribute>>,
    by_id: AHashMap<AttributeId, Arc<Attribute>>,

    // A simple counter to auto-assign IDs to new attributes
    next_id: AttributeId,
}

impl SchemaMap {
    pub fn new() -> Self {
        Self {
            by_ident: AHashMap::new(),
            by_id: AHashMap::new(),
            // We can reserve IDs 1-99 for internal system attributes later
            next_id: 100,
        }
    }

    /// Registers a new attribute in the schema.
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

    /// Looks up an attribute by its string identifier.
    pub fn get_by_ident(&self, ident: &str) -> Option<Arc<Attribute>> {
        self.by_ident.get(ident).cloned()
    }

    /// Looks up an attribute by its internal ID.
    pub fn get_by_id(&self, id: AttributeId) -> Option<Arc<Attribute>> {
        self.by_id.get(&id).cloned()
    }

    /// Checks if a string identifier already exists.
    pub fn contains_ident(&self, ident: &str) -> bool {
        self.by_ident.contains_key(ident)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_map_creation_and_lookup() {
        let mut schema = SchemaMap::new();

        // 1. Add an attribute
        let email_attr = schema.add_attribute(":user/email", ValueType::String, true);
        assert_eq!(email_attr.id, 100);
        assert_eq!(email_attr.ident, ":user/email");
        assert!(email_attr.is_unique);

        // 2. Add another attribute
        let age_attr = schema.add_attribute(":user/age", ValueType::Int64, false);
        assert_eq!(age_attr.id, 101);

        // 3. Test O(1) lookups
        let lookup_by_ident = schema.get_by_ident(":user/email").unwrap();
        assert_eq!(lookup_by_ident.id, 100);

        let lookup_by_id = schema.get_by_id(101).unwrap();
        assert_eq!(lookup_by_id.ident, ":user/age");
        assert_eq!(lookup_by_id.value_type, ValueType::Int64);

        // 4. Test missing lookup
        assert!(schema.get_by_ident(":does/not_exist").is_none());
    }

    #[test]
    fn test_schema_auto_increment() {
        let mut schema = SchemaMap::new();

        let a1 = schema.add_attribute(":sys/a", ValueType::Boolean, false);
        let a2 = schema.add_attribute(":sys/b", ValueType::Boolean, false);
        let a3 = schema.add_attribute(":sys/c", ValueType::Boolean, false);

        assert_eq!(a1.id, 100);
        assert_eq!(a2.id, 101);
        assert_eq!(a3.id, 102);
    }

    #[test]
    fn test_schema_contains_and_missing() {
        let mut schema = SchemaMap::new();
        schema.add_attribute(":user/name", ValueType::String, false);

        assert!(schema.contains_ident(":user/name"));
        assert!(!schema.contains_ident(":user/ghost"));

        assert!(schema.get_by_ident(":user/ghost").is_none());
        assert!(schema.get_by_id(9999).is_none());
    }
}
