use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NamespaceData {
    pub name: NamespaceIdent,
    pub properties: Value,
}

impl NamespaceData {
    pub fn get_name(&self) -> &NamespaceIdent {
        &self.name
    }

    pub fn get_properties(&self) -> &Value {
        &self.properties
    }
}

/// NamespaceIdent represents the identifier of a namespace in the catalog.
///
/// The namespace identifier is a list of strings, where each string is a
/// component of the namespace. It's catalog implementer's responsibility to
/// handle the namespace identifier correctly.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceIdent(pub Vec<String>);

impl NamespaceIdent {
    pub fn new(id: Vec<String>) -> NamespaceIdent {
        NamespaceIdent(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_namespace_ident() {
        let id = vec!["test".to_string()];
        let namespace_ident = NamespaceIdent::new(id.clone());

        assert_eq!(namespace_ident.0, id);
    }

    #[test]
    fn test_namespace_data() {
        let id = vec!["test".to_string()];
        let namespace_ident = NamespaceIdent::new(id.clone());
        let properties = serde_json::json!({"key": "value"});

        let namespace_data = NamespaceData {
            name: namespace_ident.clone(),
            properties: properties.clone(),
        };

        assert_eq!(*namespace_data.get_name(), namespace_ident);
        assert_eq!(*namespace_data.get_properties(), properties);
    }

    #[test]
    fn test_namespace_ident_serde() {
        let id = vec!["test".to_string()];
        let namespace_ident = NamespaceIdent::new(id.clone());

        // Serialize
        let serialized = serde_json::to_string(&namespace_ident).unwrap();
        assert_eq!(serialized, r#"["test"]"#);

        // Deserialize
        let deserialized: NamespaceIdent = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, namespace_ident);
    }

    #[test]
    fn test_namespace_data_serde() {
        let id = vec!["test".to_string()];
        let namespace_ident = NamespaceIdent::new(id.clone());
        let properties = json!({"key": "value"});

        let namespace_data = NamespaceData {
            name: namespace_ident.clone(),
            properties: properties.clone(),
        };

        // Serialize
        let serialized = serde_json::to_string(&namespace_data).unwrap();
        assert_eq!(
            serialized,
            r#"{"name":["test"],"properties":{"key":"value"}}"#
        );

        // Deserialize
        let deserialized: NamespaceData = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, namespace_data);
    }
}
