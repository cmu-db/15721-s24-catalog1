// use crate::dto::column_data::ColumnData;
use crate::dto::namespace_data::NamespaceIdent;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
// use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub id: TableIdent,
    pub metadata: TableMetadata,
}

/// TableIdent represents the identifier of a table in the catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    /// Namespace of the table.
    pub namespace: NamespaceIdent,
    /// Table name.
    pub name: String,
}

impl TableIdent {
    /// Create a new table identifier.
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        Self { namespace, name }
    }
}

#[derive(Serialize, Deserialize, Debug, TypedBuilder, Clone, PartialEq, Eq, Hash)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    // pub file_urls: Option<Vec<String>>,
    // pub columns: Option<Vec<ColumnData>>,
    // pub properties: Option<HashMap<String, String>>,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct TableMetadata {
    pub table_uuid: String,
    //   pub file_urls: Option<Vec<String>>,
    //   pub columns: Option<Vec<ColumnData>>,
    //   pub properties: Option<HashMap<String, String>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_table_ident() {
        let namespace = NamespaceIdent(vec!["namespace".to_string()]);
        let name = "table".to_string();
        let table_ident = TableIdent::new(namespace.clone(), name.clone());

        assert_eq!(table_ident.namespace, namespace);
        assert_eq!(table_ident.name, name);
    }

    #[test]
    fn test_table_creation() {
        let name = "table".to_string();
        let table_creation = TableCreation::builder().name(name.clone()).build();

        assert_eq!(table_creation.name, name);
    }

    #[test]
    fn test_table_metadata() {
        let table_uuid = "uuid".to_string();
        let table_metadata = TableMetadata {
            table_uuid: table_uuid.clone(),
        };

        assert_eq!(table_metadata.table_uuid, table_uuid);
    }

    #[test]
    fn test_table() {
        let id = TableIdent::new(
            NamespaceIdent(vec!["namespace".to_string()]),
            "table".to_string(),
        );
        let metadata = TableMetadata {
            table_uuid: "uuid".to_string(),
        };
        let table = Table {
            id: id.clone(),
            metadata: metadata.clone(),
        };

        assert_eq!(table.id, id);
        assert_eq!(table.metadata, metadata);
    }

    #[test]
    fn test_table_ident_serialization() {
        let table_ident = TableIdent::new(
            NamespaceIdent(vec!["namespace".to_string()]),
            "table".to_string(),
        );
        let serialized = serde_json::to_string(&table_ident).unwrap();

        assert!(serialized.contains("namespace"));
        assert!(serialized.contains("table"));
    }

    #[test]
    fn test_table_ident_deserialization() {
        let data = r#"
        {
            "namespace": ["namespace"],
            "name": "table"
        }
        "#;

        let table_ident: TableIdent = serde_json::from_str(data).unwrap();

        assert_eq!(table_ident.namespace.0[0], "namespace");
        assert_eq!(table_ident.name, "table");
    }
}
