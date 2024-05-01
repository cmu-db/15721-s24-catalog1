use crate::dto::column_data::ColumnData;
use crate::dto::namespace_data::NamespaceIdent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use typed_builder::TypedBuilder;
use std::collections::HashMap;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    pub id: TableIdent,
    pub metadata: TableMetadata,
    pub read_properties: Value,
    pub write_properties: Value,
    pub file_urls: Vec<String>,
    pub columns: Vec<ColumnData>,
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

    /// Get the namespace of the table.
    /// this returns the identifier of the namespace
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }
}


#[derive(Debug, TypedBuilder)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableMetadata {
  pub table_uuid: String,
  pub location: Option<String>,
  pub last_updated_ms: Option<i64>,
  pub properties: HashMap<String, String>,
  pub file_urls: Option<Vec<String>>,
  pub columns: Option<Vec<ColumnData>>,
}
