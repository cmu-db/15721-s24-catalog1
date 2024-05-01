use crate::dto::column_data::ColumnData;
use crate::dto::namespace_data::NamespaceIdent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use typed_builder::TypedBuilder;
use std::collections::HashMap;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableData {
    pub name: String,
    pub num_columns: u64,
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

// TableCreation represents the creation of a table in the catalog.
// #[derive(Debug, TypedBuilder)]
// pub struct TableCreation {
//     /// The name of the table.
//     pub name: String,
//     /// The schema of the table.
//     pub schema: Schema,
//     /// The properties of the table.
//     #[builder(default)]
//     pub properties: HashMap<String, String>,
// }