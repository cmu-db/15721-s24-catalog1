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

#[derive(Serialize, Deserialize, Debug, TypedBuilder)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    // pub file_urls: Option<Vec<String>>,
    // pub columns: Option<Vec<ColumnData>>,
    // pub properties: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableMetadata {
    pub table_uuid: String,
    //   pub file_urls: Option<Vec<String>>,
    //   pub columns: Option<Vec<ColumnData>>,
    //   pub properties: Option<HashMap<String, String>>,
}
