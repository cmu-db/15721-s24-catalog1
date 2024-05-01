use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NamespaceData {
    pub name: NamespaceIdent,
    pub properties: Value,
}

impl NamespaceData {
    pub fn get_name(&self) -> NamespaceIdent {
        self.name.clone()
    }

    pub fn get_properties(&self) -> Value {
        self.properties.clone()
    }
}

/// NamespaceIdent represents the identifier of a namespace in the catalog.
///
/// The namespace identifier is a list of strings, where each string is a
/// component of the namespace. It's catalog implementer's responsibility to
/// handle the namespace identifier correctly.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceIdent( pub Vec<String>);

impl NamespaceIdent {
    pub fn new(id: Vec<String>) -> NamespaceIdent {
        NamespaceIdent(id)
    }
}
