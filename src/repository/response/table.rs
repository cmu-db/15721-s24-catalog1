use serde::{Deserialize, Serialize};
use crate::repository::namespace::{Namespace, NamespaceIdent};

#[serde(rename_all = "kebab-case")]
#[derive(Debug, Deserialize, Serialize)]
pub struct ListTablesResponse {
  pub identifiers: Vec<TableIdentifier>,
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
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Try to create table identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        let mut vec: Vec<String> = iter.into_iter().map(|s| s.to_string()).collect();
        let table_name = vec.pop().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Table identifier can't be empty!")
        })?;
        let namespace_ident = NamespaceIdent::from_vec(vec)?;

        Ok(Self {
            namespace: namespace_ident,
            name: table_name,
        })
    }
}