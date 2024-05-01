use crate::table::Table;
use crate::structs::error::{Error, ErrorKind, Result};
use crate::structs::namespace::{Namespace, NamespaceIdent};
use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem::take;
use std::ops::Deref;
use typed_builder::TypedBuilder;
use urlencoding::encode;
use uuid::Uuid;


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

/// TableCreation represents the creation of a table in the catalog.
#[derive(Debug, TypedBuilder)]
pub struct TableCreation {
    /// The name of the table.
    pub name: String,
    /// The schema of the table.
    pub schema: Schema,
    /// The properties of the table.
    #[builder(default)]
    pub properties: HashMap<String, String>,
}
