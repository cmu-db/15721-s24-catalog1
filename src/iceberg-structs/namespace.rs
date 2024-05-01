

use async_trait::async_trait;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::mem::take;
use std::ops::Deref;
use typed_builder::TypedBuilder;
use urlencoding::encode;
use uuid::Uuid;

/// NamespaceIdent represents the identifier of a namespace in the catalog.
///
/// The namespace identifier is a list of strings, where each string is a
/// component of the namespace. It's catalog implementer's responsibility to
/// handle the namespace identifier correctly.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceIdent(Vec<String>);

impl NamespaceIdent {
    /// Create a new namespace identifier with only one level.
    pub fn new(name: String) -> Self {
        Self(vec![name])
    }

    /// Create a multi-level namespace identifier from vector.
    pub fn from_vec(names: Vec<String>) -> Result<Self> {
        if names.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Namespace identifier can't be empty!",
            ));
        }
        Ok(Self(names))
    }

    /// Try to create namespace identifier from an iterator of string.
    pub fn from_strs(iter: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        Self::from_vec(iter.into_iter().map(|s| s.to_string()).collect())
    }

    /// Returns url encoded format.
    pub fn encode_in_url(&self) -> String {
        encode(&self.as_ref().join("\u{1F}")).to_string()
    }

    /// Returns inner strings.
    pub fn inner(self) -> Vec<String> {
        self.0
    }
}

impl AsRef<Vec<String>> for NamespaceIdent {
    fn as_ref(&self) -> &Vec<String> {
        &self.0
    }
}

impl Deref for NamespaceIdent {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Namespace represents a namespace in the catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    name: NamespaceIdent,
    properties: HashMap<String, String>,
}

impl Namespace {
    /// Create a new namespace.
    pub fn new(name: NamespaceIdent) -> Self {
        Self::with_properties(name, HashMap::default())
    }

    /// Create a new namespace with properties.
    pub fn with_properties(name: NamespaceIdent, properties: HashMap<String, String>) -> Self {
        Self { name, properties }
    }

    /// Get the name of the namespace.
    pub fn name(&self) -> &NamespaceIdent {
        &self.name
    }

    /// Get the properties of the namespace.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }
}