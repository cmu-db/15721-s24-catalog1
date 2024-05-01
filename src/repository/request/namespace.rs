use serde::{Deserialize, Serialize};
use crate::dto::namespace_data::{NamespaceIdent};

#[derive(Deserialize, Serialize, Debug)]
pub struct Properties(pub std::collections::HashMap<String, String>);

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateNamespaceRequest {
    pub namespace: NamespaceIdent,
    // the properties field is optional
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Properties>,
}


#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateNamespacePropertiesRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub removals: Vec<String>,
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub updates: std::collections::HashMap<String, String>,
}

