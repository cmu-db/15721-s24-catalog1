use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Namespace(pub Vec<String>);

#[derive(Deserialize, Serialize, Debug)]
pub struct Properties(pub std::collections::HashMap<String, String>);

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateNamespaceRequest {
    pub namespace: Namespace,
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

