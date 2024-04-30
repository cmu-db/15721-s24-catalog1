use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq, Clone)]
pub struct Namespace(pub Vec<String>);

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Namespace>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CreateNamespaceResponse {
    pub namespace: Namespace,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Properties>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct UpdateNamespacePropertiesResponse {
    pub updated: Vec<String>,
    pub removed: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetNamespaceMetadataResponse {
    pub namespace: Namespace,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Properties>,
}


#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct GetNamespaceResponse {
    pub namespace: Namespace,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Properties>,
}
