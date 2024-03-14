use axum::{
    extract::{Json, Path},
    http::StatusCode,
    response::IntoResponse,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Namespace {}

pub async fn list_namespaces() -> Json<Vec<String>> {
    // Logic to list namespaces
    let namespaces: Vec<String> = vec![
        "accounting".to_string(),
        "tax".to_string(),
        "paid".to_string(),
    ];
    Json(namespaces)
}

pub async fn create_namespace(new_namespace: Json<Namespace>) -> Json<Namespace> {
    // Logic to create a new namespace

    // Logic to persist the namespace and add properties
    new_namespace
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NamespaceMetadata {
    // Define your namespace metadata properties here
    // Example: pub metadata_property: String,
    data: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NamespaceProperties {
    data: String,
}

pub async fn load_namespace_metadata(Path(namespace): Path<String>) -> Json<NamespaceMetadata> {
    print!("Namespaces: {}", namespace);
    // Logic to load metadata properties for a namespace
    let metadata = NamespaceMetadata {
        data: namespace,
        // Populate with actual metadata properties
    };
    Json(metadata)
}

pub async fn namespace_exists(Path(namespace): Path<String>) -> impl IntoResponse {
    // Logic to check if a namespace exists
    // This route just needs to return a status code, no body required
    // Return HTTP status code 200 to indicate namespace exists
    StatusCode::FOUND
}

pub async fn drop_namespace(Path(namespace): Path<String>) -> impl IntoResponse {
    // Logic to drop a namespace from the catalog
    // Ensure the namespace is empty before dropping
    // Return HTTP status code 204 to indicate successful deletion
    StatusCode::NO_CONTENT
}

pub async fn set_namespace_properties(Path(namespace): Path<String>) -> Json<NamespaceProperties> {
    // Logic to set and/or remove properties on a namespace
    // Deserialize request body and process properties
    // Return HTTP status code 200 to indicate success

    let prop = NamespaceProperties {
        data: "namespace properties".to_string(),
    };

    Json(prop)
}
