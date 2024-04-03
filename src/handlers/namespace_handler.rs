use crate::database::database::Database;
use crate::dto::namespace_data::NamespaceData;
use crate::repository::namespace::NamespaceRepository;
use axum::{
    extract::{Json, Path},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::Value;
use std::sync::Arc;

pub async fn list_namespaces() -> Json<Vec<String>> {
    // Logic to list namespaces
    let db_path = "rocksdb";
    let db = Arc::new(Database::open(db_path).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    let namespaces = namespace_repo.list_all_namespaces();
    Json(namespaces.unwrap())
}

pub async fn create_namespace(new_namespace: Json<NamespaceData>) -> Json<NamespaceData> {
    // Logic to create a new namespace
    let db_path = "rocksdb";
    let db = Arc::new(Database::open(db_path).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo
        .create_namespace(
            new_namespace.get_name(),
            Some(new_namespace.get_properties()),
        )
        .unwrap();
    // Logic to persist the namespace and add properties
    new_namespace
}

pub async fn load_namespace_metadata(Path(namespace): Path<String>) -> Json<NamespaceData> {
    print!("Namespaces: {}", namespace);
    // Logic to load metadata properties for a namespace
    let db_path = "rocksdb";
    let db = Arc::new(Database::open(db_path).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    let metadata = namespace_repo.load_namespace(namespace.as_str()).unwrap();
    Json(metadata.unwrap())
}

pub async fn namespace_exists(Path(namespace): Path<String>) -> impl IntoResponse {
    // Logic to check if a namespace exists
    // This route just needs to return a status code, no body required
    // Return HTTP status code 200 to indicate namespace exists
    let db_path = "rocksdb";
    let db = Arc::new(Database::open(db_path).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    if namespace_repo.namespace_exists(namespace.as_str()).unwrap() {
        StatusCode::FOUND
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn drop_namespace(Path(namespace): Path<String>) -> impl IntoResponse {
    // Logic to drop a namespace from the catalog
    // Ensure the namespace is empty before dropping
    let db_path = "rocksdb";
    let db = Arc::new(Database::open(db_path).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.delete_namespace(namespace.as_str());
    // Return HTTP status code 204 to indicate successful deletion
    StatusCode::NO_CONTENT
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NamespaceProperties {
    data: String,
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
