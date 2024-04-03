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

const DB_PATH: &str = "rocksdb";

pub async fn list_namespaces() -> Json<Vec<String>> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    let namespaces = namespace_repo.list_all_namespaces();
    Json(namespaces.unwrap())
}

pub async fn create_namespace(new_namespace: Json<NamespaceData>) -> Json<NamespaceData> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo
        .create_namespace(
            new_namespace.get_name(),
            Some(new_namespace.get_properties()),
        )
        .unwrap();
    new_namespace
}

pub async fn load_namespace_metadata(Path(namespace): Path<String>) -> Json<NamespaceData> {
    print!("Namespaces: {}", namespace);
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    let metadata = namespace_repo.load_namespace(namespace.as_str()).unwrap();
    Json(metadata.unwrap())
}

pub async fn namespace_exists(Path(namespace): Path<String>) -> impl IntoResponse {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    if namespace_repo.namespace_exists(namespace.as_str()).unwrap() {
        StatusCode::FOUND
    } else {
        StatusCode::NOT_FOUND
    }
}

pub async fn drop_namespace(Path(namespace): Path<String>) -> impl IntoResponse {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.delete_namespace(namespace.as_str());
    StatusCode::NO_CONTENT
}

pub async fn set_namespace_properties(
    Path(namespace): Path<String>,
    properties: Json<Value>,
) -> impl IntoResponse {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    match namespace_repo.set_namespace_properties(namespace.as_str(), properties.0) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::NOT_FOUND,
    }
}
