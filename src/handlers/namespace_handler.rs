use crate::database::database::Database;
use crate::dto::namespace_data::NamespaceData;
use crate::repository::namespace::NamespaceRepository;
use axum::{
    extract::{Json, Path},
    http::StatusCode
};
use serde_json::Value;
use std::sync::Arc;

const DB_PATH: &str = "rocksdb";

pub async fn list_namespaces() -> Result<Json<Vec<String>>, (StatusCode, String)> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.list_all_namespaces()
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn create_namespace(new_namespace: Json<NamespaceData>) -> Result<Json<NamespaceData>, (StatusCode, String)> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.create_namespace(new_namespace.get_name(), Some(new_namespace.get_properties()))
        .map(|_| new_namespace)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn load_namespace_metadata(Path(namespace): Path<String>) -> Result<Json<NamespaceData>, (StatusCode, String)> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    match namespace_repo.load_namespace(namespace.as_str()) {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err((StatusCode::NOT_FOUND, format!("Namespace {} not found", namespace))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
    }
}

pub async fn namespace_exists(Path(namespace): Path<String>) -> Result<StatusCode, (StatusCode, String)> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.namespace_exists(namespace.as_str())
        .map(|exists| if exists { StatusCode::FOUND } else { StatusCode::NOT_FOUND })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn drop_namespace(Path(namespace): Path<String>) -> Result<StatusCode, (StatusCode, String)> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.delete_namespace(namespace.as_str())
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn set_namespace_properties(
    Path(namespace): Path<String>,
    properties: Json<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    let db = Arc::new(Database::open(DB_PATH).unwrap());
    let namespace_repo = NamespaceRepository::new(db.clone());
    namespace_repo.set_namespace_properties(namespace.as_str(), properties.0)
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}
