use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::{TableIdent, TableCreation, Table};
use crate::repository::table::TableRepository;
use crate::dto::namespace_data::{NamespaceIdent};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use std::io::{ ErrorKind};
use std::sync::Arc;

pub async fn list_tables(
    State(repo): State<Arc<TableRepository>>,
    Path(namespace): Path<String>,
) -> Result<Json<Vec<TableIdent>>, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    match repo.list_all_tables(&id) {
        Ok(tables) => Ok(Json(tables.unwrap_or_default())),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => Err((StatusCode::NOT_FOUND, format!("Error: {}", e))),
            _ => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
        },
    }
}

pub async fn create_table(
    State(repo): State<Arc<TableRepository>>,
    Path(namespace): Path<String>,
    table: Json<TableCreation>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    match repo.create_table(&id, &table) {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => Err((StatusCode::NOT_FOUND, format!("Error: {}", e))),
            ErrorKind::AlreadyExists => Err((StatusCode::CONFLICT, format!("Error: {}", e))),
            _ => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
        },
    }
}

pub async fn load_table(
    State(repo): State<Arc<TableRepository>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<Table>, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );

    match repo.load_table(&id, table.clone()) {
        Ok(Some(table_data)) => Ok(Json(table_data)),
        Ok(None) => Err((StatusCode::NOT_FOUND, format!("Table {} not found", table.clone()))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
    }
}

pub async fn delete_table(
    State(repo): State<Arc<TableRepository>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    match repo.drop_table(&id, table) {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => Err((StatusCode::NOT_FOUND, format!("Error: {}", e))),
            _ => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
        }
    }
}

pub async fn table_exists(
    State(repo): State<Arc<TableRepository>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    match repo.table_exists(&id, table) {
        // Ideally this should be FOUND but Iceberg spec says 204
        Ok(true) => Ok(StatusCode::NO_CONTENT),
        Ok(false) => Ok(StatusCode::NOT_FOUND),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
    }
}

pub async fn rename_table(
    State(repo): State<Arc<TableRepository>>,
    request: Json<TableRenameRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    match repo.rename_table(&request) {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => match e.kind() {
            ErrorKind::NotFound => Err((StatusCode::NOT_FOUND, format!("Error: {}", e))),
            ErrorKind::AlreadyExists => Err((StatusCode::CONFLICT, format!("Error: {}", e))),
            _ => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
        },
    }
}

