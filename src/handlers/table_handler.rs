use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::{TableIdent, TableCreation, TableMetadata, Table};
use crate::repository::table::TableRepository;
use crate::dto::namespace_data::{NamespaceIdent};
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
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
    repo.list_all_tables(&id)
        .map(|tables| Json(tables.unwrap_or_default()))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
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
    repo.create_table(&id, &table)
        .map(|_| StatusCode::CREATED)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
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
    repo.drop_table(&id, table)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
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
        Ok(true) => Ok(StatusCode::FOUND),
        Ok(false) => Ok(StatusCode::NOT_FOUND),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
    }
}

pub async fn rename_table(
    State(repo): State<Arc<TableRepository>>,
    request: Json<TableRenameRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.rename_table(&request)
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

