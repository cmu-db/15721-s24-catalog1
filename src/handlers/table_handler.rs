use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::TableData;
use crate::repository::table::TableRepository;
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use std::sync::Arc;

pub async fn list_tables(
    State(repo): State<Arc<TableRepository>>,
    Path(namespace): Path<String>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    repo.list_all_tables(&namespace)
        .map(|tables| Json(tables.unwrap_or_default()))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn create_table(
    State(repo): State<Arc<TableRepository>>,
    Path(namespace): Path<String>,
    table: Json<TableData>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.create_table(&namespace, &table)
        .map(|_| StatusCode::CREATED)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn register_table(
    State(repo): State<Arc<TableRepository>>,
    Path(namespace): Path<String>,
    table: Json<TableData>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.register_table(&namespace, &table)
        .map(|_| StatusCode::CREATED)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn load_table(
    State(repo): State<Arc<TableRepository>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<TableData>, (StatusCode, String)> {
    match repo.load_table(&namespace, &table) {
        Ok(Some(table_data)) => Ok(Json(table_data)),
        Ok(None) => Err((StatusCode::NOT_FOUND, format!("Table {} not found", table))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
    }
}

pub async fn delete_table(
    State(repo): State<Arc<TableRepository>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.drop_table(&namespace, &table)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn table_exists(
    State(repo): State<Arc<TableRepository>>,
    Path((namespace, table)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    match repo.table_exists(&namespace, &table) {
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MetricsReport {}

pub async fn report_metrics(
    Path((namespace, table)): Path<(String, String)>,
) -> Result<Json<String>, (StatusCode, String)> {
    // Logic to process metrics report
    Ok(Json(table))
}
