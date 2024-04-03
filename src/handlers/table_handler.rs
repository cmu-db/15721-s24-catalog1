use crate::database::database::Database;
use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::TableData;
use crate::repository::table::TableRepository;
use axum::{
    extract::{Json, Path},
    http::StatusCode,
    response::IntoResponse,
};

const DB_PATH: &str = "rocksdb";

pub async fn list_tables(Path(namespace): Path<String>) -> Json<Vec<String>> {
    // Logic to get all tables in the namespace
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    Json(repo.list_all_tables(&namespace).unwrap().unwrap())
}

pub async fn create_table(
    Path(namespace): Path<String>,
    table: Json<TableData>,
) -> impl IntoResponse {
    // Logic to create a table in the given namespace
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    match repo.create_table(&namespace, &table) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub async fn register_table(
    Path(namespace): Path<String>,
    table: Json<TableData>,
) -> impl IntoResponse {
    // Logic to register a table in the given namespace using metadata file location
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    match repo.register_table(&namespace, &table) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub async fn load_table(Path((namespace, table)): Path<(String, String)>) -> Json<TableData> {
    // Logic to load a table from the catalog
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    Json(repo.load_table(&namespace, &table).unwrap().unwrap())
}

pub async fn delete_table(Path((namespace, table)): Path<(String, String)>) -> impl IntoResponse {
    // Logic to drop a table from the catalog
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    repo.drop_table(namespace.as_str(), table.as_str());
    StatusCode::NO_CONTENT
}

pub async fn table_exists(Path((namespace, table)): Path<(String, String)>) -> impl IntoResponse {
    // Logic to check if a table exists within a given namespace
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    match repo
        .table_exists(namespace.as_str(), table.as_str())
    {
        Ok(_) => StatusCode::FOUND,
        Err(_)=>StatusCode::NOT_FOUND
    }
}

// Handler functions
pub async fn rename_table(request: Json<TableRenameRequest>) -> impl IntoResponse {
    // Logic to rename a table from its current name to a new name
    let database = Database::open(DB_PATH).unwrap();
    let repo = TableRepository::new(database);
    match repo.rename_table(&request) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MetricsReport {}

pub async fn report_metrics(Path((namespace, table)): Path<(String, String)>) -> impl IntoResponse {
    // Logic to process metrics report
    Json(table)
}

pub async fn find_tuple_location(
    Path((namespace, table, tuple_id)): Path<(String, String, String)>,
) -> impl IntoResponse {
    // Logic to return the physical file location for a given tuple ID
    format!(
        "Physical file location for tuple ID {} of table {} in namespace {}.",
        tuple_id, table, namespace
    )
}
