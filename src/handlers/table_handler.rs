use crate::dto::namespace_data::NamespaceIdent;
use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::{Table, TableCreation, TableIdent};
use crate::repository::table::TableRepository;
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use std::io::ErrorKind;
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
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            format!("Table {} not found", table.clone()),
        )),
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
        },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::database::Database;
    use crate::dto::table_data::TableCreation;
    use crate::repository::namespace::NamespaceRepository;
    use axum::http::StatusCode;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_table_endpoints() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let repo = Arc::new(TableRepository::new(db.clone()));

        // Create a namespace for testing
        let namespace_ident = NamespaceIdent(vec!["test".to_string()]);
        let namespace_repo = NamespaceRepository::new(db.clone());
        namespace_repo
            .create_namespace(namespace_ident.clone(), None)
            .unwrap();

        // Test create_table
        let table_creation = Json(TableCreation {
            name: "table1".to_string(),
        });
        assert_eq!(
            create_table(
                State(repo.clone()),
                Path("test".to_string()),
                table_creation.clone()
            )
            .await
            .unwrap(),
            StatusCode::CREATED
        );

        // Test create_table with existing table
        assert_eq!(
            create_table(
                State(repo.clone()),
                Path("test".to_string()),
                table_creation.clone()
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::CONFLICT
        );

        // Test table_exists
        assert_eq!(
            table_exists(
                State(repo.clone()),
                Path(("test".to_string(), "table1".to_string()))
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        // Test load_table
        let table = load_table(
            State(repo.clone()),
            Path(("test".to_string(), "table1".to_string())),
        )
        .await
        .unwrap();
        assert_eq!(table.id.name, "table1");

        // Test rename_table
        let rename_request = Json(TableRenameRequest {
            source: TableIdent::new(namespace_ident.clone(), "table1".to_string()),
            destination: TableIdent::new(namespace_ident.clone(), "table2".to_string()),
        });
        assert_eq!(
            rename_table(State(repo.clone()), rename_request.clone())
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            table_exists(
                State(repo.clone()),
                Path(("test".to_string(), "table1".to_string()))
            )
            .await
            .unwrap(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            table_exists(
                State(repo.clone()),
                Path(("test".to_string(), "table2".to_string()))
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );

        // Test rename_table with non-existent source table
        let rename_request = Json(TableRenameRequest {
            source: TableIdent::new(namespace_ident.clone(), "non_existent".to_string()),
            destination: TableIdent::new(namespace_ident.clone(), "table3".to_string()),
        });
        assert_eq!(
            rename_table(State(repo.clone()), rename_request.clone())
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );

        // Test rename_table with existing destination table
        let rename_request = Json(TableRenameRequest {
            source: TableIdent::new(namespace_ident.clone(), "table2".to_string()),
            destination: TableIdent::new(namespace_ident.clone(), "table2".to_string()),
        });
        assert_eq!(
            rename_table(State(repo.clone()), rename_request.clone())
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );

        // Test delete_table
        assert_eq!(
            delete_table(
                State(repo.clone()),
                Path(("test".to_string(), "table2".to_string()))
            )
            .await
            .unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            table_exists(
                State(repo.clone()),
                Path(("test".to_string(), "table2".to_string()))
            )
            .await
            .unwrap(),
            StatusCode::NOT_FOUND
        );

        // Test delete_table with non-existent table
        assert_eq!(
            delete_table(
                State(repo.clone()),
                Path(("test".to_string(), "non_existent".to_string()))
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::NOT_FOUND
        );
    }
}
