use crate::dto::namespace_data::NamespaceData;
use crate::repository::namespace::NamespaceRepository;
use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use serde_json::Value;
use std::sync::Arc;

/*
    TODO:
    if a namespace or table already exists, you might want to return a StatusCode::CONFLICT
    instead of StatusCode::INTERNAL_SERVER_ERROR. Similarly, if a namespace or table is not found,
    you might want to return a StatusCode::NOT_FOUND.
*/
pub async fn list_namespaces(
    State(repo): State<Arc<NamespaceRepository>>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
    repo.list_all_namespaces()
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn create_namespace(
    State(repo): State<Arc<NamespaceRepository>>,
    new_namespace: Json<NamespaceData>,
) -> Result<Json<NamespaceData>, (StatusCode, String)> {
    repo.create_namespace(
        new_namespace.get_name(),
        Some(new_namespace.get_properties()),
    )
    .map(|_| new_namespace)
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn load_namespace_metadata(
    State(repo): State<Arc<NamespaceRepository>>,
    Path(namespace): Path<String>,
) -> Result<Json<NamespaceData>, (StatusCode, String)> {
    match repo.load_namespace(namespace.as_str()) {
        Ok(Some(metadata)) => Ok(Json(metadata)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            format!("Namespace {} not found", namespace),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e))),
    }
}

pub async fn namespace_exists(
    State(repo): State<Arc<NamespaceRepository>>,
    Path(namespace): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.namespace_exists(namespace.as_str())
        .map(|exists| {
            if exists {
                StatusCode::FOUND
            } else {
                StatusCode::NOT_FOUND
            }
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn drop_namespace(
    State(repo): State<Arc<NamespaceRepository>>,
    Path(namespace): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.delete_namespace(namespace.as_str())
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn set_namespace_properties(
    State(repo): State<Arc<NamespaceRepository>>,
    Path(namespace): Path<String>,
    properties: Json<Value>,
) -> Result<StatusCode, (StatusCode, String)> {
    repo.set_namespace_properties(namespace.as_str(), properties.0)
        .map(|_| StatusCode::OK)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

// todo: check commented tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::database::Database;
    use axum::http::StatusCode;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_list_namespaces() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let data1 = list_namespaces(State(repo)).await.unwrap();
        let data2: Json<Vec<String>> = Json(vec![]);
        assert!(*data1 == *data2);
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "namespace".to_string(),
            properties: json!({}),
        });
        assert_eq!(
            create_namespace(State(repo), new_namespace.clone())
                .await
                .unwrap()
                .name,
            new_namespace.name
        );
    }

    #[tokio::test]
    async fn test_load_namespace_metadata() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "namespace".to_string(),
            properties: json!({}),
        });
        let _ = create_namespace(State(repo.clone()), new_namespace.clone())
            .await
            .unwrap();

        assert_eq!(
            load_namespace_metadata(State(repo), Path("namespace".to_string()))
                .await
                .unwrap()
                .name,
            new_namespace.name
        );
    }

    #[tokio::test]
    async fn test_namespace_exists() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "namespace".to_string(),
            properties: json!({}),
        });
        let _ = create_namespace(State(repo.clone()), new_namespace)
            .await
            .unwrap();
        assert_eq!(
            namespace_exists(State(repo), Path("namespace".to_string()))
                .await
                .unwrap(),
            StatusCode::FOUND
        );
    }

    #[tokio::test]
    async fn test_drop_namespace() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "namespace".to_string(),
            properties: json!({}),
        });
        let _ = create_namespace(State(repo.clone()), new_namespace)
            .await
            .unwrap();
        assert_eq!(
            drop_namespace(State(repo), Path("namespace".to_string()))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn test_set_namespace_properties() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "namespace".to_string(),
            properties: json!({}),
        });
        let _ = create_namespace(State(repo.clone()), new_namespace)
            .await
            .unwrap();
        assert_eq!(
            set_namespace_properties(
                State(repo),
                Path("namespace".to_string()),
                Json(json!({"property": "value"}))
            )
            .await
            .unwrap(),
            StatusCode::OK
        );
    }

    // Negative cases
    #[tokio::test]
    async fn test_load_namespace_metadata_not_found() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        assert_eq!(
            load_namespace_metadata(State(repo), Path("nonexistent".to_string()))
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn test_namespace_exists_not_found() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        assert_eq!(
            namespace_exists(State(repo), Path("nonexistent".to_string()))
                .await
                .unwrap(),
            StatusCode::NOT_FOUND
        );
    }

    /*
    #[tokio::test]
    async fn test_drop_namespace_not_found() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        assert_eq!(
            drop_namespace(State(repo), Path("nonexistent".to_string()))
                .await
                .unwrap_err()
                .0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    */

    #[tokio::test]
    async fn test_set_namespace_properties_not_found() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        assert_eq!(
            set_namespace_properties(
                State(repo),
                Path("nonexistent".to_string()),
                Json(json!({"property": "value"}))
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /*
    // Corner cases
    #[tokio::test]
    async fn test_create_namespace_empty_name() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "".to_string(),
            properties: json!({}),
        });
        assert_eq!(
            create_namespace(State(repo), new_namespace)
                .await
                .unwrap_err()
                .0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn test_create_namespace_already_exists() {
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        let new_namespace = Json(NamespaceData {
            name: "namespace".to_string(),
            properties: json!({}),
        });
        let _ = create_namespace(State(repo.clone()), new_namespace.clone())
            .await
            .unwrap();
        assert_eq!(
            create_namespace(State(repo), new_namespace)
                .await
                .unwrap_err()
                .0,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
    */
}
