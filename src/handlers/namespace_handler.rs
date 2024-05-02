use crate::dto::namespace_data::{NamespaceData, NamespaceIdent};
use crate::dto::set_namespace_properties_req::SetNamespacePropertiesRequest;
use crate::repository::namespace::NamespaceRepository;

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use serde_json::{json, Value};
use std::sync::Arc;

/*
    TODO:
    Parent Namespace
*/
pub async fn list_namespaces(
    State(repo): State<Arc<NamespaceRepository>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    match repo.list_all_namespaces() {
        Ok(namespaces) => {
            let json_object = json!({
                "namespaces": namespaces
            });
            Ok(Json(json_object))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Internal server error: {}", e),
        )),
    }
}

pub async fn create_namespace(
    State(repo): State<Arc<NamespaceRepository>>,
    new_namespace: Json<NamespaceData>,
) -> Result<Json<NamespaceData>, (StatusCode, String)> {
    if repo.namespace_exists(new_namespace.get_name()).unwrap() {
        return Err((StatusCode::CONFLICT, format!("namespace already exists")));
    }
    repo.create_namespace(
        new_namespace.get_name().clone(),
        Some(new_namespace.get_properties().clone()),
    )
    .map(|_| new_namespace)
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn load_namespace_metadata(
    State(repo): State<Arc<NamespaceRepository>>,
    Path(namespace): Path<String>,
) -> Result<Json<NamespaceData>, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    match repo.load_namespace(&id) {
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
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    repo.namespace_exists(&id)
        .map(|exists| {
            if exists {
                // Ideally this should be FOUND but Iceberg spec says No content
                StatusCode::NO_CONTENT
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
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );
    if !repo.namespace_exists(&id).unwrap() {
        return Err((StatusCode::NOT_FOUND, format!("namespace does not exist")));
    }

    repo.delete_namespace(&id)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

pub async fn set_namespace_properties(
    State(repo): State<Arc<NamespaceRepository>>,
    Path(namespace): Path<String>,
    request_body: Json<SetNamespacePropertiesRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = NamespaceIdent::new(
        namespace
            .split('\u{1F}')
            .map(|part| part.to_string())
            .collect(),
    );

    if !repo.namespace_exists(&id).unwrap() {
        return Err((StatusCode::NOT_FOUND, format!("namespace does not exist")));
    }

    // Check if a property key was included in both `removals` and `updates`

    repo.set_namespace_properties(
        id,
        request_body.removals.clone(),
        request_body.updates.clone(),
    )
    .map(|_| StatusCode::OK)
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::database::Database;
    use crate::dto::set_namespace_properties_req::SetNamespacePropertiesRequest;
    use axum::http::StatusCode;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    #[tokio::test]
    async fn test_namespace_endpoints() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let repo = Arc::new(NamespaceRepository::new(Arc::new(Mutex::new(
            Database::open(tempdir().unwrap().path()).unwrap(),
        ))));
        // Test create_namespace
        let new_namespace = Json(NamespaceData {
            name: NamespaceIdent(vec!["test".to_string()]),
            properties: json!({"property1": "value1"}),
        });
        assert_eq!(
            create_namespace(State(repo.clone()), new_namespace.clone())
                .await
                .unwrap()
                .name,
            new_namespace.name
        );

        // Test namespace_exists
        assert_eq!(
            namespace_exists(State(repo.clone()), Path("test".to_string()))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );

        // Test load_namespace_metadata
        assert_eq!(
            load_namespace_metadata(State(repo.clone()), Path("test".to_string()))
                .await
                .unwrap()
                .name,
            new_namespace.name
        );

        // Test set_namespace_properties
        let set_namespace_properties_request = Json(SetNamespacePropertiesRequest {
            removals: vec!["property1".to_string()],
            updates: serde_json::from_value(json!({"property2": "value2"})).unwrap(),
        });
        assert_eq!(
            set_namespace_properties(
                State(repo.clone()),
                Path("test".to_string()),
                set_namespace_properties_request
            )
            .await
            .unwrap(),
            StatusCode::OK
        );

        // Test load_namespace_metadata after set_namespace_properties
        assert_eq!(
            load_namespace_metadata(State(repo.clone()), Path("test".to_string()))
                .await
                .unwrap()
                .name,
            Json(NamespaceData {
                name: NamespaceIdent(vec!["test".to_string()]),
                properties: json!({"property2": "value2"}),
            })
            .name
        );

        // Test drop_namespace
        assert_eq!(
            drop_namespace(State(repo.clone()), Path("test".to_string()))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );

        // Test namespace_exists after drop_namespace
        assert_eq!(
            namespace_exists(State(repo.clone()), Path("test".to_string()))
                .await
                .unwrap(),
            StatusCode::NOT_FOUND
        );
    }
}
