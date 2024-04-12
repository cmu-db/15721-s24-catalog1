use crate::database::database::Database;
use crate::handlers::namespace_handler;
use axum::{
    routing::{delete, get, head, post},
    Router,
};
use std::sync::{Arc, Mutex};

use crate::repository::namespace::NamespaceRepository;

pub fn routes(db: Arc<Mutex<Database>>) -> Router {
    let repo = Arc::new(NamespaceRepository::new(db));

    let router = Router::new()
        .route("/namespaces", get(namespace_handler::list_namespaces))
        .route("/namespaces", post(namespace_handler::create_namespace))
        .route(
            "/namespace/:namespace",
            get(namespace_handler::load_namespace_metadata),
        )
        .route(
            "/namespace/:namespace",
            head(namespace_handler::namespace_exists),
        )
        .route(
            "/namespace/:namespace",
            delete(namespace_handler::drop_namespace),
        )
        .route(
            "/namespace/:namespace/properties",
            post(namespace_handler::set_namespace_properties),
        )
        .with_state(repo);

    return router;
}
