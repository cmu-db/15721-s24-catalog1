use axum::{ routing::{get, post, head, delete}, Router};
use crate::handlers::namespace_handler;

pub fn routes() -> Router<> {
    let router = Router::new()
        .route("/namespaces", get(namespace_handler::list_namespaces))
        .route("/namespaces", post(namespace_handler::create_namespace))
        .route("/namespace/:namespace", get(namespace_handler::load_namespace_metadata))
        .route("/namespace/:namespace", head(namespace_handler::namespace_exists))
        .route("/namespace/:namespace", delete(namespace_handler::drop_namespace))
        .route("/namespace/:namespace/properties", post(namespace_handler::set_namespace_properties));
    return router;
}
