use crate::routes::{namespace, table};
use axum::routing::IntoMakeService;
use axum::Router;
use tower_http::trace::TraceLayer;

pub fn routes() -> Router {


    let app_router = Router::new()
    .nest("/api/tables", table::routes())
    .nest("/api/namespaces", namespace::routes());

    app_router
}
