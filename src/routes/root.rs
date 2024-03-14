use crate::routes::{namespace, table};
use axum::routing::IntoMakeService;
use axum::Router;
use tower_http::trace::TraceLayer;

pub fn routes() -> Router {
    // merge the 2 routes
    let app_router = Router::new()
        .nest("/", table::routes())
        .nest("/", namespace::routes());

    app_router
}
