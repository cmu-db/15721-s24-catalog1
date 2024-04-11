use crate::routes::{namespace, table};
use axum::Router;

pub fn routes() -> Router {
    // merge the 2 routes
    let app_router = Router::new()
        .nest("/", table::routes())
        .nest("/", namespace::routes());

    app_router
}
