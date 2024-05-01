use crate::database::database::Database;
use crate::routes::{namespace, table};
use axum::Router;
use std::sync::{Arc, Mutex};

pub fn routes(db: Arc<Mutex<Database>>) -> Router {
    // Pass the shared Database object to your routes
    let app_router = Router::new()
        .nest("/", table::routes(db.clone()))
        .nest("/", namespace::routes(db.clone()));

    app_router
}
