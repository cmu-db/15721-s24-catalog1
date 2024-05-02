use crate::database::database::Database;
use crate::handlers::table_handler;
use crate::repository::table::TableRepository;
use axum::{
    routing::{delete, get, head, post},
    Router,
};
use std::sync::{Arc, Mutex};

pub fn routes(db: Arc<Mutex<Database>>) -> Router {
    let repo = Arc::new(TableRepository::new(db));
    let router = Router::new()
        .route(
            "/namespaces/:namespace/tables",
            get(table_handler::list_tables),
        )
        .route(
            "/namespaces/:namespace/tables",
            post(table_handler::create_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table",
            get(table_handler::load_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table",
            delete(table_handler::delete_table),
        )
        .route(
            "/namespaces/:namespace/tables/:table",
            head(table_handler::table_exists),
        )
        // .route("/tables/rename", post(table_handler::rename_table))
        .with_state(repo);

    return router;
}
