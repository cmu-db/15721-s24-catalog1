use crate::handlers::table_handler;
use axum::{
    routing::{delete, get, head, post},
    Router,
};

pub fn routes() -> Router {
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
            "/namespaces/:namespace/register",
            post(table_handler::register_table),
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
        .route("/tables/rename", post(table_handler::rename_table))
        .route(
            "/namespaces/:namespace/tables/:table/metrics",
            post(table_handler::report_metrics),
        );

    return router;
}
