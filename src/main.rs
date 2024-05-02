mod config;
mod database;
mod dto;
mod handlers;
mod repository;
mod routes;

use config::parameters;
use database::database::Database;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    parameters::init();
    let host = format!("0.0.0.0:{}", parameters::get("PORT"));

    // Create a Database object
    let db = Database::open("rocksdb").unwrap();

    // Wrap it in an Arc<Mutex<>> for thread safety
    let db = Arc::new(Mutex::new(db));

    let listener = tokio::net::TcpListener::bind(host).await.unwrap();

    // Pass the shared Database object to your routes
    let app = routes::root::routes(db);

    axum::serve(listener, app).await.unwrap();
}
