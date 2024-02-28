mod routes;
mod dto;
mod handlers;
mod config;

use crate::config::parameters;

#[tokio::main]
async fn main() {
    let host = format!("0.0.0.0:{}", parameters::get("PORT"));

    let listener = tokio::net::TcpListener::bind(host).await.unwrap();
    let app = routes::root::routes();
    axum::serve(listener, app).await.unwrap();
}
