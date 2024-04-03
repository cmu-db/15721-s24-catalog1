mod config;
mod database;
mod dto;
mod handlers;
mod repository;
mod routes;
mod tests;

use config::parameters;

#[tokio::main]
async fn main() {
    parameters::init();
    let host = format!("0.0.0.0:{}", parameters::get("PORT"));

    let listener = tokio::net::TcpListener::bind(host).await.unwrap();
    let app = routes::root::routes();
    axum::serve(listener, app).await.unwrap();
}
