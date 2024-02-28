mod routes;
mod dto;
mod handlers;

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    let app = routes::root::routes();
    axum::serve(listener, app).await.unwrap();
}
