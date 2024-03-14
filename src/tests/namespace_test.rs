use axum::{http::StatusCode, response::Json};
use axum::extract::Json as JsonExtractor;
use axum::handler::post;
use axum::routing::Router;
use serde_json::json;
use axum::test::extract;

use crate::{create_namespace, list_namespaces, Namespace};

#[tokio::test]
async fn test_list_namespaces() {
    // Create a test router with the list_namespaces route
    let app = Router::new().route("/namespaces", post(list_namespaces));

    // Perform a request to the route
    let response = axum::test::call(&app, axum::test::request::Request::post("/namespaces").body(()).unwrap()).await;

    // Ensure that the response status code is OK
    assert_eq!(response.status(), StatusCode::OK);

    // Ensure that the response body contains the expected JSON data
    let body = extract::<Json<Vec<String>>>(response.into_body()).await.unwrap();
    assert_eq!(body.0, vec!["accounting", "tax", "paid"]);
}

#[tokio::test]
async fn test_create_namespace() {
    // Create a test router with the create_namespace route
    let app = Router::new().route("/namespaces", post(create_namespace));

    // Create a JSON payload representing a new namespace
    let payload = json!({});

    // Perform a request to the route with the JSON payload
    let response = axum::test::call(&app, axum::test::request::Request::post("/namespaces").body(payload.to_string()).unwrap()).await;

    // Ensure that the response status code is OK
    assert_eq!(response.status(), StatusCode::OK);

    // Ensure that the response body contains the expected JSON data
    let body = extract::<Json<Namespace>>(response.into_body()).await.unwrap();
    assert_eq!(body, Json(Namespace {}));
}
