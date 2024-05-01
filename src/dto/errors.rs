use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;


#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorModel {
  pub message: String,
  pub r#type: String, // Use `r#type` to avoid keyword conflict
  pub code: u16,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub stack: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IcebergErrorResponse {
  pub error: ErrorModel,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommonResponse {
  pub error: Option<IcebergErrorResponse>,
}


#[derive(Debug, Deserialize, Serialize)]
pub struct BadRequestErrorResponse(pub CommonResponse);


#[derive(Debug, Deserialize, Serialize)]
pub struct UnsupportedOperationResponse(pub CommonResponse);

#[derive(Debug, Deserialize, Serialize)]
pub struct ServiceUnavailableResponse(pub CommonResponse);

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerErrorResponse(pub CommonResponse);

#[derive(Debug, Deserialize, Serialize)]
pub enum ErrorTypes {
  BadRequest(String),
  Unauthorized(String),
  ServiceUnavailable(String),
  ServerError(String),
}

impl std::fmt::Display for ErrorTypes {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      ErrorTypes::BadRequest(msg) => write!(f, "Bad Request: {}", msg),
      ErrorTypes::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
      ErrorTypes::ServiceUnavailable(msg) => write!(f, "Service Unavailable: {}", msg),
      ErrorTypes::ServerError(msg) => write!(f, "Internal Server Error: {}", msg),
    }
  }
}