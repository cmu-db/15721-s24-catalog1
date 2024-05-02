use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Deserialize, Serialize)]
pub struct NamespaceNotFoundError {
    pub message: String,
}

impl From<NamespaceNotFoundError> for IcebergErrorResponse {
    fn from(err: NamespaceNotFoundError) -> Self {
        IcebergErrorResponse {
            error: ErrorModel {
                message: err.message,
                r#type: "NamespaceNotFound".to_string(),
                code: 404,
                stack: None,
            },
        }
    }
}

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
    NamespaceNotFound(String),
}

impl std::fmt::Display for ErrorTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorTypes::BadRequest(msg) => write!(f, "Bad Request: {}", msg),
            ErrorTypes::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            ErrorTypes::ServiceUnavailable(msg) => write!(f, "Service Unavailable: {}", msg),
            ErrorTypes::ServerError(msg) => write!(f, "Internal Server Error: {}", msg),
            ErrorTypes::NamespaceNotFound(msg) => write!(f, "Namespace Not Found: {}", msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn test_namespace_not_found_error() {
        let err = NamespaceNotFoundError {
            message: "Namespace 'test' not found".to_string(),
        };
        let iceberg_err: IcebergErrorResponse = err.into();

        assert_eq!(iceberg_err.error.message, "Namespace 'test' not found");
        assert_eq!(iceberg_err.error.r#type, "NamespaceNotFound");
        assert_eq!(iceberg_err.error.code, 404);
        assert!(iceberg_err.error.stack.is_none());
    }



    #[test]
    fn test_error_model_deserialization() {
        let json_str = r#"{
            "message": "Bad request",
            "type": "BadRequest",
            "code": 400,
            "stack": null
        }"#;

        let error_model: ErrorModel = serde_json::from_str(json_str).unwrap();

        assert_eq!(error_model.message, "Bad request");
        assert_eq!(error_model.r#type, "BadRequest");
        assert_eq!(error_model.code, 400);
        assert!(error_model.stack.is_none());
    }



    #[test]
    fn test_error_types_display() {
        let bad_request = ErrorTypes::BadRequest("Invalid request body".to_string());
        let unauthorized = ErrorTypes::Unauthorized("Missing authentication token".to_string());
        let service_unavailable = ErrorTypes::ServiceUnavailable("Server is under maintenance".to_string());
        let server_error = ErrorTypes::ServerError("Internal server error".to_string());
        let namespace_not_found = ErrorTypes::NamespaceNotFound("Namespace 'test' not found".to_string());

        assert_eq!(bad_request.to_string(), "Bad Request: Invalid request body");
        assert_eq!(unauthorized.to_string(), "Unauthorized: Missing authentication token");
        assert_eq!(service_unavailable.to_string(), "Service Unavailable: Server is under maintenance");
        assert_eq!(server_error.to_string(), "Internal Server Error: Internal server error");
        assert_eq!(namespace_not_found.to_string(), "Namespace Not Found: Namespace 'test' not found");
    }
}