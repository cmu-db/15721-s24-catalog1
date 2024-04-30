use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorModel {
    pub message: String,
    #[serde(rename = "type")]  // Use serde rename attribute to match the property name in JSON
    pub error_type: String,
    pub code: i32,
    #[serde(default)]  // Use serde default attribute to set a default value for an optional field
    pub stack: Vec<String>
}

pub enum ErrorType {

}