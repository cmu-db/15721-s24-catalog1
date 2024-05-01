use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize, Serialize)]
pub struct SetNamespacePropertiesRequest {
    pub removals: Vec<String>,
    pub updates: Map<String, Value>,
}
