use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceData {
    pub name: String,
    pub properties: Value,
}
