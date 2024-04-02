use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceData {
    pub name: String,
    pub properties: Value,
}

impl NamespaceData{
    pub fn get_name(&self) -> String{
        self.name.clone()
    }

    pub fn get_properties(&self) -> Value {
        self.properties.clone()
    }
}
