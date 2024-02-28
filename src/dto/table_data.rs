use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableData {
    pub name: String,
    pub num_columns: u64,
    pub read_properties: Value,
    pub write_properties: Value,
    pub file_urls: Vec<String>,
    pub columns: Vec<Vec<String>>,
    pub aggregates: Value,
    pub value_range: (i32, i32),
    pub is_strong_key: bool,
    pub is_weak_key: bool,
    pub primary_key_col_name: String,
}
