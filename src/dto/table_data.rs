use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::dto::column_data::ColumnData;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableData {
    pub name: String,
    pub num_columns: u64,
    pub read_properties: Value,
    pub write_properties: Value,
    pub file_urls: Vec<String>,
    pub columns: Vec<ColumnData>,
}
