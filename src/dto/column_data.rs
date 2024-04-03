use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnData {
    pub name: String,
    pub aggregates: Value,
    pub value_range: (i32, i32), // todo: should this be optional?
    pub is_strong_key: bool,
    pub is_weak_key: bool,
    pub primary_key_col_name: String,
}
