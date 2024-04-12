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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_column_data_serialization() {
        let column_data = ColumnData {
            name: "test_column".to_string(),
            aggregates: json!({"count": 100, "sum": 200}),
            value_range: (10, 20),
            is_strong_key: true,
            is_weak_key: false,
            primary_key_col_name: "id".to_string(),
        };

        let serialized = serde_json::to_string(&column_data).unwrap();
        let expected = r#"{"name":"test_column","aggregates":{"count":100,"sum":200},"value_range":[10,20],"is_strong_key":true,"is_weak_key":false,"primary_key_col_name":"id"}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_column_data_deserialization() {
        let data = r#"{"name":"test_column","aggregates":{"count":100,"sum":200},"value_range":[10,20],"is_strong_key":true,"is_weak_key":false,"primary_key_col_name":"id"}"#;
        let column_data: ColumnData = serde_json::from_str(data).unwrap();

        assert_eq!(column_data.name, "test_column");
        assert_eq!(column_data.aggregates, json!({"count": 100, "sum": 200}));
        assert_eq!(column_data.value_range, (10, 20));
        assert_eq!(column_data.is_strong_key, true);
        assert_eq!(column_data.is_weak_key, false);
        assert_eq!(column_data.primary_key_col_name, "id");
    }
}
