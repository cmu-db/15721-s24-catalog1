use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableRenameRequest {
    pub namespace: String,
    pub old_name: String,
    pub new_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_rename_request_serialization() {
        let table_rename_request = TableRenameRequest {
            namespace: "test_namespace".to_string(),
            old_name: "old_table_name".to_string(),
            new_name: "new_table_name".to_string(),
        };

        let serialized = serde_json::to_string(&table_rename_request).unwrap();
        let expected = r#"{"namespace":"test_namespace","old_name":"old_table_name","new_name":"new_table_name"}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_table_rename_request_deserialization() {
        let data = r#"{"namespace":"test_namespace","old_name":"old_table_name","new_name":"new_table_name"}"#;
        let table_rename_request: TableRenameRequest = serde_json::from_str(data).unwrap();

        assert_eq!(table_rename_request.namespace, "test_namespace");
        assert_eq!(table_rename_request.old_name, "old_table_name");
        assert_eq!(table_rename_request.new_name, "new_table_name");
    }
}
