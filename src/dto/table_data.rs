use crate::dto::column_data::ColumnData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::namespace_data::NamespaceIdent;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableData {
    pub name: String,
    pub num_columns: u64,
    pub read_properties: Value,
    pub write_properties: Value,
    pub file_urls: Vec<String>,
    pub columns: Vec<ColumnData>,
}

/// TableIdent represents the identifier of a table in the catalog.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdent {
    /// Namespace of the table.
    pub namespace: NamespaceIdent,
    /// Table name.
    pub name: String,
}

impl TableIdent {
    /// Create a new table identifier.
    pub fn new(namespace: NamespaceIdent, name: String) -> Self {
        Self { namespace, name }
    }

    /// Get the namespace of the table.
    /// this returns the identifier of the namespace
    pub fn namespace(&self) -> &NamespaceIdent {
        &self.namespace
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        &self.name
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_table_data_serialization() {
        let column_data = ColumnData {
            name: "test_column".to_string(),
            aggregates: json!({"count": 100, "sum": 200}),
            value_range: (10, 20),
            is_strong_key: true,
            is_weak_key: false,
            primary_key_col_name: "id".to_string(),
        };
        let table_data = TableData {
            name: "test_table".to_string(),
            num_columns: 1,
            read_properties: json!({"property1": "value1"}),
            write_properties: json!({"property2": "value2"}),
            file_urls: vec!["url1".to_string(), "url2".to_string()],
            columns: vec![column_data],
        };

        let serialized = serde_json::to_string(&table_data).unwrap();
        let expected = r#"{"name":"test_table","num_columns":1,"read_properties":{"property1":"value1"},"write_properties":{"property2":"value2"},"file_urls":["url1","url2"],"columns":[{"name":"test_column","aggregates":{"count":100,"sum":200},"value_range":[10,20],"is_strong_key":true,"is_weak_key":false,"primary_key_col_name":"id"}]}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_table_data_deserialization() {
        let data = r#"{"name":"test_table","num_columns":1,"read_properties":{"property1":"value1"},"write_properties":{"property2":"value2"},"file_urls":["url1","url2"],"columns":[{"name":"test_column","aggregates":{"count":100,"sum":200},"value_range":[10,20],"is_strong_key":true,"is_weak_key":false,"primary_key_col_name":"id"}]}"#;
        let table_data: TableData = serde_json::from_str(data).unwrap();

        assert_eq!(table_data.name, "test_table");
        assert_eq!(table_data.num_columns, 1);
        assert_eq!(table_data.read_properties, json!({"property1": "value1"}));
        assert_eq!(table_data.write_properties, json!({"property2": "value2"}));
        assert_eq!(table_data.file_urls, vec!["url1", "url2"]);
        assert_eq!(table_data.columns.len(), 1);
        assert_eq!(table_data.columns[0].name, "test_column");
        assert_eq!(
            table_data.columns[0].aggregates,
            json!({"count": 100, "sum": 200})
        );
        assert_eq!(table_data.columns[0].value_range, (10, 20));
        assert_eq!(table_data.columns[0].is_strong_key, true);
        assert_eq!(table_data.columns[0].is_weak_key, false);
        assert_eq!(table_data.columns[0].primary_key_col_name, "id");
    }
}
