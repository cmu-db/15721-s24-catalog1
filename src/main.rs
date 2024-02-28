mod dto;
mod db;

use dto::namespace_data::NamespaceData;
use dto::operator_statistics::OperatorStatistics;
use dto::table_data::TableData;
use crate::db::db::Database;

fn main() {
    {
        // Create instances of the structs
        let namespace = NamespaceData {
            name: "MyNamespace".to_string(),
            properties: serde_json::json!({"key": "value"}),
        };

        let table = TableData {
            name: "MyTable".to_string(),
            num_columns: 5,
            read_properties: serde_json::json!({"key": "value"}),
            write_properties: serde_json::json!({"key": "value"}),
            file_urls: vec!["url1".to_string(), "url2".to_string()],
            columns: vec![vec!["column1".to_string(), "column2".to_string()]],
            aggregates: serde_json::json!({"key": "value"}),
            value_range: (1, 100),
            is_strong_key: true,
            is_weak_key: false,
            primary_key_col_name: "id".to_string(),
        };

        let operator_stats = OperatorStatistics {
            operator_string: "MyOperator".to_string(),
            cardinality_prev_result: 10,
        };

        // Use the structs as required
        println!("Namespace: {:?}", namespace);
        println!("Table: {:?}", table);
        println!("Operator Stats: {:?}", operator_stats);
    }

        match Database::new("rocksdb") {
            Ok(db) => {
                // Use the database
                println!("hello, db opened");
            }
            Err(e) => {
                eprintln!("Failed to open database: {}", e);
            }
        }
}
