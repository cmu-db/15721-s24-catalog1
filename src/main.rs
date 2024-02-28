mod db;
mod dto;

use crate::db::db::Database;
use serde_json::json;
use dto::namespace_data::NamespaceData;
use dto::operator_statistics::OperatorStatistics;
use dto::table_data::TableData;

fn main() -> Result<(), std::io::Error> {
    let db = Database::open("rocksdb")?;

    // Create some data
    let namespace_data = NamespaceData {
        name: "my_namespace".to_string(),
        properties: json!({"key": "value"}),
    };

    let table_data = TableData {
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
        primary_key_col_name: "id".to_string()
    };

    let operator_statistics = OperatorStatistics {
        operator_string: "my_operator".to_string(),
        cardinality_prev_result: 10,
    };

    // Insert the data
    db.insert("NamespaceData", &namespace_data.name, &namespace_data)?;
    db.insert("TableData", &table_data.name, &table_data)?;
    db.insert("OperatorStatistics", &operator_statistics.operator_string, &operator_statistics)?;

    // Get the data
    let namespace_data: Option<NamespaceData> = db.get("NamespaceData", &namespace_data.name)?;
    let table_data: Option<TableData> = db.get("TableData", &table_data.name)?;
    let operator_statistics: Option<OperatorStatistics> = db.get("OperatorStatistics", &operator_statistics.operator_string)?;

    println!("NamespaceData: {:?}", namespace_data);
    println!("TableData: {:?}", table_data);
    println!("OperatorStatistics: {:?}", operator_statistics);

    // Update the data
    let updated_namespace_data = NamespaceData {
        name: "my_namespace".to_string(),
        properties: json!({"key": "new_value"}),
    };
    db.update("NamespaceData", &updated_namespace_data.name, &updated_namespace_data)?;
    
    // Get the updated data
    let namespace_data: Option<NamespaceData> = db.get("NamespaceData", &updated_namespace_data.name)?;
    println!("NamespaceData: {:?}", namespace_data);

    // Delete the data
    if let Some(namespace_data) = namespace_data {
        db.delete("NamespaceData", &namespace_data.name)?;
    }
    if let Some(table_data) = table_data {
        db.delete("NamespaceData", &table_data.name)?;
    }
    if let Some(operator_statistics) = operator_statistics {
        db.delete("NamespaceData", &operator_statistics.operator_string)?;
    }
    // Close the database
    db.close();

    Ok(())
}
