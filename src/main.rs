mod database;
mod dto;
mod repository;

use crate::database::database::Database;
use repository::namespace::NamespaceRepository;
use serde_json::json;
use std::sync::Arc;

fn main() -> std::io::Result<()> {
    let db_path = "rocksdb";
    let db = Arc::new(Database::open(db_path)?);
    let namespace_repo = NamespaceRepository::new(db.clone());

    // Test creating a namespace
    let properties = Some(json!({"property1": "value1", "property2": "value2"}));
    namespace_repo.create_namespace("test_namespace", properties)?;

    // Test listing all namespaces
    let namespaces = namespace_repo.list_all_namespaces()?;
    println!("Namespaces: {:?}", namespaces);

    // Test loading a namespace
    let namespace_data = namespace_repo.load_namespace("test_namespace")?;
    println!("Namespace data: {:?}", namespace_data);

    // Test checking if a namespace exists
    let exists = namespace_repo.namespace_exists("test_namespace")?;
    println!("Namespace exists: {}", exists);

    // Test setting namespace properties
    let new_properties = json!({"property1": "new_value1", "property3": "value3"});
    namespace_repo.set_namespace_properties("test_namespace", new_properties)?;

    // Test deleting a namespace
    namespace_repo.delete_namespace("test_namespace")?;

    Ok(())
}
