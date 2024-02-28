
use crate::database::database::Database;
use crate::dto::namespace_data::NamespaceData;
use serde_json::{Value, json};
use std::io;

pub struct NamespaceRepository {
    db: Database,
}

impl NamespaceRepository {
    pub fn new(db_path: &str) -> io::Result<Self> {
        let db = Database::open(db_path)?;
        Ok(Self { db })
    }

    pub fn list_all_namespaces(&self) -> io::Result<Vec<String>> {
        // This function depends on the specific implementation of MyDB
        // and whether it supports listing all keys in a column family.
        self.db.list_all_keys("NamespaceData").map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn create_namespace(&self, name: &str, properties: Option<Value>) -> io::Result<()> {
        let namespace_data = NamespaceData {
            name: name.to_string(),
            properties: properties.unwrap_or_else(|| json!({"last_modified_time": current_time()})),
        };
        self.db.insert("NamespaceData", name, &namespace_data)
    }

    pub fn load_namespace(&self, name: &str) -> io::Result<Option<NamespaceData>> {
        self.db.get("NamespaceData", name)
    }

    pub fn namespace_exists(&self, name: &str) -> io::Result<bool> {
        self.db.get::<NamespaceData>("NamespaceData", name).map(|data| data.is_some())
    }

    pub fn delete_namespace(&self, name: &str) -> io::Result<()> {
        self.db.delete("NamespaceData", name)
    }

    pub fn set_namespace_properties(&self, name: &str, properties: Value) -> io::Result<()> {
        if let Some(mut namespace_data) = self.load_namespace(name)? {
            namespace_data.properties = properties;
            self.db.update("NamespaceData", name, &namespace_data)
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Namespace not found"))
        }
    }
}

fn current_time() -> String {
    // This function returns the current time as a string.
    // It's a placeholder and should be replaced with your actual implementation.
    "current_time".to_string()
}
