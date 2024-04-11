use crate::database::database::Database;
use crate::dto::namespace_data::NamespaceData;
use serde_json::{json, Value};
use std::io;
use std::sync::{Arc, Mutex};

pub struct NamespaceRepository {
    database: Arc<Mutex<Database>>,
}

impl NamespaceRepository {
    pub fn new(database: Arc<Mutex<Database>>) -> Self {
        Self { database }
    }

    pub fn list_all_namespaces(&self) -> io::Result<Vec<String>> {
        let db = self.database.lock().unwrap();
        db.list_all_keys("NamespaceData")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn create_namespace(&self, name: String, properties: Option<Value>) -> io::Result<()> {
        let name_str: &str = name.as_str();
        let namespace_data = NamespaceData {
            name: name_str.to_string(),
            properties: properties.unwrap_or_else(|| json!({"last_modified_time": current_time()})),
        };
        let db = self.database.lock().unwrap();
        db.insert("NamespaceData", name_str, &namespace_data)
    }

    pub fn load_namespace(&self, name: &str) -> io::Result<Option<NamespaceData>> {
        let db = self.database.lock().unwrap();
        db.get("NamespaceData", name)
    }

    pub fn namespace_exists(&self, name: &str) -> io::Result<bool> {
        let db = self.database.lock().unwrap();
        db.get::<NamespaceData>("NamespaceData", name)
            .map(|data| data.is_some())
    }

    pub fn delete_namespace(&self, name: &str) -> io::Result<()> {
        let db = self.database.lock().unwrap();
        db.delete("NamespaceData", name)
    }

    pub fn set_namespace_properties(&self, name: &str, properties: Value) -> io::Result<()> {
        if let Some(mut namespace_data) = self.load_namespace(name)? {
            namespace_data.properties = properties;
            let db = self.database.lock().unwrap();
            db.update("NamespaceData", name, &namespace_data)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Namespace not found",
            ))
        }
    }
}

fn current_time() -> String {
    "current_time".to_string()
}
