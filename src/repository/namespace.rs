use crate::database::database::Database;
use crate::dto::namespace_data::{NamespaceData, NamespaceIdent};
use serde_json::{json, Map, Value};
use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};

pub struct NamespaceRepository {
    database: Arc<Mutex<Database>>,
}

impl NamespaceRepository {
    pub fn new(database: Arc<Mutex<Database>>) -> Self {
        Self { database }
    }

    pub fn list_all_namespaces(&self) -> io::Result<Vec<NamespaceIdent>> {
        let db = self.database.lock().unwrap();
        db.list_all_keys("NamespaceData")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn create_namespace(
        &self,
        name: NamespaceIdent,
        properties: Option<Value>,
    ) -> io::Result<()> {
        let namespace_data = NamespaceData {
            name: name.clone(),
            properties: properties.unwrap_or_else(|| json!({"last_modified_time": current_time()})),
        };
        let db = self.database.lock().unwrap();
        db.insert("NamespaceData", &name, &namespace_data)
    }

    pub fn delete_namespace(&self, name: &NamespaceIdent) -> io::Result<()> {
        let db = self.database.lock().unwrap();
        db.delete("NamespaceData", name)
    }

    pub fn load_namespace(&self, name: &NamespaceIdent) -> io::Result<Option<NamespaceData>> {
        let db = self.database.lock().unwrap();
        db.get::<NamespaceIdent, NamespaceData>("NamespaceData", &name)
    }

    pub fn namespace_exists(&self, name: &NamespaceIdent) -> io::Result<bool> {
        let db = self.database.lock().unwrap();
        db.get::<NamespaceIdent, NamespaceData>("NamespaceData", &name)
            .map(|data| data.is_some())
    }

    pub fn set_namespace_properties(
        &self,
        name: NamespaceIdent,
        removals: Vec<String>,
        updates: Map<String, Value>,
    ) -> io::Result<()> {
        let db = self.database.lock().unwrap();
        // Get the current properties
        let namespace_data: NamespaceData = match db.get("NamespaceData", &name)? {
            Some(data) => data,
            None => {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("Namespace {} not found", name.0.join("\u{1F}")),
                ))
            }
        };

        // Convert the properties to a mutable Map
        let mut p = namespace_data.get_properties().clone();
        let properties = p
            .as_object_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "Properties value is not an object"))?;

        // Remove properties
        for key in removals {
            properties.remove(&key);
        }

        // Update properties
        for (key, value) in updates {
            properties.insert(key, value);
        }
        let props = Value::Object(properties.clone());
        let name_copy = name.clone();
        // Save the updated properties
        db.update(
            "NamespaceData",
            &name,
            &NamespaceData {
                name: name_copy,
                properties: props,
            },
        )?;

        Ok(())
    }
}

fn current_time() -> String {
    "current_time".to_string()
}

