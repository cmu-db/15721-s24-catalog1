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


#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[test]
    fn test_namespace_repository() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let repo = NamespaceRepository::new(db.clone());

        // Test create_namespace
        let namespace_ident = NamespaceIdent(vec!["test".to_string()]);
        let properties = Some(json!({"property1": "value1"}));
        repo.create_namespace(namespace_ident.clone(), properties).unwrap();

        // Test namespace_exists
        assert!(repo.namespace_exists(&namespace_ident).unwrap());

        // Test load_namespace
        let namespace_data = repo.load_namespace(&namespace_ident).unwrap().unwrap();
        assert_eq!(namespace_data.name, namespace_ident);
        assert_eq!(namespace_data.properties, json!({"property1": "value1"}));

        // Test set_namespace_properties
        let removals = vec!["property1".to_string()];
        let mut updates = Map::new();
        updates.insert("property2".to_string(), json!("value2"));
        repo.set_namespace_properties(namespace_ident.clone(), removals, updates).unwrap();

        let updated_namespace_data = repo.load_namespace(&namespace_ident).unwrap().unwrap();
        assert_eq!(updated_namespace_data.properties, json!({"property2": "value2"}));

        // Test delete_namespace
        repo.delete_namespace(&namespace_ident).unwrap();
        assert!(!repo.namespace_exists(&namespace_ident).unwrap());
    }

    #[test]
    fn test_namespace_repository_negative() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let repo = NamespaceRepository::new(db.clone());

        // Test namespace_exists with non-existent namespace
        let non_existent_namespace = NamespaceIdent(vec!["non_existent".to_string()]);
        assert!(!repo.namespace_exists(&non_existent_namespace).unwrap());

        // Test load_namespace with non-existent namespace
        assert!(repo.load_namespace(&non_existent_namespace).unwrap().is_none());

        // Test set_namespace_properties with non-existent namespace
        let mut updates = Map::new();
        updates.insert("property2".to_string(), json!("value2"));
        assert!(repo.set_namespace_properties(non_existent_namespace.clone(), vec![], updates).is_err());
    }
}
