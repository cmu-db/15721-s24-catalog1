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

//     pub fn load_namespace(&self, name: &NamespaceIdent) -> io::Result<Option<NamespaceData>> {
//         let db = self.database.lock().unwrap();
//         db.get::<NamespaceIdent, NamespaceData>("NamespaceData", &name)
//     }

//     pub fn delete_namespace(&self, name: &str) -> io::Result<()> {
//         let db = self.database.lock().unwrap();
//         db.delete("NamespaceData", &name)
//     }

//     pub fn set_namespace_properties(&self, name: &NamespaceIdent, properties: Value) -> io::Result<()> {
//         if let Some(mut namespace_data) = self.load_namespace(name)? {
//             namespace_data.properties = properties;
//             let db = self.database.lock().unwrap();
//             db.update("NamespaceData", &name, &namespace_data)
//         } else {
//             Err(io::Error::new(
//                 io::ErrorKind::NotFound,
//                 "Namespace not found",
//             ))
//         }
//     }
// }

fn current_time() -> String {
    "current_time".to_string()
}

// todo: check commented tests

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::{Arc, Mutex};
//     use tempfile::tempdir;

//     #[test]
//     fn test_list_all_namespaces() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert_eq!(repo.list_all_namespaces().unwrap(), Vec::<String>::new());
//     }

//     #[test]
//     fn test_create_namespace() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(repo.create_namespace("test".to_string(), None).is_ok());
//     }

//     #[test]
//     fn test_load_namespace() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         repo.create_namespace("test".to_string(), None).unwrap();
//         assert!(repo.load_namespace("test").unwrap().is_some());
//     }

//     #[test]
//     fn test_namespace_exists() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         repo.create_namespace("test".to_string(), None).unwrap();
//         assert!(repo.namespace_exists("test").unwrap());
//     }

//     #[test]
//     fn test_delete_namespace() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         repo.create_namespace("test".to_string(), None).unwrap();
//         assert!(repo.delete_namespace("test").is_ok());
//     }

//     #[test]
//     fn test_set_namespace_properties() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         repo.create_namespace("test".to_string(), None).unwrap();
//         assert!(repo
//             .set_namespace_properties("test", json!({"property": "value"}))
//             .is_ok());
//     }

//     #[test]
//     fn test_load_namespace_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(repo.load_namespace("nonexistent").unwrap().is_none());
//     }

//     #[test]
//     fn test_namespace_exists_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(!repo.namespace_exists("nonexistent").unwrap());
//     }

//     // #[test]
//     // fn test_delete_namespace_not_found() {
//     //     let db = Arc::new(Mutex::new(Database::open(tempdir().unwrap().path()).unwrap()));
//     //     let repo = NamespaceRepository::new(db);
//     //     assert!(repo.delete_namespace("nonexistent").is_err());
//     // }

//     #[test]
//     fn test_set_namespace_properties_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(repo
//             .set_namespace_properties("nonexistent", json!({"property": "value"}))
//             .is_err());
//     }

//     // #[test]
//     // fn test_create_namespace_empty_name() {
//     //     let db = Arc::new(Mutex::new(Database::open(tempdir().unwrap().path()).unwrap()));
//     //     let repo = NamespaceRepository::new(db);
//     //     assert!(repo.create_namespace("".to_string(), None).is_err());
//     // }

//     // #[test]
//     // fn test_create_namespace_already_exists() {
//     //     let db = Arc::new(Mutex::new(Database::open(tempdir().unwrap().path()).unwrap()));
//     //     let repo = NamespaceRepository::new(db);
//     //     repo.create_namespace("test".to_string(), None).unwrap();
//     //     assert!(repo.create_namespace("test".to_string(), None).is_err());
//     // }

//     #[test]
//     fn test_set_namespace_properties_empty_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(repo
//             .set_namespace_properties("", json!({"property": "value"}))
//             .is_err());
//     }

//     // #[test]
//     // fn test_set_namespace_properties_invalid_json() {
//     //     let db = Arc::new(Mutex::new(Database::open(tempdir().unwrap().path()).unwrap()));
//     //     let repo = NamespaceRepository::new(db);
//     //     repo.create_namespace("test".to_string(), None).unwrap();
//     //     assert!(repo.set_namespace_properties("test", "invalid_json".into()).is_err());
//     // }

//     #[test]
//     fn test_load_namespace_empty_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(repo.load_namespace("").unwrap().is_none());
//     }

//     #[test]
//     fn test_namespace_exists_empty_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(!repo.namespace_exists("").unwrap());
//     }

//     // #[test]
//     // fn test_delete_namespace_empty_name() {
//     //     let db = Arc::new(Mutex::new(Database::open(tempdir().unwrap().path()).unwrap()));
//     //     let repo = NamespaceRepository::new(db);
//     //     assert!(repo.delete_namespace("").is_err());
//     // }

//     #[test]
//     fn test_create_namespace_null_properties() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         assert!(repo
//             .create_namespace("test".to_string(), Some(json!(null)))
//             .is_ok());
//     }

//     #[test]
//     fn test_set_namespace_properties_null() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         repo.create_namespace("test".to_string(), None).unwrap();
//         assert!(repo.set_namespace_properties("test", json!(null)).is_ok());
//     }

//     #[test]
//     fn test_set_namespace_properties_with_empty_json() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = NamespaceRepository::new(db);
//         repo.create_namespace("test".to_string(), None).unwrap();
//         assert!(repo.set_namespace_properties("test", json!({})).is_ok());
//     }
// }
