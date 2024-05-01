use crate::database::database::Database;
use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::TableData;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};

pub struct TableRepository {
    database: Arc<Mutex<Database>>,
}

impl TableRepository {
    pub fn new(database: Arc<Mutex<Database>>) -> Self {
        Self { database }
    }

    pub fn list_all_tables(&self, namespace: String) -> Result<Option<Vec<String>>, Error> {
        let db = self.database.lock().unwrap();
        db.get::<String, Vec<String>>("TableNamespaceMap", &namespace)
    }

    pub fn create_table(&self, namespace: String, table: &TableData) -> Result<(), Error> {
        let db = self.database.lock().unwrap();
        db.insert("TableData", &table.name, table)?;
        let mut tables = db
            .get::<String, Vec<String>>("TableNamespaceMap", &namespace)
            .unwrap()
            .unwrap_or_else(|| vec![]);
        tables.push(table.name.clone());
        let r_val = db.insert("TableNamespaceMap", &namespace, &tables);
        r_val
    }

    pub fn load_table(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<Option<TableData>, Error> {
        // Check if the table is in the given namespace
        let tables_in_namespace = self.list_all_tables(namespace)?;
        if let Some(tables) = tables_in_namespace {
            if !tables.contains(&table_name.to_string()) {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "Table not found in the given namespace",
                ));
            }
        }
        let db = self.database.lock().unwrap();
        // If the table is in the namespace, get the table data
        db.get::<String, TableData>("TableData", &table_name)
    }

    pub fn drop_table(&self, namespace: String, table_name: String) -> Result<(), Error> {
        let db = self.database.lock().unwrap();
        db.delete("TableData", &table_name)?;
        let mut tables = db
            .get::<String, Vec<String>>("TableNamespaceMap", &namespace)
            .unwrap()
            .unwrap();
        tables.retain(|name| name != &table_name);
        db.insert("TableNamespaceMap", &namespace, &tables)
    }

    // for the ?? route
    pub fn insert_table(&self, namespace: String, table: &TableData) -> Result<(), Error> {
        self.create_table(namespace, table)
    }

    pub fn table_exists(&self, namespace: String, table_name: String) -> Result<bool, Error> {
        let table = self.load_table(namespace, table_name)?;
        Ok(table.is_some())
    }

    pub fn rename_table(&self, rename_request: &TableRenameRequest) -> Result<(), Error> {
        let namespace = rename_request.namespace;
        let old_name = rename_request.old_name;
        let new_name = rename_request.new_name;
        let table = self
            .load_table(namespace, old_name)?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Table not found"))?;
        let mut new_table = table.clone();
        new_table.name = new_name.clone();
        self.drop_table(namespace, old_name)?;
        self.create_table(namespace, &new_table)
    }
}

// todo: check commented tests
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use serde_json::json;
//     use std::sync::{Arc, Mutex};
//     use tempfile::tempdir;

//     #[test]
//     fn test_list_all_tables() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         assert_eq!(repo.list_all_tables("namespace").unwrap(), None);
//     }

//     #[test]
//     fn test_create_table() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         assert!(repo.create_table("namespace", &table).is_ok());
//     }

//     #[test]
//     fn test_load_table() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         repo.create_table("namespace", &table).unwrap();
//         assert!(repo.load_table("namespace", "table").unwrap().is_some());
//     }

//     #[test]
//     fn test_drop_table() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         repo.create_table("namespace", &table).unwrap();
//         assert!(repo.drop_table("namespace", "table").is_ok());
//     }

//     #[test]
//     fn test_table_exists() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         repo.create_table("namespace", &table).unwrap();
//         assert!(repo.table_exists("namespace", "table").unwrap());
//     }

//     #[test]
//     fn test_rename_table() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         repo.create_table("namespace", &table).unwrap();
//         let rename_request = TableRenameRequest {
//             namespace: "namespace".to_string(),
//             old_name: "table".to_string(),
//             new_name: "new_table".to_string(),
//         };
//         assert!(repo.rename_table(&rename_request).is_ok());
//     }

//     #[test]
//     fn test_load_table_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         assert!(repo
//             .load_table("namespace", "nonexistent")
//             .unwrap()
//             .is_none());
//     }

//     #[test]
//     fn test_table_exists_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         assert!(!repo.table_exists("namespace", "nonexistent").unwrap());
//     }

//     /*
//     #[test]
//     fn test_drop_table_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         assert!(repo.drop_table("namespace", "nonexistent").is_err());
//     }
//     */

//     #[test]
//     fn test_rename_table_not_found() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let rename_request = TableRenameRequest {
//             namespace: "namespace".to_string(),
//             old_name: "nonexistent".to_string(),
//             new_name: "new_table".to_string(),
//         };
//         assert!(repo.rename_table(&rename_request).is_err());
//     }

//     /*
//     #[test]
//     fn test_create_table_empty_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         assert!(repo.create_table("namespace", &table).is_err());
//     }

//     #[test]
//     fn test_create_table_already_exists() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         repo.create_table("namespace", &table).unwrap();
//         assert!(repo.create_table("namespace", &table).is_err());
//     }
//     */

//     #[test]
//     fn test_load_table_empty_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         assert!(repo.load_table("namespace", "").unwrap().is_none());
//     }
//     /*
//     #[test]
//     fn test_drop_table_empty_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         assert!(repo.drop_table("namespace", "").is_err());
//     }

//     #[test]
//     fn test_rename_table_empty_new_name() {
//         let db = Arc::new(Mutex::new(
//             Database::open(tempdir().unwrap().path()).unwrap(),
//         ));
//         let repo = TableRepository::new(db);
//         let table = TableData {
//             name: "table".to_string(),
//             num_columns: 0,
//             read_properties: json!({}),
//             write_properties: json!({}),
//             file_urls: vec![],
//             columns: vec![],
//         };
//         repo.create_table("namespace", &table).unwrap();
//         let rename_request = TableRenameRequest {
//             namespace: "namespace".to_string(),
//             old_name: "table".to_string(),
//             new_name: "".to_string(),
//         };
//         assert!(repo.rename_table(&rename_request).is_err());
//     }
//     */
// }
