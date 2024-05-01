use crate::database::database::Database;
use crate::dto::rename_request::TableRenameRequest;
use crate::dto::column_data::ColumnData;
use crate::dto::table_data::{TableIdent, TableCreation, Table, TableMetadata};
use crate::dto::namespace_data::{NamespaceIdent};
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

pub struct TableRepository {
    database: Arc<Mutex<Database>>,
}

impl TableRepository {
    pub fn new(database: Arc<Mutex<Database>>) -> Self {
        Self { database }
    }

    pub fn list_all_tables(&self, namespace: &NamespaceIdent) -> Result<Option<Vec<TableIdent>>, Error> {
        let db = self.database.lock().unwrap();
        db.get::<NamespaceIdent, Vec<TableIdent>>("TableNamespaceMap", namespace)
    }

    pub fn create_table(&self, namespace: &NamespaceIdent, table_creation: &TableCreation) -> Result<(), Error> {
        let db = self.database.lock().unwrap();
        match db.get("NamespaceData", namespace)? {
            Some(data) => data,
            None => {
                return Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("Namespace {} not found", namespace.clone().0.join("\u{1F}")),
                ))
            }
        };

        let table_id = TableIdent::new(namespace.clone(), table_creation.name.clone());
        let table_uuid = Uuid::new_v4().to_string(); 

        let table_metadata = TableMetadata{
            table_uuid
        };

        db.insert("TableData", &table_creation.name, &Table{id: table_id.clone(), metadata: table_metadata})?;
        let mut tables = db
            .get::<NamespaceIdent, Vec<TableIdent>>("TableNamespaceMap", namespace)
            .unwrap()
            .unwrap_or_else(|| vec![]);
        
        tables.push(table_id.clone());
        let r_val = db.insert("TableNamespaceMap", namespace, &tables);
        r_val
    }


    // pub fn load_table(
    //     &self,
    //     namespace: &str,
    //     table_name: &str,
    // ) -> Result<Option<TableData>, Error> {
    //     // Check if the table is in the given namespace
    //     let tables_in_namespace = self.list_all_tables(namespace)?;
    //     if let Some(tables) = tables_in_namespace {
    //         if !tables.contains(&table_name.to_string()) {
    //             return Err(Error::new(
    //                 ErrorKind::NotFound,
    //                 "Table not found in the given namespace",
    //             ));
    //         }
    //     }
    //     let db = self.database.lock().unwrap();
    //     // If the table is in the namespace, get the table data
    //     db.get::<TableData>("TableData", table_name)
    // }

    // pub fn drop_table(&self, namespace: &str, table_name: &str) -> Result<(), Error> {
    //     let db = self.database.lock().unwrap();
    //     db.delete("TableData", table_name)?;
    //     let mut tables = db
    //         .get::<Vec<String>>("TableNamespaceMap", namespace)
    //         .unwrap()
    //         .unwrap();
    //     tables.retain(|name| name != table_name);
    //     db.insert("TableNamespaceMap", namespace, &tables)
    // }

    // // for the ?? route
    // pub fn insert_table(&self, namespace: &str, table: &TableData) -> Result<(), Error> {
    //     self.create_table(namespace, table)
    // }

    // pub fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool, Error> {
    //     let table = self.load_table(namespace, table_name)?;
    //     Ok(table.is_some())
    // }

    // pub fn rename_table(&self, rename_request: &TableRenameRequest) -> Result<(), Error> {
    //     let namespace = &rename_request.namespace;
    //     let old_name = &rename_request.old_name;
    //     let new_name = &rename_request.new_name;
    //     let table = self
    //         .load_table(namespace, old_name)?
    //         .ok_or_else(|| Error::new(ErrorKind::NotFound, "Table not found"))?;
    //     let mut new_table = table.clone();
    //     new_table.name = new_name.clone();
    //     self.drop_table(namespace, old_name)?;
    //     self.create_table(namespace, &new_table)
    // }
}

