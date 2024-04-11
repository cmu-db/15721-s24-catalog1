use crate::database::database::Database;
use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::TableData;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

pub struct TableRepository {
    database: Arc<Database>,
}

impl TableRepository {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    pub fn list_all_tables(&self, namespace: &str) -> Result<Option<Vec<String>>, Error> {
        self.database
            .get::<Vec<String>>("TableNamespaceMap", namespace)
    }

    pub fn create_table(&self, namespace: &str, table: &TableData) -> Result<(), Error> {
        self.database.insert("TableData", &table.name, table)?;
        let mut tables = self.list_all_tables(namespace).unwrap().unwrap_or_else(||vec![]);
        tables.push(table.name.clone());
        self.database
            .insert("TableNamespaceMap", namespace, &tables)
    }

    pub fn register_table(&self, namespace: &str, table: &TableData) -> Result<(), Error> {
        self.create_table(namespace, table)
    }

    pub fn load_table(
        &self,
        namespace: &str,
        table_name: &str,
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

        // If the table is in the namespace, get the table data
        self.database.get::<TableData>("TableData", table_name)
    }

    pub fn drop_table(&self, namespace: &str, table_name: &str) -> Result<(), Error> {
        self.database.delete("TableData", table_name)?;
        let mut tables = self.list_all_tables(namespace).unwrap().unwrap();
        tables.retain(|name| name != table_name);
        self.database
            .insert("TableNamespaceMap", namespace, &tables)
    }

    // for the ?? route
    pub fn insert_table(&self, namespace: &str, table: &TableData) -> Result<(), Error> {
        self.create_table(namespace, table)
    }

    pub fn table_exists(&self, namespace: &str, table_name: &str) -> Result<bool, Error> {
        let table = self.load_table(namespace, table_name)?;
        Ok(table.is_some())
    }

    pub fn rename_table(&self, rename_request: &TableRenameRequest) -> Result<(), Error> {
        let namespace = &rename_request.namespace;
        let old_name = &rename_request.old_name;
        let new_name = &rename_request.new_name;
        let table = self
            .load_table(namespace, old_name)?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Table not found"))?;
        let mut new_table = table.clone();
        new_table.name = new_name.clone();
        self.drop_table(namespace, old_name)?;
        self.create_table(namespace, &new_table)
    }
}
