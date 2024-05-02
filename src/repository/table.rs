use crate::database::database::Database;
use crate::dto::namespace_data::{NamespaceData, NamespaceIdent};
use crate::dto::rename_request::TableRenameRequest;
use crate::dto::table_data::{Table, TableCreation, TableIdent, TableMetadata};
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

    pub fn list_all_tables(
        &self,
        namespace: &NamespaceIdent,
    ) -> Result<Option<Vec<TableIdent>>, Error> {
        let db = self.database.lock().unwrap();
        let _: NamespaceData = match db.get("NamespaceData", namespace)? {
            Some(data) => data,
            None => {
                return Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("Namespace {} not found", namespace.clone().0.join("\u{1F}")),
                ))
            }
        };
        db.get::<NamespaceIdent, Vec<TableIdent>>("TableNamespaceMap", namespace)
    }

    pub fn create_table(
        &self,
        namespace: &NamespaceIdent,
        table_creation: &TableCreation,
    ) -> Result<(), Error> {
        let db = self.database.lock().unwrap();
        let _: NamespaceData = match db.get("NamespaceData", namespace)? {
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

        let table_metadata = TableMetadata { table_uuid };

        let mut tables = db
            .get::<NamespaceIdent, Vec<TableIdent>>("TableNamespaceMap", namespace)
            .unwrap()
            .unwrap_or_else(|| vec![]);

        if tables.contains(&table_id) {
            return Err(std::io::Error::new(
                ErrorKind::AlreadyExists,
                format!(
                    "Table {} already exists in namespace {}",
                    table_creation.name,
                    namespace.clone().0.join("\u{1F}")
                ),
            ));
        }

        db.insert(
            "TableData",
            &table_id,
            &Table {
                id: table_id.clone(),
                metadata: table_metadata,
            },
        )?;
        tables.push(table_id.clone());
        let r_val = db.insert("TableNamespaceMap", namespace, &tables);
        r_val
    }

    pub fn load_table(
        &self,
        namespace: &NamespaceIdent,
        table_name: String,
    ) -> Result<Option<Table>, Error> {
        let table_id = TableIdent::new(namespace.clone(), table_name.clone());
        let db = self.database.lock().unwrap();
        // If the table is in the namespace, get the table data
        db.get::<TableIdent, Table>("TableData", &table_id)
    }

    pub fn drop_table(&self, namespace: &NamespaceIdent, table_name: String) -> Result<(), Error> {
        let db = self.database.lock().unwrap();
        let table_id = TableIdent::new(namespace.clone(), table_name.clone());

        let _: Table = match db.get::<TableIdent, Table>("TableData", &table_id)? {
            Some(data) => data,
            None => {
                return Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("Namespace {} not found", namespace.clone().0.join("\u{1F}")),
                ))
            }
        };

        db.delete("TableData", &table_id)?;
        let mut tables = db
            .get::<NamespaceIdent, Vec<TableIdent>>("TableNamespaceMap", namespace)
            .unwrap()
            .unwrap();
        tables.retain(|id| id.name != table_name);
        db.insert("TableNamespaceMap", namespace, &tables)
    }

    pub fn table_exists(
        &self,
        namespace: &NamespaceIdent,
        table_name: String,
    ) -> Result<bool, Error> {
        let table = self.load_table(namespace, table_name)?;
        Ok(table.is_some())
    }

    pub fn rename_table(&self, rename_request: &TableRenameRequest) -> Result<(), Error> {
        let source = rename_request.source.clone();
        let destination = rename_request.destination.clone();
        let namespace = source.namespace.clone();

        let table = self
            .load_table(&namespace, source.name.clone())?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Source table not found"))?;

        if self.table_exists(&destination.namespace, destination.name.clone())? {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                "Destination table already exists",
            ));
        }

        let mut new_table = table.clone();
        new_table.id = destination.clone();

        self.create_table(
            &destination.namespace.clone(),
            &TableCreation {
                name: destination.name.clone(),
            },
        )?;
        self.drop_table(&namespace, source.name.clone())
    }
}
