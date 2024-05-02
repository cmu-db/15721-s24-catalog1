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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::table_data::TableCreation;
    use crate::repository::namespace::NamespaceRepository;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[test]
    fn test_table_repository() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let repo = TableRepository::new(db.clone());

        // Create a namespace for testing
        let namespace_ident = NamespaceIdent(vec!["test".to_string()]);
        let namespace_repo = NamespaceRepository::new(db.clone());
        namespace_repo
            .create_namespace(namespace_ident.clone(), None)
            .unwrap();

        // Test create_table
        let table_creation = TableCreation {
            name: "table1".to_string(),
        };
        repo.create_table(&namespace_ident, &table_creation)
            .unwrap();

        // Test table_exists
        assert!(repo
            .table_exists(&namespace_ident, "table1".to_string())
            .unwrap());

        // Test load_table
        let table = repo
            .load_table(&namespace_ident, "table1".to_string())
            .unwrap()
            .unwrap();
        assert_eq!(table.id.name, "table1");

        // Test rename_table
        let rename_request = TableRenameRequest {
            source: TableIdent::new(namespace_ident.clone(), "table1".to_string()),
            destination: TableIdent::new(namespace_ident.clone(), "table2".to_string()),
        };
        repo.rename_table(&rename_request).unwrap();
        assert!(!repo
            .table_exists(&namespace_ident, "table1".to_string())
            .unwrap());
        assert!(repo
            .table_exists(&namespace_ident, "table2".to_string())
            .unwrap());

        // Test drop_table
        repo.drop_table(&namespace_ident, "table2".to_string())
            .unwrap();
        assert!(!repo
            .table_exists(&namespace_ident, "table2".to_string())
            .unwrap());
    }

    #[test]
    fn test_table_repository_negative() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let repo = TableRepository::new(db.clone());

        // Test with non-existent namespace
        let non_existent_namespace = NamespaceIdent(vec!["non_existent".to_string()]);
        let table_creation = TableCreation {
            name: "table1".to_string(),
        };
        assert!(repo
            .create_table(&non_existent_namespace, &table_creation)
            .is_err());
        assert!(repo
            .drop_table(&non_existent_namespace, "table1".to_string())
            .is_err());

        // Test with existing table
        let namespace_ident = NamespaceIdent(vec!["test".to_string()]);
        let namespace_repo = NamespaceRepository::new(db.clone());
        namespace_repo
            .create_namespace(namespace_ident.clone(), None)
            .unwrap();
        repo.create_table(&namespace_ident, &table_creation)
            .unwrap();
        assert!(repo
            .create_table(&namespace_ident, &table_creation)
            .is_err());

        // Test rename_table with non-existent source table
        let rename_request = TableRenameRequest {
            source: TableIdent::new(namespace_ident.clone(), "non_existent".to_string()),
            destination: TableIdent::new(namespace_ident.clone(), "table2".to_string()),
        };
        assert!(repo.rename_table(&rename_request).is_err());

        // Test rename_table with existing destination table
        let rename_request = TableRenameRequest {
            source: TableIdent::new(namespace_ident.clone(), "table1".to_string()),
            destination: TableIdent::new(namespace_ident.clone(), "table1".to_string()),
        };
        assert!(repo.rename_table(&rename_request).is_err());
    }
}
