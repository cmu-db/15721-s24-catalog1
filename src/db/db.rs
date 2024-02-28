use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use std::io::{self, ErrorKind};
use std::path::Path;

pub struct Database {
    db: DB,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let namespace_cf_opts = Options::default();
        let namespace_cf = ColumnFamilyDescriptor::new("NamespaceData", namespace_cf_opts);

        let table_cf_opts = Options::default();
        let table_cf = ColumnFamilyDescriptor::new("TableData", table_cf_opts);

        let operator_cf_opts = Options::default();
        let operator_cf = ColumnFamilyDescriptor::new("OperatorStatistics", operator_cf_opts);

        let cfs_vec = vec![namespace_cf, table_cf, operator_cf];

        let db = DB::open_cf_descriptors(&opts, path, cfs_vec)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(Self { db })
    }
    pub fn insert<V: Serialize>(&self, cf: &str, key: &str, value: &V) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| io::Error::new(ErrorKind::NotFound,format!("Column family {} not found", cf)))?;
        let value = serde_json::to_vec(value).map_err(|e| io::Error::new(ErrorKind::Other,e.to_string()))?;
        self.db.put_cf(cf_handle, key.as_bytes(), &value).map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn get<V: for<'de> Deserialize<'de>>(&self, cf: &str, key: &str) -> Result<Option<V>, io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| io::Error::new(ErrorKind::NotFound,format!("Column family {} not found", cf)))?;
        let value = self.db.get_cf(cf_handle, key.as_bytes()).map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        match value {
            Some(db_vec) => {
                let v: V = serde_json::from_slice(&db_vec)?;
                Ok(Some(v))
            },
            None => Ok(None),
        }
    }

    pub fn delete(&self, cf: &str, key: &str) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| io::Error::new(ErrorKind::NotFound,format!("Column family {} not found", cf)))?;
        self.db.delete_cf(cf_handle, key.as_bytes()).map_err(|e| io::Error::new(ErrorKind::Other, e))?;   
        Ok(())
    }

    pub fn update<V: Serialize>(&self, cf: &str, key: &str, value: &V) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| io::Error::new(ErrorKind::NotFound,format!("Column family {} not found", cf)))?;
        let value = serde_json::to_vec(value).map_err(|e| io::Error::new(ErrorKind::Other,e.to_string()))?;
        self.db.put_cf(cf_handle, key.as_bytes(), &value).map_err(|e| io::Error::new(ErrorKind::Other, e))?;   
        Ok(())
    }

    pub fn close(self) {
        drop(self);
    }
}
