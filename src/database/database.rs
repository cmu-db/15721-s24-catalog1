use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::Arc;

pub struct Database {
    db: Arc<DB>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let namespace_cf = ColumnFamilyDescriptor::new("NamespaceData", Options::default());
        let table_cf = ColumnFamilyDescriptor::new("TableData", Options::default());
        let operator_cf = ColumnFamilyDescriptor::new("OperatorStatistics", Options::default());
        let table_namespace_cf =
            ColumnFamilyDescriptor::new("TableNamespaceMap", Options::default());

        let cfs_vec = vec![namespace_cf, table_cf, operator_cf, table_namespace_cf];

        let db = DB::open_cf_descriptors(&opts, path, cfs_vec)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(Self { db: db.into() })
    }

    pub fn list_all_keys(&self, cf: &str) -> Result<Vec<String>, io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let mut keys = Vec::new();
        let iter = self.db.iterator_cf(cf_handle, IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
            let key_str = String::from_utf8(key.to_vec())
                .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
            keys.push(key_str);
        }
        Ok(keys)
    }

    pub fn insert<V: Serialize>(&self, cf: &str, key: &str, value: &V) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let value = serde_json::to_vec(value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        self.db
            .put_cf(cf_handle, key.as_bytes(), &value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn get<V: for<'de> Deserialize<'de>>(
        &self,
        cf: &str,
        key: &str,
    ) -> Result<Option<V>, io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let value = self
            .db
            .get_cf(cf_handle, key.as_bytes())
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        match value {
            Some(db_vec) => {
                let v: V = serde_json::from_slice(&db_vec)?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, cf: &str, key: &str) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        self.db
            .delete_cf(cf_handle, key.as_bytes())
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn update<V: Serialize>(&self, cf: &str, key: &str, value: &V) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let value = serde_json::to_vec(value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        self.db
            .put_cf(cf_handle, key.as_bytes(), &value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }
}
