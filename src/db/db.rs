use rocksdb::{ColumnFamilyDescriptor, Options, DB};
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

    pub fn insert(&self, cf: &str, key: &[u8], value: &[u8]) -> io::Result<()> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        self.db
            .put_cf(cf_handle, key, value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn get(&self, cf: &str, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let value = self
            .db
            .get_cf(cf_handle, key)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(value)
    }

    pub fn delete(&self, cf: &str, key: &[u8]) -> io::Result<()> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        self.db
            .delete_cf(cf_handle, key)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }
    pub fn update(&self, cf: &str, key: &[u8], value: &[u8]) -> io::Result<()> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| io::Error::new(ErrorKind::NotFound, format!("Column family {} not found", cf)))?;
        self.db.put_cf(cf_handle, key, value).map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
    
    pub fn close(self) {
        drop(self);
    }
}
