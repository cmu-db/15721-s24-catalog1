use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
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

    pub fn list_all_keys<K: Serialize + for<'de> Deserialize<'de>>(
        &self,
        cf: &str,
    ) -> Result<Vec<K>, io::Error> {
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
            let key_obj: K = serde_json::from_slice(&key)
                .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
            keys.push(key_obj);
        }
        Ok(keys)
    }

    pub fn insert<K: Serialize, V: Serialize>(
        &self,
        cf: &str,
        key: &K,
        value: &V,
    ) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let value = serde_json::to_vec(value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        let key_bytes =
            serde_json::to_vec(key).map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        self.db
            .put_cf(cf_handle, key_bytes, &value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn get<K: for<'de> Deserialize<'de> + Serialize, V: for<'de> Deserialize<'de>>(
        &self,
        cf: &str,
        key: &K,
    ) -> Result<Option<V>, io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let key_bytes =
            serde_json::to_vec(key).map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        let value = self
            .db
            .get_cf(cf_handle, &key_bytes)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        match value {
            Some(db_vec) => {
                let v: V = serde_json::from_slice(&db_vec)?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    pub fn delete<K: for<'de> Deserialize<'de> + Serialize>(
        &self,
        cf: &str,
        key: &K,
    ) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let key_bytes =
            serde_json::to_vec(key).map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        self.db
            .delete_cf(cf_handle, key_bytes)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }

    pub fn update<K: Serialize, V: Serialize>(
        &self,
        cf: &str,
        key: &K,
        value: &V,
    ) -> Result<(), io::Error> {
        let cf_handle = self.db.cf_handle(cf).ok_or_else(|| {
            io::Error::new(
                ErrorKind::NotFound,
                format!("Column family {} not found", cf),
            )
        })?;
        let value = serde_json::to_vec(value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        let key_bytes =
            serde_json::to_vec(key).map_err(|e| io::Error::new(ErrorKind::Other, e.to_string()))?;
        self.db
            .put_cf(cf_handle, key_bytes, &value)
            .map_err(|e| io::Error::new(ErrorKind::Other, e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_open() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path());
        assert!(db.is_ok());
    }

    #[test]
    fn test_insert_and_get() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let key = "test_key";
        let value = "test_value";

        // Test insert
        let insert_result = db.insert("NamespaceData", &key, &value);
        assert!(insert_result.is_ok());

        // Test get
        let get_result: Result<Option<String>, _> = db.get("NamespaceData", &key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().unwrap(), value);
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let key = "test_key";
        let value = "test_value";

        // Insert a key-value pair
        db.insert("NamespaceData", &key, &value).unwrap();

        // Delete the key
        let delete_result = db.delete("NamespaceData", &key);
        assert!(delete_result.is_ok());

        // Try to get the deleted key
        let get_result: Result<Option<String>, _> = db.get("NamespaceData", &key);
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[test]
    fn test_insert_and_get_nonexistent_cf() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let key = "test_key";
        let value = "test_value";

        // Test insert with nonexistent column family
        let insert_result = db.insert("NonexistentCF", &key, &value);
        assert!(insert_result.is_err());

        // Test get with nonexistent column family
        let get_result: Result<Option<String>, _> = db.get("NonexistentCF", &key);
        assert!(get_result.is_err());
    }

    #[test]
    fn test_get_nonexistent_key() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        // Test get with nonexistent key
        let get_result: Result<Option<String>, _> = db.get("NamespaceData", &"nonexistent_key");
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[test]
    fn test_delete_nonexistent_key() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();

        // Test delete with nonexistent key
        let delete_result = db.delete("NamespaceData", &"nonexistent_key");
        assert!(delete_result.is_ok());
    }

    #[test]
    fn test_insert_and_get_empty_key() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let key = "";
        let value = "test_value";

        // Test insert with empty key
        let insert_result = db.insert("NamespaceData", &key, &value);
        assert!(insert_result.is_ok());

        // Test get with empty key
        let get_result: Result<Option<String>, _> = db.get("NamespaceData", &key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().unwrap(), value);
    }

    #[test]
    fn test_insert_and_get_empty_value() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let key = "test_key";
        let value = "";

        // Test insert with empty value
        let insert_result = db.insert("NamespaceData", &key, &value);
        assert!(insert_result.is_ok());

        // Test get with empty value
        let get_result: Result<Option<String>, _> = db.get("NamespaceData", &key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().unwrap(), value);
    }

    #[test]
    fn test_insert_and_get_large_data() {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path()).unwrap();
        let key = "test_key";
        let value = "a".repeat(1_000_000);

        // Test insert with large data
        let insert_result = db.insert("NamespaceData", &key, &value);
        assert!(insert_result.is_ok());

        // Test get with large data
        let get_result: Result<Option<String>, _> = db.get("NamespaceData", &key);
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap().unwrap(), value);
    }
}
