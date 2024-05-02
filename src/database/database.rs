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
        let table_namespace_cf =
            ColumnFamilyDescriptor::new("TableNamespaceMap", Options::default());

        let cfs_vec = vec![namespace_cf, table_cf, table_namespace_cf];

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
    fn test_database_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path();

        // Test open
        let db = Database::open(db_path).unwrap();

        // Test insert
        let key: String = "key1".to_string();
        let value = "value1";
        db.insert("NamespaceData", &key, &value).unwrap();

        // Test get
        let retrieved_value: Option<String> = db.get("NamespaceData", &key).unwrap();
        assert_eq!(retrieved_value, Some(value.to_string()));

        // Test update
        let updated_value = "updated_value1";
        db.update("NamespaceData", &key, &updated_value).unwrap();
        let retrieved_value: Option<String> = db.get("NamespaceData", &key).unwrap();
        assert_eq!(retrieved_value, Some(updated_value.to_string()));

        // Test delete
        db.delete("NamespaceData", &key).unwrap();
        let retrieved_value: Option<String> = db.get("NamespaceData", &key).unwrap();
        assert_eq!(retrieved_value, None);
    }

    #[test]
    fn test_database_operations_negative_paths() {
        let dir = tempdir().unwrap();
        let db_path = dir.path();

        // Test open
        let db = Database::open(db_path).unwrap();

        // Test get with non-existing key
        let non_existing_key = "non_existing_key".to_string();
        let retrieved_value: Option<String> = db.get("NamespaceData", &non_existing_key).unwrap();
        assert_eq!(retrieved_value, None);

        // Test update with non-existing key
        let updated_value = "updated_value1";
        db.update("NamespaceData", &non_existing_key, &updated_value)
            .unwrap();
        let retrieved_value: Option<String> = db.get("NamespaceData", &non_existing_key).unwrap();
        assert_eq!(retrieved_value, Some(updated_value.to_string()));

        // Test delete with non-existing key
        db.delete("NamespaceData", &non_existing_key).unwrap();
        let retrieved_value: Option<String> = db.get("NamespaceData", &non_existing_key).unwrap();
        assert_eq!(retrieved_value, None);

        // Test operations with non-existing column family
        let non_existing_cf = "non_existing_cf";
        let key = "key1".to_string();
        let value = "value1";
        let result = db.insert(non_existing_cf, &key, &value);
        assert!(result.is_err());
        let result: Result<Option<String>, _> = db.get(non_existing_cf, &key);
        assert!(result.is_err());
        let result = db.update(non_existing_cf, &key, &value);
        assert!(result.is_err());
        let result = db.delete(non_existing_cf, &key);
        assert!(result.is_err());
    }
}
