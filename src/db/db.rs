use rocksdb::{Options, DB, ColumnFamilyDescriptor, Error};
use std::path::Path;

pub struct Database {
    db: DB,
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
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

        let db = DB::open_cf_descriptors(&opts, path, cfs_vec)?;

        Ok(Self { db })
    }
}
