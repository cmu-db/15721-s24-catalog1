mod db;

use crate::db::db::Database;
use std::io;

fn main() -> io::Result<()> {
    let db = Database::open("rocksdb")?;

    // Insert some data
    db.insert("NamespaceData", b"my_key", b"my_value")?;
    db.insert("TableData", b"my_key", b"my_value")?;
    db.insert("OperatorStatistics", b"my_key", b"my_value")?;

    // Get the data
    let namespace_data = db.get("NamespaceData", b"my_key")?;
    let table_data = db.get("TableData", b"my_key")?;
    let operator_statistics = db.get("OperatorStatistics", b"my_key")?;

    println!("NamespaceData: {:?}", namespace_data);
    println!("TableData: {:?}", table_data);
    println!("OperatorStatistics: {:?}", operator_statistics);

    db.update("NamespaceData", b"my_key", b"new_value")?;

    // Get the updated data
    let namespace_data = db.get("NamespaceData", b"my_key")?;
    println!("NamespaceData: {:?}", namespace_data);

    // Delete the data
    db.delete("NamespaceData", b"my_key")?;
    db.delete("TableData", b"my_key")?;
    db.delete("OperatorStatistics", b"my_key")?;

    // Close the database
    db.close();

    Ok(())
}
