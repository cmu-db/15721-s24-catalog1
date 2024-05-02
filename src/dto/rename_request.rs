use serde::{Deserialize, Serialize};
use crate::dto::table_data::TableIdent;

#[derive(Debug, Serialize, Deserialize)]
pub struct TableRenameRequest {
    pub source: TableIdent,
    pub destination: TableIdent,
}

