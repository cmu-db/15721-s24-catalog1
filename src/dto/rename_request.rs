use crate::dto::table_data::TableIdent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableRenameRequest {
    pub source: TableIdent,
    pub destination: TableIdent,
}
