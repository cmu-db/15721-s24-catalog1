use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableRenameRequest {
    pub namespace: String,
    pub old_name: String,
    pub new_name: String,
}
