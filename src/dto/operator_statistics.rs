use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct OperatorStatistics {
    pub operator_string: String,
    pub cardinality_prev_result: u64,
}
