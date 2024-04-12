use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct OperatorStatistics {
    pub operator_string: String,
    pub cardinality_prev_result: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_statistics_serialization() {
        let operator_statistics = OperatorStatistics {
            operator_string: "test_operator".to_string(),
            cardinality_prev_result: 100,
        };

        let serialized = serde_json::to_string(&operator_statistics).unwrap();
        let expected = r#"{"operator_string":"test_operator","cardinality_prev_result":100}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_operator_statistics_deserialization() {
        let data = r#"{"operator_string":"test_operator","cardinality_prev_result":100}"#;
        let operator_statistics: OperatorStatistics = serde_json::from_str(data).unwrap();

        assert_eq!(operator_statistics.operator_string, "test_operator");
        assert_eq!(operator_statistics.cardinality_prev_result, 100);
    }
}
