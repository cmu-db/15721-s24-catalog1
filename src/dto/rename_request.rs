use crate::dto::table_data::TableIdent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TableRenameRequest {
    pub source: TableIdent,
    pub destination: TableIdent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::namespace_data::NamespaceIdent;
    use serde_json;
    #[test]
    fn test_table_rename_request() {
        let source = TableIdent {
            namespace: NamespaceIdent(vec!["source_namespace".to_string()]),
            name: "source_table".to_string(),
        };

        let destination = TableIdent {
            namespace: NamespaceIdent(vec!["destination_namespace".to_string()]),
            name: "destination_table".to_string(),
        };

        let request = TableRenameRequest {
            source,
            destination,
        };

        assert_eq!(request.source.namespace.0[0], "source_namespace");
        assert_eq!(request.source.name, "source_table");
        assert_eq!(request.destination.namespace.0[0], "destination_namespace");
        assert_eq!(request.destination.name, "destination_table");
    }

    #[test]
    fn test_table_rename_request_serialization() {
        let source = TableIdent {
            namespace: NamespaceIdent(vec!["source_namespace".to_string()]),
            name: "source_table".to_string(),
        };

        let destination = TableIdent {
            namespace: NamespaceIdent(vec!["destination_namespace".to_string()]),
            name: "destination_table".to_string(),
        };

        let request = TableRenameRequest {
            source,
            destination,
        };
        let serialized = serde_json::to_string(&request).unwrap();

        assert!(serialized.contains("source_namespace"));
        assert!(serialized.contains("source_table"));
        assert!(serialized.contains("destination_namespace"));
        assert!(serialized.contains("destination_table"));
    }

    #[test]
    fn test_table_rename_request_deserialization() {
        let data = r#"
            {
                "source": {
                    "namespace": ["source_namespace"],
                    "name": "source_table"
                },
                "destination": {
                    "namespace": ["destination_namespace"],
                    "name": "destination_table"
                }
            }
            "#;

        let request: TableRenameRequest = serde_json::from_str(data).unwrap();

        assert_eq!(request.source.namespace.0[0], "source_namespace");
        assert_eq!(request.source.name, "source_table");
        assert_eq!(request.destination.namespace.0[0], "destination_namespace");
        assert_eq!(request.destination.name, "destination_table");
    }
}
