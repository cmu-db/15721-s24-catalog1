use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Deserialize, Serialize)]
pub struct SetNamespacePropertiesRequest {
    pub removals: Vec<String>,
    pub updates: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_set_namespace_properties_request() {
        let removals = vec!["property1".to_string(), "property2".to_string()];
        let updates = json!({
            "property3": "value3",
            "property4": "value4"
        })
        .as_object()
        .unwrap()
        .clone();

        let request = SetNamespacePropertiesRequest { removals, updates };

        assert_eq!(request.removals[0], "property1");
        assert_eq!(request.removals[1], "property2");
        assert_eq!(request.updates["property3"], "value3");
        assert_eq!(request.updates["property4"], "value4");
    }

    #[test]
    fn test_set_namespace_properties_request_serialization() {
        let removals = vec!["property1".to_string(), "property2".to_string()];
        let updates = json!({
            "property3": "value3",
            "property4": "value4"
        })
        .as_object()
        .unwrap()
        .clone();

        let request = SetNamespacePropertiesRequest { removals, updates };
        let serialized = serde_json::to_string(&request).unwrap();

        assert!(serialized.contains("property1"));
        assert!(serialized.contains("property2"));
        assert!(serialized.contains("value3"));
        assert!(serialized.contains("value4"));
    }

    #[test]
    fn test_set_namespace_properties_request_deserialization() {
        let data = r#"
            {
                "removals": ["property1", "property2"],
                "updates": {
                    "property3": "value3",
                    "property4": "value4"
                }
            }
            "#;

        let request: SetNamespacePropertiesRequest = serde_json::from_str(data).unwrap();

        assert_eq!(request.removals[0], "property1");
        assert_eq!(request.removals[1], "property2");
        assert_eq!(request.updates["property3"], "value3");
        assert_eq!(request.updates["property4"], "value4");
    }
}
