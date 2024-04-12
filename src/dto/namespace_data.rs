use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceData {
    pub name: String,
    pub properties: Value,
}

impl NamespaceData {
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_properties(&self) -> Value {
        self.properties.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_namespace_data_methods() {
        let properties = json!({"property1": "value1", "property2": "value2"});
        let namespace_data = NamespaceData {
            name: "test_namespace".to_string(),
            properties: properties.clone(),
        };

        // Test get_name method
        assert_eq!(namespace_data.get_name(), "test_namespace");

        // Test get_properties method
        assert_eq!(namespace_data.get_properties(), properties);
    }

    #[test]
    fn test_namespace_data_serialization() {
        let properties = json!({"property1": "value1", "property2": "value2"});
        let namespace_data = NamespaceData {
            name: "test_namespace".to_string(),
            properties: properties.clone(),
        };

        let serialized = serde_json::to_string(&namespace_data).unwrap();
        let expected =
            r#"{"name":"test_namespace","properties":{"property1":"value1","property2":"value2"}}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_namespace_data_deserialization() {
        let data =
            r#"{"name":"test_namespace","properties":{"property1":"value1","property2":"value2"}}"#;
        let namespace_data: NamespaceData = serde_json::from_str(data).unwrap();

        assert_eq!(namespace_data.name, "test_namespace");
        assert_eq!(
            namespace_data.properties,
            json!({"property1": "value1", "property2": "value2"})
        );
    }
}
