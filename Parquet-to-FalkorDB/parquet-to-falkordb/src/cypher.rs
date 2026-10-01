use serde_json::Value as JsonValue;

/// Convert serde_json::Value to a Cypher literal string.
///
/// This is a minimal helper to inline JSON-like structures into Cypher queries
/// (e.g. for UNWIND batches) without relying on driver-specific parameter
/// support.
pub fn json_value_to_cypher_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            // Escape backslashes and single quotes, then wrap in single quotes.
            let escaped = s.replace("\\", "\\\\").replace("'", "\\'");
            format!("'{}'", escaped)
        }
        JsonValue::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_value_to_cypher_literal).collect();
            format!("[{}]", items.join(", "))
        }
        JsonValue::Object(map) => {
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| {
                    // Escape backticks in keys by doubling them, then wrap in backticks
                    let escaped_key = k.replace("`", "``");
                    format!("`{}`: {}", escaped_key, json_value_to_cypher_literal(v))
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn string_and_object_literals() {
        assert_eq!(json_value_to_cypher_literal(&json!(null)), "null");
        assert_eq!(json_value_to_cypher_literal(&json!(true)), "true");
        assert_eq!(json_value_to_cypher_literal(&json!(42)), "42");
        assert_eq!(json_value_to_cypher_literal(&json!("a'b")), "'a\\'b'");
        let obj = json!({"id": 1, "name": "x"});
        let lit = json_value_to_cypher_literal(&obj);
        assert!(lit.contains("`id`: 1"));
        assert!(lit.contains("`name`: 'x'"));
    }
}
