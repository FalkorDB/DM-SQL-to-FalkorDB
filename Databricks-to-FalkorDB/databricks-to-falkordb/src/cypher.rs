use serde_json::Value as JsonValue;

/// Convert a serde_json::Value into a Cypher literal string suitable for embedding.
/// This mirrors the helper used in the Snowflake tool.
pub fn json_value_to_cypher_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("\"{}\"", s.replace('\"', "\\\"")),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            // For composite values, serialize to JSON and embed as a string.
            let json = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            format!("\"{}\"", json.replace('\"', "\\\""))
        }
    }
}
