use serde_json::Value as JsonValue;

/// Convert a serde_json::Value into a Cypher literal string suitable for embedding.
/// This mirrors the helper used in the Snowflake tool.
pub fn json_value_to_cypher_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("\"{}\"", s.replace('\"', "\\\"")),
        JsonValue::Array(arr) => {
            let items = arr
                .iter()
                .map(json_value_to_cypher_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{}]", items)
        }
        JsonValue::Object(obj) => {
            let items = obj
                .iter()
                .map(|(k, v)| {
                    let escaped_key = k.replace('`', "");
                    format!("`{}`: {}", escaped_key, json_value_to_cypher_literal(v))
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{}}}", items)
        }
    }
}
