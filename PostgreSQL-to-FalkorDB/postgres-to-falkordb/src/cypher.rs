use serde_json::Value as JsonValue;

/// Convert a serde_json::Value into a Cypher literal string suitable for embedding.
///
/// Primitives are rendered as-is, while arrays and objects are rendered using
/// Cypher list/map syntax so that UNWIND can operate on them as structured
/// values rather than opaque strings.
pub fn json_value_to_cypher_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => format!("\"{}\"", s.replace('\"', "\\\"")),
        JsonValue::Array(items) => {
            let rendered: Vec<String> = items.iter().map(json_value_to_cypher_literal).collect();
            format!("[{}]", rendered.join(", "))
        }
        JsonValue::Object(map) => {
            let mut parts: Vec<String> = Vec::with_capacity(map.len());
            for (k, v) in map {
                let v_lit = json_value_to_cypher_literal(v);
                // Property keys are assumed to be valid identifier-like strings.
                parts.push(format!("{}: {}", k, v_lit));
            }
            format!("{{{}}}", parts.join(", "))
        }
    }
}
