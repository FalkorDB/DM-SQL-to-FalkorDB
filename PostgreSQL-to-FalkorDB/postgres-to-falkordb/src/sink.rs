use serde_json::{Map as JsonMap, Value as JsonValue};

/// Lightweight in-memory representation of a node ready to be sent as a UNWIND batch item.
#[derive(Clone)]
pub struct MappedNode {
    pub key: JsonValue,
    pub props: JsonMap<String, JsonValue>,
}
