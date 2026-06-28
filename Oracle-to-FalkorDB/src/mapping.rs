use anyhow::{anyhow, Result};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{EdgeMappingConfig, MatchOn, NodeMappingConfig};
use crate::sink::MappedNode;
use crate::sink_async::MappedEdge;
use crate::source::LogicalRow;

/// Neo4j/FalkorDB only allow property values that are primitives or arrays of primitives.
/// Normalise incoming JSON so that complex values (objects, nested arrays) are stringified.
fn normalise_property_value(value: JsonValue) -> JsonValue {
    fn is_primitive(v: &JsonValue) -> bool {
        matches!(
            v,
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_)
        )
    }

    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => value,
        JsonValue::Array(arr) => {
            if arr.iter().all(is_primitive) {
                JsonValue::Array(arr)
            } else {
                // Fallback: store entire JSON array as a string property
                let json = serde_json::to_string(&JsonValue::Array(arr))
                    .unwrap_or_else(|_| "[]".to_string());
                JsonValue::String(json)
            }
        }
        JsonValue::Object(_) => {
            // Fallback: store object as its JSON string representation
            let json = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            JsonValue::String(json)
        }
    }
}

/// Map tabular rows to FalkorDB nodes according to a NodeMappingConfig.
pub fn map_rows_to_nodes(
    rows: &[LogicalRow],
    mapping: &NodeMappingConfig,
) -> Result<Vec<MappedNode>> {
    let mut out = Vec::with_capacity(rows.len());

    for (idx, row) in rows.iter().enumerate() {
        let key_raw = row
            .get(&mapping.key.column)
            .cloned()
            .ok_or_else(|| anyhow!("Row {} is missing key column '{}'", idx, mapping.key.column))?;
        let key_value = normalise_property_value(key_raw);

        let mut props = JsonMap::new();
        // Always include key property
        props.insert(mapping.key.property.clone(), key_value.clone());

        for (prop_name, spec) in &mapping.properties {
            let val_raw = row.get(&spec.column).cloned().ok_or_else(|| {
                anyhow!(
                    "Row {} is missing column '{}' required for property '{}'",
                    idx,
                    spec.column,
                    prop_name
                )
            })?;
            let val = normalise_property_value(val_raw);
            props.insert(prop_name.clone(), val);
        }

        out.push(MappedNode {
            key: key_value,
            props,
        });
    }

    Ok(out)
}

/// Build a property map for matching endpoints based on MatchOn specs.
fn build_match_props(row: &LogicalRow, specs: &[MatchOn]) -> Result<JsonMap<String, JsonValue>> {
    let mut props = JsonMap::new();
    for spec in specs {
        let val_raw = row
            .get(&spec.column)
            .cloned()
            .ok_or_else(|| anyhow!("Missing column '{}' for endpoint match", spec.column))?;
        let val = normalise_property_value(val_raw);
        props.insert(spec.property.clone(), val);
    }
    Ok(props)
}

/// Map tabular rows to FalkorDB edges according to an EdgeMappingConfig.
pub fn map_rows_to_edges(
    rows: &[LogicalRow],
    mapping: &EdgeMappingConfig,
) -> Result<Vec<MappedEdge>> {
    let mut out = Vec::with_capacity(rows.len());

    for row in rows {
        let from_props = build_match_props(row, &mapping.from.match_on)?;
        let to_props = build_match_props(row, &mapping.to.match_on)?;

        let edge_key = if let Some(edge_key_spec) = &mapping.key {
            Some(normalise_property_value(
                row.get(&edge_key_spec.column).cloned().ok_or_else(|| {
                    anyhow!("Missing column '{}' for edge key", edge_key_spec.column)
                })?,
            ))
        } else {
            None
        };

        let mut props = JsonMap::new();
        for (prop_name, spec) in &mapping.properties {
            let val_raw = row.get(&spec.column).cloned().ok_or_else(|| {
                anyhow!(
                    "Missing column '{}' required for edge property '{}'",
                    spec.column,
                    prop_name
                )
            })?;
            let val = normalise_property_value(val_raw);
            props.insert(prop_name.clone(), val);
        }

        out.push(MappedEdge {
            from_props,
            to_props,
            edge_key,
            props,
        });
    }

    Ok(out)
}
