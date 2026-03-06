use anyhow::Result;
use falkordb::{FalkorClientBuilder, FalkorConnectionInfo, SyncGraph};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{FalkorConfig, NodeMappingConfig};
use crate::cypher::json_value_to_cypher_literal;

/// Establish a blocking FalkorDB client and select the configured graph.
pub fn connect_falkordb_sync(cfg: &FalkorConfig) -> Result<SyncGraph> {
    let conn_info: FalkorConnectionInfo = cfg.endpoint.as_str().try_into()?;

    let client = FalkorClientBuilder::new()
        .with_connection_info(conn_info)
        .build()?;

    Ok(client.select_graph(&cfg.graph))
}

/// Lightweight in-memory representation of a node ready to be sent as a UNWIND batch item.
#[derive(Clone)]
pub struct MappedNode {
    pub key: JsonValue,
    pub props: JsonMap<String, JsonValue>,
}

/// Build and execute a parameterised UNWIND+MERGE statement for a batch of nodes.
///
/// Cypher template (labels example: `Customer`):
///   UNWIND $rows AS row
///   MERGE (n:Customer { keyProp: row.key })
///   SET n += row.props
pub fn write_nodes_batch_sync(
    graph: &mut SyncGraph,
    mapping: &NodeMappingConfig,
    batch: &[MappedNode],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let label_clause = mapping.labels.join(":");

    let rows_value = JsonValue::Array(
        batch
            .iter()
            .map(|n| {
                let mut obj = JsonMap::new();
                obj.insert("key".to_string(), n.key.clone());
                obj.insert("props".to_string(), JsonValue::Object(n.props.clone()));
                JsonValue::Object(obj)
            })
            .collect(),
    );

    let rows_literal = json_value_to_cypher_literal(&rows_value);
    let cypher = format!(
        "UNWIND {rows} AS row \
         MERGE (n:{labels} {{ {key_prop}: row.key }}) \
         SET n += row.props",
        rows = rows_literal,
        labels = label_clause,
        key_prop = mapping.key.property,
    );

    let _res = graph.query(&cypher).execute()?;

    Ok(())
}

/// Helper to chunk a vector into batches of at most `max_batch_size` and send them sequentially.
pub fn write_nodes_in_batches_sync(
    graph: &mut SyncGraph,
    mapping: &NodeMappingConfig,
    nodes: Vec<MappedNode>,
    max_batch_size: usize,
) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    let mut start = 0usize;
    let total = nodes.len();

    while start < total {
        let end = (start + max_batch_size).min(total);
        let slice = &nodes[start..end];
        write_nodes_batch_sync(graph, mapping, slice)?;
        start = end;
    }

    Ok(())
}
