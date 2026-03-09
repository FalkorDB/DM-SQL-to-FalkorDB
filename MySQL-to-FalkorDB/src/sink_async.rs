use std::time::Duration;

use anyhow::{anyhow, Result};
use falkordb::{AsyncGraph, FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::time::sleep;

use crate::config::{EdgeDirection, EdgeMappingConfig, FalkorConfig, MatchOn, NodeMappingConfig};
use crate::cypher::json_value_to_cypher_literal;
use crate::sink::MappedNode;

/// Async connection to FalkorDB.
pub async fn connect_falkordb_async(cfg: &FalkorConfig) -> Result<AsyncGraph> {
    let conn_info: FalkorConnectionInfo = cfg.endpoint.as_str().try_into()?;

    let client: FalkorAsyncClient = FalkorClientBuilder::new_async()
        .with_connection_info(conn_info)
        .build()
        .await?;

    Ok(client.select_graph(&cfg.graph))
}

/// Lightweight in-memory representation of an edge ready to be sent as a UNWIND batch item.
#[derive(Clone)]
pub struct MappedEdge {
    pub from_props: JsonMap<String, JsonValue>,
    pub to_props: JsonMap<String, JsonValue>,
    pub edge_key: Option<JsonValue>,
    pub props: JsonMap<String, JsonValue>,
}

/// Build and execute an async parameterised UNWIND+MERGE for nodes.
pub async fn write_nodes_batch_async(
    graph: &mut AsyncGraph,
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

    let _res = graph.query(&cypher).execute().await?;

    Ok(())
}

/// Delete a batch of nodes identified by key property.
pub async fn delete_nodes_batch_async(
    graph: &mut AsyncGraph,
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
                JsonValue::Object(obj)
            })
            .collect(),
    );

    let rows_literal = json_value_to_cypher_literal(&rows_value);
    let cypher = format!(
        "UNWIND {rows} AS row \
         MATCH (n:{labels} {{ {key_prop}: row.key }}) \
         DETACH DELETE n",
        rows = rows_literal,
        labels = label_clause,
        key_prop = mapping.key.property,
    );

    let _res = graph.query(&cypher).execute().await?;

    Ok(())
}

fn escape_ident(value: &str) -> String {
    value.replace('`', "``")
}

fn build_endpoint_where(alias: &str, row_alias: &str, specs: &[MatchOn]) -> Result<String> {
    if specs.is_empty() {
        return Err(anyhow!(
            "Edge endpoint match_on must contain at least one entry"
        ));
    }

    let mut predicates = Vec::with_capacity(specs.len());
    for spec in specs {
        let prop = escape_ident(&spec.property);
        predicates.push(format!(
            "{alias}.`{prop}` = row.{row_alias}.`{prop}`",
            alias = alias,
            row_alias = row_alias,
            prop = prop
        ));
    }
    Ok(predicates.join(" AND "))
}

/// Build and execute an async parameterised UNWIND+MERGE for edges.
pub async fn write_edges_batch_async(
    graph: &mut AsyncGraph,
    mapping: &EdgeMappingConfig,
    batch: &[MappedEdge],
    from_labels: &[String],
    to_labels: &[String],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let from_label = from_labels.join(":");
    let to_label = to_labels.join(":");

    let from_where = build_endpoint_where("src", "from", &mapping.from.match_on)?;
    let to_where = build_endpoint_where("tgt", "to", &mapping.to.match_on)?;
    let endpoint_where = format!("{} AND {}", from_where, to_where);

    let merge_clause = match (&mapping.direction, &mapping.key) {
        (EdgeDirection::Out, Some(edge_key_spec)) => format!(
            "MERGE (src)-[r:{rel} {{ {ek}: row.edgeKey }}]->(tgt)",
            rel = mapping.relationship,
            ek = edge_key_spec.property,
        ),
        (EdgeDirection::Out, None) => {
            format!("MERGE (src)-[r:{rel}]->(tgt)", rel = mapping.relationship)
        }
        (EdgeDirection::In, Some(edge_key_spec)) => format!(
            "MERGE (src)<-[r:{rel} {{ {ek}: row.edgeKey }}]-(tgt)",
            rel = mapping.relationship,
            ek = edge_key_spec.property,
        ),
        (EdgeDirection::In, None) => {
            format!("MERGE (src)<-[r:{rel}]-(tgt)", rel = mapping.relationship)
        }
    };

    let rows_value = JsonValue::Array(
        batch
            .iter()
            .map(|e| {
                let mut obj = JsonMap::new();
                obj.insert("from".to_string(), JsonValue::Object(e.from_props.clone()));
                obj.insert("to".to_string(), JsonValue::Object(e.to_props.clone()));
                if let Some(ek) = &e.edge_key {
                    obj.insert("edgeKey".to_string(), ek.clone());
                }
                obj.insert("props".to_string(), JsonValue::Object(e.props.clone()));
                JsonValue::Object(obj)
            })
            .collect(),
    );

    let rows_literal = json_value_to_cypher_literal(&rows_value);
    let cypher = format!(
        "UNWIND {rows} AS row \
         MATCH (src:{from_label}) \
         MATCH (tgt:{to_label}) \
         WHERE {endpoint_where} \
         {merge_clause} \
         SET r += row.props",
        rows = rows_literal,
        from_label = from_label,
        to_label = to_label,
        endpoint_where = endpoint_where,
        merge_clause = merge_clause,
    );

    let _res = graph.query(&cypher).execute().await?;

    Ok(())
}

/// Build and execute an async parameterised UNWIND+MATCH+DELETE for edges.
pub async fn delete_edges_batch_async(
    graph: &mut AsyncGraph,
    mapping: &EdgeMappingConfig,
    batch: &[MappedEdge],
    from_labels: &[String],
    to_labels: &[String],
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let from_label = from_labels.join(":");
    let to_label = to_labels.join(":");

    let from_where = build_endpoint_where("src", "from", &mapping.from.match_on)?;
    let to_where = build_endpoint_where("tgt", "to", &mapping.to.match_on)?;
    let endpoint_where = format!("{} AND {}", from_where, to_where);

    let edge_match_clause = match (&mapping.direction, &mapping.key) {
        (EdgeDirection::Out, Some(edge_key_spec)) => format!(
            "MATCH (src)-[r:{rel} {{ {ek}: row.edgeKey }}]->(tgt)",
            rel = mapping.relationship,
            ek = edge_key_spec.property,
        ),
        (EdgeDirection::Out, None) => {
            format!("MATCH (src)-[r:{rel}]->(tgt)", rel = mapping.relationship)
        }
        (EdgeDirection::In, Some(edge_key_spec)) => format!(
            "MATCH (src)<-[r:{rel} {{ {ek}: row.edgeKey }}]-(tgt)",
            rel = mapping.relationship,
            ek = edge_key_spec.property,
        ),
        (EdgeDirection::In, None) => {
            format!("MATCH (src)<-[r:{rel}]-(tgt)", rel = mapping.relationship)
        }
    };

    let rows_value = JsonValue::Array(
        batch
            .iter()
            .map(|e| {
                let mut obj = JsonMap::new();
                obj.insert("from".to_string(), JsonValue::Object(e.from_props.clone()));
                obj.insert("to".to_string(), JsonValue::Object(e.to_props.clone()));
                if let Some(ek) = &e.edge_key {
                    obj.insert("edgeKey".to_string(), ek.clone());
                }
                JsonValue::Object(obj)
            })
            .collect(),
    );

    let rows_literal = json_value_to_cypher_literal(&rows_value);
    let cypher = format!(
        "UNWIND {rows} AS row \
         MATCH (src:{from_label}) \
         MATCH (tgt:{to_label}) \
         WHERE {endpoint_where} \
         {edge_match_clause} \
         DELETE r",
        rows = rows_literal,
        from_label = from_label,
        to_label = to_label,
        endpoint_where = endpoint_where,
        edge_match_clause = edge_match_clause,
    );

    let _res = graph.query(&cypher).execute().await?;

    Ok(())
}

/// Helper: chunk nodes and send them with retries on transient failures.
pub async fn write_nodes_in_batches_async(
    graph: &mut AsyncGraph,
    mapping: &NodeMappingConfig,
    nodes: Vec<MappedNode>,
    max_batch_size: usize,
    max_retries: u32,
) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    let mut start = 0usize;
    let total = nodes.len();

    while start < total {
        let end = (start + max_batch_size).min(total);
        let slice = nodes[start..end].to_vec();
        let mapping_ref = mapping;
        let graph_ptr: *mut AsyncGraph = graph;

        retry_with_backoff(max_retries, move || {
            let slice_cloned = slice.clone();
            async move {
                // SAFETY: batches are processed sequentially, so no concurrent access to graph.
                let graph_ref: &mut AsyncGraph = unsafe { &mut *graph_ptr };
                write_nodes_batch_async(graph_ref, mapping_ref, &slice_cloned).await
            }
        })
        .await?;

        start = end;
    }

    Ok(())
}

/// Helper: chunk deleted nodes and send them with retries on transient failures.
pub async fn delete_nodes_in_batches_async(
    graph: &mut AsyncGraph,
    mapping: &NodeMappingConfig,
    nodes: Vec<MappedNode>,
    max_batch_size: usize,
    max_retries: u32,
) -> Result<()> {
    if nodes.is_empty() {
        return Ok(());
    }

    let mut start = 0usize;
    let total = nodes.len();

    while start < total {
        let end = (start + max_batch_size).min(total);
        let slice = nodes[start..end].to_vec();
        let mapping_ref = mapping;
        let graph_ptr: *mut AsyncGraph = graph;

        retry_with_backoff(max_retries, move || {
            let slice_cloned = slice.clone();
            async move {
                // SAFETY: sequential batches => no concurrent access.
                let graph_ref: &mut AsyncGraph = unsafe { &mut *graph_ptr };
                delete_nodes_batch_async(graph_ref, mapping_ref, &slice_cloned).await
            }
        })
        .await?;

        start = end;
    }

    Ok(())
}

/// Helper: chunk edges and send them with retries on transient failures.
pub async fn write_edges_in_batches_async(
    graph: &mut AsyncGraph,
    mapping: &EdgeMappingConfig,
    edges: Vec<MappedEdge>,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    max_batch_size: usize,
    max_retries: u32,
) -> Result<()> {
    if edges.is_empty() {
        return Ok(());
    }

    let mut start = 0usize;
    let total = edges.len();

    while start < total {
        let end = (start + max_batch_size).min(total);
        let slice = edges[start..end].to_vec();
        let mapping_ref = mapping;
        let from_labels_cloned = from_labels.clone();
        let to_labels_cloned = to_labels.clone();
        let graph_ptr: *mut AsyncGraph = graph;

        retry_with_backoff(max_retries, move || {
            let slice_cloned = slice.clone();
            let from_labels_inner = from_labels_cloned.clone();
            let to_labels_inner = to_labels_cloned.clone();
            async move {
                // SAFETY: batches are processed sequentially, so no concurrent access to graph.
                let graph_ref: &mut AsyncGraph = unsafe { &mut *graph_ptr };
                write_edges_batch_async(
                    graph_ref,
                    mapping_ref,
                    &slice_cloned,
                    &from_labels_inner,
                    &to_labels_inner,
                )
                .await
            }
        })
        .await?;

        start = end;
    }

    Ok(())
}

/// Helper: chunk deleted edges and send them with retries on transient failures.
pub async fn delete_edges_in_batches_async(
    graph: &mut AsyncGraph,
    mapping: &EdgeMappingConfig,
    edges: Vec<MappedEdge>,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    max_batch_size: usize,
    max_retries: u32,
) -> Result<()> {
    if edges.is_empty() {
        return Ok(());
    }

    let mut start = 0usize;
    let total = edges.len();

    while start < total {
        let end = (start + max_batch_size).min(total);
        let slice = edges[start..end].to_vec();
        let mapping_ref = mapping;
        let from_labels_cloned = from_labels.clone();
        let to_labels_cloned = to_labels.clone();
        let graph_ptr: *mut AsyncGraph = graph;

        retry_with_backoff(max_retries, move || {
            let slice_cloned = slice.clone();
            let from_labels_inner = from_labels_cloned.clone();
            let to_labels_inner = to_labels_cloned.clone();
            async move {
                // SAFETY: sequential batches => no concurrent access.
                let graph_ref: &mut AsyncGraph = unsafe { &mut *graph_ptr };
                delete_edges_batch_async(
                    graph_ref,
                    mapping_ref,
                    &slice_cloned,
                    &from_labels_inner,
                    &to_labels_inner,
                )
                .await
            }
        })
        .await?;

        start = end;
    }

    Ok(())
}

/// Simple retry with exponential backoff.
async fn retry_with_backoff<F, Fut>(max_retries: u32, mut f: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < max_retries => {
                attempt += 1;
                let backoff = Duration::from_millis(50 * (1u64 << attempt.min(5)));
                tracing::warn!(
                    "Batch write failed (attempt {}/{}): {}. Retrying in {:?}...",
                    attempt,
                    max_retries,
                    e,
                    backoff
                );
                sleep(backoff).await;
            }
            Err(e) => {
                return Err(e.context(format!(
                    "Batch write failed after {} attempts",
                    max_retries + 1
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FalkorConfig;

    /// Optional FalkorDB connectivity smoke test.
    ///
    /// Uses environment variables:
    /// - FALKORDB_ENDPOINT (e.g. "falkor://127.0.0.1:6379")
    /// - FALKORDB_GRAPH (optional, defaults to "mysql_to_falkordb_test")
    ///
    /// If FALKORDB_ENDPOINT is not set, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn falkordb_connectivity_smoke_test() -> Result<()> {
        let endpoint = match std::env::var("FALKORDB_ENDPOINT") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let graph =
            std::env::var("FALKORDB_GRAPH").unwrap_or_else(|_| "mysql_to_falkordb_test".to_string());

        let cfg = FalkorConfig {
            endpoint,
            graph,
            max_unwind_batch_size: Some(10),
            indexes: vec![],
        };

        let mut graph = connect_falkordb_async(&cfg).await?;
        let _res = graph.query("RETURN 1").execute().await?;
        Ok(())
    }
}
