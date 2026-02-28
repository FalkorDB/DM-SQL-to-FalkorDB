use anyhow::{Context, Result};
use falkordb::{AsyncGraph, FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{EdgeDirection, EdgeMappingConfig, FalkorConfig, NodeMappingConfig};
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

    let from_match_key = &mapping
        .from
        .match_on
        .first()
        .context("from endpoint must specify at least one match_on")?
        .property;
    let to_match_key = &mapping
        .to
        .match_on
        .first()
        .context("to endpoint must specify at least one match_on")?
        .property;

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
         MATCH (src:{from_label} {{ {from_key}: row.from.{from_key} }}) \
         MATCH (tgt:{to_label} {{ {to_key}: row.to.{to_key} }}) \
         {merge_clause} \
         SET r += row.props",
        rows = rows_literal,
        from_label = from_label,
        to_label = to_label,
        from_key = from_match_key,
        to_key = to_match_key,
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

    let from_match_key = &mapping
        .from
        .match_on
        .first()
        .context("from endpoint must specify at least one match_on")?
        .property;
    let to_match_key = &mapping
        .to
        .match_on
        .first()
        .context("to endpoint must specify at least one match_on")?
        .property;

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
         MATCH (src:{from_label} {{ {from_key}: row.from.{from_key} }}) \
         MATCH (tgt:{to_label} {{ {to_key}: row.to.{to_key} }}) \
         {edge_match_clause} \
         DELETE r",
        rows = rows_literal,
        from_label = from_label,
        to_label = to_label,
        from_key = from_match_key,
        to_key = to_match_key,
        edge_match_clause = edge_match_clause,
    );

    let _res = graph.query(&cypher).execute().await?;

    Ok(())
}
