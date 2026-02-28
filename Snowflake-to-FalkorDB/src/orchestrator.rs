use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::config::{Config, EntityMapping, NodeMappingConfig};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
use crate::metrics::METRICS;
use crate::sink::MappedNode;
use crate::sink_async::{
    connect_falkordb_async, delete_edges_in_batches_async, delete_nodes_in_batches_async,
    write_edges_in_batches_async, write_nodes_in_batches_async, MappedEdge,
};
use crate::source::{fetch_rows_for_mapping, LogicalRow};
use crate::state::{load_watermarks, save_watermarks};

fn compute_max_watermark(rows: &[LogicalRow], updated_at_column: &str) -> Option<DateTime<Utc>> {
    use chrono::{NaiveDateTime, TimeZone};
    let mut max_ts: Option<DateTime<Utc>> = None;

    for row in rows {
        if let Some(value) = row.get(updated_at_column) {
            let candidate = match value {
                serde_json::Value::String(s) => {
                    // Try RFC3339 first, then "YYYY-MM-DD HH:MM:SS[.fraction]" as UTC.
                    DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&Utc))
                        .or_else(|_| {
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                                .map(|ndt| Utc.from_utc_datetime(&ndt))
                        })
                        .ok()
                }
                _ => None,
            };

            if let Some(ts) = candidate {
                if max_ts.map_or(true, |cur| ts > cur) {
                    max_ts = Some(ts);
                }
            }
        }
    }

    max_ts
}

fn partition_by_deleted<'a>(
    rows: &'a [LogicalRow],
    delta: &crate::config::DeltaSpec,
) -> (Vec<LogicalRow>, Vec<LogicalRow>) {
    let mut active = Vec::new();
    let mut deleted = Vec::new();

    if let Some(flag_col) = &delta.deleted_flag_column {
        if let Some(flag_val) = &delta.deleted_flag_value {
            for row in rows {
                let is_deleted = row.get(flag_col).map(|v| v == flag_val).unwrap_or(false);
                if is_deleted {
                    deleted.push(row.clone());
                } else {
                    active.push(row.clone());
                }
            }
        } else {
            // No explicit value configured; treat as active-only for now.
            active.extend(rows.iter().cloned());
        }
    } else {
        active.extend(rows.iter().cloned());
    }

    (active, deleted)
}

async fn purge_graph(graph: &mut falkordb::AsyncGraph) -> Result<()> {
    tracing::warn!("Purging entire graph prior to load");
    graph.query("MATCH (n) DETACH DELETE n").execute().await?;
    Ok(())
}

/// Ensure indexes exist for node key properties used in MERGE/MATCH.
///
/// For each node mapping, we create an index on (labels, key.property). We de-duplicate
/// by (labels, property) combination and treat failures as non-fatal (for example,
/// when the index already exists on the server).
async fn ensure_node_indexes(
    graph: &mut falkordb::AsyncGraph,
    mappings: &[EntityMapping],
) -> Result<()> {
    let mut seen: HashSet<(String, String)> = HashSet::new();

    for mapping in mappings {
        if let EntityMapping::Node(node_cfg) = mapping {
            if node_cfg.labels.is_empty() {
                continue;
            }

            let label_clause = node_cfg.labels.join(":");
            let prop = node_cfg.key.property.clone();
            let key = (label_clause.clone(), prop.clone());

            if !seen.insert(key) {
                continue;
            }

            let cypher = format!(
                "CREATE INDEX ON :{labels}({prop})",
                labels = label_clause,
                prop = prop
            );

            tracing::info!(
                mapping = %node_cfg.common.name,
                labels = %label_clause,
                property = %prop,
                "Ensuring index for node label on key property",
            );

            if let Err(e) = graph.query(&cypher).execute().await {
                tracing::warn!(
                    mapping = %node_cfg.common.name,
                    labels = %label_clause,
                    property = %prop,
                    error = %e,
                    "Failed to create index for node label (it may already exist)",
                );
            }
        }
    }

    Ok(())
}

async fn purge_mapping(
    graph: &mut falkordb::AsyncGraph,
    mapping: &EntityMapping,
    node_by_name: &HashMap<&str, &NodeMappingConfig>,
) -> Result<()> {
    match mapping {
        EntityMapping::Node(node_cfg) => {
            let label_clause = node_cfg.labels.join(":");
            let cypher = format!("MATCH (n:{}) DETACH DELETE n", label_clause);
            tracing::warn!(mapping = %node_cfg.common.name, "Purging node mapping");
            graph.query(&cypher).execute().await?;
        }
        EntityMapping::Edge(edge_cfg) => {
            let from_node = node_by_name
                .get(edge_cfg.from.node_mapping.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "Edge mapping '{}' refers to unknown from.node_mapping '{}'",
                        edge_cfg.common.name,
                        edge_cfg.from.node_mapping
                    )
                })?;
            let to_node = node_by_name
                .get(edge_cfg.to.node_mapping.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "Edge mapping '{}' refers to unknown to.node_mapping '{}'",
                        edge_cfg.common.name,
                        edge_cfg.to.node_mapping
                    )
                })?;

            let from_labels = edge_cfg
                .from
                .label_override
                .clone()
                .unwrap_or_else(|| from_node.labels.clone());
            let to_labels = edge_cfg
                .to
                .label_override
                .clone()
                .unwrap_or_else(|| to_node.labels.clone());

            let from_label = from_labels.join(":");
            let to_label = to_labels.join(":");
            let cypher = format!(
                "MATCH (src:{from})-[r:{rel}]->(tgt:{to}) DELETE r",
                from = from_label,
                to = to_label,
                rel = edge_cfg.relationship,
            );
            tracing::warn!(mapping = %edge_cfg.common.name, "Purging edge mapping");
            graph.query(&cypher).execute().await?;
        }
    }
    Ok(())
}

/// Run a single full or incremental synchronization over all mappings.
pub async fn run_once(
    cfg: &Config,
    purge_graph_flag: bool,
    purge_mappings: &[String],
) -> Result<()> {
    let mut graph = connect_falkordb_async(&cfg.falkordb).await?;
    let mut watermarks = load_watermarks(cfg)?;

    METRICS.inc_runs();

    // Index node mappings by name so edges can look up endpoint labels.
    let mut node_by_name: HashMap<&str, &NodeMappingConfig> = HashMap::new();
    for mapping in &cfg.mappings {
        if let EntityMapping::Node(node) = mapping {
            node_by_name.insert(node.common.name.as_str(), node);
        }
    }

    // Handle purge options
    if purge_graph_flag {
        purge_graph(&mut graph).await?;
    } else if !purge_mappings.is_empty() {
        for name in purge_mappings {
            if let Some(mapping) = cfg.mappings.iter().find(|m| match m {
                EntityMapping::Node(n) => &n.common.name == name,
                EntityMapping::Edge(e) => &e.common.name == name,
            }) {
                purge_mapping(&mut graph, mapping, &node_by_name).await?;
            } else {
                tracing::warn!(mapping = %name, "Requested purge for unknown mapping");
            }
        }
    }

    // Ensure we have indexes on node key properties before writing data. This improves
    // MERGE/MATCH performance and is safe to run repeatedly.
    ensure_node_indexes(&mut graph, &cfg.mappings).await?;

    let batch_size = cfg.falkordb.max_unwind_batch_size.unwrap_or(1000).max(1);

    // For now run mappings sequentially; concurrency can be added later.
    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(node_cfg) => {
                tracing::info!(mapping = %node_cfg.common.name, "Processing node mapping");
                METRICS.inc_mapping_run(&node_cfg.common.name);

                let watermark = watermarks.get(&node_cfg.common.name).map(|s| s.as_str());
                let rows = fetch_rows_for_mapping(cfg, &node_cfg.common, watermark).await?;
                METRICS.add_rows_fetched(rows.len() as u64);
                METRICS.add_mapping_rows_fetched(&node_cfg.common.name, rows.len() as u64);
                tracing::info!(mapping = %node_cfg.common.name, rows = rows.len(), "Fetched rows");

                let (active_rows, deleted_rows) = if let Some(delta) = &node_cfg.common.delta {
                    partition_by_deleted(&rows, delta)
                } else {
                    (rows.clone(), Vec::new())
                };

                let nodes: Vec<MappedNode> = map_rows_to_nodes(&active_rows, node_cfg)?;
                METRICS.add_rows_written(nodes.len() as u64);
                METRICS.add_mapping_rows_written(&node_cfg.common.name, nodes.len() as u64);
                tracing::info!(mapping = %node_cfg.common.name, rows = nodes.len(), "Writing nodes");
                write_nodes_in_batches_async(&mut graph, node_cfg, nodes, batch_size, 3).await?;

                if !deleted_rows.is_empty() {
                    let deleted_nodes: Vec<MappedNode> =
                        map_rows_to_nodes(&deleted_rows, node_cfg)?;
                    METRICS.add_rows_deleted(deleted_nodes.len() as u64);
                    METRICS.add_mapping_rows_deleted(
                        &node_cfg.common.name,
                        deleted_nodes.len() as u64,
                    );
                    tracing::info!(mapping = %node_cfg.common.name, rows = deleted_nodes.len(), "Deleting nodes");
                    delete_nodes_in_batches_async(
                        &mut graph,
                        node_cfg,
                        deleted_nodes,
                        batch_size,
                        3,
                    )
                    .await?;
                }

                if let Some(delta) = &node_cfg.common.delta {
                    if let Some(max_ts) = compute_max_watermark(&rows, &delta.updated_at_column) {
                        watermarks.insert(node_cfg.common.name.clone(), max_ts.to_rfc3339());
                        save_watermarks(cfg, &watermarks)?;
                    }
                }
            }
            EntityMapping::Edge(edge_cfg) => {
                tracing::info!(mapping = %edge_cfg.common.name, "Processing edge mapping");
                METRICS.inc_mapping_run(&edge_cfg.common.name);

                let from_node = node_by_name
                    .get(edge_cfg.from.node_mapping.as_str())
                    .copied()
                    .ok_or_else(|| {
                        anyhow!(
                            "Edge mapping '{}' refers to unknown from.node_mapping '{}'",
                            edge_cfg.common.name,
                            edge_cfg.from.node_mapping
                        )
                    })?;
                let to_node = node_by_name
                    .get(edge_cfg.to.node_mapping.as_str())
                    .copied()
                    .ok_or_else(|| {
                        anyhow!(
                            "Edge mapping '{}' refers to unknown to.node_mapping '{}'",
                            edge_cfg.common.name,
                            edge_cfg.to.node_mapping
                        )
                    })?;

                let from_labels = edge_cfg
                    .from
                    .label_override
                    .clone()
                    .unwrap_or_else(|| from_node.labels.clone());
                let to_labels = edge_cfg
                    .to
                    .label_override
                    .clone()
                    .unwrap_or_else(|| to_node.labels.clone());

                let watermark = watermarks.get(&edge_cfg.common.name).map(|s| s.as_str());
                let rows = fetch_rows_for_mapping(cfg, &edge_cfg.common, watermark).await?;
                METRICS.add_rows_fetched(rows.len() as u64);
                METRICS.add_mapping_rows_fetched(&edge_cfg.common.name, rows.len() as u64);
                tracing::info!(mapping = %edge_cfg.common.name, rows = rows.len(), "Fetched rows");

                let (active_rows, deleted_rows) = if let Some(delta) = &edge_cfg.common.delta {
                    partition_by_deleted(&rows, delta)
                } else {
                    (rows.clone(), Vec::new())
                };

                let edges: Vec<MappedEdge> = map_rows_to_edges(&active_rows, edge_cfg)?;
                METRICS.add_rows_written(edges.len() as u64);
                METRICS.add_mapping_rows_written(&edge_cfg.common.name, edges.len() as u64);
                tracing::info!(mapping = %edge_cfg.common.name, rows = edges.len(), "Writing edges");
                write_edges_in_batches_async(
                    &mut graph,
                    edge_cfg,
                    edges,
                    from_labels.clone(),
                    to_labels.clone(),
                    batch_size,
                    3,
                )
                .await?;

                if !deleted_rows.is_empty() {
                    let deleted_edges: Vec<MappedEdge> =
                        map_rows_to_edges(&deleted_rows, edge_cfg)?;
                    METRICS.add_rows_deleted(deleted_edges.len() as u64);
                    METRICS.add_mapping_rows_deleted(
                        &edge_cfg.common.name,
                        deleted_edges.len() as u64,
                    );
                    tracing::info!(mapping = %edge_cfg.common.name, rows = deleted_edges.len(), "Deleting edges");
                    delete_edges_in_batches_async(
                        &mut graph,
                        edge_cfg,
                        deleted_edges,
                        from_labels.clone(),
                        to_labels.clone(),
                        batch_size,
                        3,
                    )
                    .await?;
                }

                if let Some(delta) = &edge_cfg.common.delta {
                    if let Some(max_ts) = compute_max_watermark(&rows, &delta.updated_at_column) {
                        watermarks.insert(edge_cfg.common.name.clone(), max_ts.to_rfc3339());
                        save_watermarks(cfg, &watermarks)?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Run daemon mode: repeatedly call run_once at a fixed interval. Purge options are applied only
/// on the first run.
pub async fn run_daemon(
    cfg: &Config,
    purge_graph_flag: bool,
    purge_mappings: &[String],
    interval_secs: u64,
) -> Result<()> {
    use tokio::time::{interval, Duration};

    let mut ticker = interval(Duration::from_secs(interval_secs));
    let mut first = true;

    loop {
        ticker.tick().await;

        let pg = if first { purge_graph_flag } else { false };
        let pm: Vec<String> = if first {
            purge_mappings.to_vec()
        } else {
            Vec::new()
        };

        tracing::info!("Starting sync run");
        if let Err(e) = run_once(cfg, pg, &pm).await {
            tracing::error!(error = %e, "Sync run failed");
            METRICS.inc_failed_runs();
            // Mapping-level failure increments are handled where errors are detected
        }

        first = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CommonMappingFields, EntityMapping, FalkorConfig, Mode, NodeKeySpec, NodeMappingConfig,
        PropertySpec, SourceConfig, StateBackendKind, StateConfig,
    };
    use std::collections::HashMap;

    /// Optional end-to-end test that loads a small JSON file into FalkorDB.
    ///
    /// Requires FALKORDB_ENDPOINT to be set. If it's missing, the test is skipped
    /// by returning Ok(()) immediately.
    #[tokio::test]
    async fn end_to_end_file_load_into_falkordb() -> Result<()> {
        let endpoint = match std::env::var("FALKORDB_ENDPOINT") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let graph = std::env::var("FALKORDB_GRAPH")
            .unwrap_or_else(|_| "snowflake_to_falkordb_load_test".to_string());

        // Prepare a tiny in-memory config pointing at a temp JSON file.
        let tmp_dir = std::env::temp_dir();
        let input_path = tmp_dir.join("snowflake_to_falkordb_nodes.json");
        std::fs::write(
            &input_path,
            r#"[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ]"#,
        )?;

        let source = SourceConfig {
            file: Some(input_path.to_string_lossy().to_string()),
            table: None,
            stream: None,
            select: None,
            r#where: None,
        };

        let common = CommonMappingFields {
            name: "test_nodes".to_string(),
            source,
            mode: Mode::Full,
            delta: None,
        };

        let key = NodeKeySpec {
            column: "id".to_string(),
            property: "id".to_string(),
        };

        let mut properties = HashMap::new();
        properties.insert(
            "name".to_string(),
            PropertySpec {
                column: "name".to_string(),
            },
        );

        let node_mapping = NodeMappingConfig {
            common,
            labels: vec!["TestNode".to_string()],
            key,
            properties,
        };

        let cfg = Config {
            snowflake: None,
            falkordb: FalkorConfig {
                endpoint,
                graph,
                max_unwind_batch_size: Some(10),
            },
            state: Some(StateConfig {
                backend: StateBackendKind::File,
                file_path: Some(
                    std::env::temp_dir()
                        .join("snowflake_to_falkordb_state.json")
                        .to_string_lossy()
                        .to_string(),
                ),
            }),
            mappings: vec![EntityMapping::Node(node_mapping)],
        };

        run_once(&cfg, false, &[]).await?;
        Ok(())
    }
}
