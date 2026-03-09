use std::collections::{HashMap, HashSet};
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use crate::config::{Config, EntityMapping, FalkorConfig, NodeMappingConfig};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
use crate::metrics::METRICS;
use crate::sink::MappedNode;
use crate::sink_async::{
    connect_falkordb_async, delete_edges_batch_async, delete_nodes_batch_async,
    write_edges_batch_async, write_nodes_batch_async, MappedEdge,
};
use crate::source::{fetch_rows_for_mapping, LogicalRow};
use crate::state::{load_watermarks, save_watermarks};

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

fn compute_max_watermark(rows: &[LogicalRow], updated_at_column: &str) -> Option<String> {
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

    max_ts.map(|ts| ts.to_rfc3339())
}

/// Ensure indexes exist for explicit config indexes plus node/edge key-match properties.
async fn ensure_falkordb_indexes(
    graph: &mut falkordb::AsyncGraph,
    mappings: &[EntityMapping],
    node_by_name: &HashMap<&str, &NodeMappingConfig>,
    falkordb_cfg: &FalkorConfig,
) -> Result<()> {
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for index in &falkordb_cfg.indexes {
        ensure_index(
            graph,
            &mut seen,
            &index.labels,
            &index.property,
            "explicit_falkordb_config",
        )
        .await?;
    }

    for mapping in mappings {
        match mapping {
            EntityMapping::Node(node_cfg) => {
                ensure_index(
                    graph,
                    &mut seen,
                    &node_cfg.labels,
                    &node_cfg.key.property,
                    "node_key_property",
                )
                .await?;
            }
            EntityMapping::Edge(edge_cfg) => {
                let from_labels = if let Some(labels) = &edge_cfg.from.label_override {
                    labels.clone()
                } else if let Some(node_cfg) = node_by_name.get(edge_cfg.from.node_mapping.as_str())
                {
                    node_cfg.labels.clone()
                } else {
                    tracing::warn!(
                        mapping = %edge_cfg.common.name,
                        endpoint = "from",
                        node_mapping = %edge_cfg.from.node_mapping,
                        "Skipping edge endpoint index inference due to unknown node mapping"
                    );
                    Vec::new()
                };
                let to_labels = if let Some(labels) = &edge_cfg.to.label_override {
                    labels.clone()
                } else if let Some(node_cfg) = node_by_name.get(edge_cfg.to.node_mapping.as_str()) {
                    node_cfg.labels.clone()
                } else {
                    tracing::warn!(
                        mapping = %edge_cfg.common.name,
                        endpoint = "to",
                        node_mapping = %edge_cfg.to.node_mapping,
                        "Skipping edge endpoint index inference due to unknown node mapping"
                    );
                    Vec::new()
                };

                for match_on in &edge_cfg.from.match_on {
                    ensure_index(
                        graph,
                        &mut seen,
                        &from_labels,
                        &match_on.property,
                        "edge_from_match_property",
                    )
                    .await?;
                }
                for match_on in &edge_cfg.to.match_on {
                    ensure_index(
                        graph,
                        &mut seen,
                        &to_labels,
                        &match_on.property,
                        "edge_to_match_property",
                    )
                    .await?;
                }
            }
        }
    }

    Ok(())
}

async fn ensure_index(
    graph: &mut falkordb::AsyncGraph,
    seen: &mut HashSet<(String, String)>,
    labels: &[String],
    property: &str,
    reason: &str,
) -> Result<()> {
    if labels.is_empty() || property.trim().is_empty() {
        return Ok(());
    }
    let label_clause = labels.join(":");
    let key = (label_clause.clone(), property.to_string());
    if !seen.insert(key) {
        return Ok(());
    }
    let cypher = format!(
        "CREATE INDEX ON :{labels}({prop})",
        labels = label_clause,
        prop = property
    );
    tracing::info!(
        labels = %label_clause,
        property = %property,
        reason = %reason,
        "Ensuring FalkorDB index"
    );
    if let Err(e) = graph.query(&cypher).execute().await {
        tracing::warn!(
            labels = %label_clause,
            property = %property,
            reason = %reason,
            error = %e,
            "Failed to create FalkorDB index (it may already exist)"
        );
    }

    Ok(())
}

/// Run a single full or incremental synchronization over all mappings.
pub async fn run_once(cfg: &Config) -> Result<()> {
    let mut graph = connect_falkordb_async(&cfg.falkordb).await?;

    let mut watermarks = load_watermarks(cfg)?;
    METRICS.inc_runs();
    let mut node_by_name: HashMap<&str, &NodeMappingConfig> = HashMap::new();
    for mapping in &cfg.mappings {
        if let EntityMapping::Node(node_cfg) = mapping {
            node_by_name.insert(node_cfg.common.name.as_str(), node_cfg);
        }
    }
    ensure_falkordb_indexes(&mut graph, &cfg.mappings, &node_by_name, &cfg.falkordb).await?;

    let batch_size = cfg.falkordb.max_unwind_batch_size.unwrap_or(1000).max(1);

    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(node_cfg) => {
                tracing::info!(mapping = %node_cfg.common.name, "Processing node mapping");
                METRICS.inc_mapping_run(&node_cfg.common.name);
                let node_result: Result<()> = async {
                    let watermark = watermarks.get(&node_cfg.common.name).map(|s| s.as_str());
                    let rows = fetch_rows_for_mapping(cfg, &node_cfg.common, watermark).await?;
                    tracing::info!(mapping = %node_cfg.common.name, rows = rows.len(), "Fetched rows");
                    METRICS.add_rows_fetched(rows.len() as u64);
                    METRICS.add_mapping_rows_fetched(&node_cfg.common.name, rows.len() as u64);

                    let (active_rows, deleted_rows) = if let Some(delta) = &node_cfg.common.delta {
                        partition_by_deleted(&rows, delta)
                    } else {
                        (rows.clone(), Vec::new())
                    };

                    let nodes: Vec<MappedNode> = map_rows_to_nodes(&active_rows, node_cfg)?;
                    tracing::info!(mapping = %node_cfg.common.name, rows = nodes.len(), "Writing nodes");
                    METRICS.add_rows_written(nodes.len() as u64);
                    METRICS.add_mapping_rows_written(&node_cfg.common.name, nodes.len() as u64);

                    let mut start = 0usize;
                    while start < nodes.len() {
                        let end = (start + batch_size).min(nodes.len());
                        let slice = &nodes[start..end];
                        write_nodes_batch_async(&mut graph, node_cfg, slice).await?;
                        start = end;
                    }

                    if !deleted_rows.is_empty() {
                        let deleted_nodes: Vec<MappedNode> =
                            map_rows_to_nodes(&deleted_rows, node_cfg)?;
                        METRICS.add_rows_deleted(deleted_nodes.len() as u64);
                        METRICS.add_mapping_rows_deleted(
                            &node_cfg.common.name,
                            deleted_nodes.len() as u64,
                        );
                        tracing::info!(
                            mapping = %node_cfg.common.name,
                            rows = deleted_nodes.len(),
                            "Deleting soft-deleted nodes",
                        );

                        let mut start = 0usize;
                        while start < deleted_nodes.len() {
                            let end = (start + batch_size).min(deleted_nodes.len());
                            let slice = &deleted_nodes[start..end];
                            delete_nodes_batch_async(&mut graph, node_cfg, slice).await?;
                            start = end;
                        }
                    }

                    if let Some(delta) = &node_cfg.common.delta {
                        if let Some(max_ts) = compute_max_watermark(&rows, &delta.updated_at_column) {
                            watermarks.insert(node_cfg.common.name.clone(), max_ts);
                            save_watermarks(cfg, &watermarks)?;
                        }
                    }

                    Ok(())
                }
                .await;

                if let Err(e) = node_result {
                    METRICS.inc_mapping_failed_run(&node_cfg.common.name);
                    return Err(e);
                }
            }
            EntityMapping::Edge(edge_cfg) => {
                tracing::info!(mapping = %edge_cfg.common.name, "Processing edge mapping");
                METRICS.inc_mapping_run(&edge_cfg.common.name);
                let edge_result: Result<()> = async {
                    // For now, resolve endpoint labels from node mappings by name.
                    let from_node = cfg
                        .mappings
                        .iter()
                        .find_map(|m| match m {
                            EntityMapping::Node(n)
                                if n.common.name == edge_cfg.from.node_mapping =>
                            {
                                Some(n)
                            }
                            _ => None,
                        })
                        .ok_or_else(|| {
                            anyhow!(
                                "Edge mapping '{}' refers to unknown from.node_mapping '{}'",
                                edge_cfg.common.name,
                                edge_cfg.from.node_mapping
                            )
                        })?;
                    let to_node = cfg
                        .mappings
                        .iter()
                        .find_map(|m| match m {
                            EntityMapping::Node(n)
                                if n.common.name == edge_cfg.to.node_mapping =>
                            {
                                Some(n)
                            }
                            _ => None,
                        })
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
                    tracing::info!(mapping = %edge_cfg.common.name, rows = rows.len(), "Fetched rows");
                    METRICS.add_rows_fetched(rows.len() as u64);
                    METRICS.add_mapping_rows_fetched(&edge_cfg.common.name, rows.len() as u64);

                    let (active_rows, deleted_rows) = if let Some(delta) = &edge_cfg.common.delta {
                        partition_by_deleted(&rows, delta)
                    } else {
                        (rows.clone(), Vec::new())
                    };

                    let edges: Vec<MappedEdge> = map_rows_to_edges(&active_rows, edge_cfg)?;
                    tracing::info!(mapping = %edge_cfg.common.name, rows = edges.len(), "Writing edges");
                    METRICS.add_rows_written(edges.len() as u64);
                    METRICS.add_mapping_rows_written(&edge_cfg.common.name, edges.len() as u64);

                    let mut start = 0usize;
                    while start < edges.len() {
                        let end = (start + batch_size).min(edges.len());
                        let slice = &edges[start..end];
                        write_edges_batch_async(
                            &mut graph,
                            edge_cfg,
                            slice,
                            &from_labels,
                            &to_labels,
                        )
                        .await?;
                        start = end;
                    }

                    if !deleted_rows.is_empty() {
                        let deleted_edges: Vec<MappedEdge> =
                            map_rows_to_edges(&deleted_rows, edge_cfg)?;
                        METRICS.add_rows_deleted(deleted_edges.len() as u64);
                        METRICS.add_mapping_rows_deleted(
                            &edge_cfg.common.name,
                            deleted_edges.len() as u64,
                        );
                        tracing::info!(
                            mapping = %edge_cfg.common.name,
                            rows = deleted_edges.len(),
                            "Deleting soft-deleted edges",
                        );

                        let mut start = 0usize;
                        while start < deleted_edges.len() {
                            let end = (start + batch_size).min(deleted_edges.len());
                            let slice = &deleted_edges[start..end];
                            delete_edges_batch_async(
                                &mut graph,
                                edge_cfg,
                                slice,
                                &from_labels,
                                &to_labels,
                            )
                            .await?;
                            start = end;
                        }
                    }

                    if let Some(delta) = &edge_cfg.common.delta {
                        if let Some(max_ts) = compute_max_watermark(&rows, &delta.updated_at_column) {
                            watermarks.insert(edge_cfg.common.name.clone(), max_ts);
                            save_watermarks(cfg, &watermarks)?;
                        }
                    }

                    Ok(())
                }
                .await;

                if let Err(e) = edge_result {
                    METRICS.inc_mapping_failed_run(&edge_cfg.common.name);
                    return Err(e);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CommonMappingFields, EntityMapping, FalkorConfig, Mode, NodeKeySpec, NodeMappingConfig,
        PropertySpec, SourceConfig,
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
            .unwrap_or_else(|_| "databricks_to_falkordb_load_test".to_string());

        // Prepare a tiny in-memory config pointing at a temp JSON file.
        let tmp_dir = std::env::temp_dir();
        let input_path = tmp_dir.join("databricks_to_falkordb_nodes.json");
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
            databricks: None,
            falkordb: FalkorConfig {
                endpoint,
                graph,
                max_unwind_batch_size: Some(10),
                indexes: vec![],
            },
            state: None,
            mappings: vec![EntityMapping::Node(node_mapping)],
        };

        run_once(&cfg).await?;
        Ok(())
    }
}
