use std::collections::HashSet;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::config::{Config, EntityMapping};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
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

/// Run a single full or incremental synchronization over all mappings.
pub async fn run_once(cfg: &Config) -> Result<()> {
    let mut graph = connect_falkordb_async(&cfg.falkordb).await?;

    let mut watermarks = load_watermarks(cfg)?;

    // Ensure we have indexes on node key properties before writing data.
    ensure_node_indexes(&mut graph, &cfg.mappings).await?;

    let batch_size = cfg.falkordb.max_unwind_batch_size.unwrap_or(1000).max(1);

    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(node_cfg) => {
                tracing::info!(mapping = %node_cfg.common.name, "Processing node mapping");

                let watermark = watermarks.get(&node_cfg.common.name).map(|s| s.as_str());

                // If this is an incremental mapping with initial_full_load = false and
                // no existing watermark, skip the initial full load and start tracking
                // from "now".
                if let Some(delta) = &node_cfg.common.delta {
                    if matches!(node_cfg.common.mode, crate::config::Mode::Incremental)
                        && watermark.is_none()
                        && delta.initial_full_load == Some(false)
                    {
                        let now = Utc::now().to_rfc3339();
                        tracing::info!(
                            mapping = %node_cfg.common.name,
                            watermark = %now,
                            "Skipping initial full load and seeding watermark",
                        );
                        watermarks.insert(node_cfg.common.name.clone(), now);
                        save_watermarks(cfg, &watermarks)?;
                        continue;
                    }
                }

                let rows = fetch_rows_for_mapping(cfg, &node_cfg.common, watermark).await?;
                tracing::info!(mapping = %node_cfg.common.name, rows = rows.len(), "Fetched rows");

                let (active_rows, deleted_rows) = if let Some(delta) = &node_cfg.common.delta {
                    partition_by_deleted(&rows, delta)
                } else {
                    (rows.clone(), Vec::new())
                };

                let nodes: Vec<MappedNode> = map_rows_to_nodes(&active_rows, node_cfg)?;
                tracing::info!(mapping = %node_cfg.common.name, rows = nodes.len(), "Writing nodes");

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
            }
            EntityMapping::Edge(edge_cfg) => {
                tracing::info!(mapping = %edge_cfg.common.name, "Processing edge mapping");

                let watermark = watermarks.get(&edge_cfg.common.name).map(|s| s.as_str());

                // If this is an incremental mapping with initial_full_load = false and
                // no existing watermark, skip the initial full load and start tracking
                // from "now".
                if let Some(delta) = &edge_cfg.common.delta {
                    if matches!(edge_cfg.common.mode, crate::config::Mode::Incremental)
                        && watermark.is_none()
                        && delta.initial_full_load == Some(false)
                    {
                        let now = Utc::now().to_rfc3339();
                        tracing::info!(
                            mapping = %edge_cfg.common.name,
                            watermark = %now,
                            "Skipping initial full load and seeding watermark",
                        );
                        watermarks.insert(edge_cfg.common.name.clone(), now);
                        save_watermarks(cfg, &watermarks)?;
                        continue;
                    }
                }

                // Resolve endpoint labels from node mappings by name.
                let from_node = cfg
                    .mappings
                    .iter()
                    .find_map(|m| match m {
                        EntityMapping::Node(n) if n.common.name == edge_cfg.from.node_mapping => {
                            Some(n)
                        }
                        _ => None,
                    })
                    .expect("from.node_mapping not found");
                let to_node = cfg
                    .mappings
                    .iter()
                    .find_map(|m| match m {
                        EntityMapping::Node(n) if n.common.name == edge_cfg.to.node_mapping => {
                            Some(n)
                        }
                        _ => None,
                    })
                    .expect("to.node_mapping not found");

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

                let rows = fetch_rows_for_mapping(cfg, &edge_cfg.common, watermark).await?;
                tracing::info!(mapping = %edge_cfg.common.name, rows = rows.len(), "Fetched rows");

                let (active_rows, deleted_rows) = if let Some(delta) = &edge_cfg.common.delta {
                    partition_by_deleted(&rows, delta)
                } else {
                    (rows.clone(), Vec::new())
                };

                let edges: Vec<MappedEdge> = map_rows_to_edges(&active_rows, edge_cfg)?;
                tracing::info!(mapping = %edge_cfg.common.name, rows = edges.len(), "Writing edges");

                let mut start = 0usize;
                while start < edges.len() {
                    let end = (start + batch_size).min(edges.len());
                    let slice = &edges[start..end];
                    write_edges_batch_async(&mut graph, edge_cfg, slice, &from_labels, &to_labels)
                        .await?;
                    start = end;
                }

                if !deleted_rows.is_empty() {
                    let deleted_edges: Vec<MappedEdge> =
                        map_rows_to_edges(&deleted_rows, edge_cfg)?;
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
            }
        }
    }

    Ok(())
}

/// Run daemon mode: repeatedly call run_once at a fixed interval.
pub async fn run_daemon(cfg: &Config, interval_secs: u64) -> Result<()> {
    use tokio::time::{interval, Duration};

    let mut ticker = interval(Duration::from_secs(interval_secs));

    loop {
        ticker.tick().await;

        tracing::info!("Starting sync run");
        if let Err(e) = run_once(cfg).await {
            tracing::error!(error = %e, "Sync run failed");
        }
    }
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
            .unwrap_or_else(|_| "postgres_to_falkordb_load_test".to_string());

        // Prepare a tiny in-memory config pointing at a temp JSON file.
        let tmp_dir = std::env::temp_dir();
        let input_path = tmp_dir.join("postgres_to_falkordb_nodes.json");
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
            postgres: None,
            falkordb: FalkorConfig {
                endpoint,
                graph,
                max_unwind_batch_size: Some(10),
            },
            state: None,
            mappings: vec![EntityMapping::Node(node_mapping)],
        };

        run_once(&cfg).await?;
        Ok(())
    }
}
