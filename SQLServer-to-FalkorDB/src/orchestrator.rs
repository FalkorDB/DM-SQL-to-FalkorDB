use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::config::{
    CommonMappingFields, Config, EntityMapping, FalkorConfig, Mode, NodeMappingConfig,
};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
use crate::metrics::METRICS;
use crate::sink::MappedNode;
use crate::sink_async::{
    connect_falkordb_async, delete_edges_in_batches_async, delete_nodes_in_batches_async,
    write_edges_in_batches_async, write_nodes_in_batches_async, MappedEdge,
};
use crate::source::{
    fetch_rows_for_mapping, fetch_rows_page_for_incremental_table, incremental_page_size,
    should_use_incremental_paging, LogicalRow,
};
use crate::state::{load_watermarks, save_watermarks};

fn compute_max_watermark(rows: &[LogicalRow], updated_at_column: &str) -> Option<String> {
    use chrono::{NaiveDateTime, TimeZone};
    let mut max_ts: Option<DateTime<Utc>> = None;

    for row in rows {
        if let Some(value) = row.get(updated_at_column) {
            let candidate = match value {
                serde_json::Value::String(s) => DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .or_else(|_| {
                        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                            .map(|ndt| Utc.from_utc_datetime(&ndt))
                    })
                    .ok(),
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

fn partition_by_deleted(
    rows: &[LogicalRow],
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
            active.extend(rows.iter().cloned());
        }
    } else {
        active.extend(rows.iter().cloned());
    }

    (active, deleted)
}

fn should_seed_watermark(common: &CommonMappingFields, watermark_exists: bool) -> bool {
    if watermark_exists {
        return false;
    }
    if !matches!(common.mode, Mode::Incremental) {
        return false;
    }
    common
        .delta
        .as_ref()
        .and_then(|d| d.initial_full_load)
        .map(|v| !v)
        .unwrap_or(false)
}

async fn purge_graph(graph: &mut falkordb::AsyncGraph) -> Result<()> {
    tracing::warn!("Purging entire graph prior to load");
    graph.query("MATCH (n) DETACH DELETE n").execute().await?;
    Ok(())
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
                } else if let Some(node_cfg) = node_by_name.get(edge_cfg.from.node_mapping.as_str()) {
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

async fn apply_node_rows(
    graph: &mut falkordb::AsyncGraph,
    node_cfg: &crate::config::NodeMappingConfig,
    rows: &[LogicalRow],
    batch_size: usize,
) -> Result<()> {
    let (active_rows, deleted_rows) = if let Some(delta) = &node_cfg.common.delta {
        partition_by_deleted(rows, delta)
    } else {
        (rows.to_vec(), Vec::new())
    };

    let nodes: Vec<MappedNode> = map_rows_to_nodes(&active_rows, node_cfg)?;
    METRICS.add_rows_written(nodes.len() as u64);
    METRICS.add_mapping_rows_written(&node_cfg.common.name, nodes.len() as u64);
    tracing::info!(mapping = %node_cfg.common.name, rows = nodes.len(), "Writing nodes");
    write_nodes_in_batches_async(graph, node_cfg, nodes, batch_size, 3).await?;

    if !deleted_rows.is_empty() {
        let deleted_nodes: Vec<MappedNode> = map_rows_to_nodes(&deleted_rows, node_cfg)?;
        METRICS.add_rows_deleted(deleted_nodes.len() as u64);
        METRICS.add_mapping_rows_deleted(&node_cfg.common.name, deleted_nodes.len() as u64);
        tracing::info!(mapping = %node_cfg.common.name, rows = deleted_nodes.len(), "Deleting nodes");
        delete_nodes_in_batches_async(graph, node_cfg, deleted_nodes, batch_size, 3).await?;
    }

    Ok(())
}

async fn apply_edge_rows(
    graph: &mut falkordb::AsyncGraph,
    edge_cfg: &crate::config::EdgeMappingConfig,
    rows: &[LogicalRow],
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    batch_size: usize,
) -> Result<()> {
    let (active_rows, deleted_rows) = if let Some(delta) = &edge_cfg.common.delta {
        partition_by_deleted(rows, delta)
    } else {
        (rows.to_vec(), Vec::new())
    };

    let edges: Vec<MappedEdge> = map_rows_to_edges(&active_rows, edge_cfg)?;
    METRICS.add_rows_written(edges.len() as u64);
    METRICS.add_mapping_rows_written(&edge_cfg.common.name, edges.len() as u64);
    tracing::info!(mapping = %edge_cfg.common.name, rows = edges.len(), "Writing edges");
    write_edges_in_batches_async(
        graph,
        edge_cfg,
        edges,
        from_labels.clone(),
        to_labels.clone(),
        batch_size,
        3,
    )
    .await?;

    if !deleted_rows.is_empty() {
        let deleted_edges: Vec<MappedEdge> = map_rows_to_edges(&deleted_rows, edge_cfg)?;
        METRICS.add_rows_deleted(deleted_edges.len() as u64);
        METRICS.add_mapping_rows_deleted(&edge_cfg.common.name, deleted_edges.len() as u64);
        tracing::info!(mapping = %edge_cfg.common.name, rows = deleted_edges.len(), "Deleting edges");
        delete_edges_in_batches_async(
            graph,
            edge_cfg,
            deleted_edges,
            from_labels,
            to_labels,
            batch_size,
            3,
        )
        .await?;
    }

    Ok(())
}

async fn process_node_mapping(
    cfg: &Config,
    graph: &mut falkordb::AsyncGraph,
    node_cfg: &crate::config::NodeMappingConfig,
    watermarks: &mut HashMap<String, String>,
    batch_size: usize,
) -> Result<()> {
    tracing::info!(mapping = %node_cfg.common.name, "Processing node mapping");
    METRICS.inc_mapping_run(&node_cfg.common.name);

    let mut watermark = watermarks.get(&node_cfg.common.name).cloned();

    if should_seed_watermark(&node_cfg.common, watermark.is_some()) {
        let now = Utc::now().to_rfc3339();
        tracing::info!(
            mapping = %node_cfg.common.name,
            watermark = %now,
            "Skipping initial full load and seeding watermark",
        );
        watermarks.insert(node_cfg.common.name.clone(), now);
        save_watermarks(cfg, watermarks)?;
        return Ok(());
    }

    if should_use_incremental_paging(cfg, &node_cfg.common) {
        let page_size = incremental_page_size(cfg).ok_or_else(|| {
            anyhow!(
                "Paged mode selected for mapping '{}' but no page size is configured",
                node_cfg.common.name
            )
        })?;

        loop {
            let rows = fetch_rows_page_for_incremental_table(
                cfg,
                &node_cfg.common,
                watermark.as_deref(),
                page_size,
            )
            .await?;
            if rows.is_empty() {
                break;
            }

            METRICS.add_rows_fetched(rows.len() as u64);
            METRICS.add_mapping_rows_fetched(&node_cfg.common.name, rows.len() as u64);
            tracing::info!(
                mapping = %node_cfg.common.name,
                rows = rows.len(),
                "Fetched rows page"
            );

            apply_node_rows(graph, node_cfg, &rows, batch_size).await?;

            if let Some(delta) = &node_cfg.common.delta {
                let next_wm =
                    compute_max_watermark(&rows, &delta.updated_at_column).ok_or_else(|| {
                        anyhow!(
                            "Unable to compute page watermark from column '{}' for mapping '{}'",
                            delta.updated_at_column,
                            node_cfg.common.name
                        )
                    })?;

                watermarks.insert(node_cfg.common.name.clone(), next_wm.clone());
                save_watermarks(cfg, watermarks)?;
                watermark = Some(next_wm);
            }

            if rows.len() < page_size {
                break;
            }
        }

        return Ok(());
    }

    let rows = fetch_rows_for_mapping(cfg, &node_cfg.common, watermark.as_deref()).await?;
    METRICS.add_rows_fetched(rows.len() as u64);
    METRICS.add_mapping_rows_fetched(&node_cfg.common.name, rows.len() as u64);
    tracing::info!(mapping = %node_cfg.common.name, rows = rows.len(), "Fetched rows");

    apply_node_rows(graph, node_cfg, &rows, batch_size).await?;

    if let Some(delta) = &node_cfg.common.delta {
        if let Some(max_ts) = compute_max_watermark(&rows, &delta.updated_at_column) {
            watermarks.insert(node_cfg.common.name.clone(), max_ts);
            save_watermarks(cfg, watermarks)?;
        }
    }

    Ok(())
}

async fn process_edge_mapping(
    cfg: &Config,
    graph: &mut falkordb::AsyncGraph,
    edge_cfg: &crate::config::EdgeMappingConfig,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    watermarks: &mut HashMap<String, String>,
    batch_size: usize,
) -> Result<()> {
    tracing::info!(mapping = %edge_cfg.common.name, "Processing edge mapping");
    METRICS.inc_mapping_run(&edge_cfg.common.name);

    let mut watermark = watermarks.get(&edge_cfg.common.name).cloned();

    if should_seed_watermark(&edge_cfg.common, watermark.is_some()) {
        let now = Utc::now().to_rfc3339();
        tracing::info!(
            mapping = %edge_cfg.common.name,
            watermark = %now,
            "Skipping initial full load and seeding watermark",
        );
        watermarks.insert(edge_cfg.common.name.clone(), now);
        save_watermarks(cfg, watermarks)?;
        return Ok(());
    }

    if should_use_incremental_paging(cfg, &edge_cfg.common) {
        let page_size = incremental_page_size(cfg).ok_or_else(|| {
            anyhow!(
                "Paged mode selected for mapping '{}' but no page size is configured",
                edge_cfg.common.name
            )
        })?;

        loop {
            let rows = fetch_rows_page_for_incremental_table(
                cfg,
                &edge_cfg.common,
                watermark.as_deref(),
                page_size,
            )
            .await?;
            if rows.is_empty() {
                break;
            }

            METRICS.add_rows_fetched(rows.len() as u64);
            METRICS.add_mapping_rows_fetched(&edge_cfg.common.name, rows.len() as u64);
            tracing::info!(
                mapping = %edge_cfg.common.name,
                rows = rows.len(),
                "Fetched rows page"
            );

            apply_edge_rows(
                graph,
                edge_cfg,
                &rows,
                from_labels.clone(),
                to_labels.clone(),
                batch_size,
            )
            .await?;

            if let Some(delta) = &edge_cfg.common.delta {
                let next_wm =
                    compute_max_watermark(&rows, &delta.updated_at_column).ok_or_else(|| {
                        anyhow!(
                            "Unable to compute page watermark from column '{}' for mapping '{}'",
                            delta.updated_at_column,
                            edge_cfg.common.name
                        )
                    })?;

                watermarks.insert(edge_cfg.common.name.clone(), next_wm.clone());
                save_watermarks(cfg, watermarks)?;
                watermark = Some(next_wm);
            }

            if rows.len() < page_size {
                break;
            }
        }

        return Ok(());
    }

    let rows = fetch_rows_for_mapping(cfg, &edge_cfg.common, watermark.as_deref()).await?;
    METRICS.add_rows_fetched(rows.len() as u64);
    METRICS.add_mapping_rows_fetched(&edge_cfg.common.name, rows.len() as u64);
    tracing::info!(mapping = %edge_cfg.common.name, rows = rows.len(), "Fetched rows");

    apply_edge_rows(
        graph,
        edge_cfg,
        &rows,
        from_labels.clone(),
        to_labels.clone(),
        batch_size,
    )
    .await?;

    if let Some(delta) = &edge_cfg.common.delta {
        if let Some(max_ts) = compute_max_watermark(&rows, &delta.updated_at_column) {
            watermarks.insert(edge_cfg.common.name.clone(), max_ts);
            save_watermarks(cfg, watermarks)?;
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

    ensure_falkordb_indexes(&mut graph, &cfg.mappings, &node_by_name, &cfg.falkordb).await?;

    let batch_size = cfg.falkordb.max_unwind_batch_size.unwrap_or(1000).max(1);

    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(node_cfg) => {
                if let Err(err) =
                    process_node_mapping(cfg, &mut graph, node_cfg, &mut watermarks, batch_size)
                        .await
                {
                    METRICS.inc_mapping_failed_run(&node_cfg.common.name);
                    return Err(err);
                }
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

                if let Err(err) = process_edge_mapping(
                    cfg,
                    &mut graph,
                    edge_cfg,
                    from_labels,
                    to_labels,
                    &mut watermarks,
                    batch_size,
                )
                .await
                {
                    METRICS.inc_mapping_failed_run(&edge_cfg.common.name);
                    return Err(err);
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
            .unwrap_or_else(|_| "sqlserver_to_falkordb_load_test".to_string());

        let tmp_dir = std::env::temp_dir();
        let input_path = tmp_dir.join("sqlserver_to_falkordb_nodes.json");
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
            sqlserver: None,
            falkordb: FalkorConfig {
                endpoint,
                graph,
                max_unwind_batch_size: Some(10),
                indexes: vec![],
            },
            state: Some(StateConfig {
                backend: StateBackendKind::File,
                file_path: Some(
                    std::env::temp_dir()
                        .join("sqlserver_to_falkordb_state.json")
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
