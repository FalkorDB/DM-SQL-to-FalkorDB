use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use oracle::Connection;
use serde_json::{Map as JsonMap, Number, Value as JsonValue};
use tokio::time::interval;

use crate::config::{Config, EntityMapping, Mode, NodeMappingConfig, OracleConfig};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
use crate::sink::MappedNode;
use crate::sink_async::{
    connect_falkordb_async, delete_edges_batch_async, delete_nodes_batch_async,
    write_edges_batch_async, write_nodes_batch_async, MappedEdge,
};
use crate::source::{oracle_connect_parts, LogicalRow};
use crate::state::{load_watermarks, save_watermarks};

const CDC_SCN_STATE_KEY: &str = "__oracle_cdc_last_scn";

#[derive(Debug, Clone)]
struct CdcMappingMeta {
    mapping_name: String,
    owner_upper: String,
    table_upper: String,
    is_node: bool,
}

#[derive(Debug, Clone)]
struct LogMinerEvent {
    scn: u64,
    seg_owner: String,
    table_name: String,
    operation: String,
    row_id: Option<String>,
    sql_undo: Option<String>,
}

pub async fn run_cdc_stream(cfg: &Config) -> Result<()> {
    let oracle_cfg = cfg
        .oracle
        .as_ref()
        .ok_or_else(|| anyhow!("Missing oracle config for CDC"))?;
    let cdc_cfg = oracle_cfg.cdc.as_ref();
    let enabled = cdc_cfg.and_then(|c| c.enabled).unwrap_or(true);
    if !enabled {
        tracing::info!("oracle.cdc.enabled=false; skipping CDC stream");
        return Ok(());
    }

    let poll_interval_secs = cdc_cfg.and_then(|c| c.poll_interval_secs).unwrap_or(5);
    let max_scn_window = cdc_cfg.and_then(|c| c.max_scn_window).unwrap_or(50_000);

    let mapping_meta = collect_cdc_mapping_meta(cfg, oracle_cfg)?;
    if mapping_meta.is_empty() {
        tracing::warn!("No CDC mappings with source.table configured; skipping CDC stream");
        return Ok(());
    }

    let mut graph = connect_falkordb_async(&cfg.falkordb).await?;

    let mut state_map = load_watermarks(cfg)?;
    let mut last_scn = parse_scn_from_state(&state_map).or(cdc_cfg.and_then(|c| c.start_scn));
    if last_scn.is_none() {
        last_scn = Some(fetch_current_scn(oracle_cfg).await?);
    }
    let mut last_scn = last_scn.unwrap_or(0);

    tracing::info!(
        last_scn,
        poll_interval_secs,
        max_scn_window,
        "Starting Oracle CDC LogMiner stream"
    );

    let mut node_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut delete_node_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut edge_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut delete_edge_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut ticker = interval(Duration::from_secs(poll_interval_secs.max(1)));

    loop {
        ticker.tick().await;

        let current_scn = fetch_current_scn(oracle_cfg).await?;
        if current_scn <= last_scn {
            continue;
        }
        let end_scn = (last_scn + max_scn_window).min(current_scn);

        let events = fetch_logminer_events(oracle_cfg, last_scn + 1, end_scn).await?;
        if events.is_empty() {
            last_scn = end_scn;
            persist_scn(cfg, &mut state_map, last_scn)?;
            continue;
        }

        for event in events {
            for meta in mapping_meta.iter().filter(|m| {
                m.owner_upper == event.seg_owner.to_ascii_uppercase()
                    && m.table_upper == event.table_name.to_ascii_uppercase()
            }) {
                handle_event_for_mapping(
                    oracle_cfg,
                    meta,
                    &event,
                    &mut node_buffers,
                    &mut delete_node_buffers,
                    &mut edge_buffers,
                    &mut delete_edge_buffers,
                )
                .await?;
            }
            last_scn = last_scn.max(event.scn);
        }

        flush_buffers(
            cfg,
            &mut graph,
            &mut node_buffers,
            &mut delete_node_buffers,
            &mut edge_buffers,
            &mut delete_edge_buffers,
        )
        .await?;
        persist_scn(cfg, &mut state_map, last_scn)?;
    }
}

fn collect_cdc_mapping_meta(
    cfg: &Config,
    oracle_cfg: &OracleConfig,
) -> Result<Vec<CdcMappingMeta>> {
    let mut out = Vec::new();
    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(n) if n.common.mode == Mode::Cdc => {
                let table = n.common.source.table.as_deref().ok_or_else(|| {
                    anyhow!(
                        "Node mapping '{}' is mode=cdc but source.table is missing",
                        n.common.name
                    )
                })?;
                let (owner, name) = split_table_owner(table, oracle_cfg);
                out.push(CdcMappingMeta {
                    mapping_name: n.common.name.clone(),
                    owner_upper: owner.to_ascii_uppercase(),
                    table_upper: name.to_ascii_uppercase(),
                    is_node: true,
                });
            }
            EntityMapping::Edge(e) if e.common.mode == Mode::Cdc => {
                let table = e.common.source.table.as_deref().ok_or_else(|| {
                    anyhow!(
                        "Edge mapping '{}' is mode=cdc but source.table is missing",
                        e.common.name
                    )
                })?;
                let (owner, name) = split_table_owner(table, oracle_cfg);
                out.push(CdcMappingMeta {
                    mapping_name: e.common.name.clone(),
                    owner_upper: owner.to_ascii_uppercase(),
                    table_upper: name.to_ascii_uppercase(),
                    is_node: false,
                });
            }
            _ => {}
        }
    }
    Ok(out)
}

fn split_table_owner(table: &str, oracle_cfg: &OracleConfig) -> (String, String) {
    let mut parts = table.split('.');
    if let (Some(owner), Some(name), None) = (parts.next(), parts.next(), parts.next()) {
        return (owner.to_string(), name.to_string());
    }

    let owner = oracle_cfg
        .schema
        .clone()
        .or_else(|| oracle_cfg.user.clone())
        .unwrap_or_else(|| "SYSTEM".to_string());
    (owner, table.to_string())
}

async fn fetch_current_scn(oracle_cfg: &OracleConfig) -> Result<u64> {
    let (user, password, connect_string) = oracle_connect_parts(oracle_cfg)?;
    tokio::task::spawn_blocking(move || -> Result<u64> {
        let conn = Connection::connect(&user, &password, &connect_string)?;
        let scn: u64 = conn.query_row_as("SELECT CURRENT_SCN FROM V$DATABASE", &[])?;
        Ok(scn)
    })
    .await
    .map_err(|e| anyhow!("Oracle current SCN task join error: {e}"))?
}

async fn fetch_logminer_events(
    oracle_cfg: &OracleConfig,
    start_scn: u64,
    end_scn: u64,
) -> Result<Vec<LogMinerEvent>> {
    let (user, password, connect_string) = oracle_connect_parts(oracle_cfg)?;
    tokio::task::spawn_blocking(move || -> Result<Vec<LogMinerEvent>> {
        let conn = Connection::connect(&user, &password, &connect_string)?;
        start_logminer(&conn, start_scn, end_scn)?;

        let sql = r#"
            SELECT SCN, SEG_OWNER, TABLE_NAME, OPERATION, ROW_ID, SQL_UNDO
            FROM V$LOGMNR_CONTENTS
            WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE')
            ORDER BY SCN
        "#;

        let mut events = Vec::new();
        let rows = conn.query(sql, &[])?;
        for row_result in rows {
            let row = row_result?;
            let scn: u64 = row.get(0)?;
            let seg_owner: String = row.get(1)?;
            let table_name: String = row.get(2)?;
            let operation: String = row.get(3)?;
            let row_id: Option<String> = row.get(4).ok();
            let sql_undo: Option<String> = row.get(5).ok();
            events.push(LogMinerEvent {
                scn,
                seg_owner,
                table_name,
                operation,
                row_id,
                sql_undo,
            });
        }

        let _ = conn.execute("BEGIN DBMS_LOGMNR.END_LOGMNR(); END;", &[]);
        Ok(events)
    })
    .await
    .map_err(|e| anyhow!("Oracle LogMiner task join error: {e}"))?
}

fn start_logminer(conn: &Connection, start_scn: u64, end_scn: u64) -> Result<()> {
    let options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY";
    let plsql = format!(
        "BEGIN DBMS_LOGMNR.START_LOGMNR(startScn => {start_scn}, endScn => {end_scn}, options => {options}); END;"
    );
    conn.execute(&plsql, &[]).with_context(|| {
        format!(
            "Failed to start LogMiner for SCN range {}..{}; ensure required privileges and supplemental logging are enabled",
            start_scn, end_scn
        )
    })?;
    Ok(())
}

async fn handle_event_for_mapping(
    oracle_cfg: &OracleConfig,
    meta: &CdcMappingMeta,
    event: &LogMinerEvent,
    node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    delete_node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    delete_edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
) -> Result<()> {
    let operation = event.operation.to_ascii_uppercase();

    let row = if operation == "DELETE" {
        event
            .sql_undo
            .as_deref()
            .and_then(parse_sql_undo_where_clause)
            .map(|values| LogicalRow { values })
    } else if let Some(row_id) = &event.row_id {
        fetch_row_by_rowid(
            oracle_cfg,
            &meta.owner_upper,
            &meta.table_upper,
            row_id.as_str(),
        )
        .await?
    } else {
        None
    };

    let Some(row) = row else {
        tracing::warn!(
            mapping = %meta.mapping_name,
            operation = %operation,
            scn = event.scn,
            "Skipping CDC event due to unavailable row payload"
        );
        return Ok(());
    };

    let is_delete = operation == "DELETE";
    if meta.is_node {
        if is_delete {
            delete_node_bufs
                .entry(meta.mapping_name.clone())
                .or_default()
                .push(row);
        } else {
            node_bufs
                .entry(meta.mapping_name.clone())
                .or_default()
                .push(row);
        }
    } else if is_delete {
        delete_edge_bufs
            .entry(meta.mapping_name.clone())
            .or_default()
            .push(row);
    } else {
        edge_bufs
            .entry(meta.mapping_name.clone())
            .or_default()
            .push(row);
    }

    Ok(())
}

async fn fetch_row_by_rowid(
    oracle_cfg: &OracleConfig,
    owner: &str,
    table: &str,
    row_id: &str,
) -> Result<Option<LogicalRow>> {
    let (user, password, connect_string) = oracle_connect_parts(oracle_cfg)?;
    let owner = owner.to_string();
    let table = table.to_string();
    let row_id = row_id.to_string();

    tokio::task::spawn_blocking(move || -> Result<Option<LogicalRow>> {
        let conn = Connection::connect(&user, &password, &connect_string)?;
        let sql = format!("SELECT * FROM {}.{} WHERE ROWID = :1", owner, table);
        let mut result_set = conn.query(&sql, &[&row_id])?;
        let column_names = result_set
            .column_info()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<_>>();
        if let Some(row_result) = (&mut result_set).next() {
            let row = row_result?;
            return Ok(Some(row_to_logical_row(&row, &column_names)));
        }
        Ok(None)
    })
    .await
    .map_err(|e| anyhow!("Oracle ROWID fetch task join error: {e}"))?
}

fn row_to_logical_row(row: &oracle::Row, column_names: &[String]) -> LogicalRow {
    let mut out = JsonMap::new();
    for (idx, val) in row.sql_values().iter().enumerate() {
        let column = column_names
            .get(idx)
            .cloned()
            .unwrap_or_else(|| format!("col_{idx}"));
        out.insert(column, oracle_sql_value_to_json(val));
    }
    LogicalRow { values: out }
}

fn oracle_sql_value_to_json(value: &oracle::SqlValue) -> JsonValue {
    if value.is_null().unwrap_or(false) {
        return JsonValue::Null;
    }
    if let Ok(v) = value.get::<i64>() {
        return JsonValue::from(v);
    }
    if let Ok(v) = value.get::<u64>() {
        return JsonValue::from(v);
    }
    if let Ok(v) = value.get::<f64>() {
        return Number::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null);
    }
    if let Ok(v) = value.get::<bool>() {
        return JsonValue::Bool(v);
    }
    if let Ok(v) = value.get::<chrono::NaiveDateTime>() {
        return JsonValue::String(v.format("%Y-%m-%d %H:%M:%S%.f").to_string());
    }
    if let Ok(v) = value.get::<chrono::NaiveDate>() {
        return JsonValue::String(v.format("%Y-%m-%d").to_string());
    }
    if let Ok(v) = value.get::<String>() {
        return JsonValue::String(v);
    }
    JsonValue::String(value.to_string())
}

fn parse_sql_undo_where_clause(sql_undo: &str) -> Option<JsonMap<String, JsonValue>> {
    let lower = sql_undo.to_ascii_lowercase();
    let where_idx = lower.find(" where ")?;
    let where_clause = &sql_undo[where_idx + 7..];

    let mut out = JsonMap::new();
    for cond in where_clause.split(" and ") {
        let cond = cond.trim().trim_end_matches(';');
        let cond_lower = cond.to_ascii_lowercase();

        if let Some(pos) = cond_lower.find(" is null") {
            let left = &cond[..pos];
            let col = unquote_identifier(left.trim());
            if !col.eq_ignore_ascii_case("rowid") {
                out.insert(col, JsonValue::Null);
            }
            continue;
        }

        let Some((lhs, rhs)) = cond.split_once('=') else {
            continue;
        };
        let col = unquote_identifier(lhs.trim());
        if col.eq_ignore_ascii_case("rowid") {
            continue;
        }
        let rhs = rhs.trim().trim_end_matches(';');
        out.insert(col, parse_sql_literal(rhs));
    }

    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn unquote_identifier(value: &str) -> String {
    value
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

fn parse_sql_literal(value: &str) -> JsonValue {
    let v = value.trim();
    if v.eq_ignore_ascii_case("null") {
        return JsonValue::Null;
    }
    if v.starts_with('\'') && v.ends_with('\'') && v.len() >= 2 {
        let inner = &v[1..v.len() - 1];
        return JsonValue::String(inner.replace("''", "'"));
    }
    if let Ok(i) = v.parse::<i64>() {
        return JsonValue::from(i);
    }
    if let Ok(u) = v.parse::<u64>() {
        return JsonValue::from(u);
    }
    if let Ok(f) = v.parse::<f64>() {
        return Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or_else(|| JsonValue::String(v.to_string()));
    }
    JsonValue::String(v.to_string())
}

fn parse_scn_from_state(state_map: &HashMap<String, String>) -> Option<u64> {
    state_map.get(CDC_SCN_STATE_KEY)?.parse::<u64>().ok()
}

fn persist_scn(cfg: &Config, state_map: &mut HashMap<String, String>, scn: u64) -> Result<()> {
    state_map.insert(CDC_SCN_STATE_KEY.to_string(), scn.to_string());
    save_watermarks(cfg, state_map)
}

async fn flush_buffers(
    cfg: &Config,
    graph: &mut falkordb::AsyncGraph,
    node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    delete_node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    delete_edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
) -> Result<()> {
    let mut node_by_name: HashMap<&str, &NodeMappingConfig> = HashMap::new();
    for m in &cfg.mappings {
        if let EntityMapping::Node(n) = m {
            node_by_name.insert(n.common.name.as_str(), n);
        }
    }

    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(n) => {
                if n.common.mode != Mode::Cdc {
                    continue;
                }

                if let Some(rows) = node_bufs.get_mut(&n.common.name) {
                    if !rows.is_empty() {
                        let nodes: Vec<MappedNode> = map_rows_to_nodes(rows, n)?;
                        write_nodes_batch_async(graph, n, &nodes).await?;
                        rows.clear();
                    }
                }
                if let Some(rows) = delete_node_bufs.get_mut(&n.common.name) {
                    if !rows.is_empty() {
                        let nodes: Vec<MappedNode> = map_rows_to_nodes(rows, n)?;
                        delete_nodes_batch_async(graph, n, &nodes).await?;
                        rows.clear();
                    }
                }
            }
            EntityMapping::Edge(e) => {
                if e.common.mode != Mode::Cdc {
                    continue;
                }

                let from_node = node_by_name.get(e.from.node_mapping.as_str());
                let to_node = node_by_name.get(e.to.node_mapping.as_str());
                if from_node.is_none() || to_node.is_none() {
                    continue;
                }

                let from_labels = e
                    .from
                    .label_override
                    .clone()
                    .unwrap_or_else(|| from_node.expect("checked").labels.clone());
                let to_labels =
                    e.to.label_override
                        .clone()
                        .unwrap_or_else(|| to_node.expect("checked").labels.clone());

                if let Some(rows) = edge_bufs.get_mut(&e.common.name) {
                    if !rows.is_empty() {
                        let edges: Vec<MappedEdge> = map_rows_to_edges(rows, e)?;
                        write_edges_batch_async(graph, e, &edges, &from_labels, &to_labels).await?;
                        rows.clear();
                    }
                }
                if let Some(rows) = delete_edge_bufs.get_mut(&e.common.name) {
                    if !rows.is_empty() {
                        let edges: Vec<MappedEdge> = map_rows_to_edges(rows, e)?;
                        delete_edges_batch_async(graph, e, &edges, &from_labels, &to_labels)
                            .await?;
                        rows.clear();
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_sql_undo_where_clause_basic() {
        let sql = "delete from \"APP\".\"CUSTOMERS\" where \"CUSTOMER_ID\" = 7 and \"EMAIL\" = 'a@b.c' and ROWID = 'AAABBB';";
        let parsed = parse_sql_undo_where_clause(sql).expect("parsed");
        assert_eq!(parsed.get("CUSTOMER_ID"), Some(&json!(7)));
        assert_eq!(parsed.get("EMAIL"), Some(&json!("a@b.c")));
        assert!(parsed.get("ROWID").is_none());
    }

    #[test]
    fn parse_sql_undo_where_clause_with_null() {
        let sql = "delete from \"APP\".\"T\" where \"COL_A\" is null and \"COL_B\" = 'x';";
        let parsed = parse_sql_undo_where_clause(sql).expect("parsed");
        assert_eq!(parsed.get("COL_A"), Some(&JsonValue::Null));
        assert_eq!(parsed.get("COL_B"), Some(&json!("x")));
    }
}
