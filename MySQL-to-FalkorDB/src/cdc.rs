use std::collections::HashMap;

use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use mysql_async::binlog::events::{EventData, TableMapEvent};
use mysql_async::prelude::Queryable;
use mysql_async::BinlogStreamRequest;
use serde_json::{Map, Value};
use std::time::Duration;
use tokio::time::interval;

use crate::config::{Config, EntityMapping, Mode};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
use crate::sink::MappedNode;
use crate::sink_async::{
    connect_falkordb_async, delete_edges_batch_async, delete_nodes_batch_async,
    write_edges_batch_async, write_nodes_batch_async, MappedEdge,
};
use crate::source::{mysql_opts, LogicalRow};

pub async fn run_cdc_stream(cfg: &Config) -> Result<()> {
    let mysql_cfg = cfg
        .mysql
        .as_ref()
        .ok_or_else(|| anyhow!("Missing mysql config for CDC"))?;
    let cdc_cfg = mysql_cfg.cdc.as_ref();
    let server_id = cdc_cfg.and_then(|c| c.server_id).unwrap_or(9999);

    let opts = mysql_opts(mysql_cfg)?;

    // First, connect to fetch schema metadata so we can map column indexes to names
    let mut schema_conn = mysql_async::Conn::new(opts.clone()).await?;
    let mut table_id_map: HashMap<u64, TableMapEvent<'static>> = HashMap::new();

    let mut table_schemas_cache: HashMap<String, Vec<String>> = HashMap::new();

    let mut needed_tables = Vec::new();
    for mapping in &cfg.mappings {
        let (source, mode) = match mapping {
            EntityMapping::Node(n) => (&n.common.source.table, &n.common.mode),
            EntityMapping::Edge(e) => (&e.common.source.table, &e.common.mode),
        };
        if *mode == Mode::Cdc {
            if let Some(t) = source {
                needed_tables.push(t.clone());
            }
        }
    }

    for table_ref in needed_tables {
        let parts: Vec<&str> = table_ref.split('.').collect();
        let (db, table) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            (mysql_cfg.database.as_deref().unwrap_or(""), parts[0])
        };

        if db.is_empty() {
            tracing::warn!("Table '{}' is missing database prefix and no default database is set. CDC might fail.", table);
            continue;
        }

        let query = format!(
            "SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' ORDER BY ORDINAL_POSITION",
            db, table
        );
        let columns: Vec<String> = schema_conn.query(&query).await?;
        table_schemas_cache.insert(format!("{}.{}", db, table), columns);
    }
    drop(schema_conn);

    let mut stream_conn = mysql_async::Conn::new(opts.clone()).await?;

    // Determine starting position
    let row: Option<(String, u64)> = stream_conn.query_first("SHOW MASTER STATUS").await?;
    let (filename, pos) = row.unwrap_or((String::new(), 4));

    let req = BinlogStreamRequest::new(server_id)
        .with_filename(filename.as_bytes())
        .with_pos(pos);

    let mut stream = stream_conn.get_binlog_stream(req).await?;

    tracing::info!("Starting MySQL CDC stream from {} at {}", filename, pos);

    let mut graph = connect_falkordb_async(&cfg.falkordb).await?;

    let mut node_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut delete_node_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut edge_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut delete_edge_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();

    let mut changes_processed = 0;
    let flush_interval = Duration::from_secs(5); // TODO: configurable
    let mut tick = interval(flush_interval);

    loop {
        tokio::select! {
            event_result = stream.next() => {
                let event = match event_result {
                    Some(Ok(e)) => e,
                    Some(Err(e)) => {
                        tracing::error!("Binlog stream error: {}", e);
                        break;
                    }
                    None => {
                        tracing::info!("Binlog stream ended");
                        break;
                    }
                };

                if let Some(event_data) = event.read_data()? {
                    use mysql_async::binlog::events::RowsEventData;
                    match event_data {
                        EventData::TableMapEvent(tme) => {
                            table_id_map.insert(tme.table_id(), tme.into_owned());
                        }
                        EventData::RowsEvent(red) => {
                            let table_id = red.table_id();
                            if let Some(tme) = table_id_map.get(&table_id) {
                                let db = tme.database_name().into_owned();
                                let tbl = tme.table_name().into_owned();
                                let full_name = format!("{}.{}", db, tbl);
                                if let Some(col_names) = table_schemas_cache.get(&full_name) {
                                    match red {
                                        RowsEventData::WriteRowsEventV1(wre) => {
                                            for row_res in wre.rows(tme) {
                                                if let Ok((_, Some(after))) = row_res {
                                                    if let Ok(logical_row) = binlog_row_to_logical_row(after, col_names) {
                                                        buffer_row(&cfg.mappings, &full_name, logical_row, &mut node_buffers, &mut edge_buffers, false);
                                                        changes_processed += 1;
                                                    }
                                                }
                                            }
                                        }
                                        RowsEventData::WriteRowsEvent(wre) => {
                                            for row_res in wre.rows(tme) {
                                                if let Ok((_, Some(after))) = row_res {
                                                    if let Ok(logical_row) = binlog_row_to_logical_row(after, col_names) {
                                                        buffer_row(&cfg.mappings, &full_name, logical_row, &mut node_buffers, &mut edge_buffers, false);
                                                        changes_processed += 1;
                                                    }
                                                }
                                            }
                                        }
                                        RowsEventData::UpdateRowsEventV1(ure) => {
                                            for row_res in ure.rows(tme) {
                                                if let Ok((_, Some(after))) = row_res {
                                                    if let Ok(logical_row) = binlog_row_to_logical_row(after, col_names) {
                                                        buffer_row(&cfg.mappings, &full_name, logical_row, &mut node_buffers, &mut edge_buffers, false);
                                                        changes_processed += 1;
                                                    }
                                                }
                                            }
                                        }
                                        RowsEventData::UpdateRowsEvent(ure) => {
                                            for row_res in ure.rows(tme) {
                                                if let Ok((_, Some(after))) = row_res {
                                                    if let Ok(logical_row) = binlog_row_to_logical_row(after, col_names) {
                                                        buffer_row(&cfg.mappings, &full_name, logical_row, &mut node_buffers, &mut edge_buffers, false);
                                                        changes_processed += 1;
                                                    }
                                                }
                                            }
                                        }
                                        RowsEventData::DeleteRowsEventV1(dre) => {
                                            for row_res in dre.rows(tme) {
                                                if let Ok((Some(before), _)) = row_res {
                                                    if let Ok(logical_row) = binlog_row_to_logical_row(before, col_names) {
                                                        buffer_row(&cfg.mappings, &full_name, logical_row, &mut delete_node_buffers, &mut delete_edge_buffers, true);
                                                        changes_processed += 1;
                                                    }
                                                }
                                            }
                                        }
                                        RowsEventData::DeleteRowsEvent(dre) => {
                                            for row_res in dre.rows(tme) {
                                                if let Ok((Some(before), _)) = row_res {
                                                    if let Ok(logical_row) = binlog_row_to_logical_row(before, col_names) {
                                                        buffer_row(&cfg.mappings, &full_name, logical_row, &mut delete_node_buffers, &mut delete_edge_buffers, true);
                                                        changes_processed += 1;
                                                    }
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ = tick.tick() => {
                if changes_processed > 0 {
                    tracing::info!("Flushing {} CDC events to FalkorDB", changes_processed);
                    flush_buffers(
                        cfg,
                        &mut graph,
                        &mut node_buffers,
                        &mut delete_node_buffers,
                        &mut edge_buffers,
                        &mut delete_edge_buffers
                    ).await?;
                    changes_processed = 0;
                    // In mysql_async binlog stream, we just keep consuming. State tracking could save GTID/pos here.
                }
            }
        }
    }

    Ok(())
}

fn binlog_row_to_logical_row(
    row: mysql_async::binlog::row::BinlogRow,
    col_names: &[String],
) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    let values = row.unwrap();
    for (i, val) in values.into_iter().enumerate() {
        let col_name = if i < col_names.len() {
            col_names[i].clone()
        } else {
            format!("col_{}", i)
        };

        // Use standard JSON representation for the binlog value
        let json_val = match val {
            mysql_async::binlog::value::BinlogValue::Value(v) => match v {
                mysql_async::Value::NULL => Value::Null,
                mysql_async::Value::Bytes(b) => {
                    let s = String::from_utf8_lossy(&b).into_owned();
                    serde_json::from_str(&s).unwrap_or_else(|_| Value::String(s))
                }
                mysql_async::Value::Int(i) => Value::Number(i.into()),
                mysql_async::Value::UInt(u) => Value::Number(u.into()),
                mysql_async::Value::Float(f) => serde_json::Number::from_f64(f as f64)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                mysql_async::Value::Double(d) => serde_json::Number::from_f64(d)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                mysql_async::Value::Date(y, m, d, h, i, s, _u) => Value::String(format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    y, m, d, h, i, s
                )),
                mysql_async::Value::Time(neg, d, h, m, s, _u) => {
                    let sign = if neg { "-" } else { "" };
                    Value::String(format!("{}{:02} {:02}:{:02}:{:02}", sign, d, h, m, s))
                }
            },
            mysql_async::binlog::value::BinlogValue::Jsonb(_j) => {
                // To keep it simple, extract JSON as a string, then parse
                Value::Null // TODO: handle jsonb if needed, but often mapped to Bytes for string payload
            }
            mysql_async::binlog::value::BinlogValue::JsonDiff(_) => Value::Null,
        };
        map.insert(col_name, json_val);
    }
    Ok(map)
}

fn buffer_row(
    mappings: &[EntityMapping],
    full_table_name: &str,
    row: Map<String, Value>,
    node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    _is_delete: bool,
) {
    let parts: Vec<&str> = full_table_name.split('.').collect();
    let bare_table_name = parts.last().unwrap_or(&full_table_name);

    for mapping in mappings {
        let (source_table, name, is_node) = match mapping {
            EntityMapping::Node(n) => {
                if n.common.mode != Mode::Cdc {
                    continue;
                }
                (
                    n.common.source.table.as_deref().unwrap_or(""),
                    &n.common.name,
                    true,
                )
            }
            EntityMapping::Edge(e) => {
                if e.common.mode != Mode::Cdc {
                    continue;
                }
                (
                    e.common.source.table.as_deref().unwrap_or(""),
                    &e.common.name,
                    false,
                )
            }
        };

        if source_table == full_table_name || source_table == *bare_table_name {
            if is_node {
                node_bufs.entry(name.clone()).or_default().push(LogicalRow {
                    values: row.clone(),
                });
            } else {
                edge_bufs.entry(name.clone()).or_default().push(LogicalRow {
                    values: row.clone(),
                });
            }
        }
    }
}

async fn flush_buffers(
    cfg: &Config,
    graph: &mut falkordb::AsyncGraph,
    node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    delete_node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    delete_edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
) -> Result<()> {
    let mut node_by_name = HashMap::new();
    for m in &cfg.mappings {
        if let EntityMapping::Node(n) = m {
            node_by_name.insert(n.common.name.as_str(), n);
        }
    }

    for mapping in &cfg.mappings {
        match mapping {
            EntityMapping::Node(n) => {
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
                let from_node = node_by_name.get(e.from.node_mapping.as_str());
                let to_node = node_by_name.get(e.to.node_mapping.as_str());
                if from_node.is_none() || to_node.is_none() {
                    continue;
                }

                let from_labels = e
                    .from
                    .label_override
                    .clone()
                    .unwrap_or_else(|| from_node.unwrap().labels.clone());
                let to_labels =
                    e.to.label_override
                        .clone()
                        .unwrap_or_else(|| to_node.unwrap().labels.clone());

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
    use mysql_async::binlog::row::BinlogRow;
    use mysql_async::binlog::value::BinlogValue;
    use mysql_async::Value as MySqlValue;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn test_binlog_row_to_logical_row() {
        let cols = vec![
            "id".to_string(),
            "name".to_string(),
            "created_at".to_string(),
        ];

        let values = vec![
            Some(BinlogValue::Value(MySqlValue::Int(42))),
            Some(BinlogValue::Value(MySqlValue::Bytes(b"Alice".to_vec()))),
            Some(BinlogValue::Value(MySqlValue::Date(
                2023, 10, 15, 14, 30, 0, 0,
            ))),
        ];

        // The second parameter is an Arc<[Column]> which we can just pass as empty for our mock
        let binlog_row = BinlogRow::new(values, Arc::new([]));

        let logical_row = binlog_row_to_logical_row(binlog_row, &cols).expect("Should convert");

        assert_eq!(logical_row.get("id"), Some(&json!(42)));
        assert_eq!(logical_row.get("name"), Some(&json!("Alice")));
        assert_eq!(
            logical_row.get("created_at"),
            Some(&json!("2023-10-15 14:30:00"))
        );
    }

    #[test]
    fn test_binlog_row_to_logical_row_with_missing_col_names() {
        let cols = vec!["id".to_string()];

        let values = vec![
            Some(BinlogValue::Value(MySqlValue::Int(42))),
            Some(BinlogValue::Value(MySqlValue::Bytes(b"Bob".to_vec()))), // Missing col name
        ];

        let binlog_row = BinlogRow::new(values, Arc::new([]));

        let logical_row = binlog_row_to_logical_row(binlog_row, &cols).expect("Should convert");

        assert_eq!(logical_row.get("id"), Some(&json!(42)));
        assert_eq!(logical_row.get("col_1"), Some(&json!("Bob")));
    }
}
