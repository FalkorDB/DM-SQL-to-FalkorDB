#![allow(dead_code)]
use anyhow::{anyhow, Result};
use bytes::{Buf, Bytes};
use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::config::{Config, EntityMapping, Mode};
use crate::mapping::{map_rows_to_edges, map_rows_to_nodes};
use crate::sink::MappedNode;
use crate::sink_async::{
    connect_falkordb_async, delete_edges_batch_async, delete_nodes_batch_async,
    write_edges_batch_async, write_nodes_batch_async, MappedEdge,
};
use crate::source::connect_postgres;
use crate::source::LogicalRow;

#[derive(Debug, Clone)]
pub struct RelationColumn {
    pub flags: u8,
    pub name: String,
    pub type_oid: i32,
    pub type_mod: i32,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug)]
pub enum PgOutputMessage {
    Relation(Relation),
    Begin,
    Commit,
    Insert {
        rel_id: u32,
        tuple: Vec<Option<String>>,
    },
    Update {
        rel_id: u32,
        old_tuple: Option<Vec<Option<String>>>,
        new_tuple: Vec<Option<String>>,
    },
    Delete {
        rel_id: u32,
        old_tuple: Vec<Option<String>>,
    },
    Unknown(u8),
}

pub fn parse_pgoutput(mut buf: Bytes) -> Result<PgOutputMessage> {
    if !buf.has_remaining() {
        return Err(anyhow!("Empty message"));
    }
    let msg_type = buf.get_u8();

    match msg_type {
        b'R' => {
            let oid = buf.get_u32();
            let namespace = read_c_string(&mut buf)?;
            let name = read_c_string(&mut buf)?;
            let replica_identity = buf.get_u8();
            let num_cols = buf.get_u16();
            let mut columns = Vec::with_capacity(num_cols as usize);
            for _ in 0..num_cols {
                let flags = buf.get_u8();
                let col_name = read_c_string(&mut buf)?;
                let type_oid = buf.get_i32();
                let type_mod = buf.get_i32();
                columns.push(RelationColumn {
                    flags,
                    name: col_name,
                    type_oid,
                    type_mod,
                });
            }
            return Ok(PgOutputMessage::Relation(Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            }));
        }
        b'B' => {
            return Ok(PgOutputMessage::Begin);
        }
        b'C' => {
            return Ok(PgOutputMessage::Commit);
        }
        b'I' => {
            let rel_id = buf.get_u32();
            let new_tuple_type = buf.get_u8(); // 'N'
            if new_tuple_type != b'N' {
                return Err(anyhow!("Expected 'N' in Insert, got {}", new_tuple_type));
            }
            let tuple = read_tuple_data(&mut buf)?;
            return Ok(PgOutputMessage::Insert { rel_id, tuple });
        }
        b'U' => {
            let rel_id = buf.get_u32();
            let mut tuple_type = buf.get_u8();
            let mut old_tuple = None;
            if tuple_type == b'K' || tuple_type == b'O' {
                old_tuple = Some(read_tuple_data(&mut buf)?);
                tuple_type = buf.get_u8();
            }
            if tuple_type != b'N' {
                return Err(anyhow!("Expected 'N' in Update, got {}", tuple_type));
            }
            let new_tuple = read_tuple_data(&mut buf)?;
            return Ok(PgOutputMessage::Update {
                rel_id,
                old_tuple,
                new_tuple,
            });
        }
        b'D' => {
            let rel_id = buf.get_u32();
            let tuple_type = buf.get_u8();
            if tuple_type != b'K' && tuple_type != b'O' {
                return Err(anyhow!("Expected 'K' or 'O' in Delete, got {}", tuple_type));
            }
            let old_tuple = read_tuple_data(&mut buf)?;
            return Ok(PgOutputMessage::Delete { rel_id, old_tuple });
        }
        _ => return Ok(PgOutputMessage::Unknown(msg_type)),
    }
}

fn read_c_string(buf: &mut Bytes) -> Result<String> {
    let mut end = 0;
    while end < buf.len() && buf[end] != 0 {
        end += 1;
    }
    if end == buf.len() {
        return Err(anyhow!("C string missing null terminator"));
    }
    let s = String::from_utf8_lossy(&buf[..end]).into_owned();
    buf.advance(end + 1);
    Ok(s)
}

fn read_tuple_data(buf: &mut Bytes) -> Result<Vec<Option<String>>> {
    let num_cols = buf.get_u16();
    let mut tuple = Vec::with_capacity(num_cols as usize);
    for _ in 0..num_cols {
        let col_type = buf.get_u8();
        match col_type {
            b'n' => tuple.push(None),
            b'u' => tuple.push(None), // Unchanged toast
            b't' | b'b' => {
                let len = buf.get_u32() as usize;
                let data = &buf[..len];
                let s = String::from_utf8_lossy(data).into_owned();
                tuple.push(Some(s));
                buf.advance(len);
            }
            _ => return Err(anyhow!("Unknown tuple data type: {}", col_type)),
        }
    }
    Ok(tuple)
}

pub fn tuple_to_logical_row(
    tuple: &[Option<String>],
    relation: &Relation,
) -> Result<Map<String, Value>> {
    let mut row = Map::new();
    for (i, col) in relation.columns.iter().enumerate() {
        if let Some(val) = tuple.get(i).unwrap_or(&None) {
            let json_val = serde_json::from_str(val).unwrap_or_else(|_| Value::String(val.clone()));
            row.insert(col.name.clone(), json_val);
        } else {
            row.insert(col.name.clone(), Value::Null);
        }
    }
    Ok(row)
}

pub async fn run_cdc_batch(cfg: &Config) -> Result<()> {
    let pg_cfg = cfg
        .postgres
        .as_ref()
        .ok_or_else(|| anyhow!("Missing postgres config for CDC"))?;
    let pub_name = pg_cfg.publication_name.as_deref().unwrap_or("falkordb_pub");
    let slot_name = pg_cfg.slot_name.as_deref().unwrap_or("falkordb_slot");
    let batch_size = pg_cfg.fetch_batch_size.unwrap_or(10000);

    let client = connect_postgres(pg_cfg).await?;

    // Ensure publication
    let pub_exists: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = $1",
            &[&pub_name],
        )
        .await?
        .get(0);
    if pub_exists == 0 {
        tracing::info!("Creating publication {}", pub_name);
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION \"{}\" FOR ALL TABLES",
                pub_name
            ))
            .await
            .ok();
    }

    // Ensure slot
    let slot_exists: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?
        .get(0);
    if slot_exists == 0 {
        tracing::info!("Creating logical replication slot {}", slot_name);
        client
            .query(
                &format!(
                    "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                    slot_name
                ),
                &[],
            )
            .await?;
    }

    let mut graph = connect_falkordb_async(&cfg.falkordb).await?;

    // We must maintain relation schemas locally since CDC payload only references them by OID
    let mut relations: HashMap<u32, Relation> = HashMap::new();
    let mut node_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut delete_node_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut edge_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();
    let mut delete_edge_buffers: HashMap<String, Vec<LogicalRow>> = HashMap::new();

    let query = format!("SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes('{}', NULL, {}, 'proto_version', '1', 'publication_names', '{}')", slot_name, batch_size, pub_name);
    let rows = client.query(&query, &[]).await?;

    let mut last_lsn: Option<String> = None;
    let mut changes_processed = 0;

    for row in rows {
        let lsn: String = row.get("lsn");
        let data: Vec<u8> = row.get("data");

        last_lsn = Some(lsn.clone());
        changes_processed += 1;

        let parsed = parse_pgoutput(Bytes::from(data))?;
        match parsed {
            PgOutputMessage::Relation(rel) => {
                relations.insert(rel.oid, rel);
            }
            PgOutputMessage::Insert { rel_id, tuple } => {
                if let Some(rel) = relations.get(&rel_id) {
                    let r = tuple_to_logical_row(&tuple, rel)?;
                    buffer_row(
                        &cfg.mappings,
                        rel,
                        r,
                        &mut node_buffers,
                        &mut edge_buffers,
                        false,
                    );
                }
            }
            PgOutputMessage::Update {
                rel_id,
                old_tuple: _,
                new_tuple,
            } => {
                if let Some(rel) = relations.get(&rel_id) {
                    let r = tuple_to_logical_row(&new_tuple, rel)?;
                    buffer_row(
                        &cfg.mappings,
                        rel,
                        r,
                        &mut node_buffers,
                        &mut edge_buffers,
                        false,
                    );
                }
            }
            PgOutputMessage::Delete { rel_id, old_tuple } => {
                if let Some(rel) = relations.get(&rel_id) {
                    let r = tuple_to_logical_row(&old_tuple, rel)?;
                    buffer_row(
                        &cfg.mappings,
                        rel,
                        r,
                        &mut delete_node_buffers,
                        &mut delete_edge_buffers,
                        true,
                    );
                }
            }
            _ => {}
        }
    }

    if changes_processed > 0 {
        tracing::info!("Processing {} CDC events", changes_processed);
        flush_buffers(
            cfg,
            &mut graph,
            &mut node_buffers,
            &mut delete_node_buffers,
            &mut edge_buffers,
            &mut delete_edge_buffers,
        )
        .await?;

        // Advance the slot so Postgres knows we consumed these
        if let Some(lsn) = last_lsn {
            client
                .query(
                    &format!(
                        "SELECT pg_replication_slot_advance('{}', '{}')",
                        slot_name, lsn
                    ),
                    &[],
                )
                .await?;
        }
    }

    Ok(())
}

fn buffer_row(
    mappings: &[EntityMapping],
    rel: &Relation,
    row: Map<String, Value>,
    node_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    edge_bufs: &mut HashMap<String, Vec<LogicalRow>>,
    _is_delete: bool,
) {
    let table_name = format!("{}.{}", rel.namespace, rel.name);
    let table_name_bare = rel.name.clone();

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

        if source_table == table_name || source_table == table_name_bare {
            if is_node {
                node_bufs.entry(name.clone()).or_default().push(row.clone());
            } else {
                edge_bufs.entry(name.clone()).or_default().push(row.clone());
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
    use bytes::BufMut;
    use bytes::BytesMut;

    #[test]
    fn test_read_c_string() {
        let mut buf = Bytes::from_static(b"public\0");
        let s = read_c_string(&mut buf).unwrap();
        assert_eq!(s, "public");
    }

    #[test]
    fn test_parse_relation() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'R');
        buf.put_u32(12345); // oid
        buf.put_slice(b"public\0"); // namespace
        buf.put_slice(b"users\0"); // name
        buf.put_u8(b'd'); // replica identity
        buf.put_u16(2); // num cols
        
        // col 1
        buf.put_u8(1); // flags
        buf.put_slice(b"id\0"); // name
        buf.put_i32(23); // type_oid (int4)
        buf.put_i32(-1); // type_mod

        // col 2
        buf.put_u8(0); // flags
        buf.put_slice(b"name\0"); // name
        buf.put_i32(25); // type_oid (text)
        buf.put_i32(-1); // type_mod

        let msg = parse_pgoutput(buf.freeze()).unwrap();
        if let PgOutputMessage::Relation(rel) = msg {
            assert_eq!(rel.oid, 12345);
            assert_eq!(rel.namespace, "public");
            assert_eq!(rel.name, "users");
            assert_eq!(rel.columns.len(), 2);
            assert_eq!(rel.columns[0].name, "id");
            assert_eq!(rel.columns[1].name, "name");
        } else {
            panic!("Expected Relation");
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'I');
        buf.put_u32(12345); // rel_id
        buf.put_u8(b'N'); // new tuple

        buf.put_u16(2); // num cols
        // col 1: text "1"
        buf.put_u8(b't');
        buf.put_u32(1); // len
        buf.put_slice(b"1");
        
        // col 2: text "Alice"
        buf.put_u8(b't');
        buf.put_u32(5); // len
        buf.put_slice(b"Alice");

        let msg = parse_pgoutput(buf.freeze()).unwrap();
        if let PgOutputMessage::Insert { rel_id, tuple } = msg {
            assert_eq!(rel_id, 12345);
            assert_eq!(tuple.len(), 2);
            assert_eq!(tuple[0].as_deref(), Some("1"));
            assert_eq!(tuple[1].as_deref(), Some("Alice"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_tuple_to_logical_row() {
        let rel = Relation {
            oid: 12345,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'd',
            columns: vec![
                RelationColumn { flags: 1, name: "id".into(), type_oid: 23, type_mod: -1 },
                RelationColumn { flags: 0, name: "name".into(), type_oid: 25, type_mod: -1 },
            ]
        };

        let tuple = vec![Some("1".into()), Some("Alice".into())];
        let row = tuple_to_logical_row(&tuple, &rel).unwrap();
        
        assert_eq!(row.get("id").unwrap().as_i64(), Some(1)); // Should parse "1" as number
        assert_eq!(row.get("name").unwrap().as_str(), Some("Alice"));
    }
}
