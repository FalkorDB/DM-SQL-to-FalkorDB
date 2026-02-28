use std::fs;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::config::{CommonMappingFields, Config, DatabricksConfig, Mode};

/// Logical representation of a single row coming from Databricks or a file source.
pub type LogicalRow = JsonMap<String, JsonValue>;

/// Simple Databricks SQL REST client using the Statement Execution API.
struct DatabricksClient {
    client: Client,
    base_url: String,
    warehouse_id: String,
    token: String,
}

impl DatabricksClient {
    fn new(cfg: &DatabricksConfig) -> Result<Self> {
        let token = cfg
            .access_token
            .as_ref()
            .ok_or_else(|| anyhow!("databricks.access_token must be set"))?
            .clone();

        let mut host = cfg.host.trim().to_string();
        if !host.starts_with("http://") && !host.starts_with("https://") {
            host = format!("https://{}", host);
        }
        let base_url = host.trim_end_matches('/').to_string();

        let warehouse_id = extract_warehouse_id(&cfg.http_path)
            .ok_or_else(|| anyhow!("Could not infer warehouse_id from databricks.http_path"))?;

        let timeout = cfg
            .query_timeout_ms
            .map(|ms| Duration::from_millis(ms))
            .unwrap_or_else(|| Duration::from_secs(60));

        let client = Client::builder().timeout(timeout).build()?;

        Ok(Self {
            client,
            base_url,
            warehouse_id,
            token,
        })
    }

    /// Execute a SQL statement and return all rows from the first result chunk.
    /// For now we assume results fit in a single inline chunk.
    async fn execute_query(&self, statement: &str) -> Result<Vec<LogicalRow>> {
        let url = format!("{}/api/2.0/sql/statements", self.base_url);

        let body = serde_json::json!({
            "statement": statement,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": "60s",
        });

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .with_context(|| "Failed to send Databricks SQL statement request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Databricks SQL request failed with status {}: {}",
                status,
                text
            ));
        }

        let mut v: JsonValue = resp
            .json()
            .await
            .with_context(|| "Failed to decode Databricks SQL response JSON")?;

        let state = v
            .pointer("/status/state")
            .and_then(|s| s.as_str())
            .unwrap_or("UNKNOWN");

        if state != "SUCCEEDED" {
            return Err(anyhow!(
                "Databricks SQL statement did not succeed (state={}): {:?}",
                state,
                v
            ));
        }

        let cols = v
            .pointer("/result/manifest/schema/columns")
            .and_then(|c| c.as_array())
            .ok_or_else(|| {
                anyhow!("Missing result.manifest.schema.columns in Databricks response")
            })?;

        let col_names: Vec<String> = cols
            .iter()
            .map(|c| {
                c.get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();

        let mut rows: Vec<LogicalRow> = Vec::new();

        // Helper to append rows from a response JSON value.
        let mut append_rows = |root: &JsonValue| -> Result<()> {
            let data = root
                .pointer("/result/data_array")
                .or_else(|| root.pointer("/data_array"))
                .and_then(|d| d.as_array())
                .ok_or_else(|| anyhow!("Missing result.data_array in Databricks response"))?;

            for row_vals in data {
                let arr = row_vals
                    .as_array()
                    .ok_or_else(|| anyhow!("Row in data_array is not an array"))?;

                let mut obj = JsonMap::new();
                for (i, val) in arr.iter().enumerate() {
                    if let Some(col_name) = col_names.get(i) {
                        obj.insert(col_name.clone(), val.clone());
                    }
                }
                rows.push(obj);
            }
            Ok(())
        };

        append_rows(&v)?;

        // Follow chunk links if present.
        let mut next_link = v
            .pointer("/result/next_chunk_internal_link")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());

        while let Some(link) = next_link {
            let chunk_url = if link.starts_with("http://") || link.starts_with("https://") {
                link
            } else {
                format!("{}{}", self.base_url, link)
            };

            let chunk_resp = self
                .client
                .get(&chunk_url)
                .bearer_auth(&self.token)
                .send()
                .await
                .with_context(|| "Failed to fetch Databricks SQL result chunk")?;

            if !chunk_resp.status().is_success() {
                let status = chunk_resp.status();
                let text = chunk_resp.text().await.unwrap_or_default();
                return Err(anyhow!(
                    "Databricks SQL chunk request failed with status {}: {}",
                    status,
                    text
                ));
            }

            let chunk_json: JsonValue = chunk_resp
                .json()
                .await
                .with_context(|| "Failed to decode Databricks SQL chunk JSON")?;

            append_rows(&chunk_json)?;

            next_link = chunk_json
                .pointer("/next_chunk_internal_link")
                .or_else(|| chunk_json.pointer("/result/next_chunk_internal_link"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string());
        }

        Ok(rows)
    }
}

fn extract_warehouse_id(http_path: &str) -> Option<String> {
    http_path
        .split('/')
        .filter(|s| !s.is_empty())
        .last()
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Optional Databricks connectivity smoke test.
    ///
    /// Uses environment variables:
    /// - DATABRICKS_HOST
    /// - DATABRICKS_HTTP_PATH
    /// - DATABRICKS_TOKEN
    ///
    /// If any are missing, the test is a no-op and returns Ok(()).
    #[tokio::test]
    async fn databricks_connectivity_smoke_test() -> Result<()> {
        let host = match std::env::var("DATABRICKS_HOST") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let http_path = match std::env::var("DATABRICKS_HTTP_PATH") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        let token = match std::env::var("DATABRICKS_TOKEN") {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let cfg = DatabricksConfig {
            host,
            http_path,
            access_token: Some(token),
            catalog: None,
            schema: None,
            fetch_batch_size: None,
            query_timeout_ms: Some(30_000),
        };

        let client = DatabricksClient::new(&cfg)?;
        let rows = client.execute_query("SELECT 1 AS ONE").await?;
        // If we got here without error, connectivity is fine. Optionally assert on ONE.
        if let Some(row) = rows.get(0) {
            if let Some(v) = row.get("ONE") {
                assert!(v.is_number() || v.is_string());
            }
        }

        Ok(())
    }
}

/// Fetch rows for a given mapping, using either a local JSON file or Databricks SQL.
///
/// Watermark (if provided) is used only when the mapping is in incremental mode and
/// has a delta.updated_at_column configured.
pub async fn fetch_rows_for_mapping(
    cfg: &Config,
    mapping: &CommonMappingFields,
    watermark: Option<&str>,
) -> Result<Vec<LogicalRow>> {
    // File-based source for local testing.
    if let Some(path) = &mapping.source.file {
        let data = fs::read_to_string(path)
            .with_context(|| format!("Failed to read input file {}", path))?;
        let value: JsonValue = serde_json::from_str(&data)
            .with_context(|| format!("Failed to parse JSON array from {}", path))?;
        let arr = value.as_array().ok_or_else(|| {
            anyhow!(
                "Expected JSON array of objects in file {} for mapping {}",
                path,
                mapping.name
            )
        })?;

        let mut rows = Vec::with_capacity(arr.len());
        for (idx, v) in arr.iter().enumerate() {
            let obj = v.as_object().ok_or_else(|| {
                anyhow!(
                    "Element {} in file {} for mapping {} is not a JSON object",
                    idx,
                    path,
                    mapping.name
                )
            })?;
            rows.push(obj.clone());
        }
        return Ok(rows);
    }

    // Databricks SQL source.
    if let Some(db_cfg) = cfg.databricks.as_ref() {
        if mapping.source.table.is_some() || mapping.source.select.is_some() {
            let client = DatabricksClient::new(db_cfg)?;
            let sql = build_sql_for_mapping(mapping, watermark);
            let rows = client.execute_query(&sql).await?;
            return Ok(rows);
        }
    }

    Ok(Vec::new())
}

fn build_sql_for_mapping(mapping: &CommonMappingFields, watermark: Option<&str>) -> String {
    // Build predicate list from static where + optional delta watermark.
    let mut predicates: Vec<String> = Vec::new();

    if let Some(w) = &mapping.source.r#where {
        predicates.push(w.clone());
    }

    if let (Mode::Incremental, Some(delta), Some(wm)) =
        (&mapping.mode, mapping.delta.as_ref(), watermark)
    {
        let escaped = wm.replace('\'', "''");
        predicates.push(format!("{} > '{}'", delta.updated_at_column, escaped));
    }

    let combined = if predicates.is_empty() {
        None
    } else {
        Some(predicates.join(" AND "))
    };

    if let Some(select) = &mapping.source.select {
        if let Some(cond) = combined {
            format!("SELECT * FROM ({}) AS s WHERE {}", select, cond)
        } else {
            select.clone()
        }
    } else if let Some(table) = &mapping.source.table {
        if let Some(cond) = combined {
            format!("SELECT * FROM {} WHERE {}", table, cond)
        } else {
            format!("SELECT * FROM {}", table)
        }
    } else {
        // Fallback; this should normally not happen if config is validated.
        "SELECT 1".to_string()
    }
}
