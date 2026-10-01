use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::{Config, StateBackendKind};

/// Simple file-backed watermark state per mapping.
#[derive(Debug, Default, Serialize, Deserialize)]
struct FileState {
    mappings: HashMap<String, String>, // mapping name -> ISO8601 watermark
}

fn state_file_path(cfg: &Config) -> Option<&str> {
    cfg.state
        .as_ref()
        .and_then(|s| s.file_path.as_deref())
        .or(Some("state.json"))
}

/// Load watermarks for all mappings. Returns empty map if no state configured.
pub fn load_watermarks(cfg: &Config) -> Result<HashMap<String, String>> {
    let Some(path_str) = state_file_path(cfg) else {
        return Ok(HashMap::new());
    };

    let path = Path::new(path_str);
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read state file {}", path.display()))?;

    let state: FileState = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse JSON state from {}", path.display()))?;

    Ok(state.mappings)
}

/// Persist watermarks for all mappings. No-op if state backend is not file.
pub fn save_watermarks(cfg: &Config, map: &HashMap<String, String>) -> Result<()> {
    let Some(backend_cfg) = cfg.state.as_ref() else {
        return Ok(());
    };

    if !matches!(backend_cfg.backend, StateBackendKind::File) {
        // Other backends (FalkorDB, None) not yet implemented.
        return Ok(());
    }

    let path_str = backend_cfg.file_path.as_deref().unwrap_or("state.json");
    let path = Path::new(path_str);

    let state = FileState {
        mappings: map.clone(),
    };
    let contents = serde_json::to_string_pretty(&state)?;
    fs::write(path, contents)
        .with_context(|| format!("Failed to write state file {}", path.display()))?;

    Ok(())
}
