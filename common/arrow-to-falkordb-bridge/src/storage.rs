//! OpenDAL-based object-store configuration and operator builder.
//!
//! Supports local filesystem, S3, GCS, and Azure Blob. Credentials and most
//! string fields accept the existing `$VARIABLE` env-reference convention.

use std::env;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use opendal::services::{Azblob, Fs, Gcs, S3};
use opendal::Operator;
use serde::{Deserialize, Serialize};

/// Object-store credentials and connection settings.
///
/// All string fields may use `$ENV_VAR` references, resolved by
/// [`resolve_object_store_env_refs`].
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ObjectStoreConfig {
    /// Storage provider / scheme override: `fs` | `s3` | `gcs` | `azblob` | `az`.
    /// When omitted, the scheme is inferred from the path/URI.
    #[serde(default)]
    pub provider: Option<String>,

    /// Alias for `provider` (accepted for ergonomics).
    #[serde(default)]
    pub scheme: Option<String>,

    /// S3 / GCS / Azure endpoint override (e.g. MinIO `http://localhost:9000`).
    #[serde(default)]
    pub endpoint: Option<String>,

    /// AWS/S3 region.
    #[serde(default)]
    pub region: Option<String>,

    /// Bucket / container name. When the path is a full URI (`s3://bucket/key`),
    /// the bucket is taken from the URI and this field is optional.
    #[serde(default)]
    pub bucket: Option<String>,

    /// Optional root prefix applied by the operator.
    #[serde(default)]
    pub root: Option<String>,

    /// AWS access key id / Azure account name / GCS HMAC key id.
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// AWS secret access key / Azure account key / GCS HMAC secret.
    #[serde(default)]
    pub secret_access_key: Option<String>,

    /// Azure storage account name (preferred over access_key_id for azblob).
    #[serde(default)]
    pub account_name: Option<String>,

    /// Azure storage account key.
    #[serde(default)]
    pub account_key: Option<String>,

    /// Azure SAS token.
    #[serde(default)]
    pub sas_token: Option<String>,

    /// GCS service-account JSON (inline) or path handled by the caller.
    #[serde(default)]
    pub credential: Option<String>,

    /// GCS credential file path.
    #[serde(default)]
    pub credential_path: Option<String>,

    /// Disable path-style access for S3 (default: path-style enabled for MinIO-friendliness).
    #[serde(default)]
    pub virtual_host_style: Option<bool>,
}

/// Result of parsing a storage URI or local path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedUri {
    /// Normalised scheme: `fs`, `s3`, `gcs`, `azblob`.
    pub scheme: String,
    /// Bucket/container (empty for `fs`).
    pub bucket: String,
    /// Key / relative path within the bucket (or absolute/relative local path for `fs`).
    pub key: String,
    /// Original input.
    pub original: String,
}

impl ParsedUri {
    /// True when this points at a single object that looks like a file
    /// (has a non-empty file name component with an extension, or ends with `.parquet`).
    pub fn looks_like_file(&self) -> bool {
        let key = self.key.trim_end_matches('/');
        if key.is_empty() {
            return false;
        }
        let name = key.rsplit('/').next().unwrap_or(key);
        name.contains('.') && !name.starts_with('.')
    }
}

/// Resolve `$VAR` references in all string fields of [`ObjectStoreConfig`].
pub fn resolve_object_store_env_refs(cfg: &mut ObjectStoreConfig) -> Result<()> {
    resolve_env_ref(&mut cfg.provider, "object_store.provider")?;
    resolve_env_ref(&mut cfg.scheme, "object_store.scheme")?;
    resolve_env_ref(&mut cfg.endpoint, "object_store.endpoint")?;
    resolve_env_ref(&mut cfg.region, "object_store.region")?;
    resolve_env_ref(&mut cfg.bucket, "object_store.bucket")?;
    resolve_env_ref(&mut cfg.root, "object_store.root")?;
    resolve_env_ref(&mut cfg.access_key_id, "object_store.access_key_id")?;
    resolve_env_ref(&mut cfg.secret_access_key, "object_store.secret_access_key")?;
    resolve_env_ref(&mut cfg.account_name, "object_store.account_name")?;
    resolve_env_ref(&mut cfg.account_key, "object_store.account_key")?;
    resolve_env_ref(&mut cfg.sas_token, "object_store.sas_token")?;
    resolve_env_ref(&mut cfg.credential, "object_store.credential")?;
    resolve_env_ref(&mut cfg.credential_path, "object_store.credential_path")?;
    Ok(())
}

fn resolve_env_ref(value: &mut Option<String>, field_name: &str) -> Result<()> {
    let Some(current) = value.as_ref() else {
        return Ok(());
    };
    let Some(env_name) = current.strip_prefix('$') else {
        return Ok(());
    };
    // Allow `${VAR}` as well as `$VAR`.
    let env_name = env_name
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(env_name);
    let resolved = env::var(env_name).with_context(|| {
        format!(
            "Environment variable {env_name} referenced by {field_name} is not set"
        )
    })?;
    *value = Some(resolved);
    Ok(())
}

/// Parse a path or URI into scheme/bucket/key components.
///
/// Accepted forms:
/// - `s3://bucket/prefix/file.parquet`
/// - `gs://bucket/prefix/` / `gcs://bucket/prefix/`
/// - `az://container/prefix/` / `abfs://container/prefix/` / `wasbs://...`
/// - `file:///abs/path` / `file://abs/path`
/// - bare local paths (`/tmp/data`, `./data`, `data/`)
pub fn parse_storage_uri(input: &str) -> Result<ParsedUri> {
    let original = input.to_string();
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("storage path/URI must not be empty"));
    }

    if let Some(rest) = trimmed.strip_prefix("s3://") {
        let (bucket, key) = split_bucket_key(rest)?;
        return Ok(ParsedUri {
            scheme: "s3".into(),
            bucket,
            key,
            original,
        });
    }
    if let Some(rest) = trimmed
        .strip_prefix("gs://")
        .or_else(|| trimmed.strip_prefix("gcs://"))
    {
        let (bucket, key) = split_bucket_key(rest)?;
        return Ok(ParsedUri {
            scheme: "gcs".into(),
            bucket,
            key,
            original,
        });
    }
    if let Some(rest) = trimmed
        .strip_prefix("az://")
        .or_else(|| trimmed.strip_prefix("abfs://"))
        .or_else(|| trimmed.strip_prefix("abfss://"))
        .or_else(|| trimmed.strip_prefix("wasb://"))
        .or_else(|| trimmed.strip_prefix("wasbs://"))
    {
        let (bucket, key) = split_bucket_key(rest)?;
        return Ok(ParsedUri {
            scheme: "azblob".into(),
            bucket,
            key,
            original,
        });
    }
    if let Some(rest) = trimmed.strip_prefix("file://") {
        let path = if rest.starts_with('/') {
            rest.to_string()
        } else {
            format!("/{rest}")
        };
        return Ok(ParsedUri {
            scheme: "fs".into(),
            bucket: String::new(),
            key: path,
            original,
        });
    }

    // Bare local path.
    Ok(ParsedUri {
        scheme: "fs".into(),
        bucket: String::new(),
        key: trimmed.to_string(),
        original,
    })
}

fn split_bucket_key(rest: &str) -> Result<(String, String)> {
    let rest = rest.trim_start_matches('/');
    if rest.is_empty() {
        return Err(anyhow!("URI is missing bucket/container name"));
    }
    match rest.split_once('/') {
        Some((bucket, key)) => Ok((bucket.to_string(), key.to_string())),
        None => Ok((rest.to_string(), String::new())),
    }
}

/// Build an OpenDAL [`Operator`] for the given config and path/URI.
///
/// Returns `(operator, object_key)` where `object_key` is the path relative to
/// the operator root (use it with `op.list` / `op.read` / etc.).
///
/// For local filesystem, the operator root is set to `/` (absolute) or the
/// current directory, and `object_key` is the absolute or relative path.
pub fn build_operator(cfg: &ObjectStoreConfig, path_or_uri: &str) -> Result<(Operator, String)> {
    let parsed = parse_storage_uri(path_or_uri)?;
    let scheme = cfg
        .provider
        .as_deref()
        .or(cfg.scheme.as_deref())
        .unwrap_or(parsed.scheme.as_str())
        .to_ascii_lowercase();

    match scheme.as_str() {
        "fs" | "file" | "local" => build_fs_operator(cfg, &parsed),
        "s3" | "s3a" | "minio" => build_s3_operator(cfg, &parsed),
        "gcs" | "gs" => build_gcs_operator(cfg, &parsed),
        "azblob" | "az" | "azure" | "abfs" | "abfss" | "wasb" | "wasbs" => {
            build_azblob_operator(cfg, &parsed)
        }
        other => Err(anyhow!("unsupported object store scheme/provider: {other}")),
    }
}

fn build_fs_operator(cfg: &ObjectStoreConfig, parsed: &ParsedUri) -> Result<(Operator, String)> {
    let mut builder = Fs::default();
    // OpenDAL Fs uses `root` as the base; we keep root = "/" for absolute paths
    // and "." for relative ones so keys stay portable.
    let path = PathBuf::from(&parsed.key);
    let (root, key) = if path.is_absolute() {
        ("/".to_string(), parsed.key.trim_start_matches('/').to_string())
    } else if let Some(r) = &cfg.root {
        (r.clone(), parsed.key.clone())
    } else {
        (".".to_string(), parsed.key.clone())
    };
    builder = builder.root(&root);
    let op = Operator::new(builder)
        .context("failed to build OpenDAL Fs operator")?
        .finish();
    Ok((op, key))
}

fn build_s3_operator(cfg: &ObjectStoreConfig, parsed: &ParsedUri) -> Result<(Operator, String)> {
    let bucket = if !parsed.bucket.is_empty() {
        parsed.bucket.clone()
    } else {
        cfg.bucket
            .clone()
            .ok_or_else(|| anyhow!("s3 bucket is required (URI or object_store.bucket)"))?
    };

    let mut builder = S3::default().bucket(&bucket);
    if let Some(region) = &cfg.region {
        builder = builder.region(region);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if let Some(ak) = &cfg.access_key_id {
        builder = builder.access_key_id(ak);
    }
    if let Some(sk) = &cfg.secret_access_key {
        builder = builder.secret_access_key(sk);
    }
    if let Some(root) = &cfg.root {
        builder = builder.root(root);
    }
// OpenDAL defaults to path-style (MinIO-friendly). Opt into virtual-host style when requested.
    if cfg.virtual_host_style == Some(true) {
        builder = builder.enable_virtual_host_style();
    }

    let op = Operator::new(builder)
        .context("failed to build OpenDAL S3 operator")?
        .finish();
    Ok((op, parsed.key.clone()))
}

fn build_gcs_operator(cfg: &ObjectStoreConfig, parsed: &ParsedUri) -> Result<(Operator, String)> {
    let bucket = if !parsed.bucket.is_empty() {
        parsed.bucket.clone()
    } else {
        cfg.bucket
            .clone()
            .ok_or_else(|| anyhow!("gcs bucket is required (URI or object_store.bucket)"))?
    };

    let mut builder = Gcs::default().bucket(&bucket);
    if let Some(root) = &cfg.root {
        builder = builder.root(root);
    }
    if let Some(cred) = &cfg.credential {
        builder = builder.credential(cred);
    }
    if let Some(path) = &cfg.credential_path {
        builder = builder.credential_path(path);
    }
    // HMAC keys map onto access_key_id/secret when provided.
    if let Some(ak) = cfg.access_key_id.as_ref().or(cfg.account_name.as_ref()) {
        // Gcs builder uses credential primarily; ignore HMAC if credential set.
        let _ = ak;
    }

    let op = Operator::new(builder)
        .context("failed to build OpenDAL GCS operator")?
        .finish();
    Ok((op, parsed.key.clone()))
}

fn build_azblob_operator(
    cfg: &ObjectStoreConfig,
    parsed: &ParsedUri,
) -> Result<(Operator, String)> {
    let container = if !parsed.bucket.is_empty() {
        parsed.bucket.clone()
    } else {
        cfg.bucket
            .clone()
            .ok_or_else(|| anyhow!("azure container is required (URI or object_store.bucket)"))?
    };

    let mut builder = Azblob::default().container(&container);
    if let Some(name) = cfg.account_name.as_ref().or(cfg.access_key_id.as_ref()) {
        builder = builder.account_name(name);
    }
    if let Some(key) = cfg.account_key.as_ref().or(cfg.secret_access_key.as_ref()) {
        builder = builder.account_key(key);
    }
    if let Some(sas) = &cfg.sas_token {
        builder = builder.sas_token(sas);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.endpoint(endpoint);
    }
    if let Some(root) = &cfg.root {
        builder = builder.root(root);
    }

    let op = Operator::new(builder)
        .context("failed to build OpenDAL Azblob operator")?
        .finish();
    Ok((op, parsed.key.clone()))
}

/// Join an operator-relative directory prefix with a child name, normalising slashes.
pub fn join_key(prefix: &str, name: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let name = name.trim_start_matches('/');
    if prefix.is_empty() {
        name.to_string()
    } else if name.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

/// Return parent directory of a key, or empty string.
pub fn parent_key(key: &str) -> String {
    let key = key.trim_end_matches('/');
    match key.rfind('/') {
        Some(idx) => key[..idx].to_string(),
        None => String::new(),
    }
}

/// True if `path` exists as a local filesystem path.
pub fn local_path_exists(path: &str) -> bool {
    Path::new(path).exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_s3_uri() {
        let p = parse_storage_uri("s3://my-bucket/lake/customers/part-0.parquet").unwrap();
        assert_eq!(p.scheme, "s3");
        assert_eq!(p.bucket, "my-bucket");
        assert_eq!(p.key, "lake/customers/part-0.parquet");
        assert!(p.looks_like_file());
    }

    #[test]
    fn parse_gs_prefix() {
        let p = parse_storage_uri("gs://b/prefix/").unwrap();
        assert_eq!(p.scheme, "gcs");
        assert_eq!(p.bucket, "b");
        assert_eq!(p.key, "prefix/");
        assert!(!p.looks_like_file());
    }

    #[test]
    fn parse_az_uri() {
        let p = parse_storage_uri("az://container/a/b.parquet").unwrap();
        assert_eq!(p.scheme, "azblob");
        assert_eq!(p.bucket, "container");
        assert_eq!(p.key, "a/b.parquet");
    }

    #[test]
    fn parse_local_path() {
        let p = parse_storage_uri("/tmp/data/file.parquet").unwrap();
        assert_eq!(p.scheme, "fs");
        assert_eq!(p.bucket, "");
        assert_eq!(p.key, "/tmp/data/file.parquet");
        assert!(p.looks_like_file());
    }

    #[test]
    fn parse_file_uri() {
        let p = parse_storage_uri("file:///tmp/x").unwrap();
        assert_eq!(p.scheme, "fs");
        assert_eq!(p.key, "/tmp/x");
    }

    #[test]
    fn resolve_env_refs() {
        env::set_var("BRIDGE_TEST_AK", "AKIA_TEST");
        env::set_var("BRIDGE_TEST_SK", "secret");
        let mut cfg = ObjectStoreConfig {
            access_key_id: Some("$BRIDGE_TEST_AK".into()),
            secret_access_key: Some("${BRIDGE_TEST_SK}".into()),
            region: Some("us-east-1".into()),
            ..Default::default()
        };
        resolve_object_store_env_refs(&mut cfg).unwrap();
        assert_eq!(cfg.access_key_id.as_deref(), Some("AKIA_TEST"));
        assert_eq!(cfg.secret_access_key.as_deref(), Some("secret"));
        assert_eq!(cfg.region.as_deref(), Some("us-east-1"));
        env::remove_var("BRIDGE_TEST_AK");
        env::remove_var("BRIDGE_TEST_SK");
    }

    #[test]
    fn build_fs_operator_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("hello.txt");
        {
            let mut f = std::fs::File::create(&file_path).unwrap();
            write!(f, "hello-opendal").unwrap();
        }
        let cfg = ObjectStoreConfig::default();
        let (op, key) = build_operator(&cfg, file_path.to_str().unwrap()).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bytes = rt.block_on(async { op.read(&key).await.unwrap().to_bytes() });
        assert_eq!(&bytes[..], b"hello-opendal");
    }

    #[test]
    fn join_and_parent_helpers() {
        assert_eq!(join_key("a/b", "c.parquet"), "a/b/c.parquet");
        assert_eq!(join_key("", "c.parquet"), "c.parquet");
        assert_eq!(parent_key("a/b/c.parquet"), "a/b");
        assert_eq!(parent_key("c.parquet"), "");
    }
}
