use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use once_cell::sync::Lazy;

pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);

#[derive(Default, Clone)]
pub struct MappingStats {
    pub runs: u64,
    pub failed_runs: u64,
    pub rows_fetched: u64,
    pub rows_written: u64,
    pub rows_deleted: u64,
}

#[derive(Default)]
pub struct Metrics {
    pub runs: AtomicU64,
    pub failed_runs: AtomicU64,
    pub rows_fetched: AtomicU64,
    pub rows_written: AtomicU64,
    pub rows_deleted: AtomicU64,
    pub per_mapping: Mutex<HashMap<String, MappingStats>>,
}

impl Metrics {
    pub fn inc_runs(&self) {
        self.runs.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_failed_runs(&self) {
        self.failed_runs.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_rows_fetched(&self, n: u64) {
        self.rows_fetched.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_rows_written(&self, n: u64) {
        self.rows_written.fetch_add(n, Ordering::Relaxed);
    }
    pub fn add_rows_deleted(&self, n: u64) {
        self.rows_deleted.fetch_add(n, Ordering::Relaxed);
    }

    fn with_mapping<F>(&self, mapping: &str, f: F)
    where
        F: FnOnce(&mut MappingStats),
    {
        let mut guard = self.per_mapping.lock().unwrap();
        let entry = guard.entry(mapping.to_string()).or_default();
        f(entry);
    }

    pub fn inc_mapping_run(&self, mapping: &str) {
        self.with_mapping(mapping, |m| m.runs += 1);
    }
    pub fn inc_mapping_failed_run(&self, mapping: &str) {
        self.with_mapping(mapping, |m| m.failed_runs += 1);
    }
    pub fn add_mapping_rows_fetched(&self, mapping: &str, n: u64) {
        self.with_mapping(mapping, |m| m.rows_fetched += n);
    }
    pub fn add_mapping_rows_written(&self, mapping: &str, n: u64) {
        self.with_mapping(mapping, |m| m.rows_written += n);
    }
    pub fn add_mapping_rows_deleted(&self, mapping: &str, n: u64) {
        self.with_mapping(mapping, |m| m.rows_deleted += n);
    }
}

async fn handle_metrics(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let m = &*METRICS;
    let mut body = String::new();

    body.push_str(&format!(
        "snowflake_to_falkordb_runs {}\n",
        m.runs.load(Ordering::Relaxed),
    ));
    body.push_str(&format!(
        "snowflake_to_falkordb_failed_runs {}\n",
        m.failed_runs.load(Ordering::Relaxed),
    ));
    body.push_str(&format!(
        "snowflake_to_falkordb_rows_fetched {}\n",
        m.rows_fetched.load(Ordering::Relaxed),
    ));
    body.push_str(&format!(
        "snowflake_to_falkordb_rows_written {}\n",
        m.rows_written.load(Ordering::Relaxed),
    ));
    body.push_str(&format!(
        "snowflake_to_falkordb_rows_deleted {}\n",
        m.rows_deleted.load(Ordering::Relaxed),
    ));

    let guard = m.per_mapping.lock().unwrap();
    for (name, stats) in guard.iter() {
        body.push_str(&format!(
            "snowflake_to_falkordb_mapping_runs{{mapping=\"{}\"}} {}\n",
            name, stats.runs
        ));
        body.push_str(&format!(
            "snowflake_to_falkordb_mapping_failed_runs{{mapping=\"{}\"}} {}\n",
            name, stats.failed_runs
        ));
        body.push_str(&format!(
            "snowflake_to_falkordb_mapping_rows_fetched{{mapping=\"{}\"}} {}\n",
            name, stats.rows_fetched
        ));
        body.push_str(&format!(
            "snowflake_to_falkordb_mapping_rows_written{{mapping=\"{}\"}} {}\n",
            name, stats.rows_written
        ));
        body.push_str(&format!(
            "snowflake_to_falkordb_mapping_rows_deleted{{mapping=\"{}\"}} {}\n",
            name, stats.rows_deleted
        ));
    }

    Ok(Response::new(Body::from(body)))
}

pub async fn serve_metrics(addr: SocketAddr) {
    let make_svc =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle_metrics)) });

    if let Err(e) = Server::bind(&addr).serve(make_svc).await {
        tracing::error!(error = %e, "metrics server error");
    }
}
