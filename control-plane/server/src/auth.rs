use axum::body::Body;
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::models::AppState;

pub async fn require_api_key(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    // Allow health without auth (useful for k8s).
    if req.uri().path() == "/api/health" {
        return Ok(next.run(req).await);
    }

    let Some(expected) = state.api_key.clone() else {
        return Ok(next.run(req).await);
    };

    // Prefer Authorization: Bearer <token>
    let header_token = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string());

    // Fallback for SSE/EventSource (can't set headers): ?api_key=<token>
    let query_token = req
        .uri()
        .query()
        .and_then(|q| extract_query_param(q, "api_key"));

    let token = header_token.or(query_token).ok_or(StatusCode::UNAUTHORIZED)?;

    if token != expected {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(req).await)
}

fn extract_query_param(query: &str, key: &str) -> Option<String> {
    for part in query.split('&') {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };
        if k == key {
            return urlencoding::decode(v).ok().map(|s| s.to_string());
        }
    }
    None
}
