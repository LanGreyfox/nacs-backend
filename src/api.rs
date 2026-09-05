use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::{
    Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::auth::is_authorized;
use crate::db::Database;
use crate::p2p::{P2pQuery, P2pTransferInfo, PeerInfo};

#[derive(Clone)]
pub struct ApiState {
    pub database: Database,
    pub p2p_query_tx: tokio::sync::mpsc::Sender<P2pQuery>,
    pub auth_user: String,
    pub auth_pass: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime_seconds: u64,
}

static START_TIME: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();

fn get_uptime() -> u64 {
    let start = START_TIME.get_or_init(SystemTime::now);
    start.elapsed().unwrap_or(Duration::from_secs(0)).as_secs()
}

async fn health_handler() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: get_uptime(),
    })
}

fn check_auth(headers: &HeaderMap, state: &ApiState) -> Result<(), StatusCode> {
    let method = axum::http::Method::GET;
    if is_authorized(headers, &state.auth_user, &state.auth_pass, &method) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

#[derive(Serialize)]
struct StatusResponse {
    sync: SyncStatus,
    peers_connected: usize,
}

#[derive(Serialize)]
struct SyncStatus {
    is_active: bool,
    current_transfer: Option<TransferInfoResponse>,
    queue_length: usize,
}

#[derive(Serialize)]
struct TransferInfoResponse {
    path: String,
    peer_id: String,
    event_kind: String,
    progress_bytes: u64,
    total_bytes: u64,
    username: String,
}

impl From<P2pTransferInfo> for TransferInfoResponse {
    fn from(t: P2pTransferInfo) -> Self {
        Self {
            path: t.path,
            peer_id: t.peer_id,
            event_kind: t.event_kind,
            progress_bytes: t.progress_bytes,
            total_bytes: t.total_bytes,
            username: t.username,
        }
    }
}

async fn status_handler(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, StatusCode> {
    check_auth(&headers, &state)?;

    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .p2p_query_tx
        .send(P2pQuery::GetStatus(tx))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let status = rx.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let current_transfer = status.current_transfer.map(Into::into);
    Ok(Json(StatusResponse {
        sync: SyncStatus {
            is_active: status.is_syncing,
            current_transfer,
            queue_length: status.queue_length,
        },
        peers_connected: status.peers_connected,
    }))
}

#[derive(Serialize)]
struct PeersResponse {
    peers: Vec<PeerInfoResponse>,
    pagination: PaginationResponse,
}

#[derive(Serialize)]
struct PeerInfoResponse {
    peer_id: String,
    connected_since: DateTime<Utc>,
    addresses: Vec<String>,
    last_heartbeat: DateTime<Utc>,
    is_synced: bool,
}

impl From<PeerInfo> for PeerInfoResponse {
    fn from(p: PeerInfo) -> Self {
        Self {
            peer_id: p.peer_id,
            connected_since: DateTime::from(p.connected_since),
            addresses: p.addresses,
            last_heartbeat: DateTime::from(p.last_heartbeat),
            is_synced: p.is_synced,
        }
    }
}

#[derive(Deserialize)]
struct PeersQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Serialize)]
struct PaginationResponse {
    limit: usize,
    offset: usize,
    total: usize,
    has_more: bool,
}

async fn peers_handler(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Query(params): Query<PeersQuery>,
) -> Result<Json<PeersResponse>, StatusCode> {
    check_auth(&headers, &state)?;

    let (tx, rx) = tokio::sync::oneshot::channel();
    state
        .p2p_query_tx
        .send(P2pQuery::GetPeers(tx))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let peers = rx.await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);
    let total = peers.len();
    let has_more = offset + limit < total;

    let paginated_peers = peers
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(Into::into)
        .collect();

    Ok(Json(PeersResponse {
        peers: paginated_peers,
        pagination: PaginationResponse {
            limit,
            offset,
            total,
            has_more,
        },
    }))
}

#[derive(Serialize)]
struct FilesResponse {
    resources: Vec<FileEntryResponse>,
    tombstones: Vec<TombstoneResponse>,
    pagination: PaginationResponse,
}

#[derive(Serialize)]
struct FileEntryResponse {
    path: String,
    kind: String,
    size: u64,
    checksum: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct TombstoneResponse {
    path: String,
    deleted_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct FilesQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    kind: Option<String>,
    path_prefix: Option<String>,
}

async fn files_handler(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Query(params): Query<FilesQuery>,
) -> Result<Json<FilesResponse>, StatusCode> {
    check_auth(&headers, &state)?;

    let manifest = state
        .database
        .manifest()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let kind_filter = params.kind.as_deref().unwrap_or("all");
    let path_prefix = params.path_prefix.as_deref().unwrap_or("");

    let mut resources: Vec<FileEntryResponse> = manifest
        .resources
        .into_iter()
        .filter(|r| {
            if kind_filter != "all" && r.resource_kind != kind_filter {
                return false;
            }
            if !path_prefix.is_empty() && !r.resource_path.starts_with(path_prefix) {
                return false;
            }
            true
        })
        .map(|r| FileEntryResponse {
            path: r.resource_path,
            kind: r.resource_kind,
            size: r.size,
            checksum: r.checksum,
            updated_at: parse_timestamp(&r.updated_at),
        })
        .collect();

    let mut tombstones: Vec<TombstoneResponse> = manifest
        .tombstones
        .into_iter()
        .filter(|t| {
            if !path_prefix.is_empty() && !t.resource_path.starts_with(path_prefix) {
                return false;
            }
            true
        })
        .map(|t| TombstoneResponse {
            path: t.resource_path,
            deleted_at: parse_timestamp(&t.deleted_at),
        })
        .collect();

    resources.sort_by(|a, b| a.path.cmp(&b.path));
    tombstones.sort_by(|a, b| a.path.cmp(&b.path));

    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);

    let total_resources = resources.len();
    let total_tombstones = tombstones.len();
    let total = total_resources + total_tombstones;

    let resources_paginated: Vec<_> = resources.into_iter().skip(offset).take(limit).collect();
    let remaining_limit = limit.saturating_sub(resources_paginated.len());
    let tombstones_paginated: Vec<_> = tombstones.into_iter().take(remaining_limit).collect();

    let has_more = offset + limit < total;

    Ok(Json(FilesResponse {
        resources: resources_paginated,
        tombstones: tombstones_paginated,
        pagination: PaginationResponse {
            limit,
            offset,
            total,
            has_more,
        },
    }))
}

fn parse_timestamp(ts: &str) -> DateTime<Utc> {
    if let Ok(nanos) = ts.parse::<u128>() {
        let secs = (nanos / 1_000_000_000) as i64;
        let nsecs = (nanos % 1_000_000_000) as u32;
        DateTime::from_timestamp(secs, nsecs).unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    }
}

pub async fn run_server(addr: SocketAddr, state: ApiState) {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/api/v1/status", get(status_handler))
        .route("/api/v1/peers", get(peers_handler))
        .route("/api/v1/files", get(files_handler))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    info!("REST API listening on http://{}", addr);
    axum::serve(listener, app).await.unwrap();
}
