use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header::AUTHORIZATION},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use nacs_backend::{
    api::{ApiState, build_router},
    db::{Database, EventEnvelope, EventKind},
    p2p::{P2pQuery, P2pStatus, P2pTransferInfo, PeerInfo},
};
use serde_json::Value;
use tokio::sync::mpsc;
use tower::ServiceExt;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("nacs-api-{name}-{nanos}"))
}

async fn database(name: &str) -> (Database, PathBuf) {
    let dir = temp_dir(name);
    fs::create_dir_all(&dir).expect("temporary directory should be created");
    let db = Database::open(&dir, &dir)
        .await
        .expect("database should open");
    (db, dir)
}

fn auth_header() -> String {
    format!("Basic {}", STANDARD.encode("test-user:test-pass"))
}

fn state(database: Database, p2p_query_tx: mpsc::Sender<P2pQuery>) -> ApiState {
    ApiState {
        database,
        p2p_query_tx,
        auth_user: "test-user".to_string(),
        auth_pass: "test-pass".to_string(),
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body should be readable");
    serde_json::from_slice(&body).expect("response should contain JSON")
}

#[tokio::test]
async fn health_is_public_and_returns_status() {
    let (database, dir) = database("health").await;
    let (p2p_query_tx, _p2p_query_rx) = mpsc::channel(1);
    let app = build_router(state(database, p2p_query_tx));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["status"], "ok");
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
    assert!(body["uptime_seconds"].is_number());
    fs::remove_dir_all(dir).expect("temporary directory should be removed");
}

#[tokio::test]
async fn protected_endpoints_require_basic_authentication() {
    let (database, dir) = database("auth").await;
    let (p2p_query_tx, _p2p_query_rx) = mpsc::channel(1);
    let app = build_router(state(database, p2p_query_tx));

    for uri in ["/api/v1/status", "/api/v1/peers", "/api/v1/files"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/files")
                .header(AUTHORIZATION, auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    fs::remove_dir_all(dir).expect("temporary directory should be removed");
}

#[tokio::test]
async fn status_returns_sync_and_transfer_information() {
    let (database, dir) = database("status").await;
    let (p2p_query_tx, mut p2p_query_rx) = mpsc::channel(1);
    let responder = tokio::spawn(async move {
        if let Some(P2pQuery::GetStatus(reply)) = p2p_query_rx.recv().await {
            reply
                .send(P2pStatus {
                    is_syncing: true,
                    current_transfer: Some(P2pTransferInfo {
                        path: "/note.txt".to_string(),
                        peer_id: "peer-1".to_string(),
                        event_kind: "created".to_string(),
                        progress_bytes: 5,
                        total_bytes: 10,
                        username: "alice".to_string(),
                    }),
                    queue_length: 2,
                    peers_connected: 3,
                })
                .unwrap();
        }
    });
    let app = build_router(state(database, p2p_query_tx));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .header(AUTHORIZATION, auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["sync"]["is_active"], true);
    assert_eq!(body["sync"]["queue_length"], 2);
    assert_eq!(body["peers_connected"], 3);
    assert_eq!(body["sync"]["current_transfer"]["path"], "/note.txt");
    assert_eq!(body["sync"]["current_transfer"]["progress_bytes"], 5);
    responder.await.unwrap();
    fs::remove_dir_all(dir).expect("temporary directory should be removed");
}

#[tokio::test]
async fn peers_are_paginated() {
    let (database, dir) = database("peers").await;
    let (p2p_query_tx, mut p2p_query_rx) = mpsc::channel(1);
    let responder = tokio::spawn(async move {
        if let Some(P2pQuery::GetPeers(reply)) = p2p_query_rx.recv().await {
            let now = SystemTime::now();
            reply
                .send(vec![
                    PeerInfo {
                        peer_id: "peer-1".to_string(),
                        connected_since: now,
                        addresses: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                        last_heartbeat: now,
                        is_synced: true,
                    },
                    PeerInfo {
                        peer_id: "peer-2".to_string(),
                        connected_since: now,
                        addresses: vec![],
                        last_heartbeat: now,
                        is_synced: false,
                    },
                ])
                .unwrap();
        }
    });
    let app = build_router(state(database, p2p_query_tx));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/peers?limit=1&offset=1")
                .header(AUTHORIZATION, auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["peers"].as_array().unwrap().len(), 1);
    assert_eq!(body["peers"][0]["peer_id"], "peer-2");
    assert_eq!(body["pagination"]["total"], 2);
    assert_eq!(body["pagination"]["has_more"], false);
    responder.await.unwrap();
    fs::remove_dir_all(dir).expect("temporary directory should be removed");
}

#[tokio::test]
async fn files_returns_resources_tombstones_and_filters() {
    let (database, dir) = database("files").await;
    let note_path = dir.join("note.txt");
    fs::write(&note_path, b"hello").expect("test file should be written");
    database.record(EventEnvelope {
        event_kind: EventKind::Created,
        source_path: "/note.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "PUT".to_string(),
        status_code: 201,
        username: "alice".to_string(),
    });
    fs::remove_file(note_path).expect("test file should be removed");
    database.record(EventEnvelope {
        event_kind: EventKind::Deleted,
        source_path: "/note.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "DELETE".to_string(),
        status_code: 204,
        username: "alice".to_string(),
    });

    let (p2p_query_tx, _p2p_query_rx) = mpsc::channel(1);
    let app = build_router(state(database, p2p_query_tx));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/files?path_prefix=/note")
                .header(AUTHORIZATION, auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["resources"].as_array().unwrap().len(), 0);
    assert_eq!(body["tombstones"][0]["path"], "/note.txt");
    assert_eq!(body["pagination"]["total"], 1);
    fs::remove_dir_all(dir).expect("temporary directory should be removed");
}

#[tokio::test]
async fn p2p_channel_failure_returns_internal_server_error() {
    let (database, dir) = database("p2p-error").await;
    let (p2p_query_tx, p2p_query_rx) = mpsc::channel(1);
    drop(p2p_query_rx);
    let app = build_router(state(database, p2p_query_tx));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status")
                .header(AUTHORIZATION, auth_header())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    fs::remove_dir_all(dir).expect("temporary directory should be removed");
}
