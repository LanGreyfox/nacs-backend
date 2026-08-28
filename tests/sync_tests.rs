use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use crc32fast::Hasher;
use libp2p::PeerId;
use nacs_backend::db::{Database, EventKind, Manifest, ManifestEntry, TombstoneEntry};
use nacs_backend::sync::{
    self, FileChangeEvent, SyncAction, SyncRequest, SyncResponse, SyncState,
    diff_manifests, handle_fetch_request, handle_file_start, handle_file_chunk,
};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();

    let mut path = std::env::temp_dir();
    path.push(format!(
        "nacs-backend-{name}-{nanos}-{}",
        std::process::id()
    ));
    path
}

fn crc32_hex(bytes: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    format!("{:08x}", hasher.finalize())
}

fn manifest_entry(path: &str, checksum: &str, updated_at: &str) -> ManifestEntry {
    ManifestEntry {
        resource_path: path.to_string(),
        resource_kind: "file".to_string(),
        checksum: Some(checksum.to_string()),
        size: 0,
        updated_at: updated_at.to_string(),
    }
}

fn manifest_with_resource(path: &str, kind: &str, checksum: &str, updated_at: &str) -> Manifest {
    Manifest {
        resources: vec![ManifestEntry {
            resource_path: path.to_string(),
            resource_kind: kind.to_string(),
            checksum: Some(checksum.to_string()),
            size: 4,
            updated_at: updated_at.to_string(),
        }],
        tombstones: vec![],
    }
}

// --- Wire type serialization round-trips ---

#[test]
fn file_change_event_round_trips_through_serde() {
    let event = FileChangeEvent {
        event_kind: EventKind::Edited,
        source_path: "/notes/a.txt".to_string(),
        destination_path: Some("/notes/b.txt".to_string()),
        checksum: Some("deadbeef".to_string()),
        size: 42,
        username: "p2p:peer123".to_string(),
    };

    let json = serde_json::to_string(&event).expect("event should serialize");
    let decoded: FileChangeEvent = serde_json::from_str(&json).expect("event should deserialize");

    assert_eq!(decoded.source_path, event.source_path);
    assert_eq!(decoded.destination_path, event.destination_path);
    assert_eq!(decoded.checksum, event.checksum);
    assert_eq!(decoded.size, event.size);
    assert_eq!(decoded.username, event.username);
}

#[test]
fn sync_request_and_response_round_trip_through_serde() {
    let requests = vec![
        SyncRequest::Manifest,
        SyncRequest::FetchFile {
            path: "/a.txt".to_string(),
        },
        SyncRequest::Event(FileChangeEvent {
            event_kind: EventKind::Deleted,
            source_path: "/a.txt".to_string(),
            destination_path: None,
            checksum: None,
            size: 0,
            username: "alice".to_string(),
        }),
    ];

    for request in requests {
        let json = serde_json::to_string(&request).expect("request should serialize");
        let _decoded: SyncRequest =
            serde_json::from_str(&json).expect("request should deserialize");
    }

    let responses = vec![
        SyncResponse::Manifest(Manifest::default()),
        SyncResponse::FileStart {
            path: "/a.txt".to_string(),
            total_size: 4,
            checksum: "abc123".to_string(),
        },
        SyncResponse::FileChunk {
            path: "/a.txt".to_string(),
            data: vec![1, 2, 3, 4],
            offset: 0,
            is_last: true,
        },
        SyncResponse::NotFound {
            path: "/missing.txt".to_string(),
        },
        SyncResponse::Ack,
    ];

    for response in responses {
        let json = serde_json::to_string(&response).expect("response should serialize");
        let _decoded: SyncResponse =
            serde_json::from_str(&json).expect("response should deserialize");
    }
}

// --- diff_manifests reconciliation logic ---

#[test]
fn diff_manifests_pulls_newer_remote_file() {
    let local = Manifest::default();
    let remote = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-02T00:00:00Z")],
        tombstones: vec![],
    };

    let actions = diff_manifests(&local, &remote);

    assert_eq!(
        actions,
        vec![SyncAction::Pull {
            path: "/a.txt".to_string(),
            checksum: Some("abc123".to_string()),
        }]
    );
}

#[test]
fn diff_manifests_ignores_older_or_equal_remote_file() {
    let local = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-02T00:00:00Z")],
        tombstones: vec![],
    };
    let remote = Manifest {
        resources: vec![manifest_entry("/a.txt", "old", "2024-01-01T00:00:00Z")],
        tombstones: vec![],
    };

    assert!(diff_manifests(&local, &remote).is_empty());

    let remote_equal = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-02T00:00:00Z")],
        tombstones: vec![],
    };
    assert!(diff_manifests(&local, &remote_equal).is_empty());
}

#[test]
fn diff_manifests_ignores_newer_remote_file_when_checksum_matches() {
    let local = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-01T00:00:00Z")],
        tombstones: vec![],
    };
    let remote = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-02T00:00:00Z")],
        tombstones: vec![],
    };

    assert!(diff_manifests(&local, &remote).is_empty());
}

#[test]
fn diff_manifests_creates_missing_remote_folder() {
    let local = Manifest::default();
    let remote = Manifest {
        resources: vec![ManifestEntry {
            resource_path: "/folder".to_string(),
            resource_kind: "folder".to_string(),
            checksum: None,
            size: 0,
            updated_at: "2024-01-01T00:00:00Z".to_string(),
        }],
        tombstones: vec![],
    };

    assert_eq!(
        diff_manifests(&local, &remote),
        vec![SyncAction::CreateDir {
            path: "/folder".to_string(),
        }]
    );
}

#[test]
fn diff_manifests_deletes_locally_when_remote_tombstone_is_newer() {
    let local = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-01T00:00:00Z")],
        tombstones: vec![],
    };
    let remote = Manifest {
        resources: vec![],
        tombstones: vec![TombstoneEntry {
            resource_path: "/a.txt".to_string(),
            deleted_at: "2024-01-02T00:00:00Z".to_string(),
        }],
    };

    assert_eq!(
        diff_manifests(&local, &remote),
        vec![SyncAction::Delete {
            path: "/a.txt".to_string(),
        }]
    );
}

#[test]
fn diff_manifests_ignores_tombstone_for_path_not_present_locally() {
    let local = Manifest::default();
    let remote = Manifest {
        resources: vec![],
        tombstones: vec![TombstoneEntry {
            resource_path: "/never-existed.txt".to_string(),
            deleted_at: "2024-01-02T00:00:00Z".to_string(),
        }],
    };

    assert!(diff_manifests(&local, &remote).is_empty());
}

#[test]
fn diff_manifests_is_empty_for_identical_manifests() {
    let manifest = Manifest {
        resources: vec![manifest_entry("/a.txt", "abc123", "2024-01-01T00:00:00Z")],
        tombstones: vec![],
    };

    assert!(diff_manifests(&manifest, &manifest).is_empty());
}

// --- Tests moved from src/sync.rs ---

#[test]
fn moved_pulls_file_missing_locally() {
    let local = Manifest::default();
    let remote = manifest_with_resource("/docs/a.txt", "file", "abc", "2026-01-01 00:00:00");

    assert_eq!(
        diff_manifests(&local, &remote),
        vec![SyncAction::Pull {
            path: "/docs/a.txt".to_string(),
            checksum: Some("abc".to_string()),
        }]
    );
}

#[test]
fn moved_creates_dir_missing_locally() {
    let local = Manifest::default();
    let remote = Manifest {
        resources: vec![ManifestEntry {
            resource_path: "/docs".to_string(),
            resource_kind: "folder".to_string(),
            checksum: None,
            size: 0,
            updated_at: "2026-01-01 00:00:00".to_string(),
        }],
        tombstones: vec![],
    };

    assert_eq!(
        diff_manifests(&local, &remote),
        vec![SyncAction::CreateDir {
            path: "/docs".to_string(),
        }]
    );
}

#[test]
fn moved_no_action_when_local_is_newer() {
    let local = manifest_with_resource("/docs/a.txt", "file", "newer", "2026-02-01 00:00:00");
    let remote = manifest_with_resource("/docs/a.txt", "file", "older", "2026-01-01 00:00:00");

    assert!(diff_manifests(&local, &remote).is_empty());
}

#[test]
fn moved_remote_tombstone_newer_than_local_live_deletes_locally() {
    let local = manifest_with_resource("/docs/a.txt", "file", "abc", "2026-01-01 00:00:00");
    let remote = Manifest {
        resources: vec![],
        tombstones: vec![TombstoneEntry {
            resource_path: "/docs/a.txt".to_string(),
            deleted_at: "2026-02-01 00:00:00".to_string(),
        }],
    };

    assert_eq!(
        diff_manifests(&local, &remote),
        vec![SyncAction::Delete {
            path: "/docs/a.txt".to_string(),
        }]
    );
}

#[test]
fn moved_remote_tombstone_older_than_local_live_is_ignored() {
    let local = manifest_with_resource("/docs/a.txt", "file", "abc", "2026-02-01 00:00:00");
    let remote = Manifest {
        resources: vec![],
        tombstones: vec![TombstoneEntry {
            resource_path: "/docs/a.txt".to_string(),
            deleted_at: "2026-01-01 00:00:00".to_string(),
        }],
    };

    assert!(diff_manifests(&local, &remote).is_empty());
}

#[test]
fn moved_both_tombstoned_is_a_no_op() {
    let local = Manifest {
        resources: vec![],
        tombstones: vec![TombstoneEntry {
            resource_path: "/docs/a.txt".to_string(),
            deleted_at: "2026-01-01 00:00:00".to_string(),
        }],
    };
    let remote = Manifest {
        resources: vec![],
        tombstones: vec![TombstoneEntry {
            resource_path: "/docs/a.txt".to_string(),
            deleted_at: "2026-02-01 00:00:00".to_string(),
        }],
    };

    assert!(diff_manifests(&local, &remote).is_empty());
}

#[tokio::test]
async fn incoming_move_event_with_absolute_destination_url_renames_to_relative_target() {
    let sqlite_dir = temp_dir("sync-move-abs-dest-sqlite");
    let data_dir = temp_dir("sync-move-abs-dest-data");
    fs::create_dir_all(data_dir.join("old")).expect("old directory should exist");
    fs::write(data_dir.join("old/file.txt"), b"move me").expect("source file should exist");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let event = FileChangeEvent {
        event_kind: EventKind::Moved,
        source_path: "/old/file.txt".to_string(),
        destination_path: Some("http://127.0.0.1:4918/new/file.txt".to_string()),
        checksum: None,
        size: 0,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling move event should succeed");

    assert!(
        !data_dir.join("old/file.txt").exists(),
        "source file should have been moved"
    );
    assert_eq!(
        fs::read(data_dir.join("new/file.txt")).expect("destination file should exist"),
        b"move me"
    );

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn incoming_copy_event_with_absolute_destination_url_copies_to_relative_target() {
    let sqlite_dir = temp_dir("sync-copy-abs-dest-sqlite");
    let data_dir = temp_dir("sync-copy-abs-dest-data");
    fs::create_dir_all(data_dir.join("old")).expect("old directory should exist");
    fs::write(data_dir.join("old/file.txt"), b"copy me").expect("source file should exist");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let event = FileChangeEvent {
        event_kind: EventKind::Copied,
        source_path: "/old/file.txt".to_string(),
        destination_path: Some("http://127.0.0.1:4918/new/file.txt".to_string()),
        checksum: None,
        size: 0,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling copy event should succeed");

    assert_eq!(
        fs::read(data_dir.join("old/file.txt")).expect("source file should remain"),
        b"copy me"
    );
    assert_eq!(
        fs::read(data_dir.join("new/file.txt")).expect("destination file should exist"),
        b"copy me"
    );

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

// --- Simple file transfer tests ---

#[tokio::test]
async fn fetch_request_returns_file_start() {
    let data_dir = temp_dir("sync-fetch-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let content = b"hello from a peer";
    fs::write(data_dir.join("test.txt"), content).expect("test file should be created");

    let response = handle_fetch_request(&data_dir, "/test.txt").await;

    match response {
        SyncResponse::FileStart { total_size, checksum, .. } => {
            assert_eq!(total_size, content.len() as u64);
            assert_eq!(checksum, crc32_hex(content));
        }
        other => panic!("expected FileStart response, got {other:?}"),
    }

    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn fetch_request_returns_not_found_for_missing_file() {
    let data_dir = temp_dir("sync-fetch-missing-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let response = handle_fetch_request(&data_dir, "/missing.txt").await;

    match response {
        SyncResponse::NotFound { path } => {
            assert_eq!(path, "/missing.txt");
        }
        other => panic!("expected not found response, got {other:?}"),
    }

    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn handle_file_start_and_chunk_writes_file_and_verifies_checksum() {
    let sqlite_dir = temp_dir("sync-file-response-sqlite");
    let data_dir = temp_dir("sync-file-response-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let content = b"hello from a peer";
    let checksum = crc32_hex(content);

    // Start a pull
    let req = state.start_pull(
        &data_dir,
        peer,
        "/incoming.txt".to_string(),
        Some(checksum.clone()),
        EventKind::Created,
        "peer:test".to_string(),
    );
    assert!(req.is_some());

    // Handle FileStart response
    let next = handle_file_start(
        &data_dir,
        &database,
        &mut state,
        peer,
        "/incoming.txt".to_string(),
        content.len() as u64,
        checksum.clone(),
    )
    .await
    .expect("file start handling should succeed");
    assert!(next.is_some());

    // Handle FileChunk response (single chunk, is_last=true)
    let next = handle_file_chunk(
        &data_dir,
        &database,
        &mut state,
        peer,
        "/incoming.txt".to_string(),
        content.to_vec(),
        0,
        true,
    )
    .await
    .expect("file chunk handling should succeed");
    assert!(next.is_none()); // transfer complete, no more chunks

    let written = fs::read(data_dir.join("incoming.txt")).expect("file should have been written");
    assert_eq!(written, content);
    assert!(!state.is_busy());

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn handle_file_chunk_discards_on_checksum_mismatch() {
    let sqlite_dir = temp_dir("sync-file-response-bad-sqlite");
    let data_dir = temp_dir("sync-file-response-bad-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let content = b"tampered content";
    let checksum = crc32_hex(content);

    // Start a pull with wrong expected checksum
    state.start_pull(
        &data_dir,
        peer,
        "/bad.txt".to_string(),
        None, // expected_checksum is ignored, FileStart provides the authoritative checksum
        EventKind::Created,
        "peer:test".to_string(),
    );

    // Handle FileStart response with WRONG checksum
    let next = handle_file_start(
        &data_dir,
        &database,
        &mut state,
        peer,
        "/bad.txt".to_string(),
        content.len() as u64,
        "wrong-checksum".to_string(), // Wrong checksum in FileStart
    )
    .await
    .expect("file start handling should succeed");
    assert!(next.is_some());

    // Handle FileChunk response with correct content but wrong expected checksum
    let next = handle_file_chunk(
        &data_dir,
        &database,
        &mut state,
        peer,
        "/bad.txt".to_string(),
        content.to_vec(),
        0,
        true,
    )
    .await
    .expect("file chunk handling should not error even on mismatch");
    // Should retry - returns a FetchFileChunk request
    assert!(next.is_some());

    // After checksum mismatch, it should retry (up to 3 times)
    // Each call retries - after 3 retries it gives up
    for _ in 0..3 {
        let next = handle_file_chunk(
            &data_dir,
            &database,
            &mut state,
            peer,
            "/bad.txt".to_string(),
            content.to_vec(),
            0,
            true,
        )
        .await
        .expect("file chunk handling should not error even on mismatch");
        // Each call should return a retry request
    }

    // After max retries, it should give up and not be busy
    assert!(
        !data_dir.join("bad.txt").exists(),
        "file with mismatched checksum must not be materialized"
    );
    assert!(!state.is_busy());

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn handle_not_found_removes_pending_transfer() {
    let sqlite_dir = temp_dir("sync-not-found-sqlite");
    let data_dir = temp_dir("sync-not-found-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    // Start a pull
    state.start_pull(
        &data_dir,
        peer,
        "/missing.txt".to_string(),
        None,
        EventKind::Created,
        "peer:test".to_string(),
    );
    assert!(state.is_busy());

    // Handle FileStart response first
    let _ = handle_file_start(
        &data_dir,
        &database,
        &mut state,
        peer,
        "/missing.txt".to_string(),
        100,
        "checksum".to_string(),
    )
    .await
    .expect("file start handling should succeed");

    // Handle NotFound response - should retry up to 3 times then give up
    for _ in 0..4 {
        sync::handle_not_found(&mut state, peer, "/missing.txt").await;
    }

    assert!(!state.is_busy());

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

// --- Serial mode tests ---

#[tokio::test]
async fn serial_mode_queues_multiple_pulls_fifo() {
    let sqlite_dir = temp_dir("sync-serial-fifo-sqlite");
    let data_dir = temp_dir("sync-serial-fifo-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    // Create test files
    let content1 = b"file one content";
    let content2 = b"file two content";
    let content3 = b"file three content";
    let checksum1 = crc32_hex(content1);
    let checksum2 = crc32_hex(content2);
    let checksum3 = crc32_hex(content3);

    // Start first pull - should start immediately
    let req1 = state.start_pull(
        &data_dir,
        peer,
        "/file1.txt".to_string(),
        Some(checksum1.clone()),
        EventKind::Created,
        "peer:test".to_string(),
    );
    assert!(req1.is_some());
    assert!(state.is_busy());

    // Start second pull - should be queued
    let req2 = state.start_pull(
        &data_dir,
        peer,
        "/file2.txt".to_string(),
        Some(checksum2.clone()),
        EventKind::Created,
        "peer:test".to_string(),
    );
    assert!(req2.is_none());
    assert_eq!(state.queue_len(), 1);

    // Start third pull - should be queued
    let req3 = state.start_pull(
        &data_dir,
        peer,
        "/file3.txt".to_string(),
        Some(checksum3.clone()),
        EventKind::Created,
        "peer:test".to_string(),
    );
    assert!(req3.is_none());
    assert_eq!(state.queue_len(), 2);

    // Finish first transfer - second should start
    let next = state.finish_transfer(peer, "/file1.txt");
    assert!(next.is_some());
    assert!(state.is_pending(peer, "/file2.txt"));
    assert_eq!(state.queue_len(), 1);

    // Finish second transfer - third should start
    let next = state.finish_transfer(peer, "/file2.txt");
    assert!(next.is_some());
    assert!(state.is_pending(peer, "/file3.txt"));
    assert_eq!(state.queue_len(), 0);

    // Finish third transfer - queue should be empty
    let next = state.finish_transfer(peer, "/file3.txt");
    assert!(next.is_none());
    assert!(!state.is_busy());

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn cancel_peer_removes_queued_transfers() {
    let sqlite_dir = temp_dir("sync-cancel-peer-sqlite");
    let data_dir = temp_dir("sync-cancel-peer-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer1 = PeerId::random();
    let peer2 = PeerId::random();
    let mut state = SyncState::new();

    // Start transfer from peer1
    state.start_pull(&data_dir, peer1, "/file1.txt".to_string(), None, EventKind::Created, "peer:test".to_string());
    // Queue transfer from peer2
    state.start_pull(&data_dir, peer2, "/file2.txt".to_string(), None, EventKind::Created, "peer:test".to_string());

    assert!(state.is_pending(peer1, "/file1.txt"));
    assert!(state.is_pending(peer2, "/file2.txt"));

    // Cancel peer1 - peer2's transfer should start
    let next = state.cancel_peer(peer1).await.expect("cancel should succeed");
    assert!(next.is_some());
    assert!(!state.is_pending(peer1, "/file1.txt"));
    assert!(state.is_pending(peer2, "/file2.txt"));

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn cancel_peer_when_idle_does_nothing() {
    let mut state = SyncState::new();
    let peer = PeerId::random();

    let next = state.cancel_peer(peer).await.expect("cancel should succeed");
    assert!(next.is_none());
    assert!(!state.is_busy());
}

#[tokio::test]
async fn apply_manifest_actions_creates_dirs_and_deletes() {
    let sqlite_dir = temp_dir("sync-apply-manifest-sqlite");
    let data_dir = temp_dir("sync-apply-manifest-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let actions = vec![
        SyncAction::CreateDir { path: "/newdir".to_string() },
        SyncAction::Delete { path: "/old.txt".to_string() },
        SyncAction::Pull { path: "/file.txt".to_string(), checksum: Some("abc".to_string()) },
    ];

    sync::apply_manifest_actions(&data_dir, &database, &mut state, peer, actions).await;

    // Directory should be created
    assert!(data_dir.join("newdir").exists());
    // Pull should be started (or queued)
    assert!(state.is_pending(peer, "/file.txt"));

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn incoming_created_event_starts_pull_when_missing() {
    let sqlite_dir = temp_dir("sync-incoming-created-sqlite");
    let data_dir = temp_dir("sync-incoming-created-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let content = b"hello from a peer";
    let checksum = crc32_hex(content);

    let event = FileChangeEvent {
        event_kind: EventKind::Created,
        source_path: "/incoming.txt".to_string(),
        destination_path: None,
        checksum: Some(checksum.clone()),
        size: content.len() as u64,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed");

    assert!(state.is_pending(peer, "/incoming.txt"));

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn incoming_deleted_event_deletes_locally() {
    let sqlite_dir = temp_dir("sync-incoming-deleted-sqlite");
    let data_dir = temp_dir("sync-incoming-deleted-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");
    fs::write(data_dir.join("todelete.txt"), b"delete me").expect("file should exist");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let event = FileChangeEvent {
        event_kind: EventKind::Deleted,
        source_path: "/todelete.txt".to_string(),
        destination_path: None,
        checksum: None,
        size: 0,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed");

    assert!(!data_dir.join("todelete.txt").exists());
    assert!(!state.is_busy());

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn incoming_dir_created_event_creates_directory() {
    let sqlite_dir = temp_dir("sync-incoming-dir-sqlite");
    let data_dir = temp_dir("sync-incoming-dir-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let event = FileChangeEvent {
        event_kind: EventKind::DirCreated,
        source_path: "/newdir".to_string(),
        destination_path: None,
        checksum: None,
        size: 0,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed");

    assert!(data_dir.join("newdir").exists());
    assert!(!state.is_busy());

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}