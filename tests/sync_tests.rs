use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use libp2p::PeerId;
use nacs_backend::db::{Database, EventKind, Manifest, ManifestEntry, TombstoneEntry};
use nacs_backend::sync::{
    self, diff_manifests, FileChangeEvent, SyncAction, SyncRequest, SyncResponse, SyncState,
};
use sha2::{Digest, Sha256};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();

    let mut path = std::env::temp_dir();
    path.push(format!("nacs-backend-{name}-{nanos}-{}", std::process::id()));
    path
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
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

// --- Wire type serialization round-trips (JSON stand-in for the CBOR codec
// actually used on the wire; guards against accidentally introducing
// non-serializable fields into the wire types). ---

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
            offset: 128,
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
        let _decoded: SyncRequest = serde_json::from_str(&json).expect("request should deserialize");
    }

    let responses = vec![
        SyncResponse::Manifest(Manifest::default()),
        SyncResponse::Chunk {
            path: "/a.txt".to_string(),
            data: vec![1, 2, 3, 4],
            offset: 0,
            total_size: 4,
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

    let request = sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling move event should succeed");

    assert!(request.is_none(), "local rename should not trigger a pull");
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

    let request = sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling copy event should succeed");

    assert!(request.is_none(), "local copy should not trigger a pull");
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

// --- Chunked transfer state machine (finalize + checksum verification) ---

#[tokio::test]
async fn incoming_created_event_pulls_and_finalizes_on_matching_checksum() {
    let sqlite_dir = temp_dir("sync-pull-ok-sqlite");
    let data_dir = temp_dir("sync-pull-ok-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let content = b"hello from a peer";
    let checksum = sha256_hex(content);

    let event = FileChangeEvent {
        event_kind: EventKind::Created,
        source_path: "/incoming.txt".to_string(),
        destination_path: None,
        checksum: Some(checksum.clone()),
        size: content.len() as u64,
        username: "peer:test".to_string(),
    };

    let next_request = sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed")
        .expect("a FetchFile request should be produced");

    assert!(matches!(
        next_request,
        SyncRequest::FetchFile { ref path, offset: 0 } if path == "/incoming.txt"
    ));

    let chunk_response = SyncResponse::Chunk {
        path: "/incoming.txt".to_string(),
        data: content.to_vec(),
        offset: 0,
        total_size: content.len() as u64,
        is_last: true,
    };

    let follow_up = sync::handle_chunk_response(&mut state, &database, peer, chunk_response)
        .await
        .expect("chunk handling should succeed");

    assert!(follow_up.is_none(), "single-chunk transfer should complete");
    assert!(
        !state.is_pending(peer, "/incoming.txt"),
        "transfer should no longer be pending after completion"
    );

    let written = fs::read(data_dir.join("incoming.txt")).expect("file should have been written");
    assert_eq!(written, content);

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn chunk_transfer_discarded_on_checksum_mismatch() {
    let sqlite_dir = temp_dir("sync-pull-bad-sqlite");
    let data_dir = temp_dir("sync-pull-bad-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let content = b"tampered content";

    let event = FileChangeEvent {
        event_kind: EventKind::Created,
        source_path: "/bad.txt".to_string(),
        destination_path: None,
        checksum: Some("expected-but-wrong-checksum".to_string()),
        size: content.len() as u64,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed");

    let chunk_response = SyncResponse::Chunk {
        path: "/bad.txt".to_string(),
        data: content.to_vec(),
        offset: 0,
        total_size: content.len() as u64,
        is_last: true,
    };

    let follow_up = sync::handle_chunk_response(&mut state, &database, peer, chunk_response)
        .await
        .expect("chunk handling should not error even on mismatch");

    assert!(follow_up.is_none());
    assert!(
        !data_dir.join("bad.txt").exists(),
        "file with mismatched checksum must not be materialized"
    );
    assert!(
        !data_dir.join("bad.txt.p2p-tmp").exists(),
        "temp file should be removed when checksum verification fails"
    );
    assert!(!state.is_pending(peer, "/bad.txt"));

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn not_found_response_removes_pending_temp_file() {
    let sqlite_dir = temp_dir("sync-pull-not-found-sqlite");
    let data_dir = temp_dir("sync-pull-not-found-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let event = FileChangeEvent {
        event_kind: EventKind::Created,
        source_path: "/missing.txt".to_string(),
        destination_path: None,
        checksum: None,
        size: 123,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed");

    assert!(
        data_dir.join("missing.txt.p2p-tmp").exists(),
        "starting a pull should create a temp file"
    );

    let response = SyncResponse::NotFound {
        path: "/missing.txt".to_string(),
    };

    let follow_up = sync::handle_chunk_response(&mut state, &database, peer, response)
        .await
        .expect("not found should cancel cleanly");

    assert!(follow_up.is_none());
    assert!(
        !state.is_pending(peer, "/missing.txt"),
        "transfer should be removed from pending state"
    );
    assert!(
        !data_dir.join("missing.txt.p2p-tmp").exists(),
        "temp file should be removed when the peer reports the file missing"
    );

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}

#[tokio::test]
async fn multi_chunk_transfer_requests_next_offset_until_last_chunk() {
    let sqlite_dir = temp_dir("sync-pull-multi-sqlite");
    let data_dir = temp_dir("sync-pull-multi-data");
    fs::create_dir_all(&data_dir).expect("data dir should be created");

    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let peer = PeerId::random();
    let mut state = SyncState::new();

    let part_a = b"first-half-".to_vec();
    let part_b = b"second-half".to_vec();
    let mut full_content = part_a.clone();
    full_content.extend_from_slice(&part_b);
    let checksum = sha256_hex(&full_content);

    let event = FileChangeEvent {
        event_kind: EventKind::Created,
        source_path: "/multi.txt".to_string(),
        destination_path: None,
        checksum: Some(checksum),
        size: full_content.len() as u64,
        username: "peer:test".to_string(),
    };

    sync::handle_incoming_event(&data_dir, &database, &mut state, peer, event)
        .await
        .expect("handling the event should succeed");

    let first_chunk = SyncResponse::Chunk {
        path: "/multi.txt".to_string(),
        data: part_a.clone(),
        offset: 0,
        total_size: full_content.len() as u64,
        is_last: false,
    };

    let follow_up = sync::handle_chunk_response(&mut state, &database, peer, first_chunk)
        .await
        .expect("chunk handling should succeed");

    match follow_up {
        Some(SyncRequest::FetchFile { path, offset }) => {
            assert_eq!(path, "/multi.txt");
            assert_eq!(offset, part_a.len() as u64);
        }
        other => panic!("expected a FetchFile follow-up request, got {other:?}"),
    }
    assert!(
        state.is_pending(peer, "/multi.txt"),
        "transfer should still be pending after a non-final chunk"
    );

    let second_chunk = SyncResponse::Chunk {
        path: "/multi.txt".to_string(),
        data: part_b.clone(),
        offset: part_a.len() as u64,
        total_size: full_content.len() as u64,
        is_last: true,
    };

    let follow_up = sync::handle_chunk_response(&mut state, &database, peer, second_chunk)
        .await
        .expect("chunk handling should succeed");

    assert!(follow_up.is_none());
    assert!(!state.is_pending(peer, "/multi.txt"));

    let written = fs::read(data_dir.join("multi.txt")).expect("file should have been written");
    assert_eq!(written, full_content);

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}
