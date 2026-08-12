use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use nacs_backend::db::{Database, EventEnvelope, EventKind};
use rusqlite::Connection;
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

fn wait_for_condition<F>(timeout: Duration, mut check: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;

    loop {
        if check() {
            return;
        }

        if Instant::now() >= deadline {
            panic!("condition not met before timeout");
        }

        thread::sleep(Duration::from_millis(20));
    }
}

fn open_conn(path: &Path) -> Connection {
    Connection::open(path).expect("should open sqlite database")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

async fn open_database(sqlite_dir: &Path, data_dir: &Path) -> Database {
    Database::open(sqlite_dir, data_dir)
        .await
        .expect("database should open")
}

#[tokio::test]
async fn database_open_creates_sqlite_file_and_tables() {
    let base_dir = temp_dir("db-open");
    let sqlite_path = base_dir.join("webdav.db");

    let _database = open_database(&base_dir, &base_dir).await;

    wait_for_condition(Duration::from_secs(2), || sqlite_path.exists());

    let conn = open_conn(&sqlite_path);
    let table_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('resources', 'resource_archive', 'events')",
            [],
            |row| row.get(0),
        )
        .expect("should query table count");

    assert_eq!(table_count, 3);

    fs::remove_dir_all(&base_dir).expect("temp dir should be removed");
}

#[tokio::test]
async fn record_created_event_persists_resource_and_event() {
    let base_dir = temp_dir("db-created");
    let sqlite_path = base_dir.join("webdav.db");
    let file_path = base_dir.join("note.txt");
    let content = b"hello sqlite from webdav";

    fs::create_dir_all(&base_dir).expect("temp dir should exist");
    let mut file = fs::File::create(&file_path).expect("test file should be created");
    file.write_all(content).expect("test file should be writable");

    let database = open_database(&base_dir, &base_dir).await;
    database.record(EventEnvelope {
        event_kind: EventKind::Created,
        source_path: "/note.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "PUT".to_string(),
        status_code: 201,
        username: "alice".to_string(),
    });

    let expected_checksum = sha256_hex(content);

    wait_for_condition(Duration::from_secs(2), || {
        if !sqlite_path.exists() {
            return false;
        }

        let conn = open_conn(&sqlite_path);
        let resource_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))
            .expect("should query resource count");
        let event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("should query event count");

        resource_count == 1 && event_count == 1
    });

    let conn = open_conn(&sqlite_path);
    let (resource_kind, current_folder, checksum): (String, String, Option<String>) = conn
        .query_row(
            "SELECT resource_kind, current_folder, checksum FROM resources WHERE resource_path = ?1",
            ["/note.txt"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("resource row should exist");

    assert_eq!(resource_kind, "file");
    assert_eq!(current_folder, "/");
    assert_eq!(checksum.as_deref(), Some(expected_checksum.as_str()));

    let (event_type, event_resource_kind, method, status_code): (String, String, String, i64) = conn
        .query_row(
            "SELECT event_type, resource_kind, method, status_code FROM events WHERE source_path = ?1",
            ["/note.txt"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("event row should exist");

    assert_eq!(event_type, "created");
    assert_eq!(event_resource_kind, "file");
    assert_eq!(method, "PUT");
    assert_eq!(status_code, 201);

    fs::remove_dir_all(&base_dir).expect("temp dir should be removed");
}

#[tokio::test]
async fn record_deleted_event_archives_resource_and_removes_active_row() {
    let base_dir = temp_dir("db-deleted");
    let sqlite_path = base_dir.join("webdav.db");
    let file_path = base_dir.join("old.txt");

    fs::create_dir_all(&base_dir).expect("temp dir should exist");
    fs::write(&file_path, b"to be removed").expect("test file should be created");

    let database = open_database(&base_dir, &base_dir).await;
    database.record(EventEnvelope {
        event_kind: EventKind::Created,
        source_path: "/old.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "PUT".to_string(),
        status_code: 201,
        username: "alice".to_string(),
    });

    wait_for_condition(Duration::from_secs(2), || {
        if !sqlite_path.exists() {
            return false;
        }

        let conn = open_conn(&sqlite_path);
        let resource_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))
            .expect("should query resource count");
        resource_count == 1
    });

    fs::remove_file(&file_path).expect("test file should be removable");

    database.record(EventEnvelope {
        event_kind: EventKind::Deleted,
        source_path: "/old.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "DELETE".to_string(),
        status_code: 204,
        username: "alice".to_string(),
    });

    wait_for_condition(Duration::from_secs(2), || {
        if !sqlite_path.exists() {
            return false;
        }

        let conn = open_conn(&sqlite_path);
        let resource_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))
            .expect("should query resource count");
        let archive_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_archive", [], |row| row.get(0))
            .expect("should query archive count");
        let event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .expect("should query event count");

        resource_count == 0 && archive_count == 1 && event_count == 2
    });

    let conn = open_conn(&sqlite_path);
    let archived_checksum: Option<String> = conn
        .query_row(
            "SELECT checksum FROM resource_archive WHERE resource_path = ?1",
            ["/old.txt"],
            |row| row.get(0),
        )
        .expect("archive row should exist");

    assert!(archived_checksum.is_some());

    fs::remove_dir_all(&base_dir).expect("temp dir should be removed");
}

#[tokio::test]
async fn checksum_is_computed_from_webdav_path_relative_to_data_dir() {
    let sqlite_dir = temp_dir("db-checksum-sqlite");
    let data_dir = temp_dir("db-checksum-data");
    let sqlite_path = sqlite_dir.join("webdav.db");
    let content = b"checksum test content";

    fs::create_dir_all(&data_dir).expect("data dir should be created");
    let mut file = fs::File::create(data_dir.join("report.txt")).expect("test file should be created");
    file.write_all(content).expect("test file should be writable");

    let database = open_database(&sqlite_dir, &data_dir).await;
    database.record(EventEnvelope {
        event_kind: EventKind::Created,
        source_path: "/report.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "PUT".to_string(),
        status_code: 201,
        username: "bob".to_string(),
    });

    let expected_checksum = sha256_hex(content);

    wait_for_condition(Duration::from_secs(2), || {
        if !sqlite_path.exists() {
            return false;
        }
        let conn = open_conn(&sqlite_path);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))
            .expect("should query resource count");
        count == 1
    });

    let conn = open_conn(&sqlite_path);
    let checksum: Option<String> = conn
        .query_row(
            "SELECT checksum FROM resources WHERE resource_path = ?1",
            ["/report.txt"],
            |row| row.get(0),
        )
        .expect("resource row should exist");

    assert_eq!(
        checksum.as_deref(),
        Some(expected_checksum.as_str()),
        "checksum must be computed from data_dir/report.txt, not the raw WebDAV path"
    );

    fs::remove_dir_all(&sqlite_dir).expect("sqlite temp dir should be removed");
    fs::remove_dir_all(&data_dir).expect("data temp dir should be removed");
}

#[tokio::test]
async fn manifest_includes_existing_files_and_directories_without_prior_events() {
    let base_dir = temp_dir("db-manifest-scan");
    let data_dir = base_dir.join("data");
    let sqlite_dir = base_dir.join("sqlite");

    fs::create_dir_all(&data_dir).expect("data dir should be created");
    fs::create_dir_all(&data_dir.join("docs")).expect("docs dir should be created");
    fs::write(data_dir.join("docs/readme.txt"), b"hello from disk").expect("test file should be created");

    let database = open_database(&sqlite_dir, &data_dir).await;
    let manifest = database.manifest().await.expect("manifest should be readable");

    let paths: Vec<_> = manifest.resources.iter().map(|entry| entry.resource_path.clone()).collect();
    assert!(paths.contains(&"/docs".to_string()));
    assert!(paths.contains(&"/docs/readme.txt".to_string()));
    assert!(manifest.resources.iter().any(|entry| entry.resource_kind == "file"));

    fs::remove_dir_all(&base_dir).expect("temp dir should be removed");
}

#[tokio::test]
async fn manifest_lists_live_resources_and_tombstones_for_deleted_ones() {
    let sqlite_dir = temp_dir("db-manifest-sqlite");
    let data_dir = temp_dir("db-manifest-data");
    let sqlite_path = sqlite_dir.join("webdav.db");
    let keep_content = b"keep me around";

    fs::create_dir_all(&sqlite_dir).expect("sqlite dir should exist");
    fs::create_dir_all(&data_dir).expect("data dir should exist");
    fs::write(data_dir.join("keep.txt"), keep_content).expect("keep.txt should be created");
    fs::write(data_dir.join("gone.txt"), b"will be deleted").expect("gone.txt should be created");

    let database = open_database(&sqlite_dir, &data_dir).await;

    database.record(EventEnvelope {
        event_kind: EventKind::Created,
        source_path: "/keep.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "PUT".to_string(),
        status_code: 201,
        username: "alice".to_string(),
    });
    database.record(EventEnvelope {
        event_kind: EventKind::Created,
        source_path: "/gone.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "PUT".to_string(),
        status_code: 201,
        username: "alice".to_string(),
    });

    wait_for_condition(Duration::from_secs(2), || {
        if !sqlite_path.exists() {
            return false;
        }
        let conn = open_conn(&sqlite_path);
        let resource_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))
            .expect("should query resource count");
        resource_count == 2
    });

    fs::remove_file(data_dir.join("gone.txt")).expect("gone.txt should be removable");
    database.record(EventEnvelope {
        event_kind: EventKind::Deleted,
        source_path: "/gone.txt".to_string(),
        destination_path: None,
        checksum: None,
        method: "DELETE".to_string(),
        status_code: 204,
        username: "alice".to_string(),
    });

    wait_for_condition(Duration::from_secs(2), || {
        let conn = open_conn(&sqlite_path);
        let resource_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))
            .expect("should query resource count");
        let archive_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_archive", [], |row| row.get(0))
            .expect("should query archive count");
        resource_count == 1 && archive_count == 1
    });

    let manifest = database.manifest().await.expect("manifest should be readable");

    assert!(
        manifest.resources.iter().all(|entry| entry.resource_path != "/gone.txt"),
        "deleted resources should no longer appear as live manifest entries"
    );
    let entry = manifest
        .resources
        .iter()
        .find(|entry| entry.resource_path == "/keep.txt")
        .expect("keep.txt should remain in the live manifest");
    assert_eq!(entry.resource_kind, "file");
    assert_eq!(entry.size, keep_content.len() as u64);
    assert_eq!(entry.checksum.as_deref(), Some(sha256_hex(keep_content).as_str()));

    assert_eq!(manifest.tombstones.len(), 1);
    assert_eq!(manifest.tombstones[0].resource_path, "/gone.txt");

    fs::remove_dir_all(&sqlite_dir).expect("sqlite dir should be removed");
    fs::remove_dir_all(&data_dir).expect("data dir should be removed");
}

#[tokio::test]
async fn manifest_scan_ignores_sync_temp_files() {
    let sqlite_dir = temp_dir("manifest-ignores-sync-temp-sqlite");
    let data_dir = temp_dir("manifest-ignores-sync-temp-data");

    fs::create_dir_all(data_dir.join("nested")).expect("nested dir should be created");
    fs::write(data_dir.join("stable.txt"), b"stable").expect("stable file should be written");
    fs::write(data_dir.join("ghost.txt.p2p-tmp"), b"ghost").expect("tmp file should be written");
    fs::write(
        data_dir.join("nested/ghost.txt.p2p-tmp.p2p-tmp"),
        b"ghost nested",
    )
    .expect("nested tmp file should be written");

    let database = open_database(&sqlite_dir, &data_dir).await;
    let manifest = database.manifest().await.expect("manifest should be readable");

    assert!(
        manifest.resources.iter().any(|entry| entry.resource_path == "/stable.txt"),
        "regular files should remain visible in the manifest"
    );
    assert!(
        manifest
            .resources
            .iter()
            .all(|entry| !entry.resource_path.contains(".p2p-tmp")),
        "sync temp files must never be advertised in manifests"
    );

    fs::remove_dir_all(&sqlite_dir).ok();
    fs::remove_dir_all(&data_dir).ok();
}