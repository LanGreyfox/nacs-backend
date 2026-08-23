use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use hyper::{Method, StatusCode};
use nacs_backend::db::{Database, EventKind};
use nacs_backend::sync::P2pHandle;
use nacs_backend::webdav::{
    FileEvent, build_unauthorized_response, ensure_data_dir, map_to_event, parse_basic_credentials,
    spawn_p2p_announcement,
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

#[tokio::test]
async fn ensure_data_dir_creates_missing_directory() {
    let dir = temp_dir("ensure-data-dir");
    assert!(!dir.exists());

    ensure_data_dir(&dir)
        .await
        .expect("data dir should be created");
    assert!(dir.exists());
    assert!(dir.is_dir());

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}

#[test]
fn parse_basic_credentials_accepts_case_insensitive_scheme() {
    let auth = "bAsIc dXNlcjpwYXNz";
    let creds = parse_basic_credentials(auth).expect("credentials should parse");
    assert_eq!(creds.0, "user");
    assert_eq!(creds.1, "pass");
}

#[test]
fn parse_basic_credentials_rejects_missing_password_separator() {
    let auth = "Basic dXNlcg==";
    assert!(parse_basic_credentials(auth).is_none());
}

#[test]
fn parse_basic_credentials_accepts_colon_in_password() {
    let auth = "Basic dXNlcjpwYTpzcw==";
    let creds = parse_basic_credentials(auth).expect("credentials should parse");
    assert_eq!(creds.0, "user");
    assert_eq!(creds.1, "pa:ss");
}

// ---------------------------------------------------------------------------
// map_to_event tests
// ---------------------------------------------------------------------------

fn uri(path: &str) -> hyper::Uri {
    path.parse().expect("valid URI")
}

fn method(name: &str) -> Method {
    Method::from_bytes(name.as_bytes()).expect("valid method")
}

#[test]
fn map_to_event_put_201_is_created() {
    assert_eq!(
        map_to_event(&Method::PUT, StatusCode::CREATED, &uri("/file.txt"), None),
        FileEvent::Created
    );
}

#[test]
fn map_to_event_put_204_is_edited() {
    assert_eq!(
        map_to_event(
            &Method::PUT,
            StatusCode::NO_CONTENT,
            &uri("/file.txt"),
            None
        ),
        FileEvent::Edited
    );
}

#[test]
fn map_to_event_delete_204_is_deleted() {
    assert_eq!(
        map_to_event(
            &Method::DELETE,
            StatusCode::NO_CONTENT,
            &uri("/file.txt"),
            None
        ),
        FileEvent::Deleted
    );
}

#[test]
fn map_to_event_move_same_dir_is_renamed() {
    assert_eq!(
        map_to_event(
            &method("MOVE"),
            StatusCode::CREATED,
            &uri("/docs/old.txt"),
            Some("http://localhost:4918/docs/new.txt"),
        ),
        FileEvent::Renamed
    );
}

#[test]
fn map_to_event_move_root_same_dir_is_renamed() {
    assert_eq!(
        map_to_event(
            &method("MOVE"),
            StatusCode::NO_CONTENT,
            &uri("/old.txt"),
            Some("http://localhost:4918/new.txt"),
        ),
        FileEvent::Renamed
    );
}

#[test]
fn map_to_event_move_different_dir_is_moved() {
    assert_eq!(
        map_to_event(
            &method("MOVE"),
            StatusCode::CREATED,
            &uri("/docs/file.txt"),
            Some("http://localhost:4918/archive/file.txt"),
        ),
        FileEvent::Moved
    );
}

#[test]
fn map_to_event_move_no_destination_is_moved() {
    assert_eq!(
        map_to_event(
            &method("MOVE"),
            StatusCode::NO_CONTENT,
            &uri("/file.txt"),
            None
        ),
        FileEvent::Moved
    );
}

#[test]
fn map_to_event_copy_201_is_copied() {
    assert_eq!(
        map_to_event(
            &method("COPY"),
            StatusCode::CREATED,
            &uri("/file.txt"),
            None
        ),
        FileEvent::Copied
    );
}

#[test]
fn map_to_event_mkcol_201_is_dir_created() {
    assert_eq!(
        map_to_event(&method("MKCOL"), StatusCode::CREATED, &uri("/newdir"), None),
        FileEvent::DirCreated
    );
}

#[test]
fn map_to_event_propfind_207_is_listed() {
    assert_eq!(
        map_to_event(
            &method("PROPFIND"),
            StatusCode::MULTI_STATUS,
            &uri("/"),
            None
        ),
        FileEvent::Listed
    );
}

#[test]
fn map_to_event_proppatch_207_is_prop_patched() {
    assert_eq!(
        map_to_event(
            &method("PROPPATCH"),
            StatusCode::MULTI_STATUS,
            &uri("/file.txt"),
            None
        ),
        FileEvent::PropPatched
    );
}

#[test]
fn map_to_event_get_200_is_read() {
    assert_eq!(
        map_to_event(&Method::GET, StatusCode::OK, &uri("/file.txt"), None),
        FileEvent::Read
    );
}

#[test]
fn map_to_event_lock_200_is_locked() {
    assert_eq!(
        map_to_event(&method("LOCK"), StatusCode::OK, &uri("/file.txt"), None),
        FileEvent::Locked
    );
}

#[test]
fn map_to_event_unlock_204_is_unlocked() {
    assert_eq!(
        map_to_event(
            &method("UNLOCK"),
            StatusCode::NO_CONTENT,
            &uri("/file.txt"),
            None
        ),
        FileEvent::Unlocked
    );
}

#[test]
fn map_to_event_options_200_is_options() {
    assert_eq!(
        map_to_event(&Method::OPTIONS, StatusCode::OK, &uri("/"), None),
        FileEvent::Options
    );
}

#[test]
fn map_to_event_put_404_is_unknown() {
    assert_eq!(
        map_to_event(&Method::PUT, StatusCode::NOT_FOUND, &uri("/file.txt"), None),
        FileEvent::Unknown
    );
}

#[test]
fn unauthorized_response_includes_basic_challenge_and_content_length() {
    let response = build_unauthorized_response(&method("PROPFIND"));

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get("WWW-Authenticate")
            .and_then(|v| v.to_str().ok()),
        Some("Basic realm=\"webdav\", charset=\"UTF-8\"")
    );
    assert_eq!(
        response
            .headers()
            .get("Content-Length")
            .and_then(|v| v.to_str().ok()),
        Some("0")
    );
}

#[test]
fn unauthorized_options_response_includes_dav_capability_headers() {
    let response = build_unauthorized_response(&Method::OPTIONS);

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get("DAV").and_then(|v| v.to_str().ok()),
        Some("1,2")
    );
    assert_eq!(
        response
            .headers()
            .get("MS-Author-Via")
            .and_then(|v| v.to_str().ok()),
        Some("DAV")
    );
    assert_eq!(
        response
            .headers()
            .get("Allow")
            .and_then(|v| v.to_str().ok()),
        Some(
            "OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, COPY, MOVE, LOCK, UNLOCK"
        )
    );
}

#[tokio::test]
async fn spawn_p2p_announcement_computes_checksum_in_background() {
    let data_dir = temp_dir("spawn-p2p-announcement");
    let sqlite_dir = temp_dir("spawn-p2p-announcement-sqlite");
    fs::create_dir_all(&data_dir).expect("temp dir should be created");
    fs::write(data_dir.join("report.txt"), b"hello webdav").expect("test file should be written");
    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let (p2p, mut rx) = P2pHandle::channel();
    let handle = spawn_p2p_announcement(
        EventKind::Created,
        "/report.txt".to_string(),
        None,
        "user".to_string(),
        "PUT".to_string(),
        201,
        data_dir.clone(),
        "/report.txt".to_string(),
        database,
        p2p,
    );

    handle.await.expect("background task should complete");
    let event = rx.recv().await.expect("announcement should be queued");

    assert_eq!(event.event_kind, EventKind::Created);
    assert_eq!(event.source_path, "/report.txt");
    assert_eq!(event.destination_path, None);
    assert_eq!(event.size, 12);
    assert!(event.checksum.is_some());

    fs::remove_dir_all(&data_dir).expect("temp dir should be removed");
    fs::remove_dir_all(&sqlite_dir).expect("temp dir should be removed");
}

#[tokio::test]
async fn spawn_p2p_announcement_falls_back_when_file_is_missing() {
    let data_dir = temp_dir("spawn-p2p-announcement-missing");
    let sqlite_dir = temp_dir("spawn-p2p-announcement-missing-sqlite");
    fs::create_dir_all(&data_dir).expect("temp dir should be created");
    let database = Database::open(&sqlite_dir, &data_dir)
        .await
        .expect("database should open");

    let (p2p, mut rx) = P2pHandle::channel();
    let handle = spawn_p2p_announcement(
        EventKind::Deleted,
        "/missing.txt".to_string(),
        None,
        "user".to_string(),
        "DELETE".to_string(),
        204,
        data_dir.clone(),
        "/missing.txt".to_string(),
        database,
        p2p,
    );

    handle.await.expect("background task should complete");
    let event = rx.recv().await.expect("announcement should be queued");

    assert_eq!(event.event_kind, EventKind::Deleted);
    assert_eq!(event.size, 0);
    assert_eq!(event.checksum, None);

    fs::remove_dir_all(&data_dir).expect("temp dir should be removed");
    fs::remove_dir_all(&sqlite_dir).expect("temp dir should be removed");
}
