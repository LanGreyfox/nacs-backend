use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use crate::db::{Database, EventEnvelope, EventKind, file_checksum_and_size};
use crate::sync::{FileChangeEvent, P2pHandle};
use dav_server::body::Body;
use dav_server::{DavHandler, fakels::FakeLs, localfs::LocalFs};
use hyper::header::{AUTHORIZATION, CONTENT_LENGTH, USER_AGENT, WWW_AUTHENTICATE};
use hyper::{
    Method, Response, StatusCode, Uri, header::HeaderMap, server::conn::http1, service::service_fn,
};
// use dav_server::Body for response bodies (imported above)
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

pub async fn ensure_data_dir(dir: impl AsRef<Path>) -> io::Result<()> {
    tokio::fs::create_dir_all(dir).await
}

pub fn build_handler(dir: impl AsRef<Path>) -> DavHandler {
    DavHandler::builder()
        .filesystem(LocalFs::new(dir.as_ref(), false, false, false))
        .locksystem(FakeLs::new())
        .build_handler()
}

pub fn parse_basic_credentials(value: &str) -> Option<(String, String)> {
    let mut parts = value.split_whitespace();
    let scheme = parts.next()?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }

    let token = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let decoded = STANDARD.decode(token).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

fn method_allows_unauthenticated(method: &Method) -> bool {
    method.as_str() == "OPTIONS"
}

fn is_authorized(
    headers: &HeaderMap,
    expected_user: &str,
    expected_pass: &str,
    method: &Method,
) -> bool {
    if method_allows_unauthenticated(method) {
        return true;
    }

    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_basic_credentials)
        .map(|(u, p)| u == expected_user && p == expected_pass)
        .unwrap_or(false)
}

/// Semantic file event derived from a WebDAV HTTP method and its response status code.
#[derive(Debug, PartialEq)]
pub enum FileEvent {
    /// A new file was uploaded (PUT → 201 Created)
    Created,
    /// An existing file was overwritten (PUT → 204 No Content)
    Edited,
    /// A file or directory was removed (DELETE → 204 No Content)
    Deleted,
    /// A file was renamed within the same directory (MOVE → 201/204, same parent path)
    Renamed,
    /// A file or directory was relocated to a different path (MOVE → 201/204, different parent)
    Moved,
    /// A file was duplicated (COPY → 201/204)
    Copied,
    /// A new collection (directory) was created (MKCOL → 201 Created)
    DirCreated,
    /// Properties on a resource were updated (PROPPATCH → 207 Multi-Status)
    PropPatched,
    /// A lock was acquired on a resource (LOCK → 200 OK)
    Locked,
    /// A lock was released from a resource (UNLOCK → 204 No Content)
    Unlocked,
    /// A file was downloaded (GET/HEAD → 200 OK)
    Read,
    /// A collection listing or properties were retrieved (PROPFIND → 207 Multi-Status)
    Listed,
    /// Supported DAV capabilities were queried (OPTIONS → 200 OK)
    Options,
    /// An unrecognised method, or a 4xx/5xx error response
    Unknown,
}

/// Extracts the path component from a `Destination` header value, which may be
/// either a full URL (`http://host/path`) or a plain absolute path (`/path`).
fn destination_path(destination: &str) -> &str {
    if destination.starts_with("http://") {
        let after = &destination[7..];
        return after.find('/').map(|i| &after[i..]).unwrap_or("/");
    }
    if destination.starts_with("https://") {
        let after = &destination[8..];
        return after.find('/').map(|i| &after[i..]).unwrap_or("/");
    }
    destination
}

fn normalize_destination(destination: &str) -> String {
    let path = destination_path(destination)
        .split(['?', '#'])
        .next()
        .unwrap_or("/");
    let path = percent_decode_lossy(path);

    if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    }
}

fn normalize_request_path(path: &str) -> String {
    let path = path.split(['?', '#']).next().unwrap_or("/");
    let path = percent_decode_lossy(path);
    if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    }
}

fn percent_decode_lossy(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_value(bytes[i + 1]);
            let lo = hex_value(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Returns the parent directory portion of an absolute path.
/// `/a/b/c` → `/a/b`, `/file.txt` → `/`, `/` → `/`.
fn parent_path(path: &str) -> &str {
    match path.rfind('/') {
        Some(0) | None => "/",
        Some(idx) => &path[..idx],
    }
}

/// Maps a WebDAV HTTP method and the resulting response status to a semantic
/// [`FileEvent`]. For MOVE requests the optional `destination` (value of the
/// `Destination` request header) is used to distinguish a rename (same
/// directory) from a move (different directory).
pub fn map_to_event(
    method: &Method,
    status: StatusCode,
    uri: &Uri,
    destination: Option<&str>,
) -> FileEvent {
    match method.as_str() {
        "PUT" => match status {
            StatusCode::CREATED => FileEvent::Created,
            StatusCode::NO_CONTENT => FileEvent::Edited,
            _ => FileEvent::Unknown,
        },
        "DELETE" => match status {
            StatusCode::NO_CONTENT | StatusCode::OK => FileEvent::Deleted,
            _ => FileEvent::Unknown,
        },
        "MOVE" => match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT => {
                let normalized_src = normalize_request_path(uri.path());
                let src_parent = parent_path(&normalized_src);
                let is_rename = destination
                    .map(|dest| parent_path(destination_path(dest)) == src_parent)
                    .unwrap_or(false);
                if is_rename {
                    FileEvent::Renamed
                } else {
                    FileEvent::Moved
                }
            }
            _ => FileEvent::Unknown,
        },
        "COPY" => match status {
            StatusCode::CREATED | StatusCode::NO_CONTENT => FileEvent::Copied,
            _ => FileEvent::Unknown,
        },
        "MKCOL" => match status {
            StatusCode::CREATED => FileEvent::DirCreated,
            _ => FileEvent::Unknown,
        },
        "PROPPATCH" => match status {
            StatusCode::MULTI_STATUS => FileEvent::PropPatched,
            _ => FileEvent::Unknown,
        },
        "LOCK" => match status {
            StatusCode::OK => FileEvent::Locked,
            _ => FileEvent::Unknown,
        },
        "UNLOCK" => match status {
            StatusCode::NO_CONTENT => FileEvent::Unlocked,
            _ => FileEvent::Unknown,
        },
        "GET" | "HEAD" => match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => FileEvent::Read,
            _ => FileEvent::Unknown,
        },
        "PROPFIND" => match status {
            StatusCode::MULTI_STATUS => FileEvent::Listed,
            _ => FileEvent::Unknown,
        },
        "OPTIONS" => match status {
            StatusCode::OK => FileEvent::Options,
            _ => FileEvent::Unknown,
        },
        _ => FileEvent::Unknown,
    }
}

/// Logs a [`FileEvent`] to stdout on success or stderr on 4xx/5xx errors,
/// including the HTTP method, URI, response status, and user-agent string.
pub fn log_file_event(
    event: &FileEvent,
    method: &Method,
    uri: &Uri,
    status: StatusCode,
    user_agent: &str,
) {
    let tag = match event {
        FileEvent::Created => "FILE CREATED",
        FileEvent::Edited => "FILE EDITED",
        FileEvent::Deleted => "FILE DELETED",
        FileEvent::Renamed => "FILE RENAMED",
        FileEvent::Moved => "FILE MOVED",
        FileEvent::Copied => "FILE COPIED",
        FileEvent::DirCreated => "DIR CREATED",
        FileEvent::PropPatched => "PROPS PATCHED",
        FileEvent::Locked => "RESOURCE LOCKED",
        FileEvent::Unlocked => "RESOURCE UNLOCKED",
        FileEvent::Read => "FILE READ",
        FileEvent::Listed => "DIR LISTED",
        FileEvent::Options => "OPTIONS",
        FileEvent::Unknown => "UNKNOWN",
    };

    let line = format!("[{tag}] {method} {uri} -> {status} (ua={user_agent})");

    if status.is_client_error() || status.is_server_error() {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

pub fn build_unauthorized_response(method: &Method) -> Response<Body> {
    // Dolphin/KIO expects a strict challenge response and
    // benefits from DAV capability headers on preflight OPTIONS.
    let mut builder = Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            WWW_AUTHENTICATE,
            "Basic realm=\"webdav\", charset=\"UTF-8\"",
        )
        .header(CONTENT_LENGTH, "0");

    if method.as_str() == "OPTIONS" {
        builder = builder
            .header("DAV", "1,2")
            .header("MS-Author-Via", "DAV")
            .header(
                "Allow",
                "OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, COPY, MOVE, LOCK, UNLOCK",
            );
    }

    builder.body(Body::empty()).unwrap()
}

fn event_kind_for_storage(event: &FileEvent) -> Option<EventKind> {
    match event {
        FileEvent::Created => Some(EventKind::Created),
        FileEvent::Edited => Some(EventKind::Edited),
        FileEvent::Deleted => Some(EventKind::Deleted),
        FileEvent::Renamed => Some(EventKind::Renamed),
        FileEvent::Moved => Some(EventKind::Moved),
        FileEvent::Copied => Some(EventKind::Copied),
        FileEvent::DirCreated => Some(EventKind::DirCreated),
        _ => None,
    }
}

fn required_env_var(name: &str) -> io::Result<String> {
    std::env::var(name).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Missing required environment variable {name}: {err}"),
        )
    })
}

#[doc(hidden)]
pub fn spawn_p2p_announcement(
    event_kind: EventKind,
    source_path: String,
    destination_path: Option<String>,
    username: String,
    method: String,
    status_code: u16,
    data_dir: PathBuf,
    final_path: String,
    database: Database,
    p2p: P2pHandle,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let (checksum, size) = match file_checksum_and_size(&data_dir, &final_path) {
            Ok(Some((checksum, size))) => (Some(checksum), size),
            Ok(None) | Err(_) => (None, 0),
        };

        database.record(EventEnvelope {
            event_kind,
            source_path: source_path.clone(),
            destination_path: destination_path.clone(),
            checksum: checksum.clone(),
            method,
            status_code,
            username: username.clone(),
        });

        p2p.announce(FileChangeEvent {
            event_kind,
            source_path,
            destination_path,
            checksum,
            size,
            username,
        });
    })
}

pub async fn run_server(
    addr: SocketAddr,
    dir: impl AsRef<Path>,
    database: Database,
    p2p: P2pHandle,
) -> io::Result<()> {
    ensure_data_dir(dir.as_ref()).await?;
    // Read credentials from environment
    let username = required_env_var("WEBDAV_USER")?;
    let password = required_env_var("WEBDAV_PASS")?;
    let data_dir = dir.as_ref().to_path_buf();

    let dav_server = build_handler(dir.as_ref());
    let listener = TcpListener::bind(addr).await?;

    println!("WebDAV listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let dav_server = dav_server.clone();
        let database = database.clone();
        let p2p = p2p.clone();
        let data_dir = data_dir.clone();
        let username = username.clone();
        let password = password.clone();
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn({
                        move |req| {
                            let dav_server = dav_server.clone();
                            let username = username.clone();
                            let password = password.clone();
                            let database = database.clone();
                            let p2p = p2p.clone();
                            let data_dir = data_dir.clone();
                            async move {
                                let method = req.method().clone();
                                let uri = req.uri().clone();
                                let user_agent = req
                                    .headers()
                                    .get(USER_AGENT)
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or("<none>")
                                    .to_string();
                                let destination = req
                                    .headers()
                                    .get("Destination")
                                    .and_then(|v| v.to_str().ok())
                                    .map(normalize_destination);

                                let authorized = is_authorized(req.headers(), &username, &password, &method);

                                if !authorized {
                                    let auth_reason = match req
                                        .headers()
                                        .get(AUTHORIZATION)
                                        .and_then(|v| v.to_str().ok())
                                    {
                                        None => "missing or invalid Authorization header",
                                        Some(raw) if parse_basic_credentials(raw).is_none() => {
                                            "could not parse Basic credentials"
                                        }
                                        Some(_) => "username/password mismatch",
                                    };
                                    eprintln!(
                                        "WebDAV {} {} unauthorized (ua={user_agent}): {auth_reason}",
                                        method, uri
                                    );

                                    let resp = build_unauthorized_response(&method);
                                    return Ok::<_, Infallible>(resp);
                                }

                                let response = dav_server.handle(req).await;
                                let status = response.status();
                                let event = map_to_event(&method, status, &uri, destination.as_deref());
                                if let Some(event_kind) = event_kind_for_storage(&event) {
                                    let source_path = normalize_request_path(uri.path());
                                    let final_path =
                                        destination.clone().unwrap_or_else(|| source_path.clone());
                                    let should_hash = matches!(
                                        event_kind,
                                        EventKind::Created
                                            | EventKind::Edited
                                            | EventKind::Renamed
                                            | EventKind::Moved
                                            | EventKind::Copied
                                    );

                                    if should_hash {
                                        spawn_p2p_announcement(
                                            event_kind,
                                            source_path,
                                            destination.clone(),
                                            username.clone(),
                                            method.as_str().to_string(),
                                            status.as_u16(),
                                            data_dir.clone(),
                                            final_path,
                                            database.clone(),
                                            p2p.clone(),
                                        );
                                    } else {
                                        database.record(EventEnvelope {
                                            event_kind,
                                            source_path: source_path.clone(),
                                            destination_path: destination.clone(),
                                            checksum: None,
                                            method: method.as_str().to_string(),
                                            status_code: status.as_u16(),
                                            username: username.clone(),
                                        });

                                        p2p.announce(FileChangeEvent {
                                            event_kind,
                                            source_path,
                                            destination_path: destination.clone(),
                                            checksum: None,
                                            size: 0,
                                            username: username.clone(),
                                        });
                                    }
                                }
                                log_file_event(&event, &method, &uri, status, &user_agent);

                                Ok::<_, Infallible>(response)
                            }
                        }
                    }),
                )
                .await
            {
                eprintln!("Failed serving: {err:?}");
            }
        });
    }
}
