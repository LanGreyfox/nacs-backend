use std::{convert::Infallible, io, net::SocketAddr, path::Path};

use dav_server::{fakels::FakeLs, localfs::LocalFs, DavHandler};
use dav_server::body::Body;
use hyper::{header::HeaderMap, server::conn::http1, service::service_fn, Method, Response, StatusCode, Uri};
use hyper::header::{AUTHORIZATION, USER_AGENT};
// use dav_server::Body for response bodies (imported above)
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;

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

fn is_authorized(headers: &HeaderMap, expected_user: &str, expected_pass: &str) -> bool {
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
                let src_parent = parent_path(uri.path());
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
        FileEvent::Created     => "FILE CREATED",
        FileEvent::Edited      => "FILE EDITED",
        FileEvent::Deleted     => "FILE DELETED",
        FileEvent::Renamed     => "FILE RENAMED",
        FileEvent::Moved       => "FILE MOVED",
        FileEvent::Copied      => "FILE COPIED",
        FileEvent::DirCreated  => "DIR CREATED",
        FileEvent::PropPatched => "PROPS PATCHED",
        FileEvent::Locked      => "RESOURCE LOCKED",
        FileEvent::Unlocked    => "RESOURCE UNLOCKED",
        FileEvent::Read        => "FILE READ",
        FileEvent::Listed      => "DIR LISTED",
        FileEvent::Options     => "OPTIONS",
        FileEvent::Unknown     => "UNKNOWN",
    };

    let line = format!("[{tag}] {method} {uri} -> {status} (ua={user_agent})");

    if status.is_client_error() || status.is_server_error() {
        eprintln!("{line}");
    } else {
        println!("{line}");
    }
}

pub async fn run_server(addr: SocketAddr, dir: impl AsRef<Path>) -> io::Result<()> {
    ensure_data_dir(dir.as_ref()).await?;
    // Read credentials from environment
    let username = std::env::var("WEBDAV_USER").expect("WEBDAV_USER must be set");
    let password = std::env::var("WEBDAV_PASS").expect("WEBDAV_PASS must be set");

    let dav_server = build_handler(dir.as_ref());
    let listener = TcpListener::bind(addr).await?;

    println!("WebDAV listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let dav_server = dav_server.clone();
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
                                    .map(|s| s.to_string());

                                let authorized = is_authorized(req.headers(), &username, &password);

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

                                    let resp = Response::builder()
                                        .status(StatusCode::UNAUTHORIZED)
                                        .header("WWW-Authenticate", "Basic realm=\"webdav\"")
                                        .body(Body::empty())
                                        .unwrap();
                                    return Ok::<_, Infallible>(resp);
                                }

                                let response = dav_server.handle(req).await;
                                let status = response.status();
                                let event = map_to_event(&method, status, &uri, destination.as_deref());
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


