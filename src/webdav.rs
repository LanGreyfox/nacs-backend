use std::{convert::Infallible, io, net::SocketAddr, path::Path};

use dav_server::{fakels::FakeLs, localfs::LocalFs, DavHandler};
use dav_server::body::Body;
use hyper::{header::HeaderMap, server::conn::http1, service::service_fn, Response, StatusCode};
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

fn parse_basic_credentials(value: &str) -> Option<(String, String)> {
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
                                println!("WebDAV {} {} -> {} (ua={user_agent})", method, uri, status);

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

#[cfg(test)]
mod tests {
    use super::parse_basic_credentials;

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
}
