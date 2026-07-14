use std::{convert::Infallible, io, net::SocketAddr, path::Path};

use dav_server::{fakels::FakeLs, localfs::LocalFs, DavHandler};
use dav_server::body::Body;
use hyper::{server::conn::http1, service::service_fn, Response, StatusCode};
use hyper::header::AUTHORIZATION;
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

pub async fn run_server(addr: SocketAddr, dir: impl AsRef<Path>) -> io::Result<()> {
    ensure_data_dir(dir.as_ref()).await?;
    // Read credentials from environment
    let username = std::env::var("WEBDAV_USER").expect("WEBDAV_USER must be set");
    let password = std::env::var("WEBDAV_PASS").expect("WEBDAV_PASS must be set");
    let expected_auth = {
        let creds = format!("{}:{}", username, password);
        format!("Basic {}", STANDARD.encode(creds))
    };

    let dav_server = build_handler(dir.as_ref());
    let listener = TcpListener::bind(addr).await?;

    println!("WebDAV listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
        let dav_server = dav_server.clone();
        let expected_auth = expected_auth.clone();
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn({
                        move |req| {
                            let dav_server = dav_server.clone();
                            let expected_auth = expected_auth.clone();
                            async move {
                                // Check Authorization header
                                let authorized = req
                                    .headers()
                                    .get(AUTHORIZATION)
                                    .and_then(|v| v.to_str().ok())
                                    .map(|s| s == expected_auth)
                                    .unwrap_or(false);

                                if !authorized {
                                    let resp = Response::builder()
                                        .status(StatusCode::UNAUTHORIZED)
                                        .header("WWW-Authenticate", "Basic realm=\"webdav\"")
                                        .body(Body::empty())
                                        .unwrap();
                                    return Ok::<_, Infallible>(resp);
                                }

                                Ok::<_, Infallible>(dav_server.handle(req).await)
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
