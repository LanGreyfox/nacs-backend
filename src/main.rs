mod webdav;

use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let dir = "./data";
    let host = std::env::var("WEBDAV_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("WEBDAV_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(4918);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .expect("Invalid WEBDAV_HOST/WEBDAV_PORT combination");

    if let Err(err) = webdav::run_server(addr, dir).await {
        eprintln!("Failed to start WebDAV server: {err}");
    }
}
