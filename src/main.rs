use std::net::SocketAddr;

use nacs_backend::{db, p2p, webdav};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Failed to start WebDAV server: {err}");
    }
}

async fn run() -> std::io::Result<()> {
    let dir = "./data";
    let sqlite_dir = "./sqlite";
    let host = std::env::var("WEBDAV_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("WEBDAV_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(4918);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .expect("Invalid WEBDAV_HOST/WEBDAV_PORT combination");

    let database = db::Database::open(sqlite_dir, dir).await?;

    tokio::spawn(async move {
        if let Err(err) = p2p::run_discovery(sqlite_dir).await {
            eprintln!("p2p discovery stopped: {err}");
        }
    });

    webdav::run_server(addr, dir, database).await
}
