use std::net::SocketAddr;

use nacs_backend::{db, p2p, sync, webdav};

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
    let (p2p_handle, p2p_rx) = sync::P2pHandle::channel();

    tokio::spawn({
        let database = database.clone();
        async move {
            if let Err(err) = p2p::run_discovery(sqlite_dir, dir, database, p2p_rx).await {
                eprintln!("p2p discovery stopped: {err}");
            }
        }
    });

    webdav::run_server(addr, dir, database, p2p_handle).await
}
