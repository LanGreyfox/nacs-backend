use std::net::SocketAddr;

use nacs_backend::{api, db, p2p, sync, webdav};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Failed to start server: {err}");
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

    let api_host = std::env::var("API_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let api_port: u16 = std::env::var("API_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);
    let api_addr: SocketAddr = format!("{api_host}:{api_port}")
        .parse()
        .expect("Invalid API_HOST/API_PORT combination");

    let username = std::env::var("WEBDAV_USER").map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Missing required environment variable WEBDAV_USER: {err}"),
        )
    })?;
    let password = std::env::var("WEBDAV_PASS").map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Missing required environment variable WEBDAV_PASS: {err}"),
        )
    })?;

    let database = db::Database::open(sqlite_dir, dir).await?;
    let (p2p_handle, p2p_rx) = sync::P2pHandle::channel();
    let (p2p_query_tx, p2p_query_rx) = tokio::sync::mpsc::channel(32);

    tokio::spawn({
        let database = database.clone();
        async move {
            if let Err(err) =
                p2p::run_discovery(sqlite_dir, dir, database, p2p_rx, p2p_query_rx).await
            {
                eprintln!("p2p discovery stopped: {err}");
            }
        }
    });

    let api_state = api::ApiState {
        database: database.clone(),
        p2p_query_tx,
        auth_user: username.clone(),
        auth_pass: password.clone(),
    };
    let web_dir =
        std::path::PathBuf::from(std::env::var("WEB_DIR").unwrap_or_else(|_| "./web".to_string()));
    let web_dir = if web_dir.join("index.html").is_file() {
        Some(web_dir)
    } else {
        eprintln!("No index.html in {}; web UI disabled", web_dir.display());
        None
    };
    tokio::spawn(api::run_server(api_addr, api_state, web_dir));

    webdav::run_server(addr, dir, database, p2p_handle).await
}
