mod webdav;

use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let dir = "./data";
    let addr: SocketAddr = ([127, 0, 0, 1], 4918).into();

    if let Err(err) = webdav::run_server(addr, dir).await {
        eprintln!("Failed to start WebDAV server: {err}");
    }
}
