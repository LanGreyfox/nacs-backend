use std::{convert::Infallible, net::SocketAddr};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use dav_server::{fakels::FakeLs, localfs::LocalFs, DavHandler};

#[tokio::main]
async fn main() {
    let dir = "./data";

    // Ensure data directory exists
    if let Err(e) = tokio::fs::create_dir_all(dir).await {
        eprintln!("Failed to create data directory '{}': {e}", dir);
        return;
    }
    let addr: SocketAddr = ([127, 0, 0, 1], 4918).into();

    let dav_server = DavHandler::builder()
        .filesystem(LocalFs::new(dir, false, false, false))
        .locksystem(FakeLs::new())
        .build_handler();

    let listener = TcpListener::bind(addr).await.unwrap();

    println!("WebDAV listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let dav_server = dav_server.clone();

        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn({
                        move |req| {
                            let dav_server = dav_server.clone();
                            async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
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
