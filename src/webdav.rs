use std::{convert::Infallible, io, net::SocketAddr, path::Path};

use dav_server::{fakels::FakeLs, localfs::LocalFs, DavHandler};
use hyper::{server::conn::http1, service::service_fn};
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

pub async fn run_server(addr: SocketAddr, dir: impl AsRef<Path>) -> io::Result<()> {
    ensure_data_dir(dir.as_ref()).await?;

    let dav_server = build_handler(dir.as_ref());
    let listener = TcpListener::bind(addr).await?;

    println!("WebDAV listening on http://{addr}");

    loop {
        let (stream, _) = listener.accept().await?;
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
