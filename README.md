# NACS Backend — WebDAV Server

This repository contains a WebDAV server built with dav-server and Tokio.

The server also persists WebDAV file events to SQLite through a background worker.

## Requirements

- Rust (cargo) toolchain

## Project layout

- `src/main.rs` — binary entry point and startup wiring
- `src/webdav.rs` — WebDAV request handling, auth, and event mapping
- `src/db.rs` — SQLite persistence layer and background worker
- `src/p2p.rs` — libp2p discovery, heartbeat, and peer connection lifecycle
- `tests/webdav_tests.rs` — WebDAV helper tests
- `tests/db_tests.rs` — SQLite persistence integration tests
- `tests/p2p_tests.rs` — P2P identity/discovery helper tests

## Default configuration

- Default listening address: `127.0.0.1:4918`
- Default P2P listening port: `4001`
- Data directory: `./data` (created automatically)
- SQLite directory: `./sqlite` (created automatically)

The SQLite database file is stored as `./sqlite/webdav.db`.
The persistent P2P identity key is stored as `./sqlite/p2p_identity.key`.

You can override host and port with environment variables:

- `WEBDAV_HOST` — bind host/IP (default: `127.0.0.1`)
- `WEBDAV_PORT` — bind port (default: `4918`)
- `P2P_PORT` — libp2p listen port (default: `4001`)

## Authentication

This server enforces HTTP Basic Authentication. Credentials are read from environment variables at startup:

- `WEBDAV_USER` — username
- `WEBDAV_PASS` — password

The server expects the environment variables to be set; it will exit with an error if they are missing.

The server also allows unauthenticated `OPTIONS` requests, while all other WebDAV methods require Basic Authentication.

## Persistence

WebDAV file events are written to SQLite in the background after a request has been handled.

The database stores:

- the current resource state in `resources`
- deleted or replaced resources in `resource_archive`
- the append-only event history in `events`

Each stored record includes the current folder, whether the resource is a file or folder, and a checksum for files.

## P2P behavior

- Discovery uses mDNS on the local network.
- Transport uses TCP with Noise encryption and Yamux multiplexing.
- Swarm idle timeout is set to 60 seconds.
- Ping heartbeat runs every 10 seconds with an 8-second timeout.
- A custom keepalive behaviour is enabled so established connections stay open even though ping streams are excluded from keepalive in this libp2p version.

Current failure/reconnect semantics:

- On heartbeat failure, the peer is removed from active peer tracking immediately.
- There is no dedicated reconnect backoff scheduler at the moment.
- Reconnect attempts happen through the normal discovery/dial flow when peers are discovered again.

## Build & Run

Set your credentials and start the server:

```bash
export WEBDAV_USER="youruser"
export WEBDAV_PASS="yourpassword"
export WEBDAV_HOST="127.0.0.1"
export WEBDAV_PORT="4918"
export P2P_PORT="4001"
cargo run
```

If `P2P_PORT` is not set, the application listens on `4001`.

## Multi-instance example (3 nodes)

Start three instances in separate terminals so each node has unique WebDAV and P2P ports:

Terminal 1:

```bash
WEBDAV_USER="youruser" WEBDAV_PASS="yourpassword" WEBDAV_HOST="127.0.0.1" WEBDAV_PORT="4918" P2P_PORT="4001" cargo run
```

Terminal 2:

```bash
WEBDAV_USER="youruser" WEBDAV_PASS="yourpassword" WEBDAV_HOST="127.0.0.1" WEBDAV_PORT="4919" P2P_PORT="4002" cargo run
```

Terminal 3:

```bash
WEBDAV_USER="youruser" WEBDAV_PASS="yourpassword" WEBDAV_HOST="127.0.0.1" WEBDAV_PORT="4920" P2P_PORT="4003" cargo run
```

Expected behavior:

- Every instance logs its own peer ID (`p2p node started: ...`).
- Every instance logs discovered peers (`new node discovered: <peer_id>`).
- With one single instance running, discovery stays idle without errors (0 neighbors).

On first start the application creates `./sqlite/webdav.db` automatically.
On later starts, the existing database is reused.

You can also build first and run the binary:

```bash
cargo build --release
./target/release/nacs-backend
```

Linux Dolphin example URL:

- `webdav://127.0.0.1:4918/`
- If `localhost` resolves to IPv6 first in your environment, prefer `127.0.0.1` unless you bind `WEBDAV_HOST` to an IPv6 address.

## Notes

- The Basic Auth header is parsed as standard HTTP Basic Auth (`Authorization: Basic <base64(user:pass)>`) and then compared to `WEBDAV_USER`/`WEBDAV_PASS`.
- SQLite writes are handled by a dedicated background worker, so the WebDAV request path stays responsive.
- Checksum values are computed for files and stored in the database.
- If you prefer a different behavior (for example: allow missing credentials, read from a config file, or support multiple users), I can update the implementation accordingly.

## License

No license specified in this repository.
