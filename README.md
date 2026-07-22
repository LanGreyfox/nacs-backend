# NACS Backend — WebDAV Server

This repository contains a WebDAV server built with dav-server and Tokio.

The server also persists WebDAV file events to SQLite through a background worker.

## Requirements

- Rust (cargo) toolchain

## Project layout

- `src/main.rs` — binary entry point and startup wiring
- `src/webdav.rs` — WebDAV request handling, auth, and event mapping
- `src/db.rs` — SQLite persistence layer and background worker
- `tests/webdav_tests.rs` — WebDAV helper tests
- `tests/db_tests.rs` — SQLite persistence integration tests

## Default configuration

- Default listening address: `127.0.0.1:4918`
- Data directory: `./data` (created automatically)
- SQLite directory: `./sqlite` (created automatically)

The SQLite database file is stored as `./sqlite/webdav.db`.

You can override host and port with environment variables:

- `WEBDAV_HOST` — bind host/IP (default: `127.0.0.1`)
- `WEBDAV_PORT` — bind port (default: `4918`)

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

## Build & Run

Set your credentials and start the server:

```bash
export WEBDAV_USER="youruser"
export WEBDAV_PASS="yourpassword"
export WEBDAV_HOST="127.0.0.1"
export WEBDAV_PORT="4918"
cargo run
```

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
