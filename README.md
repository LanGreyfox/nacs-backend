# NACS Backend — WebDAV Server

This repository contains a minimal WebDAV server built with dav-server and Tokio.

## Requirements

- Rust (cargo) toolchain

## Default configuration

- Default listening address: `127.0.0.1:4918`
- Data directory: `./data` (created automatically)

You can override host and port with environment variables:

- `WEBDAV_HOST` — bind host/IP (default: `127.0.0.1`)
- `WEBDAV_PORT` — bind port (default: `4918`)

## Authentication

This server enforces HTTP Basic Authentication. Credentials are read from environment variables at startup:

- `WEBDAV_USER` — username
- `WEBDAV_PASS` — password

The server expects the environment variables to be set; it will exit with an error if they are missing.

## Build & Run

Set your credentials and start the server:

```bash
export WEBDAV_USER="youruser"
export WEBDAV_PASS="yourpassword"
export WEBDAV_HOST="127.0.0.1"
export WEBDAV_PORT="4918"
cargo run
```

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
- If you prefer a different behavior (for example: allow missing credentials, read from a config file, or support multiple users), I can update the implementation accordingly.

## License

No license specified in this repository.
