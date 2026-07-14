# NACS Backend — WebDAV Server

This repository contains a minimal WebDAV server built with dav-server and Tokio.

## Requirements

- Rust (cargo) toolchain

## Default configuration

- Default listening address: `127.0.0.1:4918`
- Data directory: `./data` (created automatically)

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
cargo run
```

You can also build first and run the binary:

```bash
cargo build --release
./target/release/nacs-backend
```

## Notes

- The Basic Auth header is validated by comparing the incoming `Authorization` header to `Basic <base64(user:pass)>` computed from the environment variables.
- If you prefer a different behavior (for example: allow missing credentials, read from a config file, or support multiple users), I can update the implementation accordingly.

## License

No license specified in this repository.
