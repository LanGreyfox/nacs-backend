# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-11-07

### Changed

- Updated dependencies:
  - `axum` 0.7 → 0.8.9
  - `tower-http` 0.5 → 0.7.1
  - `libp2p` 0.56.0 → 0.57.1
  - `rusqlite` 0.40.1 → 0.41.1
  - `tokio` 1.52.3 → 1.54.1
  - `hyper` 1.10.1 → 1.12.0
  - `hyper-util` 0.1 → 0.1.21
  - `base64` 0.21 → 0.23.1
  - `crc32fast` 1.4 → 1.5.4
  - `serde` 1 → 1.0.229
  - `serde_json` 1 → 1.0.154
  - `chrono` 0.4 → 0.4.45
  - `tracing` 0.1 → 0.1.44
  - `tower` (dev-dependency) 0.5 → 0.5.3
- Migrated the REST API 404 fallback route to the axum 0.8 path syntax (`/api/{*rest}` instead of `/api/*rest`).

### Removed

- Unused `sha2` dependency (checksums have been based on CRC32 since #3).

## [0.1.0] - 2026-10-30

### Added

- WebDAV server built on `dav-server` with local filesystem storage, HTTP Basic Auth (`WEBDAV_USER` / `WEBDAV_PASS`), and unauthenticated `OPTIONS` support.
- SQLite persistence layer with a background worker storing current resources, archived resources, and the WebDAV event history.
- P2P peer discovery via libp2p (mDNS) with encrypted TCP transport (Noise + Yamux), persistent peer identity, heartbeat pings, keepalive behaviour, and automatic reconnects.
- P2P file replication protocol (`/nacs-backend/sync/1`): manifest reconciliation and serial chunked pull transfers (FIFO queue, 1 MiB chunks, CRC32 verification, up to 3 retries).
- REST monitoring API with Basic Auth: `/health` (public), `/api/v1/status`, `/api/v1/peers`, and `/api/v1/files`.
- Optional public web UI (static Vue build) served from `WEB_DIR` on the REST API port with SPA fallback.
- CI workflow for building and testing the project.
