# NACS-BACKEND WebDAV Server - Project Information for Agents 🦀

## 📌 Overview
| Attribute | Value |
|-----------|-------|
| **Language** | Rust |
| **Version** | 0.1.1 (see CHANGELOG.md; Cargo.toml still declares 0.1.0) |
| **Primary Function** | WebDAV server with P2P discovery, file sync, and a monitoring REST API based on `dav-server`, `libp2p`, and `axum` |

---

## 🗂️ File Structure
```text
├── src/
│   ├── main.rs              # Binary entry point and startup wiring
│   ├── api.rs               # REST monitoring API: health, sync status, peers, and file manifest endpoints
│   ├── auth.rs              # Shared HTTP Basic Auth parsing/validation (WebDAV + REST API)
│   ├── db.rs                # SQLite persistence layer and background worker
│   ├── webdav.rs            # WebDAV server implementation and semantic FileEvent classification
│   ├── p2p.rs               # P2P peer discovery with libp2p and mDNS
│   ├── sync.rs              # P2P file replication protocol (serial chunked transfers)
│   └── lib.rs               # Library exports and shared functionality
├── web/                     # Static Vue build served by the REST API
├── tests/
│   ├── webdav_tests.rs      # WebDAV helper tests
│   ├── db_tests.rs          # SQLite persistence integration tests
│   ├── api_tests.rs         # REST API endpoint tests (axum router tests)
│   ├── p2p_tests.rs         # P2P discovery and peer tests
│   └── sync_tests.rs        # P2P sync protocol tests
├── Cargo.toml               # Dependencies & package configuration
├── Cargo.lock               # Pinned dependency lockfile
├── AGENTS.md                # Agent project information ✅
├── README.md                # User-facing project documentation
├── CHANGELOG.md             # Keep a Changelog history (0.1.0 / 0.1.1)
├── .github/workflows/ci.yml # CI: fmt check, clippy -D warnings, release build, cargo test --all
├── data/                    # WebDAV storage directory (auto-created)
├── sqlite/                  # SQLite storage directory (auto-created)
├── .gitignore               # Git ignore rules
└── target/                  # Rust build artifacts (ignore in future)
```

---

## 📦 Important Dependencies
| Package | Version | Purpose |
|---------|---------|---------|
| `dav-server` | 0.11.0 | WebDAV Server implementation (LocalFs, FakeLs, DavHandler) |
| `tokio` | 1.54.1 | Async runtime (full features) |
| `hyper` / `hyper-util` | 1.12.0 / 0.1.21 | HTTP/1.1 server layer for the WebDAV listener |
| `axum` | 0.8.9 | REST API routing (path syntax `/api/{*rest}`) |
| `tower-http` | 0.7.1 | API middleware: auth, CORS, static file serving (`ServeDir`/`ServeFile`), tracing |
| `tower` (dev-dep) | 0.5.3 | `Oneshot` stack used by REST API tests |
| `libp2p` | 0.57.1 | P2P networking (mdns, ping, request-response CBOR, noise, yamux, tcp, tokio) |
| `rusqlite` | 0.41.1 | SQLite database support (bundled background worker) |
| `crc32fast` | 1.5.4 | CRC32 checksum calculation (faster on ARM/Raspberry PI) |
| `base64` | 0.23.1 | Basic Auth credential decoding |
| `serde` / `serde_json` | 1.0.229 / 1.0.154 | Serialization (web UI status payloads, JSON handling) |
| `chrono` | 0.4.45 | Timestamp parsing/formatting (RFC 3339) |
| `tracing` | 0.1.44 | Structured logging (API layer) |

---

## ⚙️ Configuration Summary
```
✅ Default WebDAV Port: http://127.0.0.1:4918 (override via WEBDAV_HOST / WEBDAV_PORT)
✅ Default REST API Port: http://127.0.0.1:3000 (override via API_HOST / API_PORT)
✅ Default P2P Port: 4001 (override via P2P_PORT)
✅ Data Directory: ./data (auto-created)
✅ SQLite Directory: ./sqlite (auto-created)
✅ SQLite File: ./sqlite/webdav.db
✅ P2P Identity File: ./sqlite/p2p_identity.key for port 4001; ./sqlite/p2p_identity-<port>.key for other P2P ports (auto-created)
✅ Required Environment Variables: WEBDAV_USER / WEBDAV_PASS (startup aborts if either is missing)
✅ Lock System: FakeLs (for simple tests)
✅ WebDAV Auth: HTTP Basic Auth with WEBDAV_USER / WEBDAV_PASS, shared auth helpers in src/auth.rs; OPTIONS is unauthenticated
✅ REST API Auth: same Basic Auth credentials as WebDAV, with /health publicly readable; CORS allows any origin/method
✅ REST API Endpoints: /health, /api/v1/status, /api/v1/peers (paginated query), /api/v1/files
✅ Web UI: static Vue build served from WEB_DIR (default ./web; place built files including index.html there) on the REST API port, public, with SPA fallback; disabled if index.html is missing
✅ P2P Discovery: mDNS-based peer discovery (query interval ~15s, advertisement TTL 60s)
✅ P2P Transport: TCP with Noise encryption & Yamux multiplexing
✅ P2P Security: encrypted transport, but no mutual peer authentication yet
✅ P2P Sync Protocol: request-response CBOR under /nacs-backend/sync/1 (wire format unchanged and backwards compatible)
✅ P2P Sync: Serial chunked transfers - one file at a time globally, FIFO queue, 1 MiB chunks streamed; CRC32 checksum verification
✅ Sync Request Timeout: 30 minutes (1800s) for large file transfers
✅ Idle Connection Timeout: 5 minutes (300s) to allow slow chunk transfers
✅ Max Response Size: 200 MB
✅ Swarm Idle Timeout: 300 seconds
✅ Heartbeat: Ping every 10s, timeout 8s
✅ Keepalive: custom behaviour keeps connections open despite ping stream keepalive opt-out
```

---

## 🎯 Core Features Checklist
- [x] WebDAV HTTP Server
- [x] REST monitoring API with axum
- [x] Local filesystem support (`LocalFs`)
- [x] Async event loop with Tokio
- [x] Auto-creation of data directories
- [x] SQLite persistence with background worker
- [x] WebDAV Basic Auth enforcement
- [x] REST API Basic Auth enforcement for protected routes with public /health endpoint
- [x] P2P peer discovery with libp2p (mDNS)
- [x] Encrypted P2P transport (Noise + Yamux)
- [x] P2P identity management and persistence
- [x] P2P file replication and manifest reconciliation
- [x] 300s idle timeout with explicit keepalive behaviour
- [x] Heartbeat-based peer reachability checks
- [x] Integration tests in `tests/db_tests.rs`
- [x] P2P discovery tests in `tests/p2p_tests.rs`
- [x] P2P sync protocol tests in `tests/sync_tests.rs`
- [x] REST API tests in `tests/api_tests.rs` (router tests via `tower::Oneshot`)
- [ ] Prepared SQL statements in `src/main.rs` (DB worker currently re-prepares in `build_manifest`)

---

## 🔍 Typical Optimizations (Priority Order)
1. ~~P2P Content Replication~~ ✅ Done – file sync across peers via P2P network (0.1.0)
2. **Connection Pooling / prepared statements** – SQLite worker currently re-prepares statements in `build_manifest`
3. **Error Handling** – Improve error logging with context (`tracing` is set up for the API layer only)
4. **Authentication** – Add mutual P2P authentication (shared secret or certificates) and stronger access control
5. **Configuration Externalization** – With `.env` file for ports and paths
6. ~~Health Checks~~ ✅ Done – public `/health` endpoint on the REST API
7. ~~P2P Event Broadcasting~~ ✅ Done – WebDAV events are broadcast to connected peers via the `P2pHandle`

---

## 📚 Key Code Structures

### WebDAV Handler Setup
```rust
// Core WebDAV Handler setup:
let dav_server = DavHandler::builder()
    .filesystem(LocalFs::new(dir, false, false, false))
    .locksystem(FakeLs::new())
    .build_handler();
```

### Binary Entry Point (`main.rs`)
```rust
let database = db::Database::open(sqlite_dir, dir).await?;
let (p2p_handle, p2p_rx) = sync::P2pHandle::channel();
let (p2p_query_tx, p2p_query_rx) = tokio::sync::mpsc::channel(32);

// P2P worker: owns the swarm, consumes WebDAV events and API queries
tokio::spawn(p2p::run_discovery(sqlite_dir, dir, database, p2p_rx, p2p_query_rx));

// REST API + optional web UI (runs concurrently with the WebDAV server)
let api_state = api::ApiState { database, p2p_query_tx, auth_user, auth_pass };
tokio::spawn(api::run_server(api_addr, api_state, web_dir));

// WebDAV server runs in the foreground and returns on shutdown
webdav::run_server(addr, dir, database, p2p_handle).await
```

### REST API Bootstrapping
```rust
let api_host = std::env::var("API_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
let api_port: u16 = std::env::var("API_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
let api_addr: SocketAddr = format!("{api_host}:{api_port}").parse().expect("Invalid API_HOST/API_PORT combination");

let web_dir = std::path::PathBuf::from(
    std::env::var("WEB_DIR").unwrap_or_else(|_| "./web".to_string()),
);
let web_dir = web_dir.join("index.html").is_file().then_some(web_dir);
tokio::spawn(api::run_server(api_addr, api_state, web_dir));
```

The API exposes `/health`, `/api/v1/status`, `/api/v1/peers`, and `/api/v1/files` and reuses the same WebDAV credentials for protected routes. The optional public web UI is served on this listener; unknown client-side routes fall back to `index.html`, while unknown `/api/...` GET routes return 404.

### SQLite Event Persistence
```rust
let database = db::Database::open("./sqlite", "./data").await?;
let (p2p_handle, p2p_rx) = sync::P2pHandle::channel();
webdav::run_server(addr, "./data", database, p2p_handle).await?;
```

The WebDAV layer sends events to a background SQLite worker via `mpsc`.
The worker stores current resources, archived resources, and the event history.
`webdav::run_server` classifies each request method + response status into a semantic `FileEvent`
(Created, Edited, Deleted, Renamed, Moved, Copied, DirCreated, PropPatched, Locked, Unlocked, Read, Listed, Options)
and additionally announces it to the P2P layer via the `P2pHandle`.

### P2P Peer Discovery
```rust
// mDNS-based peer discovery with libp2p
pub async fn run_discovery(
    base_dir: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    database: Database,
    announce_rx: mpsc::UnboundedReceiver<FileChangeEvent>,
    query_rx: mpsc::Receiver<P2pQuery>,
) -> io::Result<()> {
    let local_key = load_or_create_identity(&key_path).await?;
    let local_peer_id = PeerId::from(local_key.public());
    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)
        .with_behaviour(|key| DiscoveryBehaviour { keep_alive, mdns, ping, sync })
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
        .build();
    // Event loop: `tokio::select!` { 5s reconnect tick ..., swarm.select_next_some() => match event }
}
```

**P2P Features:**
- **Transport:** TCP with Noise encryption + Yamux multiplexing
- **Authentication:** encrypted transport is enabled, but mutual peer authentication is not implemented yet
- **Discovery:** mDNS for automatic peer detection on LAN
- **Identity:** Persistent peer identity stored in `./sqlite/p2p_identity.key`
- **Port:** Configurable via `P2P_PORT` env var, defaults to 4001
- **Sync mode:** Serial chunked - one file at a time globally, FIFO queue, 1 MiB chunks streamed; CRC32 checksum verification
- **Request timeout:** 30 minutes (1800s) for large file transfers
- **Idle connection timeout:** 5 minutes (300s) to allow slow chunk transfers
- **Max response size:** 200 MB
- **Heartbeat policy:** Ping interval 10s, timeout 8s
- **Idle policy:** Swarm idle timeout 300s with custom keepalive behaviour
- **Reconnect policy:** New discoveries are dialed immediately; every 5 seconds, peers with at least one valid mDNS address that are neither connected nor already dialing are retried. mDNS queries run approximately every 15 seconds, and local advertisements use a 60-second record TTL. Heartbeat failures close the affected connection; a peer is disconnected only when its last connection closes.
- **Retry logic:** Up to 3 retries per transfer on checksum mismatch or failure before moving to next queued file

---

## 🛠️ Common Actions for Agents

| Action | Command |
|--------|---------|
| **Build** | `cargo build --release` |
| **Run** | `cargo run` or `target/release/nacs-backend` |
| **API port override** | `API_PORT=3100 cargo run` |
| **Tests** | `cargo test` |
| **DB tests** | `cargo test --test db_tests` |
| **API tests** | `cargo test --test api_tests` |
| **P2P tests** | `cargo test --test p2p_tests` |
| **Sync tests** | `cargo test --test sync_tests` |
| **WebDAV tests** | `cargo test --test webdav_tests` |
| **All tests** | `cargo test --all` |
| **Format check** | `cargo fmt --all -- --check` (part of CI) |
| **Lint (as in CI)** | `cargo clippy --all-targets --all-features -- -D warnings` |
| **Set P2P Port** | `P2P_PORT=5001 cargo run` |
| **Set WebDAV host/port** | `WEBDAV_HOST=0.0.0.0 WEBDAV_PORT=4918 cargo run` |
| **Set Web UI directory** | `WEB_DIR=/path/to/web cargo run` |

---

## ⚠️ Ignore in Future Reads
- [x] `target/` – Build artifacts
- [x] `.git/` – Version control  
- [x] `.env` – Environment variable files (if exist)
- [x] `sqlite/` – SQLite storage directory and database file

---

## 📅 Metadata
| Field | Value |
|-------|-------|
| **Created** | Automatically for agent project context |
| **Last Updated** | 2026-11-07 (v0.1.1: dependency refresh — axum 0.8, tower-http 0.7, libp2p 0.57, rusqlite 0.41; documented `src/auth.rs`, `tests/api_tests.rs`, CHANGELOG.md, and CI workflow; corrected code snippets, env vars, and reconnect/heartbeat policy notes) |
| **Status** | ✅ Active project info for future interactions |
| **P2P Status** | ✅ Peer discovery, keepalive, heartbeat, dial guards, reconnect sweep, serial chunked file sync, and REST monitoring API implemented |

---

> **Note:** All source code should be implemented and commented in English.  
> This document ensures future agents can work without re-reading the entire project. 🚀