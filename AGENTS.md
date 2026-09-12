# NACS-BACKEND WebDAV Server - Project Information for Agents 🦀

## 📌 Overview
| Attribute | Value |
|-----------|-------|
| **Language** | Rust |
| **Version** | 0.1.0 |
| **Primary Function** | WebDAV server with P2P discovery, file sync, and a monitoring REST API based on `dav-server`, `libp2p`, and `axum` |

---

## 🗂️ File Structure
```text
├── src/
│   ├── main.rs              # Binary entry point and startup wiring
│   ├── api.rs               # REST monitoring API: health, sync status, peers, and file manifest endpoints
│   ├── db.rs                # SQLite persistence layer and background worker
│   ├── webdav.rs            # WebDAV server implementation
│   ├── p2p.rs               # P2P peer discovery with libp2p and mDNS
│   ├── sync.rs              # P2P file replication protocol (serial chunked transfers)
│   └── lib.rs               # Library exports and shared functionality
├── tests/
│   ├── webdav_tests.rs      # WebDAV helper tests
│   ├── db_tests.rs          # SQLite persistence integration tests
│   ├── p2p_tests.rs         # P2P discovery and peer tests
│   └── sync_tests.rs        # P2P sync protocol tests
├── Cargo.toml               # Dependencies & package configuration
├── AGENTS.md                # Agent project information ✅
├── data/                    # WebDAV storage directory (auto-created)
├── sqlite/                  # SQLite storage directory (auto-created)
├── .gitignore               # Git ignore rules
└── target/                  # Rust build artifacts (ignore in future)
```

---

## 📦 Important Dependencies
| Package | Version | Purpose |
|---------|---------|---------|
| `dav-server` | 0.11.0 | WebDAV Server implementation |
| `tokio` | 1.52.3 | Async runtime (full features) |
| `hyper` | 1.10.1 | HTTP/Server layer |
| `libp2p` | 0.56.0 | Peer-to-peer communication and discovery |
| `rusqlite` | 0.40.1 | SQLite database support |
| `crc32fast` | 1.4 | CRC32 checksum calculation (faster on ARM/Raspberry PI) |

---

## ⚙️ Configuration Summary
```
✅ Default WebDAV Port: http://127.0.0.1:4918
✅ Default REST API Port: http://127.0.0.1:3000
✅ Default P2P Port: 4001
✅ Data Directory: ./data (auto-created)
✅ SQLite Directory: ./sqlite (auto-created)
✅ SQLite File: ./sqlite/webdav.db
✅ P2P Identity File: ./sqlite/p2p_identity.key for port 4001; ./sqlite/p2p_identity-<port>.key for other P2P ports (auto-created)
✅ Lock System: FakeLs (for simple tests)
✅ WebDAV Auth: HTTP Basic Auth with WEBDAV_USER / WEBDAV_PASS
✅ REST API Auth: same Basic Auth credentials as WebDAV, with /health publicly readable
✅ REST API Endpoints: /health, /api/v1/status, /api/v1/peers, /api/v1/files
✅ P2P Discovery: mDNS-based peer discovery
✅ P2P Transport: TCP with Noise encryption & Yamux multiplexing
✅ P2P Security: encrypted transport, but no mutual peer authentication yet
✅ P2P Sync Protocol: request-response CBOR under /nacs-backend/sync/1 (wire format unchanged and backwards compatible)
✅ P2P Sync: Serial chunked transfers - one file at a time globally, FIFO queue, 1 MiB chunks streamed; CRC32 checksum verification
✅ Sync Request Timeout: 30 minutes (1800s) for large file transfers
✅ Idle Connection Timeout: 5 minutes (300s) to allow slow chunk transfers
✅ Max Response Size: 200 MB
✅ Swarm Idle Timeout: 60 seconds
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
- [x] 60s idle timeout with explicit keepalive behaviour
- [x] Heartbeat-based peer reachability checks
- [x] Integration tests in `tests/db_tests.rs`
- [x] P2P discovery tests in `tests/p2p_tests.rs`
- [x] P2P sync protocol tests in `tests/sync_tests.rs`
- [ ] Prepared SQL statements in `src/main.rs`

---

## 🔍 Typical Optimizations (Priority Order)
1. **P2P Content Replication** – Sync files across peers via P2P network
2. **Connection Pooling** – For SQLite database, if concurrency grows beyond the current single-worker model
3. **Error Handling** – Improve error logging with context
4. **Authentication** – Add mutual P2P authentication (shared secret or certificates) and stronger access control
5. **Configuration Externalization** – With `.env` file for ports and paths
6. **Health Checks** – Implement `/health` endpoint
7. **P2P Event Broadcasting** – Notify peers of file changes in real-time

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

### REST API Bootstrapping
```rust
let api_host = std::env::var("API_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
let api_port: u16 = std::env::var("API_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(3000);
let api_addr: SocketAddr = format!("{api_host}:{api_port}").parse().expect("Invalid API_HOST/API_PORT combination");

tokio::spawn(api::run_server(api_addr, api_state));
```

The API exposes `/health`, `/api/v1/status`, `/api/v1/peers`, and `/api/v1/files` and reuses the same WebDAV credentials for protected routes.

### SQLite Event Persistence
```rust
let database = db::Database::open("./sqlite").await?;
webdav::run_server(addr, "./data", database).await?;
```

The WebDAV layer sends events to a background SQLite worker via `mpsc`.
The worker stores current resources, archived resources, and the event history.

### P2P Peer Discovery
```rust
// mDNS-based peer discovery with libp2p
pub async fn run_discovery(base_dir: impl AsRef<Path>) -> io::Result<()> {
    let local_key = load_or_create_identity(&key_path).await?;
    let local_peer_id = PeerId::from(local_key.public());
    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tcp(tcp::Config::default(), noise::Config::new, yamux::Config::default)
        .with_behaviour(|key| mdns::tokio::Behaviour::new(...))
        .build();
    // Listen for peer discovery events via `swarm.select_next_some()` loop
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
- **Idle policy:** Swarm idle timeout 60s with custom keepalive behaviour
- **Reconnect policy:** No dedicated backoff scheduler; reconnect relies on discovery/dial flow
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
| **P2P tests** | `cargo test --test p2p_tests` |
| **Sync tests** | `cargo test --test sync_tests` |
| **All tests** | `cargo test --all` |
| **Set P2P Port** | `P2P_PORT=5001 cargo run` |

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
| **Last Updated** | 2026-09-07 (added the monitoring REST API: /health, /api/v1/status, /api/v1/peers, /api/v1/files and API_HOST/API_PORT config) |
| **Status** | ✅ Active project info for future interactions |
| **P2P Status** | ✅ Peer discovery, keepalive, heartbeat, dial guards, serial chunked file sync, and REST monitoring API implemented |

---

> **Note:** All source code should be implemented and commented in English.  
> This document ensures future agents can work without re-reading the entire project. 🚀