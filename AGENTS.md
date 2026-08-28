# NACS-BACKEND WebDAV Server - Project Information for Agents 🦀

## 📌 Overview
| Attribute | Value |
|-----------|-------|
| **Language** | Rust |
| **Version** | 0.1.0 |
| **Primary Function** | WebDAV Server with P2P Discovery and file sync based on `dav-server` and `libp2p` |

---

## 🗂️ File Structure
```text
├── src/
│   ├── main.rs              # Binary entry point and startup wiring
│   ├── db.rs                # SQLite persistence layer and background worker
│   ├── webdav.rs            # WebDAV server implementation
│   ├── p2p.rs               # P2P peer discovery with libp2p and mDNS
│   ├── sync.rs              # P2P file replication protocol and reconciliation
│   └── lib.rs               # Library exports and shared functionality
├── tests/
│   ├── webdav_tests.rs      # WebDAV helper tests
│   ├── db_tests.rs          # SQLite persistence integration tests
│   └── p2p_tests.rs         # P2P discovery and peer tests
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
✅ Default P2P Port: 4001
✅ Data Directory: ./data (auto-created)
✅ SQLite Directory: ./sqlite (auto-created)
✅ SQLite File: ./sqlite/webdav.db
✅ P2P Identity File: ./sqlite/p2p_identity.key for port 4001; ./sqlite/p2p_identity-<port>.key for other P2P ports (auto-created)
✅ Lock System: FakeLs (for simple tests)
✅ WebDAV Auth: HTTP Basic Auth with WEBDAV_USER / WEBDAV_PASS
✅ P2P Discovery: mDNS-based peer discovery
✅ P2P Transport: TCP with Noise encryption & Yamux multiplexing
✅ P2P Security: encrypted transport, but no mutual peer authentication yet
✅ P2P Sync Protocol: request-response CBOR under /nacs-backend/sync/1 (wire format unchanged and backwards compatible)
✅ P2P Sync Chunking: default 4 MiB pull-based file transfers with CRC32 checksum verification, configurable via SYNC_CHUNK_SIZE_BYTES; the CBOR response limit is sized to the configured chunk plus 1 MiB protocol overhead
✅ P2P Sync Pipelining: up to SYNC_WINDOW_REQUESTS chunk requests in flight per transfer (default 4); out-of-order responses are reordered before writing; memory bound per transfer ≈ window × chunk size
✅ P2P Sync Serial Mode: optional serial execution via SYNC_SERIAL=1 (default 0); when enabled, only one file transfers at a time globally and window is forced to 1 (no pipelining)
✅ Swarm Idle Timeout: 60 seconds
✅ Heartbeat: Ping every 10s, timeout 8s
✅ Keepalive: custom behaviour keeps connections open despite ping stream keepalive opt-out
```

---

## 🎯 Core Features Checklist
- [x] WebDAV HTTP Server
- [x] Local filesystem support (`LocalFs`)
- [x] Async event loop with Tokio
- [x] Auto-creation of data directories
- [x] SQLite persistence with background worker
- [x] WebDAV Basic Auth enforcement
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
- **Chunk size:** Configurable via `SYNC_CHUNK_SIZE_BYTES`, defaults to 4194304 bytes (4 MiB); invalid values warn and fall back to default. The CBOR response limit is configured as chunk size plus 1 MiB protocol overhead.
- **Request window:** Configurable via `SYNC_WINDOW_REQUESTS`, defaults to 4 in-flight chunk requests per transfer; invalid values warn and fall back to default
- **Serial mode:** Configurable via `SYNC_SERIAL` (1/true/yes/on), defaults to false; forces single global file transfer and window=1
- **Request timeout:** 60 s per sync request; failed chunk requests are retried up to 3 times before the transfer is aborted
- **Heartbeat policy:** Ping interval 10s, timeout 8s
- **Idle policy:** Swarm idle timeout 60s with custom keepalive behaviour
- **Reconnect policy:** No dedicated backoff scheduler; reconnect relies on discovery/dial flow

---

## 🛠️ Common Actions for Agents

| Action | Command |
|--------|---------|
| **Build** | `cargo build --release` |
| **Run** | `cargo run` or `target/release/nacs-backend` |
| **Tests** | `cargo test` |
| **DB tests** | `cargo test --test db_tests` |
| **P2P tests** | `cargo test --test p2p_tests` |
| **Sync tests** | `cargo test --test sync_tests` |
| **All tests** | `cargo test --all` |
| **Set P2P Port** | `P2P_PORT=5001 cargo run` |
| **Set sync chunk size** | `SYNC_CHUNK_SIZE_BYTES=8388608 cargo run` |
| **Set sync window** | `SYNC_WINDOW_REQUESTS=8 cargo run` |
| **Enable serial sync** | `SYNC_SERIAL=1 cargo run` |

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
| **Last Updated** | 2026-08-26 (serial sync mode: SYNC_SERIAL=1 forces single global file transfer, window=1, FIFO queue; chunk size remains configurable) |
| **Status** | ✅ Active project info for future interactions |
| **P2P Status** | ✅ Peer discovery, keepalive, heartbeat, dial guards, and file sync implemented |

---

> **Note:** All source code should be implemented and commented in English.  
> This document ensures future agents can work without re-reading the entire project. 🚀