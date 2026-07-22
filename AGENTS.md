# NACS-BACKEND WebDAV Server - Project Information for Agents 🦀

## 📌 Overview
| Attribute | Value |
|-----------|-------|
| **Language** | Rust |
| **Version** | 0.1.0 |
| **Primary Function** | WebDAV Server based on `dav-server` framework |

---

## 🗂️ File Structure
```text
├── src/
│   ├── main.rs              # Binary entry point and startup wiring
│   ├── db.rs                # SQLite persistence layer and background worker
│   └── webdav.rs            # WebDAV server implementation
├── tests/
│   ├── webdav_tests.rs      # WebDAV helper tests
│   └── db_tests.rs          # SQLite persistence integration tests
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
| `libp2p` | 0.56.0 | Peer-to-peer communication (optional) |
| `rusqlite` | 0.40.1 | SQLite database support |
| `sha2` | 0.10 | SHA-256 checksum calculation |

---

## ⚙️ Configuration Summary
```
✅ Default Port: http://127.0.0.1:4918
✅ Data Directory: ./data (auto-created)
✅ SQLite Directory: ./sqlite (auto-created)
✅ SQLite File: ./sqlite/webdav.db
✅ Lock System: FakeLs (for simple tests)
```

---

## 🎯 Core Features Checklist
- [x] WebDAV HTTP Server
- [x] Local filesystem support (`LocalFs`)
- [x] Async event loop with Tokio
- [x] Auto-creation of data directories
- [x] SQLite persistence with background worker
- [x] Integration tests in `tests/db_tests.rs`
- [ ] Prepared SQL statements in `src/main.rs`

---

## 🔍 Typical Optimizations (Priority Order)
1. **Error Handling** – Improve error logging with context
2. **Connection Pooling** – For SQLite database, if concurrency grows beyond the current single-worker model
3. **Authentication** – Add JWT or Basic Auth
4. **Configuration Externalization** – With `.env` file
5. **Health Checks** – Implement `/health` endpoint

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

---

## 🛠️ Common Actions for Agents

| Action | Command |
|--------|---------|
| **Build** | `cargo build --release` |
| **Run** | `cargo run` or `target/release/nacs-backend` |
| **Tests** | `cargo test` |
| **DB tests** | `cargo test --test db_tests` |
| **Optimize** | Code refactoring, performance tuning |

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
| **Last Updated** | 2026 |
| **Status** | ✅ Active project info for future interactions |

---

> **Note:** All source code should be implemented and commented in English.  
> This document ensures future agents can work without re-reading the entire project. 🚀