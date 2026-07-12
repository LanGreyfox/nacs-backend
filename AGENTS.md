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
│   └── main.rs              # Main file - WebDAV server implementation
├── Cargo.toml               # Dependencies & package configuration
├── AGENTS.md                # Agent project information ✅
├── data/                    # WebDAV storage directory (auto-created)
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

---

## ⚙️ Configuration Summary
```
✅ Default Port: http://127.0.0.1:4918
✅ Data Directory: ./data (auto-created)
✅ Lock System: FakeLs (for simple tests)
```

---

## 🎯 Core Features Checklist
- [x] WebDAV HTTP Server
- [x] Local filesystem support (`LocalFs`)
- [x] Async event loop with Tokio
- [x] Auto-creation of data directories
- [ ] Prepared SQL statements in `src/main.rs`

---

## 🔍 Typical Optimizations (Priority Order)
1. **Error Handling** – Improve error logging with context
2. **Connection Pooling** – For SQLite database
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

---

## 🛠️ Common Actions for Agents

| Action | Command |
|--------|---------|
| **Build** | `cargo build --release` |
| **Run** | `cargo run` or `target/release/nacs-backend` |
| **Tests** | `cargo test` |
| **Optimize** | Code refactoring, performance tuning |

---

## ⚠️ Ignore in Future Reads
- [x] `target/` – Build artifacts
- [x] `.git/` – Version control  
- [x] `.env` – Environment variable files (if exist)

---

## 📅 Metadata
| Field | Value |
|-------|-------|
| **Created** | Automatically for agent project context |
| **Last Updated** | 2024 |
| **Status** | ✅ Active project info for future interactions |

---

> **Note:** All source code should be implemented and commented in English.  
> This document ensures future agents can work without re-reading the entire project. 🚀