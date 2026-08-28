//! Peer-to-peer file replication protocol.
//!
//! This module defines the wire messages exchanged between nodes over the
//! libp2p `request-response` behaviour (see [`crate::p2p`]), the local
//! channel used by the WebDAV layer to announce changes to the p2p worker,
//! and the (pure, network-independent) reconciliation logic used to decide
//! what to pull/delete when a peer's manifest is received.
//!
//! Content is never pushed eagerly: an [`Event`](SyncRequest::Event) push
//! notification only carries metadata. Receivers pull the actual bytes in
//! fixed-size chunks via [`SyncRequest::FetchFile`], which keeps memory
//! bounded to one chunk regardless of file size.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    env, io,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use crc32fast::Hasher;
use libp2p::PeerId;
use tokio::{
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, mpsc},
};

use crate::db::{self, Database, EventEnvelope, EventKind};

#[derive(Debug, Clone)]
struct StartPullParams<'a> {
    data_dir: &'a Path,
    fetch_queue: &'a FetchQueue,
    peer: PeerId,
    resource_path: String,
    expected_checksum: Option<String>,
    event_kind: EventKind,
    destination_path: Option<String>,
    username: String,
}

/// Maximum number of bytes transferred per chunk request/response round trip.
///
/// Note: the libp2p CBOR codec limits responses to 10 MiB by default, so
/// values above ~10 MiB require raising the codec's response size limit via
/// `Behaviour::with_codec(... .set_response_size_maximum(...))`.
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;
const SYNC_CHUNK_SIZE_ENV: &str = "SYNC_CHUNK_SIZE_BYTES";

/// Number of chunk requests kept in flight per in-progress transfer.
pub const DEFAULT_WINDOW_REQUESTS: usize = 4;
const SYNC_WINDOW_REQUESTS_ENV: &str = "SYNC_WINDOW_REQUESTS";

/// Enable serial sync mode: only one file transfer at a time globally,
/// and one chunk at a time per transfer (window = 1).
const SYNC_SERIAL_ENV: &str = "SYNC_SERIAL";

/// Number of file handles cached on the sending side to avoid re-opening the
/// same file for every chunk request.
const CHUNK_READER_CACHE_CAPACITY: usize = 8;

/// Returns the configured per-chunk transfer size in bytes.
///
/// This value is resolved once from `SYNC_CHUNK_SIZE_BYTES` at runtime and
/// then cached for the process lifetime.
pub fn configured_chunk_size() -> usize {
    static CHUNK_SIZE: OnceLock<usize> = OnceLock::new();

    *CHUNK_SIZE.get_or_init(|| {
        let chunk_size = resolve_chunk_size_from_env(env::var(SYNC_CHUNK_SIZE_ENV));
        println!("sync: configured chunk size = {chunk_size} bytes ({SYNC_CHUNK_SIZE_ENV})");
        chunk_size
    })
}

/// Returns the configured number of in-flight chunk requests per transfer.
///
/// Resolved once from `SYNC_WINDOW_REQUESTS` and cached for the process
/// lifetime. The memory bound per transfer is roughly
/// window x chunk size (see [`configured_chunk_size`]).
pub fn configured_window_requests() -> usize {
    static WINDOW_REQUESTS: OnceLock<usize> = OnceLock::new();

    *WINDOW_REQUESTS.get_or_init(|| {
        let window = resolve_window_requests_from_env(env::var(SYNC_WINDOW_REQUESTS_ENV));
        println!(
            "sync: configured window = {window} in-flight requests ({SYNC_WINDOW_REQUESTS_ENV})"
        );
        window
    })
}

/// Returns whether serial sync mode is enabled.
///
/// In serial mode, only one file transfer happens at a time globally,
/// and the request window is forced to 1 (no pipelining).
/// Resolved once from `SYNC_SERIAL` at startup and cached.
pub fn configured_serial_mode() -> bool {
    static SERIAL_MODE: OnceLock<bool> = OnceLock::new();

    *SERIAL_MODE.get_or_init(|| {
        let serial = match env::var(SYNC_SERIAL_ENV) {
            Ok(value) => matches!(value.as_str(), "1" | "true" | "yes" | "on"),
            Err(env::VarError::NotPresent) => false,
            Err(err) => {
                eprintln!(
                    "sync: unable to read {SYNC_SERIAL_ENV} ({err}); defaulting to false"
                );
                false
            }
        };
        println!("sync: serial mode = {serial} ({SYNC_SERIAL_ENV})");
        serial
    })
}

#[doc(hidden)]
pub fn resolve_window_requests_from_env(raw: Result<String, env::VarError>) -> usize {
    match raw {
        Ok(value) => match value.parse::<usize>() {
            Ok(0) => {
                eprintln!(
                    "sync: {SYNC_WINDOW_REQUESTS_ENV}=0 is invalid; using default {DEFAULT_WINDOW_REQUESTS}"
                );
                DEFAULT_WINDOW_REQUESTS
            }
            Ok(size) => size,
            Err(_) => {
                eprintln!(
                    "sync: invalid {SYNC_WINDOW_REQUESTS_ENV} value '{value}'; using default {DEFAULT_WINDOW_REQUESTS}"
                );
                DEFAULT_WINDOW_REQUESTS
            }
        },
        Err(env::VarError::NotPresent) => DEFAULT_WINDOW_REQUESTS,
        Err(err) => {
            eprintln!(
                "sync: unable to read {SYNC_WINDOW_REQUESTS_ENV} ({err}); using default {DEFAULT_WINDOW_REQUESTS}"
            );
            DEFAULT_WINDOW_REQUESTS
        }
    }
}

#[doc(hidden)]
pub fn resolve_chunk_size_from_env(raw: Result<String, env::VarError>) -> usize {
    match raw {
        Ok(value) => match value.parse::<usize>() {
            Ok(0) => {
                eprintln!(
                    "sync: {SYNC_CHUNK_SIZE_ENV}=0 is invalid; using default {DEFAULT_CHUNK_SIZE} bytes"
                );
                DEFAULT_CHUNK_SIZE
            }
            Ok(size) => size,
            Err(_) => {
                eprintln!(
                    "sync: invalid {SYNC_CHUNK_SIZE_ENV} value '{value}'; using default {DEFAULT_CHUNK_SIZE} bytes"
                );
                DEFAULT_CHUNK_SIZE
            }
        },
        Err(env::VarError::NotPresent) => DEFAULT_CHUNK_SIZE,
        Err(err) => {
            eprintln!(
                "sync: unable to read {SYNC_CHUNK_SIZE_ENV} ({err}); using default {DEFAULT_CHUNK_SIZE} bytes"
            );
            DEFAULT_CHUNK_SIZE
        }
    }
}

#[doc(hidden)]
pub fn resolve_serial_mode_from_env(raw: Result<String, env::VarError>) -> bool {
    match raw {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "yes" | "on"),
        Err(env::VarError::NotPresent) => false,
        Err(err) => {
            eprintln!(
                "sync: unable to read {SYNC_SERIAL_ENV} ({err}); defaulting to false"
            );
            false
        }
    }
}

/// A lightweight, content-free description of a change that happened
/// locally. Used both as the local channel message from the WebDAV layer to
/// the p2p worker, and as the wire payload of [`SyncRequest::Event`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileChangeEvent {
    pub event_kind: EventKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub checksum: Option<String>,
    pub size: u64,
    pub username: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SyncRequest {
    /// Ask the peer for its full resource/tombstone manifest.
    Manifest,
    /// Ask the peer for one chunk of a file, starting at `offset`.
    FetchFile { path: String, offset: u64 },
    /// Notify the peer that a local change happened; carries no content.
    Event(FileChangeEvent),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SyncResponse {
    Manifest(db::Manifest),
    Chunk {
        path: String,
        data: Vec<u8>,
        offset: u64,
        total_size: u64,
        is_last: bool,
    },
    NotFound {
        path: String,
    },
    Ack,
}

/// Handle used by the WebDAV layer to announce locally-originated changes to
/// the p2p worker, which then broadcasts them to connected peers.
#[derive(Clone, Debug)]
pub struct P2pHandle {
    tx: mpsc::UnboundedSender<FileChangeEvent>,
}

impl P2pHandle {
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<FileChangeEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Queues an event for broadcast to all currently connected peers.
    /// Silently drops the event if the p2p worker has shut down.
    pub fn announce(&self, event: FileChangeEvent) {
        let _ = self.tx.send(event);
    }
}

/// An action to take locally in response to a peer's manifest being newer
/// for a given path than the local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    /// Pull file content for `path` (the peer has a live version that's newer).
    Pull {
        path: String,
        checksum: Option<String>,
    },
    /// Create an (empty) directory; the peer has a live folder we're missing.
    CreateDir { path: String },
    /// Delete a locally-present path; the peer's tombstone is newer.
    Delete { path: String },
}

/// Queued initial fetch requests for a transfer that has just been started.
/// The pull-side handlers (`start_pull`, `handle_incoming_event`,
/// `apply_manifest_actions`) cannot push requests themselves, so the requests
/// are collected here and drained by the caller in `p2p.rs`, which sends them
/// to the peer (one per iteration of the swarm loop).
#[derive(Debug, Clone)]
pub struct FetchQueue {
    inner: Arc<Mutex<Vec<SyncRequest>>>,
}

impl FetchQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn push(&self, request: SyncRequest) {
        self.inner.lock().await.push(request);
    }

    pub async fn pop(&self) -> Option<SyncRequest> {
        self.inner.lock().await.pop()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
    }
}

impl Default for FetchQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for a queued pull operation, stored when serial mode is active
/// and another transfer is already in progress.
#[derive(Debug, Clone)]
struct QueuedPullParams {
    data_dir: PathBuf,
    fetch_queue: FetchQueue,
    peer: PeerId,
    resource_path: String,
    expected_checksum: Option<String>,
    event_kind: EventKind,
    destination_path: Option<String>,
    username: String,
}

#[derive(Debug, Clone)]
enum PathState {
    Live {
        resource_kind: String,
        checksum: Option<String>,
    },
    Deleted,
}

fn latest_states(manifest: &db::Manifest) -> HashMap<String, (String, PathState)> {
    let mut map = HashMap::new();
    for entry in &manifest.resources {
        map.insert(
            entry.resource_path.clone(),
            (
                entry.updated_at.clone(),
                PathState::Live {
                    resource_kind: entry.resource_kind.clone(),
                    checksum: entry.checksum.clone(),
                },
            ),
        );
    }
    for tombstone in &manifest.tombstones {
        map.insert(
            tombstone.resource_path.clone(),
            (tombstone.deleted_at.clone(), PathState::Deleted),
        );
    }
    map
}

fn normalize_wire_path(path: &str) -> String {
    let path = if let Some(after) = path.strip_prefix("http://") {
        after.find('/').map(|i| &after[i..]).unwrap_or("/")
    } else if let Some(after) = path.strip_prefix("https://") {
        after.find('/').map(|i| &after[i..]).unwrap_or("/")
    } else {
        path
    };

    let path = path.split(['?', '#']).next().unwrap_or("/");
    let path = percent_decode_lossy(path);

    if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    }
}

fn percent_decode_lossy(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_value(bytes[i + 1]);
            let lo = hex_value(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn fs_path_from_wire_path(data_dir: &Path, path: &str) -> PathBuf {
    let decoded = percent_decode_lossy(path);
    let rel = decoded.trim_start_matches('/');
    data_dir.join(rel)
}

/// Compares two manifests and returns the actions the *local* node should
/// take. Only considers paths where the remote's latest known state (live
/// resource or tombstone) is newer than the local one — the symmetric
/// exchange (both sides request each other's manifest on connect) handles
/// the reverse direction independently. Pure and network-independent.
pub fn diff_manifests(local: &db::Manifest, remote: &db::Manifest) -> Vec<SyncAction> {
    let local_states = latest_states(local);
    let remote_states = latest_states(remote);

    let mut actions = Vec::new();
    for (path, (remote_ts, remote_state)) in &remote_states {
        let local_state = local_states.get(path);

        // If both sides already report the same file checksum, skip timestamp-based
        // reconciliation to prevent repeated transfers caused by clock/mtime drift.
        if let (
            PathState::Live {
                resource_kind: remote_kind,
                checksum: remote_checksum,
            },
            Some((
                _,
                PathState::Live {
                    resource_kind: local_kind,
                    checksum: local_checksum,
                },
            )),
        ) = (remote_state, local_state)
            && remote_kind == "file"
            && local_kind == "file"
            && remote_checksum.is_some()
            && remote_checksum == local_checksum
        {
            continue;
        }

        let is_remote_newer = match local_state {
            Some((local_ts, _)) => remote_ts > local_ts,
            None => true,
        };

        if !is_remote_newer {
            continue;
        }

        match remote_state {
            PathState::Live {
                resource_kind,
                checksum,
            } if resource_kind == "folder" => {
                actions.push(SyncAction::CreateDir { path: path.clone() });
                let _ = checksum;
            }
            PathState::Live { checksum, .. } => {
                actions.push(SyncAction::Pull {
                    path: path.clone(),
                    checksum: checksum.clone(),
                });
            }
            PathState::Deleted => {
                if let Some((_, PathState::Live { .. })) = local_states.get(path) {
                    actions.push(SyncAction::Delete { path: path.clone() });
                }
            }
        }
    }

    actions
}

/// State for a single in-flight chunked file download from a peer.
///
/// Up to [`configured_window_requests`] chunk requests are kept in flight at
/// the same time. Responses may arrive out of order (separate substreams), so
/// chunks are staged in `buffered` and only written to disk in offset order,
/// which also lets us hash incrementally while writing.
struct PendingTransfer {
    tmp_path: PathBuf,
    final_path: PathBuf,
    resource_path: String,
    file: tokio::fs::File,
    expected_checksum: Option<String>,
    event_kind: EventKind,
    destination_path: Option<String>,
    username: String,
    /// Total file size, learned from the first chunk response.
    total_size: Option<u64>,
    /// Offset up to which fetch requests have been issued.
    next_request_offset: u64,
    /// Number of chunk requests currently awaiting a response.
    in_flight: usize,
    /// Next offset to write to disk.
    write_offset: u64,
    /// Last offset at which progress was logged.
    last_log_offset: u64,
    /// Chunks that arrived ahead of their position, keyed by offset.
    buffered: BTreeMap<u64, Vec<u8>>,
    /// Rolling checksum over the bytes written so far.
    hasher: Hasher,
    /// When the last chunk was received or a request was sent, for
    /// stalled-transfer detection.
    last_activity: std::time::Instant,
}

/// Tracks in-flight chunked downloads, keyed by (peer, path), so multiple
/// concurrent pulls (from different triggers) never duplicate work.
#[derive(Default)]
pub struct SyncState {
    pending: HashMap<(PeerId, String), PendingTransfer>,
    /// Serial mode: currently active transfer (peer, path).
    current_transfer: Option<(PeerId, String)>,
    /// Serial mode: FIFO queue of pulls waiting for the current transfer to finish.
    queued_pulls: VecDeque<QueuedPullParams>,
}

impl SyncState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_pending(&self, peer: PeerId, path: &str) -> bool {
        self.pending.contains_key(&(peer, path.to_string()))
    }

    /// Cancels every in-flight transfer owned by `peer` and removes any temp
    /// files left behind by partially received chunks.
    pub async fn cancel_peer(&mut self, peer: PeerId) -> io::Result<()> {
        let paths: Vec<String> = self
            .pending
            .keys()
            .filter(|(pending_peer, _)| *pending_peer == peer)
            .map(|(_, path)| path.clone())
            .collect();

        let was_current = configured_serial_mode()
            && self.current_transfer.as_ref().is_some_and(|(p, _)| *p == peer);

        for path in paths {
            self.cancel(peer, &path).await?;
        }

        // If we cancelled the current transfer, start the next queued one.
        // Note: individual cancel() calls would have already tried to start
        // the next, but we ensure it here in case the last cancelled was current.
        if was_current && self.current_transfer.is_none() && !self.queued_pulls.is_empty() {
            if let Some(next) = self.queued_pulls.pop_front() {
                println!(
                    "sync: serial mode - starting next queued pull for {} from peer {} (after cancel_peer)",
                    next.resource_path, next.peer
                );
                let _ = self.start_pull(StartPullParams {
                    data_dir: &next.data_dir,
                    fetch_queue: &next.fetch_queue,
                    peer: next.peer,
                    resource_path: next.resource_path,
                    expected_checksum: next.expected_checksum,
                    event_kind: next.event_kind,
                    destination_path: next.destination_path,
                    username: next.username,
                }).await;
            }
        }

        Ok(())
    }

    /// Aborts the transfer for `(peer, path)` and removes its temp file.
    /// Used by the caller when an outbound chunk request fails permanently.
    pub async fn cancel_transfer(&mut self, peer: PeerId, path: &str) -> io::Result<()> {
        self.cancel(peer, path).await
    }

    /// Re-queues a failed chunk request: drops one in-flight slot and
    /// re-requests the failed offset. Returns the follow-up request, if any.
    pub async fn retry_chunk(
        &mut self,
        peer: PeerId,
        path: &str,
        failed_offset: u64,
    ) -> Option<SyncRequest> {
        let transfer = self.pending.get_mut(&(peer, path.to_string()))?;

        transfer.in_flight = transfer.in_flight.saturating_sub(1);
        transfer.last_activity = std::time::Instant::now();

        let chunk_size = configured_chunk_size() as u64;
        let already_buffered = transfer
            .buffered
            .range(failed_offset..failed_offset.saturating_add(chunk_size))
            .next()
            .is_some();
        let already_written = failed_offset < transfer.write_offset;
        if already_written || already_buffered {
            // Another reply already covers this range; nothing to do.
            return None;
        }

        // Re-request the failed offset. If the transfer is stalled (all
        // requested but nothing arriving), the caller can detect this via
        // `is_stalled` and restart from `write_offset`.
        Some(SyncRequest::FetchFile {
            path: path.to_string(),
            offset: failed_offset,
        })
    }

    /// Returns true if the transfer has made no progress for longer than
    /// `threshold` and all requested chunks are in flight.
    pub fn is_stalled(&self, peer: PeerId, path: &str, threshold: std::time::Duration) -> bool {
        let Some(transfer) = self.pending.get(&(peer, path.to_string())) else {
            return false;
        };
        if transfer.in_flight == 0 {
            return false;
        }
        transfer.last_activity.elapsed() > threshold
    }

    /// Resets the request window for a stalled transfer, re-requesting from
    /// the current write offset. Returns the new fetch requests.
    pub async fn restart_stalled(&mut self, peer: PeerId, path: &str) -> Vec<SyncRequest> {
        let Some(transfer) = self.pending.get_mut(&(peer, path.to_string())) else {
            return Vec::new();
        };

        // Reset in-flight counter: we assume all outstanding requests are lost.
        transfer.in_flight = 0;
        transfer.next_request_offset = transfer.write_offset;
        transfer.last_activity = std::time::Instant::now();

        let mut requests = Vec::new();
        let window = if configured_serial_mode() { 1 } else { configured_window_requests() };
        let chunk_size = configured_chunk_size() as u64;
        while transfer.in_flight < window {
            let offset = transfer.next_request_offset;
            if let Some(total) = transfer.total_size
                && offset >= total
            {
                break;
            }
            requests.push(SyncRequest::FetchFile {
                path: path.to_string(),
                offset,
            });
            transfer.in_flight += 1;
            transfer.next_request_offset = offset + chunk_size;
        }
        requests
    }

    /// Removes transfers that have been pending longer than `max_age` without
    /// any activity (chunk received or request sent). Returns the number of
    /// removed transfers.
    pub async fn cleanup_stale_transfers(&mut self, max_age: std::time::Duration) -> usize {
        let now = std::time::Instant::now();
        let stale: Vec<(PeerId, String)> = self
            .pending
            .iter()
            .filter(|(_, transfer)| now.duration_since(transfer.last_activity) > max_age)
            .map(|((peer, path), _)| (*peer, path.clone()))
            .collect();

        let count = stale.len();
        for (peer, path) in stale {
            eprintln!(
                "sync: removing stale transfer for {path} from {peer} (no activity > {max_age:?})"
            );
            let _ = self.cancel(peer, &path).await;
        }
        count
    }

    async fn cancel(&mut self, peer: PeerId, path: &str) -> io::Result<()> {
        let was_current = configured_serial_mode()
            && self.current_transfer.as_ref().is_some_and(|(p, pt)| *p == peer && *pt == path);

        if let Some(transfer) = self.pending.remove(&(peer, path.to_string())) {
            drop(transfer.file);
            let _ = tokio::fs::remove_file(&transfer.tmp_path).await;
        }

        if was_current {
            self.current_transfer = None;
            if let Some(next) = self.queued_pulls.pop_front() {
                println!(
                    "sync: serial mode - starting next queued pull for {} from peer {} (after cancel)",
                    next.resource_path, next.peer
                );
                let _ = self.start_pull(StartPullParams {
                    data_dir: &next.data_dir,
                    fetch_queue: &next.fetch_queue,
                    peer: next.peer,
                    resource_path: next.resource_path,
                    expected_checksum: next.expected_checksum,
                    event_kind: next.event_kind,
                    destination_path: next.destination_path,
                    username: next.username,
                }).await;
            }
        }

        Ok(())
    }

    /// Begins a chunked pull of `resource_path` (written to `destination_path`
    /// if given, otherwise to `resource_path` itself) from `peer`. No-op if a
    /// pull for the same (peer, path) is already in flight. The first fetch
    /// request is queued on `fetch_queue` for the caller to send.
    async fn start_pull(&mut self, params: StartPullParams<'_>) -> io::Result<()> {
        let key = (params.peer, params.resource_path.clone());
        if self.pending.contains_key(&key) {
            return Ok(());
        }

        // Serial mode: if another transfer is active, queue this one.
        if configured_serial_mode() && self.current_transfer.is_some() {
            println!(
                "sync: serial mode - queuing pull for {} from peer {} (waiting for {})",
                params.resource_path,
                params.peer,
                self.current_transfer.as_ref().unwrap().1
            );
            self.queued_pulls.push_back(QueuedPullParams {
                data_dir: params.data_dir.to_path_buf(),
                fetch_queue: params.fetch_queue.clone(),
                peer: params.peer,
                resource_path: params.resource_path.clone(),
                expected_checksum: params.expected_checksum,
                event_kind: params.event_kind,
                destination_path: params.destination_path,
                username: params.username,
            });
            return Ok(());
        }

        let target_path = params
            .destination_path
            .as_deref()
            .unwrap_or(&params.resource_path);
        let final_path = fs_path_from_wire_path(params.data_dir, target_path);
        println!(
            "sync: start pull for {} from peer {} -> {}",
            params.resource_path,
            params.peer,
            final_path.display()
        );
        if let Some(parent) = final_path.parent() {
            println!("sync: ensure parent directory {}", parent.display());
            tokio::fs::create_dir_all(parent).await?;
        }

        let tmp_name = format!(
            "{}.p2p-tmp",
            final_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("download")
        );
        let tmp_path = final_path.with_file_name(tmp_name);
        println!("sync: create temp file {}", tmp_path.display());
        let file = tokio::fs::File::create(&tmp_path).await?;

        self.pending.insert(
            key,
            PendingTransfer {
                tmp_path,
                final_path,
                resource_path: params.resource_path.clone(),
                file,
                expected_checksum: params.expected_checksum,
                event_kind: params.event_kind,
                destination_path: params.destination_path,
                username: params.username,
                total_size: None,
                next_request_offset: 0,
                in_flight: 0,
                write_offset: 0,
                last_log_offset: 0,
                buffered: BTreeMap::new(),
                hasher: Hasher::new(),
                last_activity: std::time::Instant::now(),
            },
        );

        if configured_serial_mode() {
            self.current_transfer = Some((params.peer, params.resource_path.clone()));
        }

        params
            .fetch_queue
            .push(SyncRequest::FetchFile {
                path: params.resource_path,
                offset: 0,
            })
            .await;
        Ok(())
    }

    /// Feeds one received chunk into the matching in-flight transfer.
    /// Chunks are staged until they can be written in offset order; the
    /// checksum is updated incrementally while writing. Returns follow-up
    /// `FetchFile` requests to keep the request window full, or an empty vec
    /// once the transfer is complete (or was unexpected and ignored).
    async fn on_chunk(
        &mut self,
        database: &Database,
        peer: PeerId,
        path: String,
        data: Vec<u8>,
        offset: u64,
        total_size: u64,
    ) -> io::Result<Vec<SyncRequest>> {
        let key = (peer, path.clone());
        let transfer = match self.pending.get_mut(&key) {
            Some(t) => t,
            None => return Ok(Vec::new()),
        };

        transfer.total_size = Some(total_size);
        transfer.in_flight = transfer.in_flight.saturating_sub(1);
        transfer.last_activity = std::time::Instant::now();

        // Empty files complete immediately: there is nothing to request.
        if total_size == 0 {
            let transfer = self.pending.remove(&key).expect("checked above");
            drop(transfer.file);
            let _ = tokio::fs::remove_file(&transfer.tmp_path).await;
            // Materialize an empty file at the destination.
            if let Some(parent) = transfer.final_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&transfer.final_path, b"").await?;
            println!(
                "sync: finalized empty file {}",
                transfer.final_path.display()
            );

            // Serial mode: clear current transfer and start next queued.
            if configured_serial_mode() {
                self.current_transfer = None;
                if let Some(next) = self.queued_pulls.pop_front() {
                    println!(
                        "sync: serial mode - starting next queued pull for {} from peer {}",
                        next.resource_path, next.peer
                    );
                    let _ = self.start_pull(StartPullParams {
                        data_dir: &next.data_dir,
                        fetch_queue: &next.fetch_queue,
                        peer: next.peer,
                        resource_path: next.resource_path,
                        expected_checksum: next.expected_checksum,
                        event_kind: next.event_kind,
                        destination_path: next.destination_path,
                        username: next.username,
                    }).await;
                }
            }
            return Ok(Vec::new());
        }

        if data.is_empty() {
            // Chunks we already passed (e.g. a retried request racing the
            // original response) carry no new data; just refill the window.
            let window = if configured_serial_mode() { 1 } else { configured_window_requests() };
            return Ok(refill_window(transfer, &path, window));
        }

        transfer.buffered.entry(offset).or_insert(data);

        // Write everything that is contiguous from write_offset onwards and
        // update the rolling checksum in the same pass.
        while let Some(chunk) = transfer.buffered.remove(&transfer.write_offset) {
            transfer.file.write_all(&chunk).await?;
            transfer.hasher.update(&chunk);
            transfer.write_offset += chunk.len() as u64;
        }

        // Log progress every ~10% or every 50 MiB, whichever is smaller.
        let chunk_size = configured_chunk_size() as u64;
        let log_interval = (total_size / 10).min(50 * 1024 * 1024).max(chunk_size);
        if transfer.write_offset >= transfer.last_log_offset + log_interval
            || transfer.write_offset == total_size
        {
            transfer.last_log_offset = transfer.write_offset;
            let pct = if total_size > 0 {
                transfer.write_offset as f64 / total_size as f64 * 100.0
            } else {
                0.0
            };
            println!(
                "sync: {} {:.1}% ({}/{} bytes, {} in-flight)",
                transfer.resource_path, pct, transfer.write_offset, total_size, transfer.in_flight
            );
        }

        if transfer.write_offset < total_size {
            let window = if configured_serial_mode() { 1 } else { configured_window_requests() };
            return Ok(refill_window(transfer, &path, window));
        }

        let mut transfer = self.pending.remove(&key).expect("checked above");
        transfer.file.flush().await?;
        drop(transfer.file);

        println!("sync: verify checksum for {}", transfer.resource_path);
        let actual_checksum = format!("{:08x}", transfer.hasher.finalize());
        if let Some(expected) = &transfer.expected_checksum
            && expected != &actual_checksum
        {
            eprintln!(
                "sync: checksum mismatch for {} from peer {peer} (expected {expected}, got {actual_checksum}); discarding transfer",
                transfer.resource_path
            );
            println!("sync: remove temp file {}", transfer.tmp_path.display());
            let _ = tokio::fs::remove_file(&transfer.tmp_path).await;
            // Serial mode: clear current transfer and start next queued.
            if configured_serial_mode() {
                self.current_transfer = None;
                if let Some(next) = self.queued_pulls.pop_front() {
                    println!(
                        "sync: serial mode - starting next queued pull for {} from peer {}",
                        next.resource_path, next.peer
                    );
                    let _ = self.start_pull(StartPullParams {
                        data_dir: &next.data_dir,
                        fetch_queue: &next.fetch_queue,
                        peer: next.peer,
                        resource_path: next.resource_path,
                        expected_checksum: next.expected_checksum,
                        event_kind: next.event_kind,
                        destination_path: next.destination_path,
                        username: next.username,
                    }).await;
                }
            }
            return Ok(Vec::new());
        }

        println!(
            "sync: finalize file {} -> {}",
            transfer.tmp_path.display(),
            transfer.final_path.display()
        );
        tokio::fs::rename(&transfer.tmp_path, &transfer.final_path).await?;

        database.record(EventEnvelope {
            event_kind: transfer.event_kind,
            source_path: transfer.resource_path,
            destination_path: transfer.destination_path,
            checksum: Some(actual_checksum),
            method: "P2P".to_string(),
            status_code: 200,
            username: transfer.username,
        });

        // Serial mode: clear current transfer and start next queued.
        if configured_serial_mode() {
            self.current_transfer = None;
            if let Some(next) = self.queued_pulls.pop_front() {
                println!(
                    "sync: serial mode - starting next queued pull for {} from peer {}",
                    next.resource_path, next.peer
                );
                let _ = self.start_pull(StartPullParams {
                    data_dir: &next.data_dir,
                    fetch_queue: &next.fetch_queue,
                    peer: next.peer,
                    resource_path: next.resource_path,
                    expected_checksum: next.expected_checksum,
                    event_kind: next.event_kind,
                    destination_path: next.destination_path,
                    username: next.username,
                }).await;
            }
        }

        Ok(Vec::new())
    }
}

/// Issues new fetch requests until the window is full or the whole file has
/// been requested. Beyond the first (size-probing) request, offsets are only
/// issued once the total size is known, so we never ask for ranges past the
/// end of the file.
fn refill_window(transfer: &mut PendingTransfer, path: &str, window: usize) -> Vec<SyncRequest> {
    let mut requests = Vec::new();
    while transfer.in_flight < window {
        let Some(total) = transfer.total_size else {
            break;
        };
        let offset = transfer.next_request_offset;
        if offset >= total {
            break;
        }
        requests.push(SyncRequest::FetchFile {
            path: path.to_string(),
            offset,
        });
        transfer.in_flight += 1;
        transfer.next_request_offset = offset + configured_chunk_size() as u64;
    }
    requests
}

async fn apply_delete(data_dir: &Path, path: &str) -> io::Result<()> {
    let fs_path = fs_path_from_wire_path(data_dir, path);
    println!("sync: delete {}", fs_path.display());
    match tokio::fs::metadata(&fs_path).await {
        Ok(meta) if meta.is_dir() => tokio::fs::remove_dir_all(&fs_path).await,
        Ok(_) => tokio::fs::remove_file(&fs_path).await,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Sender-side chunk reader with a small LRU cache of open file handles, so
/// pipelined chunk requests against the same file don't each pay for an
/// open()+seek() pair.
pub struct ChunkReader {
    handles: Vec<(String, tokio::fs::File)>,
}

impl ChunkReader {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    /// Reads one chunk of `path` at `offset` from `data_dir`. Never fails:
    /// I/O errors are logged and reported as [`SyncResponse::NotFound`] so the
    /// request always gets a response.
    pub async fn read_chunk(&mut self, data_dir: &Path, path: &str, offset: u64) -> SyncResponse {
        let fs_path = fs_path_from_wire_path(data_dir, path);
        let metadata = match tokio::fs::metadata(&fs_path).await {
            Ok(m) => m,
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    eprintln!("sync: failed to stat {path} for chunk request: {err}");
                }
                return SyncResponse::NotFound {
                    path: path.to_string(),
                };
            }
        };
        let total_size = metadata.len();
        let chunk_size = configured_chunk_size();

        if offset >= total_size && total_size > 0 {
            return SyncResponse::NotFound {
                path: path.to_string(),
            };
        }

        let file = match self.get_handle(path, &fs_path).await {
            Ok(file) => file,
            Err(err) => {
                eprintln!("sync: failed to open {path} for chunk request: {err}");
                return SyncResponse::NotFound {
                    path: path.to_string(),
                };
            }
        };

        let read_result: io::Result<Vec<u8>> = async {
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            let remaining = total_size.saturating_sub(offset);
            let to_read = remaining.min(chunk_size as u64) as usize;
            let mut buffer = vec![0_u8; to_read];
            tokio::io::AsyncReadExt::read_exact(file, &mut buffer).await?;
            Ok(buffer)
        }
        .await;

        match read_result {
            Ok(buffer) => {
                let new_offset = offset + buffer.len() as u64;
                SyncResponse::Chunk {
                    path: path.to_string(),
                    data: buffer,
                    offset,
                    total_size,
                    is_last: new_offset >= total_size,
                }
            }
            Err(err) => {
                eprintln!("sync: failed to read chunk of {path} at offset {offset}: {err}");
                // Drop the (possibly broken) cached handle so the next request
                // re-opens the file.
                self.handles.retain(|(cached, _)| cached != path);
                SyncResponse::NotFound {
                    path: path.to_string(),
                }
            }
        }
    }

    async fn get_handle(&mut self, path: &str, fs_path: &Path) -> io::Result<&mut tokio::fs::File> {
        if let Some(pos) = self.handles.iter().position(|(cached, _)| cached == path) {
            let entry = self.handles.remove(pos);
            self.handles.push(entry);
        } else {
            let file = tokio::fs::File::open(fs_path).await?;
            self.handles.push((path.to_string(), file));
            if self.handles.len() > CHUNK_READER_CACHE_CAPACITY {
                self.handles.remove(0);
            }
        }
        let (_, file) = self.handles.last_mut().expect("just inserted or moved");
        Ok(file)
    }
}

impl Default for ChunkReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Applies an inbound `Response::Chunk`/`Response::NotFound` to the matching
/// pending transfer. Returns follow-up requests to send, if any.
pub async fn handle_chunk_response(
    state: &mut SyncState,
    database: &Database,
    peer: PeerId,
    response: SyncResponse,
) -> io::Result<Vec<SyncRequest>> {
    match response {
        SyncResponse::Chunk {
            path,
            data,
            offset,
            total_size,
            ..
        } => {
            state
                .on_chunk(database, peer, path, data, offset, total_size)
                .await
        }
        SyncResponse::NotFound { path } => {
            println!("sync: peer {peer} no longer has {path}; dropping pending transfer");
            state.cancel(peer, &path).await?;
            Ok(Vec::new())
        }
        SyncResponse::Manifest(_) | SyncResponse::Ack => Ok(Vec::new()),
    }
}

/// Applies an inbound [`SyncRequest::Event`] push notification: either
/// mutates the filesystem directly (deletes, directory creation, or a local
/// rename/copy when the source is already present) or starts a chunked pull
/// when content needs to be fetched from the peer. Returns a `FetchFile`
/// request to send, if a pull was started.
pub async fn handle_incoming_event(
    data_dir: &Path,
    database: &Database,
    state: &mut SyncState,
    fetch_queue: &FetchQueue,
    peer: PeerId,
    event: FileChangeEvent,
) -> io::Result<()> {
    let source_path = normalize_wire_path(&event.source_path);
    let destination_path = event.destination_path.as_deref().map(normalize_wire_path);

    match event.event_kind {
        EventKind::Deleted => {
            println!("sync: apply remote delete {}", source_path);
            apply_delete(data_dir, &source_path).await?;
            database.record(EventEnvelope {
                event_kind: EventKind::Deleted,
                source_path,
                destination_path: None,
                checksum: None,
                method: "P2P".to_string(),
                status_code: 200,
                username: event.username,
            });
            Ok(())
        }
        EventKind::DirCreated => {
            let dir_path = fs_path_from_wire_path(data_dir, &source_path);
            println!("sync: create remote directory {}", dir_path.display());
            tokio::fs::create_dir_all(dir_path).await?;
            database.record(EventEnvelope {
                event_kind: EventKind::DirCreated,
                source_path,
                destination_path: None,
                checksum: None,
                method: "P2P".to_string(),
                status_code: 200,
                username: event.username,
            });
            Ok(())
        }
        EventKind::Renamed | EventKind::Moved => {
            let dest = destination_path
                .clone()
                .unwrap_or_else(|| source_path.clone());
            let src_fs = fs_path_from_wire_path(data_dir, &source_path);
            let dest_fs = fs_path_from_wire_path(data_dir, &dest);

            if tokio::fs::try_exists(&src_fs).await.unwrap_or(false) {
                if let Some(parent) = dest_fs.parent() {
                    println!("sync: ensure parent directory {}", parent.display());
                    tokio::fs::create_dir_all(parent).await?;
                }
                println!("sync: rename {} -> {}", src_fs.display(), dest_fs.display());
                tokio::fs::rename(&src_fs, &dest_fs).await?;
                database.record(EventEnvelope {
                    event_kind: event.event_kind,
                    source_path,
                    destination_path,
                    checksum: event.checksum.clone(),
                    method: "P2P".to_string(),
                    status_code: 200,
                    username: event.username,
                });
                Ok(())
            } else if state.is_pending(peer, &dest) {
                Ok(())
            } else {
                println!(
                    "sync: missing source {}, start pull for {}",
                    src_fs.display(),
                    dest
                );
                state
                    .start_pull(StartPullParams {
                        data_dir,
                        fetch_queue,
                        peer,
                        resource_path: dest.clone(),
                        expected_checksum: event.checksum,
                        event_kind: event.event_kind,
                        destination_path: None,
                        username: event.username,
                    })
                    .await?;
                Ok(())
            }
        }
        EventKind::Copied => {
            let dest = destination_path
                .clone()
                .unwrap_or_else(|| source_path.clone());
            let src_fs = fs_path_from_wire_path(data_dir, &source_path);
            let dest_fs = fs_path_from_wire_path(data_dir, &dest);

            if tokio::fs::try_exists(&src_fs).await.unwrap_or(false) {
                if let Some(parent) = dest_fs.parent() {
                    println!("sync: ensure parent directory {}", parent.display());
                    tokio::fs::create_dir_all(parent).await?;
                }
                println!("sync: copy {} -> {}", src_fs.display(), dest_fs.display());
                tokio::fs::copy(&src_fs, &dest_fs).await?;
                database.record(EventEnvelope {
                    event_kind: EventKind::Copied,
                    source_path,
                    destination_path,
                    checksum: event.checksum.clone(),
                    method: "P2P".to_string(),
                    status_code: 200,
                    username: event.username,
                });
                Ok(())
            } else if state.is_pending(peer, &dest) {
                Ok(())
            } else {
                println!(
                    "sync: missing source {}, start pull for {}",
                    src_fs.display(),
                    dest
                );
                state
                    .start_pull(StartPullParams {
                        data_dir,
                        fetch_queue,
                        peer,
                        resource_path: dest.clone(),
                        expected_checksum: event.checksum,
                        event_kind: EventKind::Copied,
                        destination_path: None,
                        username: event.username,
                    })
                    .await?;
                Ok(())
            }
        }
        EventKind::Created | EventKind::Edited => {
            let path = source_path;
            if state.is_pending(peer, &path) {
                Ok(())
            } else {
                println!("sync: start pull for incoming {}", path);
                state
                    .start_pull(StartPullParams {
                        data_dir,
                        fetch_queue,
                        peer,
                        resource_path: path.clone(),
                        expected_checksum: event.checksum,
                        event_kind: event.event_kind,
                        destination_path: None,
                        username: event.username,
                    })
                    .await?;
                Ok(())
            }
        }
    }
}

/// Applies the actions computed by [`diff_manifests`] against local state:
/// directory creation and deletion happen immediately, while file pulls are
/// started via [`SyncState`] and their initial fetch requests are queued on
/// `fetch_queue` for the caller to send.
pub async fn apply_manifest_actions(
    data_dir: &Path,
    database: &Database,
    state: &mut SyncState,
    fetch_queue: &FetchQueue,
    peer: PeerId,
    actions: Vec<SyncAction>,
) {
    let username = format!("p2p:{peer}");

    for action in actions {
        match action {
            SyncAction::CreateDir { path } => {
                let dir_path = fs_path_from_wire_path(data_dir, &path);
                println!("sync: create dir {}", dir_path.display());
                if let Err(err) = tokio::fs::create_dir_all(dir_path).await {
                    eprintln!("sync: failed to create directory {path}: {err}");
                    continue;
                }
                database.record(EventEnvelope {
                    event_kind: EventKind::DirCreated,
                    source_path: path,
                    destination_path: None,
                    checksum: None,
                    method: "P2P".to_string(),
                    status_code: 200,
                    username: username.clone(),
                });
            }
            SyncAction::Delete { path } => {
                println!("sync: delete {}", path);
                if let Err(err) = apply_delete(data_dir, &path).await {
                    eprintln!("sync: failed to delete {path}: {err}");
                    continue;
                }
                database.record(EventEnvelope {
                    event_kind: EventKind::Deleted,
                    source_path: path,
                    destination_path: None,
                    checksum: None,
                    method: "P2P".to_string(),
                    status_code: 200,
                    username: username.clone(),
                });
            }
            SyncAction::Pull { path, checksum } => {
                if state.is_pending(peer, &path) {
                    continue;
                }
                println!("sync: start manifest pull for {}", path);
                if let Err(err) = state
                    .start_pull(StartPullParams {
                        data_dir,
                        fetch_queue,
                        peer,
                        resource_path: path.clone(),
                        expected_checksum: checksum,
                        event_kind: EventKind::Edited,
                        destination_path: None,
                        username: username.clone(),
                    })
                    .await
                {
                    eprintln!("sync: failed to start pull for {path}: {err}");
                    continue;
                }
            }
        }
    }
}
