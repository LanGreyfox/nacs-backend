//! Peer-to-peer file replication protocol - Simple Serial Implementation.
//!
//! This module implements a simple serial synchronization protocol:
//! - One file transfers at a time globally (no parallel transfers)
//! - Files are transferred in their entirety (no chunking)
//! - Manifest-based reconciliation decides what to pull/delete
//! - Simple request/response over libp2p request-response

use std::{
    collections::{HashMap, VecDeque},
    io,
    path::{Path, PathBuf},
};

use crc32fast::Hasher;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::db::{Database, EventEnvelope, EventKind, Manifest};

/// A lightweight, content-free description of a change that happened
/// locally. Used both as the local channel message from the WebDAV layer to
/// the p2p worker, and as the wire payload of [`SyncRequest::Event`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChangeEvent {
    pub event_kind: EventKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub checksum: Option<String>,
    pub size: u64,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncRequest {
    /// Ask the peer for its full resource/tombstone manifest.
    Manifest,
    /// Ask the peer for a complete file.
    FetchFile { path: String },
    /// Notify the peer that a local change happened; carries no content.
    Event(FileChangeEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncResponse {
    Manifest(Manifest),
    File {
        path: String,
        data: Vec<u8>,
        checksum: String,
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

/// Parameters for a queued pull operation.
#[derive(Debug, Clone)]
struct QueuedPullParams {
    peer: PeerId,
    resource_path: String,
    event_kind: EventKind,
    username: String,
}

/// Simple sync state - tracks at most one active transfer and a FIFO queue.
#[derive(Default)]
pub struct SyncState {
    /// Currently active transfer params - None if idle.
    current_transfer: Option<QueuedPullParams>,
    /// FIFO queue of pulls waiting for the current transfer to finish.
    queued_pulls: VecDeque<QueuedPullParams>,
    /// Retry count for current transfer (reset on success or new transfer)
    current_retry: u32,
}

impl SyncState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if a transfer is currently in progress.
    pub fn is_busy(&self) -> bool {
        self.current_transfer.is_some()
    }

    /// Returns true if a pull for (peer, path) is either active or queued.
    pub fn is_pending(&self, peer: PeerId, path: &str) -> bool {
        if self.current_transfer.as_ref().is_some_and(|p| p.peer == peer && p.resource_path == path) {
            return true;
        }
        self.queued_pulls.iter().any(|q| q.peer == peer && q.resource_path == path)
    }

    /// Returns the number of queued pulls waiting.
    pub fn queue_len(&self) -> usize {
        self.queued_pulls.len()
    }

    /// Returns the event_kind and username for the current transfer, if any.
    pub fn current_transfer_info(&self) -> Option<(EventKind, String)> {
        self.current_transfer.as_ref().map(|p| (p.event_kind, p.username.clone()))
    }

    /// Starts a pull if idle, otherwise queues it.
    /// Returns the initial FetchFile request to send, if started.
    pub fn start_pull(
        &mut self,
        _data_dir: &Path,
        peer: PeerId,
        resource_path: String,
        _expected_checksum: Option<String>,
        event_kind: EventKind,
        username: String,
    ) -> Option<SyncRequest> {
        // Already active or queued?
        if self.is_pending(peer, &resource_path) {
            return None;
        }

        if self.current_transfer.is_none() {
            // Start immediately
            let params = QueuedPullParams {
                peer,
                resource_path: resource_path.clone(),
                event_kind,
                username,
            };
            self.current_transfer = Some(params);
            self.current_retry = 0;
            println!(
                "sync: start pull for {} from peer {}",
                resource_path, peer
            );
            Some(SyncRequest::FetchFile { path: resource_path })
        } else {
            // Queue it
            println!(
                "sync: queue pull for {} from peer {} (waiting for {})",
                resource_path,
                peer,
                self.current_transfer.as_ref().unwrap().resource_path
            );
            self.queued_pulls.push_back(QueuedPullParams {
                peer,
                resource_path,
                event_kind,
                username,
            });
            None
        }
    }

    /// Called when a file transfer completes (success or failure).
    /// Returns the next FetchFile request to send, if any queued.
    pub fn finish_transfer(&mut self, peer: PeerId, path: &str) -> Option<SyncRequest> {
        let was_current = self
            .current_transfer
            .as_ref()
            .is_some_and(|p| p.peer == peer && p.resource_path == path);

        if !was_current {
            // Might have been cancelled, check queue
            return None;
        }

self.current_transfer = None;

        // Start next queued pull
        if let Some(next) = self.queued_pulls.pop_front() {
            println!(
                "sync: starting next queued pull for {} from peer {}",
                next.resource_path, next.peer
            );
            self.current_transfer = Some(next.clone());
            self.current_retry = 0;
            return Some(SyncRequest::FetchFile {
                path: next.resource_path,
            });
        }

        None
    }

    /// Retry the current transfer from the beginning (increment retry count).
    /// Returns the FetchFile request to resend, or None if max retries exceeded.
    pub fn retry_current(&mut self) -> Option<SyncRequest> {
        const MAX_RETRIES: u32 = 3;
        if self.current_retry >= MAX_RETRIES {
            eprintln!("sync: max retries ({MAX_RETRIES}) exceeded for current transfer, giving up");
            self.current_transfer = None;
            self.current_retry = 0;
            // Start next queued pull
            if let Some(next) = self.queued_pulls.pop_front() {
                println!(
                    "sync: starting next queued pull for {} from peer {} (after retries exhausted)",
                    next.resource_path, next.peer
                );
                self.current_transfer = Some(next.clone());
                self.current_retry = 0;
                return Some(SyncRequest::FetchFile {
                    path: next.resource_path,
                });
            }
            return None;
        }

        self.current_retry += 1;
        if let Some(current) = &self.current_transfer {
            println!(
                "sync: retrying transfer for {} from peer {} (attempt {}/{})",
                current.resource_path, current.peer, self.current_retry, MAX_RETRIES
            );
            Some(SyncRequest::FetchFile {
                path: current.resource_path.clone(),
            })
        } else {
            None
        }
    }

    /// Cancels all transfers for a peer. Returns next request if a new transfer starts.
    pub async fn cancel_peer(&mut self, peer: PeerId) -> io::Result<Option<SyncRequest>> {
        let was_current = self
            .current_transfer
            .as_ref()
            .is_some_and(|p| p.peer == peer);

        // Remove from queue
        self.queued_pulls.retain(|q| q.peer != peer);

        if was_current {
            self.current_transfer = None;
            self.current_retry = 0;
            // Start next queued pull (from any peer)
            if let Some(next) = self.queued_pulls.pop_front() {
                println!(
                    "sync: starting next queued pull for {} from peer {} (after cancel)",
                    next.resource_path, next.peer
                );
                self.current_transfer = Some(next.clone());
                self.current_retry = 0;
                return Ok(Some(SyncRequest::FetchFile {
                    path: next.resource_path,
                }));
            }
        }

        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct PathState {
    resource_kind: String,
    checksum: Option<String>,
}

fn latest_states(manifest: &Manifest) -> HashMap<String, (String, PathState)> {
    let mut map = HashMap::new();
    for entry in &manifest.resources {
        map.insert(
            entry.resource_path.clone(),
            (
                entry.updated_at.clone(),
                PathState {
                    resource_kind: entry.resource_kind.clone(),
                    checksum: entry.checksum.clone(),
                },
            ),
        );
    }
    for tombstone in &manifest.tombstones {
        map.insert(
            tombstone.resource_path.clone(),
            (tombstone.deleted_at.clone(), PathState { resource_kind: "deleted".to_string(), checksum: None }),
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
/// resource or tombstone) is newer than the local one.
pub fn diff_manifests(local: &Manifest, remote: &Manifest) -> Vec<SyncAction> {
    let local_states = latest_states(local);
    let remote_states = latest_states(remote);

    let mut actions = Vec::new();
    for (path, (remote_ts, remote_state)) in &remote_states {
        let local_state = local_states.get(path);

        // If both sides already report the same file checksum, skip timestamp-based
        // reconciliation to prevent repeated transfers caused by clock/mtime drift.
        if let (
            PathState { resource_kind: remote_kind, checksum: remote_checksum },
            Some((_, PathState { resource_kind: local_kind, checksum: local_checksum })),
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
            PathState { resource_kind, checksum: _ } if resource_kind == "deleted" => {
                if let Some((_, PathState { resource_kind, .. })) = local_states.get(path) {
                    if resource_kind != "deleted" {
                        actions.push(SyncAction::Delete { path: path.clone() });
                    }
                }
            }
            PathState { resource_kind, checksum } if resource_kind == "folder" => {
                actions.push(SyncAction::CreateDir { path: path.clone() });
                let _ = checksum;
            }
            PathState { checksum, .. } => {
                actions.push(SyncAction::Pull {
                    path: path.clone(),
                    checksum: checksum.clone(),
                });
            }
        }
    }

    actions
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

/// Reads an entire file into memory and returns it with its checksum.
async fn read_file_with_checksum(data_dir: &Path, path: &str) -> io::Result<(Vec<u8>, String)> {
    let fs_path = fs_path_from_wire_path(data_dir, path);
    let data = tokio::fs::read(&fs_path).await?;
    let mut hasher = Hasher::new();
    hasher.update(&data);
    let checksum = format!("{:08x}", hasher.finalize());
    Ok((data, checksum))
}

/// Applies an inbound `SyncRequest::Event` push notification.
/// Returns the initial FetchFile request to send, if a pull was started.
pub async fn handle_incoming_event(
    data_dir: &Path,
    database: &Database,
    state: &mut SyncState,
    peer: PeerId,
    event: FileChangeEvent,
) -> io::Result<Option<SyncRequest>> {
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
            Ok(None)
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
            Ok(None)
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
                Ok(None)
            } else if state.is_pending(peer, &dest) {
                Ok(None)
            } else {
                println!(
                    "sync: missing source {}, start pull for {}",
                    src_fs.display(),
                    dest
                );
                let req = state.start_pull(
                    data_dir,
                    peer,
                    dest.clone(),
                    event.checksum,
                    event.event_kind,
                    event.username,
                );
                Ok(req)
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
                Ok(None)
            } else if state.is_pending(peer, &dest) {
                Ok(None)
            } else {
                println!(
                    "sync: missing source {}, start pull for {}",
                    src_fs.display(),
                    dest
                );
                let req = state.start_pull(
                    data_dir,
                    peer,
                    dest.clone(),
                    event.checksum,
                    EventKind::Copied,
                    event.username,
                );
                Ok(req)
            }
        }
        EventKind::Created | EventKind::Edited => {
            let path = source_path;
            if state.is_pending(peer, &path) {
                Ok(None)
            } else {
                println!("sync: start pull for incoming {}", path);
                let req = state.start_pull(
                    data_dir,
                    peer,
                    path.clone(),
                    event.checksum,
                    event.event_kind,
                    event.username,
                );
                Ok(req)
            }
        }
    }
}

/// Handles a file response from a peer.
/// Returns the next FetchFile request to send, if any.
pub async fn handle_file_response(
    data_dir: &Path,
    database: &Database,
    state: &mut SyncState,
    peer: PeerId,
    path: String,
    data: Vec<u8>,
    checksum: String,
    event_kind: EventKind,
    username: String,
) -> io::Result<Option<SyncRequest>> {
    println!("sync: received file {} ({} bytes)", path, data.len());

// Verify checksum
    let mut hasher = Hasher::new();
    hasher.update(&data);
    let actual_checksum = format!("{:08x}", hasher.finalize());

    if actual_checksum != checksum {
        eprintln!(
            "sync: checksum mismatch for {} from peer {} (expected {}, got {}); retrying",
            path, peer, checksum, actual_checksum
        );
        return Ok(state.retry_current());
    }

    // Write file
    let final_path = fs_path_from_wire_path(data_dir, &path);
    if let Some(parent) = final_path.parent() {
        if let Err(err) = tokio::fs::create_dir_all(parent).await {
            eprintln!("sync: failed to create dir for {}: {}; retrying", path, err);
            return Ok(state.retry_current());
        }
    }
    if let Err(err) = tokio::fs::write(&final_path, &data).await {
        eprintln!("sync: failed to write {}: {}; retrying", path, err);
        return Ok(state.retry_current());
    }

    println!("sync: finalized file {}", final_path.display());

    // Record in database
    database.record(EventEnvelope {
        event_kind,
        source_path: path.clone(),
        destination_path: None,
        checksum: Some(actual_checksum),
        method: "P2P".to_string(),
        status_code: 200,
        username,
    });

    // Move to next queued transfer
    let next = state.finish_transfer(peer, &path);
    Ok(next)
}

/// Handles NotFound response - peer doesn't have the file.
/// Returns the next FetchFile request to send, if any.
pub async fn handle_not_found(
    state: &mut SyncState,
    peer: PeerId,
    path: &str,
) -> Option<SyncRequest> {
    println!("sync: peer {} no longer has {}; retrying", peer, path);
    state.retry_current()
}

/// Applies the actions computed by [`diff_manifests`] against local state.
pub async fn apply_manifest_actions(
    data_dir: &Path,
    database: &Database,
    state: &mut SyncState,
    peer: PeerId,
    actions: Vec<SyncAction>,
) -> Vec<SyncRequest> {
    let username = format!("p2p:{peer}");
    let mut fetch_requests = Vec::new();

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
                if let Some(req) = state.start_pull(
                    data_dir,
                    peer,
                    path.clone(),
                    checksum,
                    EventKind::Edited,
                    username.clone(),
                ) {
                    fetch_requests.push(req);
                }
            }
        }
    }
    fetch_requests
}

/// Handles the sender side: reads a complete file and sends it.
pub async fn handle_fetch_request(
    data_dir: &Path,
    path: &str,
) -> SyncResponse {
    match read_file_with_checksum(data_dir, path).await {
        Ok((data, checksum)) => {
            println!("sync: sending file {} ({} bytes)", path, data.len());
            SyncResponse::File {
                path: path.to_string(),
                data,
                checksum,
            }
        }
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                eprintln!("sync: failed to read {path} for fetch request: {err}");
            }
            SyncResponse::NotFound {
                path: path.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_state_starts_idle() {
        let state = SyncState::new();
        assert!(!state.is_busy());
    }

    #[test]
    fn sync_state_tracks_active_transfer() {
        let mut state = SyncState::new();
        let peer = PeerId::random();
        let path = "/test.txt".to_string();

        let req = state.start_pull(Path::new("."), peer, path.clone(), None, EventKind::Created, "user".to_string());
        assert!(req.is_some());
        assert!(state.is_busy());
        assert!(state.is_pending(peer, &path));
    }

    #[test]
    fn sync_state_queues_second_transfer() {
        let mut state = SyncState::new();
        let peer = PeerId::random();

        state.start_pull(Path::new("."), peer, "/file1.txt".to_string(), None, EventKind::Created, "user".to_string());
        let req2 = state.start_pull(Path::new("."), peer, "/file2.txt".to_string(), None, EventKind::Created, "user".to_string());

        assert!(req2.is_none()); // Second transfer queued
        assert_eq!(state.queued_pulls.len(), 1);
    }

    #[test]
    fn sync_state_finishes_and_starts_next() {
        let mut state = SyncState::new();
        let peer = PeerId::random();

        state.start_pull(Path::new("."), peer, "/file1.txt".to_string(), None, EventKind::Created, "user".to_string());
        state.start_pull(Path::new("."), peer, "/file2.txt".to_string(), None, EventKind::Created, "user".to_string());

        let next = state.finish_transfer(peer, "/file1.txt");
        assert!(next.is_some());
        assert!(state.is_pending(peer, "/file2.txt"));
    }

    #[test]
    fn sync_state_cancel_peer_removes_from_queue() {
        let mut state = SyncState::new();
        let peer1 = PeerId::random();
        let peer2 = PeerId::random();

        state.start_pull(Path::new("."), peer1, "/file1.txt".to_string(), None, EventKind::Created, "user".to_string());
        state.start_pull(Path::new("."), peer2, "/file2.txt".to_string(), None, EventKind::Created, "user".to_string());

        let next = tokio::runtime::Runtime::new().unwrap().block_on(state.cancel_peer(peer1)).unwrap();
        assert!(next.is_some()); // peer2's transfer should start
        assert!(!state.is_pending(peer1, "/file1.txt"));
        assert!(state.is_pending(peer2, "/file2.txt"));
    }
}