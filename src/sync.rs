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
    collections::HashMap,
    env,
    io,
    path::{Path, PathBuf},
    sync::OnceLock,
};

use libp2p::PeerId;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};

use crate::db::{self, Database, EventEnvelope, EventKind};

/// Maximum number of bytes transferred per chunk request/response round trip.
pub const DEFAULT_CHUNK_SIZE: usize = 2 * 1024 * 1024;
const SYNC_CHUNK_SIZE_ENV: &str = "SYNC_CHUNK_SIZE_BYTES";

/// Returns the configured per-chunk transfer size in bytes.
///
/// This value is resolved once from `SYNC_CHUNK_SIZE_BYTES` at runtime and
/// then cached for the process lifetime.
pub fn configured_chunk_size() -> usize {
    static CHUNK_SIZE: OnceLock<usize> = OnceLock::new();

    *CHUNK_SIZE.get_or_init(|| {
        let chunk_size = resolve_chunk_size_from_env(env::var(SYNC_CHUNK_SIZE_ENV));
        println!(
            "sync: configured chunk size = {chunk_size} bytes ({SYNC_CHUNK_SIZE_ENV})"
        );
        chunk_size
    })
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
#[derive(Clone)]
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

    if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
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
        {
            if remote_kind == "file"
                && local_kind == "file"
                && remote_checksum.is_some()
                && remote_checksum == local_checksum
            {
                continue;
            }
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
struct PendingTransfer {
    tmp_path: PathBuf,
    final_path: PathBuf,
    resource_path: String,
    file: tokio::fs::File,
    expected_checksum: Option<String>,
    event_kind: EventKind,
    destination_path: Option<String>,
    username: String,
}

/// Tracks in-flight chunked downloads, keyed by (peer, path), so multiple
/// concurrent pulls (from different triggers) never duplicate work.
#[derive(Default)]
pub struct SyncState {
    pending: HashMap<(PeerId, String), PendingTransfer>,
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

        for path in paths {
            self.cancel(peer, &path).await?;
        }

        Ok(())
    }

    async fn cancel(&mut self, peer: PeerId, path: &str) -> io::Result<()> {
        if let Some(transfer) = self.pending.remove(&(peer, path.to_string())) {
            drop(transfer.file);
            let _ = tokio::fs::remove_file(&transfer.tmp_path).await;
        }

        Ok(())
    }

    /// Begins a chunked pull of `resource_path` (written to `destination_path`
    /// if given, otherwise to `resource_path` itself) from `peer`. No-op if a
    /// pull for the same (peer, path) is already in flight.
    async fn start_pull(
        &mut self,
        data_dir: &Path,
        peer: PeerId,
        resource_path: String,
        expected_checksum: Option<String>,
        event_kind: EventKind,
        destination_path: Option<String>,
        username: String,
    ) -> io::Result<()> {
        let key = (peer, resource_path.clone());
        if self.pending.contains_key(&key) {
            return Ok(());
        }

        let target_path = destination_path.as_deref().unwrap_or(&resource_path);
        let rel = target_path.trim_start_matches('/');
        let final_path = data_dir.join(rel);
        println!("sync: start pull for {resource_path} from peer {peer} -> {}", final_path.display());
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
                resource_path,
                file,
                expected_checksum,
                event_kind,
                destination_path,
                username,
            },
        );
        Ok(())
    }

    /// Feeds one received chunk into the matching in-flight transfer.
    /// Returns a follow-up `FetchFile` request if more chunks are needed, or
    /// `None` once the transfer is complete (or was unexpected and ignored).
    async fn on_chunk(
        &mut self,
        database: &Database,
        peer: PeerId,
        path: String,
        data: Vec<u8>,
        offset: u64,
        is_last: bool,
    ) -> io::Result<Option<SyncRequest>> {
        let key = (peer, path.clone());
        let transfer = match self.pending.get_mut(&key) {
            Some(t) => t,
            None => return Ok(None),
        };

        println!(
            "sync: write chunk for {} from peer {peer} at offset {offset} ({} bytes)",
            path,
            data.len()
        );
        transfer.file.write_all(&data).await?;

        if !is_last {
            let next_offset = offset + data.len() as u64;
            return Ok(Some(SyncRequest::FetchFile {
                path,
                offset: next_offset,
            }));
        }

        let mut transfer = self.pending.remove(&key).expect("checked above");
        transfer.file.flush().await?;
        drop(transfer.file);

        println!("sync: verify checksum for {}", transfer.resource_path);
        let actual_checksum = checksum_file(&transfer.tmp_path).await?;
        if let Some(expected) = &transfer.expected_checksum {
            if expected != &actual_checksum {
                eprintln!(
                    "sync: checksum mismatch for {} from peer {peer} (expected {expected}, got {actual_checksum}); discarding transfer",
                    transfer.resource_path
                );
                println!("sync: remove temp file {}", transfer.tmp_path.display());
                let _ = tokio::fs::remove_file(&transfer.tmp_path).await;
                return Ok(None);
            }
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
            method: "P2P".to_string(),
            status_code: 200,
            username: transfer.username,
        });

        Ok(None)
    }
}

async fn checksum_file(path: &Path) -> io::Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

async fn apply_delete(data_dir: &Path, path: &str) -> io::Result<()> {
    let rel = path.trim_start_matches('/');
    let fs_path = data_dir.join(rel);
    println!("sync: delete {}", fs_path.display());
    match tokio::fs::metadata(&fs_path).await {
        Ok(meta) if meta.is_dir() => tokio::fs::remove_dir_all(&fs_path).await,
        Ok(_) => tokio::fs::remove_file(&fs_path).await,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Reads one chunk of `path` at `offset` from `data_dir`, for responding to
/// an inbound [`SyncRequest::FetchFile`]. Never fails: I/O errors are logged
/// and reported to the peer as [`SyncResponse::NotFound`] so the request
/// always gets a response.
pub async fn read_chunk(data_dir: &Path, path: &str, offset: u64) -> SyncResponse {
    let rel = path.trim_start_matches('/');
    let fs_path = data_dir.join(rel);
    println!("sync: serve chunk request for {path} at offset {offset} from {}", fs_path.display());

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

    let read_result: io::Result<Vec<u8>> = async {
        let mut file = tokio::fs::File::open(&fs_path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let remaining = total_size.saturating_sub(offset);
        let to_read = remaining.min(chunk_size as u64) as usize;
        let mut buffer = vec![0_u8; to_read];
        file.read_exact(&mut buffer).await?;
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
            SyncResponse::NotFound {
                path: path.to_string(),
            }
        }
    }
}

/// Applies an inbound `Response::Chunk`/`Response::NotFound` to the matching
/// pending transfer. Returns a follow-up request to send, if any.
pub async fn handle_chunk_response(
    state: &mut SyncState,
    database: &Database,
    peer: PeerId,
    response: SyncResponse,
) -> io::Result<Option<SyncRequest>> {
    match response {
        SyncResponse::Chunk {
            path,
            data,
            offset,
            is_last,
            ..
        } => state.on_chunk(database, peer, path, data, offset, is_last).await,
        SyncResponse::NotFound { path } => {
            println!("sync: peer {peer} no longer has {path}; dropping pending transfer");
            state.cancel(peer, &path).await?;
            Ok(None)
        }
        SyncResponse::Manifest(_) | SyncResponse::Ack => Ok(None),
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
    peer: PeerId,
    event: FileChangeEvent,
) -> io::Result<Option<SyncRequest>> {
    let source_path = normalize_wire_path(&event.source_path);
    let destination_path = event
        .destination_path
        .as_deref()
        .map(normalize_wire_path);

    match event.event_kind {
        EventKind::Deleted => {
            println!("sync: apply remote delete {}", source_path);
            apply_delete(data_dir, &source_path).await?;
            database.record(EventEnvelope {
                event_kind: EventKind::Deleted,
                source_path,
                destination_path: None,
                method: "P2P".to_string(),
                status_code: 200,
                username: event.username,
            });
            Ok(None)
        }
        EventKind::DirCreated => {
            let rel = source_path.trim_start_matches('/');
            println!("sync: create remote directory {}", data_dir.join(rel).display());
            tokio::fs::create_dir_all(data_dir.join(rel)).await?;
            database.record(EventEnvelope {
                event_kind: EventKind::DirCreated,
                source_path,
                destination_path: None,
                method: "P2P".to_string(),
                status_code: 200,
                username: event.username,
            });
            Ok(None)
        }
        EventKind::Renamed | EventKind::Moved => {
            let dest = destination_path.clone().unwrap_or_else(|| source_path.clone());
            let src_fs = data_dir.join(source_path.trim_start_matches('/'));
            let dest_fs = data_dir.join(dest.trim_start_matches('/'));

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
                    method: "P2P".to_string(),
                    status_code: 200,
                    username: event.username,
                });
                Ok(None)
            } else if state.is_pending(peer, &dest) {
                Ok(None)
            } else {
                println!("sync: missing source {}, start pull for {}", src_fs.display(), dest);
                state
                    .start_pull(
                        data_dir,
                        peer,
                        dest.clone(),
                        event.checksum,
                        event.event_kind,
                        None,
                        event.username,
                    )
                    .await?;
                Ok(Some(SyncRequest::FetchFile { path: dest, offset: 0 }))
            }
        }
        EventKind::Copied => {
            let dest = destination_path.clone().unwrap_or_else(|| source_path.clone());
            let src_fs = data_dir.join(source_path.trim_start_matches('/'));
            let dest_fs = data_dir.join(dest.trim_start_matches('/'));

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
                    method: "P2P".to_string(),
                    status_code: 200,
                    username: event.username,
                });
                Ok(None)
            } else if state.is_pending(peer, &dest) {
                Ok(None)
            } else {
                println!("sync: missing source {}, start pull for {}", src_fs.display(), dest);
                state
                    .start_pull(
                        data_dir,
                        peer,
                        dest.clone(),
                        event.checksum,
                        EventKind::Copied,
                        None,
                        event.username,
                    )
                    .await?;
                Ok(Some(SyncRequest::FetchFile { path: dest, offset: 0 }))
            }
        }
        EventKind::Created | EventKind::Edited => {
            let path = source_path;
            if state.is_pending(peer, &path) {
                Ok(None)
            } else {
                println!("sync: start pull for incoming {}", path);
                state
                    .start_pull(
                        data_dir,
                        peer,
                        path.clone(),
                        event.checksum,
                        event.event_kind,
                        None,
                        event.username,
                    )
                    .await?;
                Ok(Some(SyncRequest::FetchFile { path, offset: 0 }))
            }
        }
    }
}

/// Applies the actions computed by [`diff_manifests`] against local state:
/// directory creation and deletion happen immediately, while file pulls are
/// started via [`SyncState`] and returned as requests for the caller to send.
pub async fn apply_manifest_actions(
    data_dir: &Path,
    database: &Database,
    state: &mut SyncState,
    peer: PeerId,
    actions: Vec<SyncAction>,
) -> Vec<SyncRequest> {
    let username = format!("p2p:{peer}");
    let mut requests = Vec::new();

    for action in actions {
        match action {
            SyncAction::CreateDir { path } => {
                let rel = path.trim_start_matches('/');
                println!("sync: create dir {}", data_dir.join(rel).display());
                if let Err(err) = tokio::fs::create_dir_all(data_dir.join(rel)).await {
                    eprintln!("sync: failed to create directory {path}: {err}");
                    continue;
                }
                database.record(EventEnvelope {
                    event_kind: EventKind::DirCreated,
                    source_path: path,
                    destination_path: None,
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
                    .start_pull(
                        data_dir,
                        peer,
                        path.clone(),
                        checksum,
                        EventKind::Edited,
                        None,
                        username.clone(),
                    )
                    .await
                {
                    eprintln!("sync: failed to start pull for {path}: {err}");
                    continue;
                }
                requests.push(SyncRequest::FetchFile { path, offset: 0 });
            }
        }
    }

    requests
}

