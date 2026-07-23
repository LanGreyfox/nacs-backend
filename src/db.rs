use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

const DB_FILENAME: &str = "webdav.db";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Created,
    Edited,
    Deleted,
    Renamed,
    Moved,
    Copied,
    DirCreated,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Edited => "edited",
            Self::Deleted => "deleted",
            Self::Renamed => "renamed",
            Self::Moved => "moved",
            Self::Copied => "copied",
            Self::DirCreated => "dir_created",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub event_kind: EventKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub method: String,
    pub status_code: u16,
    pub username: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceKind {
    File,
    Folder,
}

impl ResourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Folder => "folder",
        }
    }
}

#[derive(Debug, Clone)]
struct ResourceRow {
    id: i64,
    resource_path: String,
    current_folder: String,
    resource_kind: ResourceKind,
    checksum: Option<String>,
    checksum_algorithm: String,
}

#[derive(Clone)]
pub struct Database {
    tx: mpsc::UnboundedSender<Command>,
}

enum Command {
    Record(EventEnvelope),
}

impl Database {
    pub async fn open(base_dir: impl AsRef<Path>, data_dir: impl AsRef<Path>) -> io::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let data_dir = data_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&base_dir).await?;

        let db_path = base_dir.join(DB_FILENAME);
        let (tx, rx) = mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = oneshot::channel();

        thread::spawn(move || run_worker(db_path, data_dir, rx, ready_tx));

        match ready_rx.await {
            Ok(Ok(())) => Ok(Self { tx }),
            Ok(Err(err)) => Err(io::Error::other(err)),
            Err(_) => Err(io::Error::other("sqlite worker failed to initialize")),
        }
    }

    pub fn record(&self, event: EventEnvelope) {
        let _ = self.tx.send(Command::Record(event));
    }
}

fn run_worker(
    db_path: PathBuf,
    data_dir: PathBuf,
    mut rx: mpsc::UnboundedReceiver<Command>,
    ready_tx: oneshot::Sender<Result<(), String>>,
) {
    let mut conn = match Connection::open(&db_path) {
        Ok(conn) => conn,
        Err(err) => {
            let _ = ready_tx.send(Err(err.to_string()));
            return;
        }
    };

    if let Err(err) = init_schema(&mut conn) {
        let _ = ready_tx.send(Err(err.to_string()));
        return;
    }

    let _ = ready_tx.send(Ok(()));

    while let Some(command) = rx.blocking_recv() {
        match command {
            Command::Record(event) => {
                if let Err(err) = apply_event(&mut conn, &data_dir, event) {
                    eprintln!("failed to persist webdav event: {err}");
                }
            }
        }
    }
}

fn init_schema(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_path TEXT NOT NULL UNIQUE,
            current_folder TEXT NOT NULL,
            resource_kind TEXT NOT NULL CHECK (resource_kind IN ('file', 'folder')),
            checksum TEXT NULL,
            checksum_algorithm TEXT NOT NULL DEFAULT 'sha256',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_event_type TEXT NOT NULL,
            last_method TEXT NOT NULL,
            last_status_code INTEGER NOT NULL,
            username TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS resource_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_path TEXT NOT NULL,
            current_folder TEXT NOT NULL,
            resource_kind TEXT NOT NULL CHECK (resource_kind IN ('file', 'folder')),
            checksum TEXT NULL,
            checksum_algorithm TEXT NOT NULL DEFAULT 'sha256',
            archived_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            archived_reason TEXT NOT NULL CHECK (archived_reason IN ('delete', 'move', 'copy_replace')),
            deleted_by_event_type TEXT NOT NULL,
            deleted_method TEXT NOT NULL,
            deleted_status_code INTEGER NOT NULL,
            username TEXT NULL,
            source_resource_id INTEGER NULL,
            replacement_resource_id INTEGER NULL
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            occurred_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT NOT NULL CHECK (event_type IN ('created', 'edited', 'deleted', 'renamed', 'moved', 'copied', 'dir_created')),
            source_path TEXT NOT NULL,
            destination_path TEXT NULL,
            current_folder TEXT NOT NULL,
            resource_kind TEXT NOT NULL CHECK (resource_kind IN ('file', 'folder')),
            checksum TEXT NULL,
            checksum_algorithm TEXT NOT NULL DEFAULT 'sha256',
            method TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            username TEXT NULL,
            source_resource_id INTEGER NULL,
            archive_resource_id INTEGER NULL
        );

        CREATE INDEX IF NOT EXISTS idx_resources_current_folder ON resources(current_folder);
        CREATE INDEX IF NOT EXISTS idx_resources_kind ON resources(resource_kind);
        CREATE INDEX IF NOT EXISTS idx_resources_last_event_type ON resources(last_event_type);
        CREATE INDEX IF NOT EXISTS idx_resource_archive_path ON resource_archive(resource_path);
        CREATE INDEX IF NOT EXISTS idx_resource_archive_archived_at ON resource_archive(archived_at);
        CREATE INDEX IF NOT EXISTS idx_resource_archive_reason ON resource_archive(archived_reason);
        CREATE INDEX IF NOT EXISTS idx_events_source_path ON events(source_path);
        CREATE INDEX IF NOT EXISTS idx_events_destination_path ON events(destination_path);
        CREATE INDEX IF NOT EXISTS idx_events_current_folder ON events(current_folder);
        CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events(occurred_at);
        CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
        "#,
    )
}

fn apply_event(conn: &mut Connection, data_dir: &Path, event: EventEnvelope) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    let source_path = normalize_path(&event.source_path);
    let destination_path = event.destination_path.as_deref().map(normalize_path);
    let final_path = destination_path
        .as_deref()
        .unwrap_or(&source_path)
        .to_string();

    let source_row = load_resource(&tx, &source_path)?;
    let resource_kind = resolve_resource_kind(event.event_kind, source_row.as_ref());
    let mut checksum = if resource_kind == ResourceKind::Folder {
        None
    } else {
        compute_checksum(data_dir, &final_path).ok().flatten()
    };

    let mut archive_resource_id = None;
    let source_resource_id = source_row.as_ref().map(|row| row.id);

    match event.event_kind {
        EventKind::Deleted => {
            if let Some(row) = source_row.as_ref() {
                archive_resource_id = Some(archive_resource(
                    &tx,
                    row,
                    "delete",
                    &event,
                    None,
                )?);
                tx.execute("DELETE FROM resources WHERE id = ?1", params![row.id])?;
                checksum = row.checksum.clone();
            }
        }
        EventKind::Renamed | EventKind::Moved => {
            if let Some(path) = destination_path.as_deref() {
                if let Some(destination_row) = load_resource(&tx, path)? {
                    if source_row.as_ref().map(|row| row.id) != Some(destination_row.id) {
                        archive_resource_id = Some(archive_resource(
                            &tx,
                            &destination_row,
                            "move",
                            &event,
                            None,
                        )?);
                        tx.execute("DELETE FROM resources WHERE id = ?1", params![destination_row.id])?;
                    }
                }
            }

            if let Some(row) = source_row.as_ref() {
                tx.execute(
                    r#"
                    UPDATE resources
                    SET resource_path = ?1,
                        current_folder = ?2,
                        resource_kind = ?3,
                        checksum = ?4,
                        checksum_algorithm = 'sha256',
                        updated_at = CURRENT_TIMESTAMP,
                        last_event_type = ?5,
                        last_method = ?6,
                        last_status_code = ?7,
                        username = ?8
                    WHERE id = ?9
                    "#,
                    params![
                        final_path,
                        current_folder(&final_path),
                        resource_kind.as_str(),
                        checksum,
                        event.event_kind.as_str(),
                        event.method,
                        i64::from(event.status_code),
                        event.username,
                        row.id,
                    ],
                )?;
            } else {
                insert_or_replace_resource(&tx, &final_path, resource_kind, checksum.clone(), &event)?;
            }
        }
        EventKind::Copied => {
            if let Some(path) = destination_path.as_deref() {
                if let Some(destination_row) = load_resource(&tx, path)? {
                    archive_resource_id = Some(archive_resource(
                        &tx,
                        &destination_row,
                        "copy_replace",
                        &event,
                        None,
                    )?);
                    tx.execute("DELETE FROM resources WHERE id = ?1", params![destination_row.id])?;
                }
            }

            insert_or_replace_resource(&tx, &final_path, resource_kind, checksum.clone(), &event)?;
        }
        EventKind::Created | EventKind::Edited | EventKind::DirCreated => {
            insert_or_replace_resource(&tx, &source_path, resource_kind, checksum.clone(), &event)?;
        }
    }

    let final_kind = match event.event_kind {
        EventKind::Deleted => source_row
            .as_ref()
            .map(|row| row.resource_kind)
            .unwrap_or(resource_kind),
        _ => resource_kind,
    };

    tx.execute(
        r#"
        INSERT INTO events (
            event_type,
            source_path,
            destination_path,
            current_folder,
            resource_kind,
            checksum,
            checksum_algorithm,
            method,
            status_code,
            username,
            source_resource_id,
            archive_resource_id
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'sha256', ?7, ?8, ?9, ?10, ?11)
        "#,
        params![
            event.event_kind.as_str(),
            source_path,
            destination_path,
            current_folder(&final_path),
            final_kind.as_str(),
            checksum.clone(),
            event.method,
            i64::from(event.status_code),
            event.username,
            source_resource_id,
            archive_resource_id,
        ],
    )?;

    tx.commit()
}

fn insert_or_replace_resource(
    tx: &rusqlite::Transaction<'_>,
    resource_path: &str,
    resource_kind: ResourceKind,
    checksum: Option<String>,
    event: &EventEnvelope,
) -> rusqlite::Result<()> {
    tx.execute(
        r#"
        INSERT INTO resources (
            resource_path,
            current_folder,
            resource_kind,
            checksum,
            checksum_algorithm,
            created_at,
            updated_at,
            last_event_type,
            last_method,
            last_status_code,
            username
        ) VALUES (?1, ?2, ?3, ?4, 'sha256', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?5, ?6, ?7, ?8)
        ON CONFLICT(resource_path) DO UPDATE SET
            current_folder = excluded.current_folder,
            resource_kind = excluded.resource_kind,
            checksum = excluded.checksum,
            checksum_algorithm = excluded.checksum_algorithm,
            updated_at = CURRENT_TIMESTAMP,
            last_event_type = excluded.last_event_type,
            last_method = excluded.last_method,
            last_status_code = excluded.last_status_code,
            username = excluded.username
        "#,
        params![
            resource_path,
            current_folder(resource_path),
            resource_kind.as_str(),
            checksum,
            event.event_kind.as_str(),
            event.method,
            i64::from(event.status_code),
            event.username,
        ],
    )?;

    Ok(())
}

fn archive_resource(
    tx: &rusqlite::Transaction<'_>,
    row: &ResourceRow,
    reason: &str,
    event: &EventEnvelope,
    replacement_resource_id: Option<i64>,
) -> rusqlite::Result<i64> {
    tx.execute(
        r#"
        INSERT INTO resource_archive (
            resource_path,
            current_folder,
            resource_kind,
            checksum,
            checksum_algorithm,
            archived_at,
            archived_reason,
            deleted_by_event_type,
            deleted_method,
            deleted_status_code,
            username,
            source_resource_id,
            replacement_resource_id
        ) VALUES (?1, ?2, ?3, ?4, ?5, CURRENT_TIMESTAMP, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            row.resource_path,
            row.current_folder,
            row.resource_kind.as_str(),
            row.checksum,
            row.checksum_algorithm,
            reason,
            event.event_kind.as_str(),
            event.method,
            i64::from(event.status_code),
            event.username,
            row.id,
            replacement_resource_id,
        ],
    )?;

    Ok(tx.last_insert_rowid())
}

fn load_resource(
    tx: &rusqlite::Transaction<'_>,
    resource_path: &str,
) -> rusqlite::Result<Option<ResourceRow>> {
    tx.query_row(
        r#"
        SELECT
            id,
            resource_path,
            current_folder,
            resource_kind,
            checksum,
            checksum_algorithm
        FROM resources
        WHERE resource_path = ?1
        "#,
        params![resource_path],
        |row| {
            let resource_kind = match row.get::<_, String>(3)?.as_str() {
                "folder" => ResourceKind::Folder,
                _ => ResourceKind::File,
            };

            Ok(ResourceRow {
                id: row.get(0)?,
                resource_path: row.get(1)?,
                current_folder: row.get(2)?,
                resource_kind,
                checksum: row.get(4)?,
                checksum_algorithm: row.get(5)?,
            })
        },
    )
    .optional()
}

fn resolve_resource_kind(event_kind: EventKind, existing: Option<&ResourceRow>) -> ResourceKind {
    existing
        .map(|row| row.resource_kind)
        .unwrap_or(match event_kind {
            EventKind::DirCreated => ResourceKind::Folder,
            _ => ResourceKind::File,
        })
}

fn compute_checksum(data_dir: &Path, webdav_path: &str) -> io::Result<Option<String>> {
    let rel = webdav_path.trim_start_matches('/');
    let fs_path = data_dir.join(rel);
    let mut file = fs::File::open(&fs_path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(Some(format!("{:x}", hasher.finalize())))
}

fn normalize_path(path: &str) -> String {
    if path.is_empty() || path == "/" {
        return "/".to_string();
    }

    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

fn current_folder(path: &str) -> String {
    let path = normalize_path(path);

    match path.rsplit_once('/') {
        Some(("", _)) | None => "/".to_string(),
        Some((parent, _)) if parent.is_empty() => "/".to_string(),
        Some((parent, _)) => parent.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_folder_handles_nested_paths() {
        assert_eq!(current_folder("/docs/file.txt"), "/docs");
        assert_eq!(current_folder("/file.txt"), "/");
        assert_eq!(current_folder("/"), "/");
    }

    #[test]
    fn normalize_path_trims_trailing_slash() {
        assert_eq!(normalize_path("/docs/"), "/docs");
        assert_eq!(normalize_path("/"), "/");
    }
}