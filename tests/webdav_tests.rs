use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use nacs_backend::webdav::ensure_data_dir;

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();

    let mut path = std::env::temp_dir();
    path.push(format!("nacs-backend-{name}-{nanos}-{}", std::process::id()));
    path
}

#[tokio::test]
async fn ensure_data_dir_creates_missing_directory() {
    let dir = temp_dir("ensure-data-dir");
    assert!(!dir.exists());

    ensure_data_dir(&dir)
        .await
        .expect("data dir should be created");
    assert!(dir.exists());
    assert!(dir.is_dir());

    fs::remove_dir_all(&dir).expect("temp dir should be removed");
}
