use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use nacs_backend::webdav::{ensure_data_dir, parse_basic_credentials};

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

#[test]
fn parse_basic_credentials_accepts_case_insensitive_scheme() {
    let auth = "bAsIc dXNlcjpwYXNz";
    let creds = parse_basic_credentials(auth).expect("credentials should parse");
    assert_eq!(creds.0, "user");
    assert_eq!(creds.1, "pass");
}

#[test]
fn parse_basic_credentials_rejects_missing_password_separator() {
    let auth = "Basic dXNlcg==";
    assert!(parse_basic_credentials(auth).is_none());
}

#[test]
fn parse_basic_credentials_accepts_colon_in_password() {
    let auth = "Basic dXNlcjpwYTpzcw==";
    let creds = parse_basic_credentials(auth).expect("credentials should parse");
    assert_eq!(creds.0, "user");
    assert_eq!(creds.1, "pa:ss");
}
