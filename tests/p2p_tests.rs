use std::{
    collections::HashSet,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use libp2p::{PeerId, identity};
use nacs_backend::p2p::{load_or_create_identity, newly_discovered_peers};

fn temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();

    std::env::temp_dir().join(format!(
        "nacs-backend-{name}-{nanos}-{}",
        std::process::id()
    ))
}

#[tokio::test]
async fn identity_is_persisted_and_reloaded() {
    let base = temp_path("p2p-key");
    let key_path = base.join("p2p_identity.key");

    let first = load_or_create_identity(&key_path)
        .await
        .expect("key should be created");
    let second = load_or_create_identity(&key_path)
        .await
        .expect("key should be reloaded");

    assert_eq!(PeerId::from(first.public()), PeerId::from(second.public()));

    if base.exists() {
        tokio::fs::remove_dir_all(&base)
            .await
            .expect("temp key directory should be removed");
    }
}

#[test]
fn discovery_filter_reports_each_peer_once() {
    let mut seen = HashSet::new();
    let p1 = PeerId::from(identity::Keypair::generate_ed25519().public());
    let p2 = PeerId::from(identity::Keypair::generate_ed25519().public());

    let first_batch = newly_discovered_peers(&mut seen, vec![p1, p2, p1]);
    assert_eq!(first_batch.len(), 2);
    assert!(first_batch.contains(&p1));
    assert!(first_batch.contains(&p2));

    let second_batch = newly_discovered_peers(&mut seen, vec![p1, p2]);
    assert!(second_batch.is_empty());
}

#[test]
fn discovery_filter_reports_peer_again_after_removal() {
    let mut seen = HashSet::new();
    let peer = PeerId::from(identity::Keypair::generate_ed25519().public());

    let first_batch = newly_discovered_peers(&mut seen, vec![peer]);
    assert_eq!(first_batch, vec![peer]);

    assert!(seen.remove(&peer));

    let rediscovered_batch = newly_discovered_peers(&mut seen, vec![peer]);
    assert_eq!(rediscovered_batch, vec![peer]);
}
