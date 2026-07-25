use std::{
    collections::HashSet,
    error::Error,
    io,
    path::{Path, PathBuf},
    time::Duration,
};

use libp2p::{
    futures::StreamExt,
    identity,
    mdns,
    noise,
    ping,
    swarm::SwarmEvent,
    tcp, yamux, PeerId, SwarmBuilder,
};

const KEY_FILENAME: &str = "p2p_identity.key";
const DEFAULT_P2P_PORT: u16 = 4001;
const HEARTBEAT_INTERVAL_SECS: u64 = 10;
const HEARTBEAT_TIMEOUT_SECS: u64 = 8;

#[derive(libp2p::swarm::NetworkBehaviour)]
struct DiscoveryBehaviour {
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
}

pub async fn run_discovery(base_dir: impl AsRef<Path>) -> io::Result<()> {
    let key_path = key_path(base_dir.as_ref());
    let local_key = load_or_create_identity(&key_path).await?;
    let local_peer_id = PeerId::from(local_key.public());
    let listen_port = configured_peer_port()?;

    println!("p2p node started: {local_peer_id}");

    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(io::Error::other)?
        .with_behaviour(|key| {
            let peer_id = PeerId::from(key.public());
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
            let ping = ping::Behaviour::new(
                ping::Config::new()
                    .with_interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS))
                    .with_timeout(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS)),
            );

            Ok(DiscoveryBehaviour { mdns, ping })
        })
        .map_err(io::Error::other)?
        .build();

    swarm
        .listen_on(
            format!("/ip4/0.0.0.0/tcp/{listen_port}")
                .parse()
                .map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;

    let mut seen_peers = HashSet::new();
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("p2p listening on {address}");
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for peer_id in
                    newly_discovered_peers(&mut seen_peers, peers.into_iter().map(|(peer_id, _)| peer_id))
                {
                    println!("new node discovered: {peer_id}");
                }
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                for (peer_id, _) in peers {
                    if seen_peers.remove(&peer_id) {
                        eprintln!("peer no longer discoverable: {peer_id}");
                    }
                }
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Ping(ping::Event { peer, result, .. })) => {
                match result {
                    Ok(rtt) => {
                        println!("heartbeat ok: {peer} rtt={rtt:?}");
                    }
                    Err(err) => {
                        seen_peers.remove(&peer);
                        eprintln!("peer unreachable (heartbeat failed): {peer}: {err}");
                    }
                }
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                cause,
                ..
            } => {
                seen_peers.remove(&peer_id);
                match cause {
                    Some(err) => eprintln!("peer disconnected: {peer_id} ({err})"),
                    None => eprintln!("peer disconnected: {peer_id}"),
                }
            }
            _ => {}
        }
    }
}

fn key_path(base_dir: &Path) -> PathBuf {
    base_dir.join(KEY_FILENAME)
}

fn configured_peer_port() -> io::Result<u16> {
    match std::env::var("P2P_PORT") {
        Ok(raw) => raw.parse::<u16>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid P2P_PORT value: {raw}"),
            )
        }),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_P2P_PORT),
        Err(err) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Unable to read P2P_PORT: {err}"),
        )),
    }
}

pub async fn load_or_create_identity(path: &Path) -> io::Result<identity::Keypair> {
    if tokio::fs::try_exists(path).await? {
        let encoded = tokio::fs::read(path).await?;
        return identity::Keypair::from_protobuf_encoding(&encoded).map_err(io::Error::other);
    }

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let keypair = identity::Keypair::generate_ed25519();
    let encoded = keypair.to_protobuf_encoding().map_err(io::Error::other)?;
    tokio::fs::write(path, encoded).await?;
    Ok(keypair)
}

pub fn newly_discovered_peers(
    seen: &mut HashSet<PeerId>,
    discovered: impl IntoIterator<Item = PeerId>,
) -> Vec<PeerId> {
    let mut new_peers = Vec::new();
    for peer in discovered {
        if seen.insert(peer) {
            new_peers.push(peer);
        }
    }
    new_peers
}
