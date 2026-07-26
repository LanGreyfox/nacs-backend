use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    convert::Infallible,
    error::Error,
    hash::{Hash, Hasher},
    io,
    path::{Path, PathBuf},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use libp2p::{
    core::{transport::PortUse, upgrade::DeniedUpgrade, Endpoint, Multiaddr},
    futures::StreamExt,
    identity,
    mdns,
    noise,
    ping,
    swarm::{
        behaviour::{FromSwarm, NetworkBehaviour, ToSwarm},
        ConnectionDenied, ConnectionError, ConnectionHandler, ConnectionHandlerEvent,
        ConnectionId, SwarmEvent,
        THandler, THandlerInEvent, THandlerOutEvent,
        handler::{ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        StreamUpgradeError, SubstreamProtocol,
    },
    tcp, yamux, PeerId, SwarmBuilder,
};

const KEY_FILENAME: &str = "p2p_identity.key";
const DEFAULT_P2P_PORT: u16 = 4001;
// Keep this well below the idle timeout so periodic ping traffic keeps
// otherwise quiet connections open.
const HEARTBEAT_INTERVAL_SECS: u64 = 10;
const HEARTBEAT_TIMEOUT_SECS: u64 = 8;
const IDLE_CONNECTION_TIMEOUT_SECS: u64 = 60;
const RECONNECT_BASE_DELAY_MS: u64 = 500;
const RECONNECT_MAX_DELAY_MS: u64 = 30_000;
const RECONNECT_JITTER_PERCENT: u64 = 20;

#[derive(Debug, Clone, Copy)]
pub struct RetryState {
    failures: u32,
    next_attempt_at: Instant,
}

fn next_retry_delay(retries: &HashMap<PeerId, RetryState>) -> Option<Duration> {
    let now = Instant::now();

    retries
        .values()
        .map(|state| {
            if state.next_attempt_at <= now {
                Duration::ZERO
            } else {
                state.next_attempt_at.duration_since(now)
            }
        })
        .min()
}

pub fn reconnect_delay(peer_id: PeerId, failures: u32) -> Duration {
    let exp = failures.saturating_sub(1).min(16);
    let exp_factor = 1u64 << exp;
    let base_ms = RECONNECT_BASE_DELAY_MS
        .saturating_mul(exp_factor)
        .min(RECONNECT_MAX_DELAY_MS);

    // Deterministic jitter per peer and attempt to avoid synchronized re-dials.
    let mut hasher = DefaultHasher::new();
    peer_id.hash(&mut hasher);
    failures.hash(&mut hasher);
    let jitter_seed = hasher.finish();
    let jitter_ceiling = base_ms.saturating_mul(RECONNECT_JITTER_PERCENT) / 100;
    let jitter = if jitter_ceiling == 0 {
        0
    } else {
        jitter_seed % (jitter_ceiling + 1)
    };

    Duration::from_millis(base_ms.saturating_add(jitter))
}

pub fn schedule_retry(retries: &mut HashMap<PeerId, RetryState>, peer_id: PeerId) -> Duration {
    let now = Instant::now();
    let failures = retries
        .get(&peer_id)
        .map_or(1, |state| state.failures.saturating_add(1));
    let delay = reconnect_delay(peer_id, failures);

    retries.insert(
        peer_id,
        RetryState {
            failures,
            next_attempt_at: now + delay,
        },
    );

    delay
}

#[derive(libp2p::swarm::NetworkBehaviour)]
struct DiscoveryBehaviour {
    keep_alive: KeepAliveBehaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
}

// In libp2p 0.56, ping streams are intentionally ignored for keep-alive.
// This no-op behaviour keeps established connections alive while ping still
// provides liveness checks.
struct KeepAliveBehaviour;

impl NetworkBehaviour for KeepAliveBehaviour {
    type ConnectionHandler = KeepAliveConnectionHandler;
    type ToSwarm = Infallible;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(KeepAliveConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(KeepAliveConnectionHandler)
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        libp2p::core::util::unreachable(event)
    }

    fn poll(&mut self, _: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        Poll::Pending
    }

    fn on_swarm_event(&mut self, _event: FromSwarm) {}
}

#[derive(Clone)]
struct KeepAliveConnectionHandler;

impl ConnectionHandler for KeepAliveConnectionHandler {
    type FromBehaviour = Infallible;
    type ToBehaviour = Infallible;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn connection_keep_alive(&self) -> bool {
        true
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        libp2p::core::util::unreachable(event)
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>> {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<Self::InboundProtocol, Self::OutboundProtocol>,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound { protocol, .. }) => {
                libp2p::core::util::unreachable(protocol)
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound { protocol, .. }) => {
                libp2p::core::util::unreachable(protocol)
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => match error {
                StreamUpgradeError::Timeout => unreachable!(),
                StreamUpgradeError::Apply(e) => libp2p::core::util::unreachable(e),
                StreamUpgradeError::NegotiationFailed | StreamUpgradeError::Io(_) => {
                    unreachable!("Denied upgrade does not support any protocols")
                }
            },
            ConnectionEvent::AddressChange(_)
            | ConnectionEvent::ListenUpgradeError(_)
            | ConnectionEvent::LocalProtocolsChange(_)
            | ConnectionEvent::RemoteProtocolsChange(_) => {}
            _ => {}
        }
    }
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
            let keep_alive = KeepAliveBehaviour;
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?;
            let ping = ping::Behaviour::new(
                ping::Config::new()
                    .with_interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS))
                    .with_timeout(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS)),
            );

            Ok(DiscoveryBehaviour {
                keep_alive,
                mdns,
                ping,
            })
        })
        .map_err(io::Error::other)?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(IDLE_CONNECTION_TIMEOUT_SECS))
        })
        .build();

    swarm
        .listen_on(
            format!("/ip4/0.0.0.0/tcp/{listen_port}")
                .parse()
                .map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;

    let mut seen_peers = HashSet::new();
    let mut connected_peers = HashSet::new();
    let mut dialing_peers = HashSet::new();
    let mut pending_retries = HashMap::new();
    loop {
        let maybe_event = if let Some(delay) = next_retry_delay(&pending_retries) {
            tokio::select! {
                event = swarm.select_next_some() => Some(event),
                _ = tokio::time::sleep(delay) => None,
            }
        } else {
            Some(swarm.select_next_some().await)
        };

        if maybe_event.is_none() {
            let now = Instant::now();
            let due_peers: Vec<PeerId> = pending_retries
                .iter()
                .filter_map(|(peer_id, state)| {
                    if state.next_attempt_at <= now {
                        Some(*peer_id)
                    } else {
                        None
                    }
                })
                .collect();

            for peer_id in due_peers {
                if connected_peers.contains(&peer_id) || dialing_peers.contains(&peer_id) {
                    pending_retries.remove(&peer_id);
                    continue;
                }

                if !seen_peers.contains(&peer_id) {
                    pending_retries.remove(&peer_id);
                    continue;
                }

                match swarm.dial(peer_id) {
                    Ok(()) => {
                        dialing_peers.insert(peer_id);
                        pending_retries.remove(&peer_id);
                        println!("reconnect dial started: {peer_id}");
                    }
                    Err(err) => {
                        let delay = schedule_retry(&mut pending_retries, peer_id);
                        eprintln!(
                            "reconnect dial failed for {peer_id}: {err}; next retry in {delay:?}"
                        );
                    }
                }
            }

            continue;
        }

        match maybe_event.expect("event should be present when not handling retry tick") {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("p2p listening on {address}");
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                for (peer_id, peer_addr) in peers {
                    if peer_id == local_peer_id {
                        continue;
                    }

                    let is_new_discovery = seen_peers.insert(peer_id);

                    if connected_peers.contains(&peer_id) || dialing_peers.contains(&peer_id) {
                        continue;
                    }

                    let retry_was_pending = pending_retries.remove(&peer_id).is_some();

                    if is_new_discovery || retry_was_pending {
                        println!("new node discovered: {peer_id} at {peer_addr}");
                        match swarm.dial(peer_id) {
                            // Dial by peer id lets mDNS provide/refresh usable addresses.
                            Ok(()) => {
                                dialing_peers.insert(peer_id);
                                println!("dialing peer: {peer_id} (discovered at {peer_addr})");
                            }
                            Err(err) => {
                                let delay = schedule_retry(&mut pending_retries, peer_id);
                                eprintln!(
                                    "failed to dial discovered peer {peer_id}: {err}; next retry in {delay:?}"
                                );
                            }
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                for (peer_id, _) in peers {
                    if connected_peers.contains(&peer_id) {
                        continue;
                    }

                    pending_retries.remove(&peer_id);
                    dialing_peers.remove(&peer_id);
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
                        connected_peers.remove(&peer);
                        dialing_peers.remove(&peer);
                        seen_peers.insert(peer);
                        let delay = schedule_retry(&mut pending_retries, peer);
                        eprintln!(
                            "peer unreachable (heartbeat failed): {peer}: {err}; reconnect in {delay:?}"
                        );
                    }
                }
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                cause,
                ..
            } => {
                connected_peers.remove(&peer_id);
                dialing_peers.remove(&peer_id);
                seen_peers.insert(peer_id);
                let delay = schedule_retry(&mut pending_retries, peer_id);
                match cause {
                    Some(ConnectionError::KeepAliveTimeout) => {
                        eprintln!(
                            "peer disconnected: {peer_id} (keepalive timeout expired - no protocol requested connection keep-alive); reconnect in {delay:?}"
                        )
                    }
                    Some(ConnectionError::IO(err)) => {
                        eprintln!(
                            "peer disconnected: {peer_id} (I/O error: {err}); reconnect in {delay:?}"
                        )
                    }
                    None => eprintln!("peer disconnected: {peer_id}; reconnect in {delay:?}"),
                }
            }
            SwarmEvent::Dialing { peer_id, .. } => match peer_id {
                Some(peer_id) => {
                    dialing_peers.insert(peer_id);
                    println!("dial started: {peer_id}")
                }
                None => println!("dial started: unknown peer"),
            },
            SwarmEvent::ConnectionEstablished {
                peer_id,
                endpoint,
                ..
            } => {
                connected_peers.insert(peer_id);
                dialing_peers.remove(&peer_id);
                seen_peers.insert(peer_id);
                pending_retries.remove(&peer_id);
                println!("peer connected: {peer_id} via {endpoint:?}");
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => match peer_id {
                Some(peer_id) => {
                    dialing_peers.remove(&peer_id);

                    if connected_peers.contains(&peer_id) {
                        eprintln!(
                            "outgoing dial failed for {peer_id}, but peer is already connected: {error}"
                        );
                    } else {
                        seen_peers.insert(peer_id);
                        let delay = schedule_retry(&mut pending_retries, peer_id);
                        eprintln!(
                            "outgoing dial failed for {peer_id}: {error}; next retry in {delay:?}"
                        );
                    }
                }
                None => eprintln!("outgoing dial failed for unknown peer: {error}"),
            },
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
