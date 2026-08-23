use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    error::Error,
    io,
    path::{Path, PathBuf},
    task::{Context, Poll},
    time::Duration,
};

use libp2p::{
    core::{transport::PortUse, upgrade::DeniedUpgrade, Endpoint, Multiaddr},
    futures::StreamExt,
    identity,
    mdns,
    noise,
    ping,
    request_response::{self, OutboundRequestId},
    swarm::{
        behaviour::{FromSwarm, NetworkBehaviour, ToSwarm},
        ConnectionDenied, ConnectionError, ConnectionHandler, ConnectionHandlerEvent,
        ConnectionId, SwarmEvent,
        THandler, THandlerInEvent, THandlerOutEvent,
        handler::{ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        StreamUpgradeError, SubstreamProtocol,
    },
    tcp, yamux, PeerId, StreamProtocol, SwarmBuilder,
};
use tokio::sync::mpsc;

use crate::db::Database;
use crate::sync::{self, SyncRequest, SyncResponse, SyncState};

const KEY_FILENAME: &str = "p2p_identity.key";
const DEFAULT_P2P_PORT: u16 = 4001;
// Keep this well below the idle timeout so periodic ping traffic keeps
// otherwise quiet connections open.
const HEARTBEAT_INTERVAL_SECS: u64 = 10;
const HEARTBEAT_TIMEOUT_SECS: u64 = 8;
const IDLE_CONNECTION_TIMEOUT_SECS: u64 = 60;
// Generous per-request timeout: large chunks on slow links must not time out.
// 5 minutes allows for very slow connections and large chunks.
const SYNC_REQUEST_TIMEOUT_SECS: u64 = 300;
// How often a failed chunk request is retried before the transfer is aborted.
const MAX_FETCH_RETRIES: u32 = 5;
// Stale transfers are aborted after 10 minutes without progress.
const STALE_TRANSFER_TIMEOUT_SECS: u64 = 600;
const SYNC_PROTOCOL: &str = "/nacs-backend/sync/1";

#[derive(libp2p::swarm::NetworkBehaviour)]
struct DiscoveryBehaviour {
    keep_alive: KeepAliveBehaviour,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    sync: request_response::cbor::Behaviour<SyncRequest, SyncResponse>,
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

pub async fn run_discovery(
    base_dir: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    database: Database,
    mut announce_rx: mpsc::UnboundedReceiver<sync::FileChangeEvent>,
) -> io::Result<()> {
    let data_dir = data_dir.as_ref().to_path_buf();
    let listen_port = configured_peer_port()?;
    let key_path = key_path(base_dir.as_ref(), listen_port);
    let local_key = load_or_create_identity(&key_path).await?;
    let local_peer_id = PeerId::from(local_key.public());

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
            let sync = request_response::cbor::Behaviour::new(
                [(
                    StreamProtocol::new(SYNC_PROTOCOL),
                    request_response::ProtocolSupport::Full,
                )],
                request_response::Config::default()
                    .with_request_timeout(Duration::from_secs(SYNC_REQUEST_TIMEOUT_SECS)),
            );

            Ok(DiscoveryBehaviour {
                keep_alive,
                mdns,
                ping,
                sync,
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
    let mut sync_state = SyncState::new();
    let fetch_queue = sync::FetchQueue::new();
    let mut chunk_reader = sync::ChunkReader::new();
    let mut pending_fetches: HashMap<OutboundRequestId, (PeerId, String, u64, u32)> = HashMap::new();
    let mut cleanup_interval = tokio::time::interval(Duration::from_secs(30));
    cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        // Drain all queued fetch requests before waiting for new swarm events.
        while let Some(request) = fetch_queue.pop().await {
            if let SyncRequest::FetchFile { path, offset } = &request {
                let path = path.clone();
                let offset = *offset;
                if let Some(peer) = connected_peers.iter().next().copied() {
                    let id = swarm.behaviour_mut().sync.send_request(&peer, request);
                    pending_fetches.insert(id, (peer, path, offset, 0));
                } else {
                    // No peer connected right now (e.g. reconnect in
                    // progress); put the request back so it is retried
                    // once a peer is available again.
                    fetch_queue.push(request).await;
                    break;
                }
            }
        }

        tokio::select! {
            _ = cleanup_interval.tick() => {
                let removed = sync_state
                    .cleanup_stale_transfers(Duration::from_secs(STALE_TRANSFER_TIMEOUT_SECS))
                    .await;
                if removed > 0 {
                    eprintln!("sync: cleaned up {removed} stale transfer(s)");
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("p2p listening on {address}");
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Discovered(peers))) => {
                        for (peer_id, peer_addr) in peers {
                            if peer_id == local_peer_id {
                                continue;
                            }

                            println!("new node discovered: {peer_id} at {peer_addr}");

                            if connected_peers.contains(&peer_id) || dialing_peers.contains(&peer_id) {
                                continue;
                            }

                            if seen_peers.insert(peer_id) {
                                match swarm.dial(peer_id) {
                                    // Dial by peer id lets mDNS provide/refresh usable addresses.
                                    Ok(()) => {
                                        dialing_peers.insert(peer_id);
                                        println!("dialing peer: {peer_id} (discovered at {peer_addr})");
                                    }
                                    Err(err) => {
                                        eprintln!("failed to dial discovered peer {peer_id}: {err}");
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
                                eprintln!("peer unreachable (heartbeat failed): {peer}: {err}");
                            }
                        }
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::Message {
                        peer,
                        message,
                        ..
                    })) => match message {
                        request_response::Message::Request { request, channel, .. } => match request {
                            SyncRequest::Manifest => {
                                let response = match database.manifest().await {
                                    Ok(manifest) => SyncResponse::Manifest(manifest),
                                    Err(err) => {
                                        eprintln!("failed to build manifest for {peer}: {err}");
                                        SyncResponse::Manifest(Default::default())
                                    }
                                };
                                let _ = swarm.behaviour_mut().sync.send_response(channel, response);
                            }
                            SyncRequest::FetchFile { path, offset } => {
                                let response = chunk_reader.read_chunk(&data_dir, &path, offset).await;
                                let _ = swarm.behaviour_mut().sync.send_response(channel, response);
                            }
                            SyncRequest::Event(event) => {
                                let _ = swarm.behaviour_mut().sync.send_response(channel, SyncResponse::Ack);
                                if let Err(err) = sync::handle_incoming_event(&data_dir, &database, &mut sync_state, &fetch_queue, peer, event).await {
                                    eprintln!("failed to apply incoming p2p event from {peer}: {err}");
                                }
                            }
                        },
                        request_response::Message::Response { response, .. } => match response {
                            SyncResponse::Manifest(remote_manifest) => match database.manifest().await {
                                Ok(local_manifest) => {
                                    let actions = sync::diff_manifests(&local_manifest, &remote_manifest);
                                    sync::apply_manifest_actions(
                                        &data_dir,
                                        &database,
                                        &mut sync_state,
                                        &fetch_queue,
                                        peer,
                                        actions,
                                    )
                                    .await;
                                }
                                Err(err) => eprintln!("failed to read local manifest for reconciliation with {peer}: {err}"),
                            },
                            other => {
                                match sync::handle_chunk_response(&mut sync_state, &database, peer, other).await {
                                    Ok(requests) => {
                                        for request in requests {
                                            if let SyncRequest::FetchFile { path, offset } = &request {
                                                let path = path.clone();
                                                let offset = *offset;
                                                let id = swarm.behaviour_mut().sync.send_request(&peer, request);
                                                pending_fetches.insert(id, (peer, path, offset, 0));
                                            }
                                        }
                                    }
                                    Err(err) => eprintln!("failed to process chunk response from {peer}: {err}"),
                                }
                            }
                        },
                    },
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::OutboundFailure {
                        peer,
                        request_id,
                        error,
                        ..
                    })) => {
                        let Some((fetch_peer, path, offset, attempts)) = pending_fetches.remove(&request_id) else {
                            eprintln!("sync request to {peer} failed (untracked): {error}");
                            continue;
                        };
                        if !sync_state.is_pending(fetch_peer, &path) {
                            continue;
                        }
                        if attempts + 1 >= MAX_FETCH_RETRIES {
                            // After too many failures, restart the transfer from the
                            // current write offset instead of giving up entirely.
                            eprintln!(
                                "sync: too many failures for {path} from {fetch_peer}; restarting from last write position"
                            );
                            let restart_requests = sync_state.restart_stalled(fetch_peer, &path).await;
                            for request in restart_requests {
                                if let SyncRequest::FetchFile { path, offset } = &request {
                                    let path = path.clone();
                                    let offset = *offset;
                                    let id = swarm.behaviour_mut().sync.send_request(&fetch_peer, request);
                                    pending_fetches.insert(id, (fetch_peer, path, offset, 0));
                                }
                            }
                            continue;
                        }
                        eprintln!(
                            "sync: chunk request for {path} from {fetch_peer} failed ({error}); retrying (attempt {})",
                            attempts + 2
                        );
                        match sync_state.retry_chunk(fetch_peer, &path, offset).await {
                            Some(request) => {
                                let id = swarm.behaviour_mut().sync.send_request(&fetch_peer, request);
                                pending_fetches.insert(id, (fetch_peer, path, offset, attempts + 1));
                            }
                            None => {
                                // All chunks already requested; a reply may
                                // still arrive, so keep the transfer pending.
                            }
                        }
                    },
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::InboundFailure {
                        peer,
                        error,
                        ..
                    })) => {
                        eprintln!("inbound sync request from {peer} failed: {error}");
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::ResponseSent { .. })) => {},
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        cause,
                        ..
                    } => {
                        connected_peers.remove(&peer_id);
                        dialing_peers.remove(&peer_id);
                        seen_peers.insert(peer_id);
                        if let Err(err) = sync_state.cancel_peer(peer_id).await {
                            eprintln!("failed to cancel pending sync transfers for {peer_id}: {err}");
                        }
                        match cause {
                            Some(ConnectionError::KeepAliveTimeout) => {
                                eprintln!("peer disconnected: {peer_id} (keepalive timeout expired - no protocol requested connection keep-alive)")
                            }
                            Some(ConnectionError::IO(err)) => {
                                eprintln!("peer disconnected: {peer_id} (I/O error: {err})")
                            }
                            None => eprintln!("peer disconnected: {peer_id}"),
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
                        println!("peer connected: {peer_id} via {endpoint:?}");
                        // Kick off initial reconciliation with the newly (re)connected peer.
                        swarm.behaviour_mut().sync.send_request(&peer_id, SyncRequest::Manifest);
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
                                eprintln!("outgoing dial failed for {peer_id}: {error}");
                            }
                        }
                        None => eprintln!("outgoing dial failed for unknown peer: {error}"),
                    },
                    _ => {}
                }
            }
            Some(change_event) = announce_rx.recv() => {
                for peer in connected_peers.iter() {
                    swarm
                        .behaviour_mut()
                        .sync
                        .send_request(peer, SyncRequest::Event(change_event.clone()));
                }
            }
        }
    }
}

fn key_path(base_dir: &Path, listen_port: u16) -> PathBuf {
    if listen_port == DEFAULT_P2P_PORT {
        base_dir.join(KEY_FILENAME)
    } else {
        base_dir.join(format!("p2p_identity-{listen_port}.key"))
    }
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
