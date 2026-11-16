use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    error::Error,
    io,
    path::{Path, PathBuf},
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

use libp2p::{
    PeerId, StreamProtocol, SwarmBuilder,
    core::{Endpoint, Multiaddr, transport::PortUse, upgrade::DeniedUpgrade},
    futures::StreamExt,
    identity, mdns, noise, ping, request_response,
    swarm::{
        ConnectionDenied, ConnectionError, ConnectionHandler, ConnectionHandlerEvent, ConnectionId,
        DialError, StreamUpgradeError, SubstreamProtocol, Swarm, SwarmEvent, THandler,
        THandlerInEvent, THandlerOutEvent,
        behaviour::{FromSwarm, NetworkBehaviour, ToSwarm, toggle::Toggle},
        handler::{
            ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
        },
    },
    tcp, yamux,
};
use tokio::sync::{mpsc, oneshot};

use crate::db::{Database, Manifest};
use crate::sync::{self, FileChangeEvent, SyncRequest, SyncResponse, SyncState};

/// Query types for the P2P worker to expose status to the REST API.
#[derive(Debug)]
pub enum P2pQuery {
    GetStatus(oneshot::Sender<P2pStatus>),
    GetPeers(oneshot::Sender<Vec<PeerInfo>>),
}

/// Current sync status for the REST API.
#[derive(Debug, Clone, serde::Serialize)]
pub struct P2pStatus {
    pub is_syncing: bool,
    pub current_transfer: Option<P2pTransferInfo>,
    pub queue_length: usize,
    pub peers_connected: usize,
}

/// Information about an in-progress file transfer (P2P version for API).
#[derive(Debug, Clone, serde::Serialize)]
pub struct P2pTransferInfo {
    pub path: String,
    pub peer_id: String,
    pub event_kind: String,
    pub progress_bytes: u64,
    pub total_bytes: u64,
    pub username: String,
}

/// Information about a connected peer.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PeerInfo {
    pub peer_id: String,
    pub connected_since: SystemTime,
    pub addresses: Vec<String>,
    pub last_heartbeat: SystemTime,
    pub is_synced: bool,
}

struct PeerConnectionInfo {
    peer_id: PeerId,
    connected_since: SystemTime,
    addresses: HashMap<ConnectionId, String>,
    last_heartbeat: SystemTime,
    has_synced: bool,
}

const KEY_FILENAME: &str = "p2p_identity.key";
const DEFAULT_P2P_PORT: u16 = 4001;
const HEARTBEAT_INTERVAL_SECS: u64 = 10;
const HEARTBEAT_TIMEOUT_SECS: u64 = 8;
const MDNS_QUERY_INTERVAL_SECS: u64 = 15;
const MDNS_TTL_SECS: u64 = 60;
const IDLE_CONNECTION_TIMEOUT_SECS: u64 = 300; // 5 minutes to allow slow chunk transfers
const SYNC_REQUEST_TIMEOUT_SECS: u64 = 1800; // 30 minutes for large file transfers
const SYNC_PROTOCOL: &str = "/nacs-backend/sync/1";
// Response size limit: allow large files (200 MB for chunked transfers)
const SYNC_RESPONSE_SIZE_MAXIMUM: u64 = 200 * 1024 * 1024;
const MAX_BACKGROUND_REQUESTS: usize = 32;

enum BackgroundResult {
    Response {
        peer: PeerId,
        request_id: request_response::InboundRequestId,
        channel: request_response::ResponseChannel<SyncResponse>,
        response: io::Result<SyncResponse>,
    },
    Reconcile {
        peer: PeerId,
        connection: ConnectionId,
        remote: Manifest,
        local: io::Result<Manifest>,
    },
}

#[derive(libp2p::swarm::NetworkBehaviour)]
struct DiscoveryBehaviour {
    keep_alive: KeepAliveBehaviour,
    mdns: Toggle<mdns::tokio::Behaviour>,
    ping: ping::Behaviour,
    sync: request_response::cbor::Behaviour<SyncRequest, SyncResponse>,
}

// Ping streams are intentionally ignored for keep-alive.
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
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<Self::InboundProtocol, Self::OutboundProtocol>,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol, ..
            }) => libp2p::core::util::unreachable(protocol),
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol, ..
            }) => libp2p::core::util::unreachable(protocol),
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
    announce_rx: mpsc::UnboundedReceiver<FileChangeEvent>,
    query_rx: mpsc::Receiver<P2pQuery>,
) -> io::Result<()> {
    let data_dir = data_dir.as_ref().to_path_buf();
    let listen_port = configured_peer_port()?;
    let key_path = key_path(base_dir.as_ref(), listen_port);
    let local_key = load_or_create_identity(&key_path).await?;
    let local_peer_id = PeerId::from(local_key.public());

    println!("p2p node started: {local_peer_id}");
    println!("sync: configured CBOR response limit = {SYNC_RESPONSE_SIZE_MAXIMUM} bytes");

    let mut swarm = build_discovery_swarm(local_key, true)?;
    swarm
        .listen_on(
            format!("/ip4/0.0.0.0/tcp/{listen_port}")
                .parse()
                .map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?;

    run_swarm(swarm, data_dir, database, announce_rx, query_rx).await
}

fn build_discovery_swarm(
    local_key: identity::Keypair,
    enable_mdns: bool,
) -> io::Result<Swarm<DiscoveryBehaviour>> {
    let swarm = SwarmBuilder::with_existing_identity(local_key)
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
            let mdns = if enable_mdns {
                Some(
                    mdns::tokio::Behaviour::new(
                        mdns::Config {
                            query_interval: Duration::from_secs(MDNS_QUERY_INTERVAL_SECS),
                            ttl: Duration::from_secs(MDNS_TTL_SECS),
                            ..Default::default()
                        },
                        peer_id,
                    )
                    .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync>)?,
                )
            } else {
                None
            };
            let ping = ping::Behaviour::new(
                ping::Config::new()
                    .with_interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS))
                    .with_timeout(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS)),
            );
            let sync = request_response::cbor::Behaviour::with_codec(
                request_response::cbor::codec::Codec::<SyncRequest, SyncResponse>::default()
                    .set_response_size_maximum(SYNC_RESPONSE_SIZE_MAXIMUM),
                [(
                    StreamProtocol::new(SYNC_PROTOCOL),
                    request_response::ProtocolSupport::Full,
                )],
                request_response::Config::default()
                    .with_request_timeout(Duration::from_secs(SYNC_REQUEST_TIMEOUT_SECS)),
            );

            Ok(DiscoveryBehaviour {
                keep_alive,
                mdns: mdns.into(),
                ping,
                sync,
            })
        })
        .map_err(io::Error::other)?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(IDLE_CONNECTION_TIMEOUT_SECS))
        })
        .build();

    Ok(swarm)
}

async fn run_swarm(
    mut swarm: Swarm<DiscoveryBehaviour>,
    data_dir: PathBuf,
    database: Database,
    mut announce_rx: mpsc::UnboundedReceiver<FileChangeEvent>,
    mut query_rx: mpsc::Receiver<P2pQuery>,
) -> io::Result<()> {
    let local_peer_id = *swarm.local_peer_id();
    let mut connected_peers = HashSet::new();
    let mut sync_state = SyncState::new();
    let mut peer_info: HashMap<PeerId, PeerConnectionInfo> = HashMap::new();
    let mut reconnect_interval = tokio::time::interval(Duration::from_secs(5));
    let mut background = tokio::task::JoinSet::new();
    let mut inbound_jobs = HashMap::new();

    loop {
        tokio::select! {
            Some(result) = background.join_next(), if !background.is_empty() => {
                match result {
                    Ok(BackgroundResult::Response { peer, request_id, channel, response }) => {
                        inbound_jobs.remove(&request_id);
                        match response {
                            Ok(response) => {
                                let is_manifest = matches!(response, SyncResponse::Manifest(_));
                                if swarm.behaviour_mut().sync.send_response(channel, response).is_err() {
                                    eprintln!("sync: response channel closed for {peer} ({request_id})");
                                } else if is_manifest && let Some(info) = peer_info.get_mut(&peer) {
                                    info.has_synced = true;
                                }
                            }
                            Err(err) => eprintln!("sync: request preparation failed for {peer} ({request_id}): {err}"),
                        }
                    }
                    Ok(BackgroundResult::Reconcile { peer, connection, remote, local }) => {
                        if !peer_info.get(&peer).is_some_and(|info| info.addresses.contains_key(&connection)) {
                            continue;
                        }
                        match local {
                            Ok(local) => {
                                if let Some(info) = peer_info.get_mut(&peer) {
                                    info.has_synced = true;
                                }
                                let actions = sync::diff_manifests(&local, &remote);
                                let requests = sync::apply_manifest_actions(
                                    &data_dir, &database, &mut sync_state, peer, actions,
                                ).await;
                                for request in requests {
                                    swarm.behaviour_mut().sync.send_request(&peer, request);
                                }
                            }
                            Err(err) => eprintln!("failed to read local manifest for reconciliation with {peer}: {err}"),
                        }
                    }
                    Err(err) => {
                        inbound_jobs.retain(|_, (_, handle): &mut (PeerId, tokio::task::AbortHandle)| !handle.is_finished());
                        if !err.is_cancelled() {
                            eprintln!("sync: background task failed: {err}");
                        }
                    }
                }
            }
            _ = reconnect_interval.tick() => {
                if let Some(mdns) = swarm.behaviour().mdns.as_ref() {
                    let discovered = discovered_peer_ids(mdns.discovered_nodes().copied(), local_peer_id);
                    for peer_id in discovered {
                        dial_discovered_peer(&mut swarm, peer_id);
                    }
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

                            dial_discovered_peer(&mut swarm, peer_id);
                        }
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Mdns(mdns::Event::Expired(peers))) => {
                        for (peer_id, peer_addr) in peers {
                            eprintln!("discovered address expired: {peer_id} at {peer_addr}");
                        }
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Ping(ping::Event { peer, connection, result })) => {
                        match result {
                            Ok(rtt) => {
                                println!("heartbeat ok: {peer} rtt={rtt:?}");
                                if let Some(info) = peer_info.get_mut(&peer) {
                                    info.last_heartbeat = SystemTime::now();
                                }
                            }
                            Err(err) => {
                                close_failed_heartbeat(&mut swarm, peer, connection, &err);
                            }
                        }
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::Message {
                        peer,
                        connection_id,
                        message,
                        ..
                    })) => match message {
                        request_response::Message::Request { request_id, request, channel, .. } => match request {
                            SyncRequest::Manifest => {
                                if background.len() >= MAX_BACKGROUND_REQUESTS {
                                    eprintln!("sync: too many pending requests; dropping manifest request from {peer}");
                                    continue;
                                }
                                let database = database.clone();
                                let handle = background.spawn(async move {
                                    let response = database.manifest().await.map(SyncResponse::Manifest);
                                    BackgroundResult::Response { peer, request_id, channel, response }
                                });
                                inbound_jobs.insert(request_id, (peer, handle));
                            }
                            SyncRequest::FetchFile { path } => {
                                if background.len() >= MAX_BACKGROUND_REQUESTS {
                                    eprintln!("sync: too many pending requests; dropping fetch request from {peer} for {path}");
                                    continue;
                                }
                                let data_dir = data_dir.clone();
                                let handle = background.spawn(async move {
                                    let response = sync::handle_fetch_request(&data_dir, &path).await;
                                    BackgroundResult::Response { peer, request_id, channel, response: Ok(response) }
                                });
                                inbound_jobs.insert(request_id, (peer, handle));
                            }
                            SyncRequest::FetchFileChunk { path, offset } => {
                                let response = sync::handle_fetch_chunk(&data_dir, &path, offset).await;
                                let _ = swarm.behaviour_mut().sync.send_response(channel, response);
                            }
                            SyncRequest::Event(event) => {
                                let _ = swarm.behaviour_mut().sync.send_response(channel, SyncResponse::Ack);
                                match sync::handle_incoming_event(&data_dir, &database, &mut sync_state, peer, event).await {
                                    Ok(Some(fetch_request)) => {
                                        let _ = swarm.behaviour_mut().sync.send_request(&peer, fetch_request);
                                    }
                                    Err(err) => {
                                        eprintln!("failed to apply incoming p2p event from {peer}: {err}");
                                    }
                                    Ok(None) => {}
                                }
                            }
                        },
                        request_response::Message::Response { response, .. } => match response {
                            SyncResponse::Manifest(remote) => {
                                if background.len() >= MAX_BACKGROUND_REQUESTS {
                                    eprintln!("sync: too many pending requests; cannot reconcile manifest from {peer}");
                                    continue;
                                }
                                let database = database.clone();
                                background.spawn(async move {
                                    let local = database.manifest().await;
                                    BackgroundResult::Reconcile { peer, connection: connection_id, remote, local }
                                });
                            }
                            SyncResponse::FileStart { path, total_size, checksum } => {
                                if let Ok(Some(next_request)) = sync::handle_file_start(
                                    &data_dir,
                                    &database,
                                    &mut sync_state,
                                    peer,
                                    path,
                                    total_size,
                                    checksum,
                                ).await {
                                    // Send first chunk request
                                    let _ = swarm.behaviour_mut().sync.send_request(&peer, next_request);
                                }
                            }
                            SyncResponse::FileChunk { path, data, offset, is_last } => {
                                if let Ok(Some(next_request)) = sync::handle_file_chunk(
                                    &data_dir,
                                    &database,
                                    &mut sync_state,
                                    path,
                                    data,
                                    offset,
                                    is_last,
                                ).await {
                                    // Send next chunk request or next file request
                                    let _ = swarm.behaviour_mut().sync.send_request(&peer, next_request);
                                }
                            }
                            SyncResponse::NotFound { path } => {
                                if let Some(next_request) = sync::handle_not_found(&mut sync_state, peer, &path).await {
                                    // Send next fetch request if queued
                                    let _ = swarm.behaviour_mut().sync.send_request(&peer, next_request);
                                }
                            }
                            SyncResponse::Ack => {
                                // Event was acknowledged, nothing to do
                            }
                        },
                    },
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::OutboundFailure {
                        peer,
                        request_id,
                        error,
                        ..
                    })) => {
                        eprintln!("sync request to {peer} failed (id={request_id}): {error}");
                        if swarm.is_connected(&peer)
                            && sync_state.current_transfer_peer() == Some(peer)
                            && let Some(retry_request) = sync_state.retry_current()
                            && let Some(transfer_peer) = sync_state.current_transfer_peer()
                        {
                            swarm.behaviour_mut().sync.send_request(&transfer_peer, retry_request);
                        }
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::InboundFailure {
                        peer,
                        request_id,
                        error,
                        ..
                    })) => {
                        if let Some((_, handle)) = inbound_jobs.remove(&request_id) {
                            handle.abort();
                        }
                        eprintln!("inbound sync request from {peer} failed: {error}");
                    }
                    SwarmEvent::Behaviour(DiscoveryBehaviourEvent::Sync(request_response::Event::ResponseSent { .. })) => {},
                    SwarmEvent::ConnectionClosed {
                        peer_id,
                        connection_id,
                        num_established,
                        cause,
                        ..
                    } => {
                        if untrack_connection(&mut connected_peers, &mut peer_info, peer_id, connection_id, num_established) {
                            inbound_jobs.retain(|_, (peer, handle)| {
                                if *peer == peer_id {
                                    handle.abort();
                                    false
                                } else {
                                    true
                                }
                            });
                            match sync_state.cancel_peer(peer_id).await {
                                Ok(Some(request)) => {
                                    if let Some(transfer_peer) = sync_state.current_transfer_peer() {
                                        swarm.behaviour_mut().sync.send_request(&transfer_peer, request);
                                    } else {
                                        eprintln!("sync: canceled peer {peer_id} but next transfer is missing");
                                    }
                                }
                                Ok(None) => {}
                                Err(err) => eprintln!("failed to cancel pending sync transfers for {peer_id}: {err}"),
                            }
                        }
                        match cause {
                            Some(ConnectionError::KeepAliveTimeout) => {
                                eprintln!("connection closed for {peer_id} ({num_established} remaining): keepalive timeout")
                            }
                            Some(ConnectionError::IO(err)) => {
                                eprintln!("connection closed for {peer_id} ({num_established} remaining): I/O error: {err}")
                            }
                            None => eprintln!("connection closed for {peer_id} ({num_established} remaining)"),
                        }
                    }
                    SwarmEvent::Dialing { peer_id, .. } => match peer_id {
                        Some(peer_id) => {
                            println!("dial started: {peer_id}")
                        }
                        None => println!("dial started: unknown peer"),
                    },
                    SwarmEvent::ConnectionEstablished {
                        peer_id,
                        connection_id,
                        num_established,
                        endpoint,
                        ..
                    } => {
                        track_connection(&mut connected_peers, &mut peer_info, peer_id, connection_id, endpoint.get_remote_address().to_string());
                        println!("peer connected: {peer_id} via {endpoint:?}");
                        if num_established.get() == 1 {
                            swarm.behaviour_mut().sync.send_request(&peer_id, SyncRequest::Manifest);
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => match peer_id {
                        Some(peer_id) => {
                            if connected_peers.contains(&peer_id) {
                                eprintln!(
                                    "outgoing dial failed for {peer_id}, but peer is already connected: {error}"
                                );
                            } else {
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
            Some(query) = query_rx.recv() => {
                match query {
                    P2pQuery::GetStatus(reply_tx) => {
                        let current_transfer = sync_state.get_transfer_info().map(|t| P2pTransferInfo {
                            path: t.path,
                            peer_id: t.peer_id.to_string(),
                            event_kind: t.event_kind.as_str().to_string(),
                            progress_bytes: t.progress_bytes,
                            total_bytes: t.total_bytes,
                            username: t.username,
                        });
                        let status = P2pStatus {
                            is_syncing: sync_state.is_busy(),
                            current_transfer,
                            queue_length: sync_state.queue_len(),
                            peers_connected: connected_peers.len(),
                        };
                        let _ = reply_tx.send(status);
                    }
                    P2pQuery::GetPeers(reply_tx) => {
                        let peers: Vec<PeerInfo> = peer_info
                            .values()
                            .filter(|info| connected_peers.contains(&info.peer_id))
                            .map(|info| PeerInfo {
                                peer_id: info.peer_id.to_string(),
                                connected_since: info.connected_since,
                                addresses: info.addresses.values().cloned().collect(),
                                last_heartbeat: info.last_heartbeat,
                                is_synced: info.has_synced,
                            })
                            .collect();
                        let _ = reply_tx.send(peers);
                    }
                }
            }
        }
    }
}

fn discovered_peer_ids(
    peers: impl IntoIterator<Item = PeerId>,
    local_peer: PeerId,
) -> HashSet<PeerId> {
    peers
        .into_iter()
        .filter(|peer| *peer != local_peer)
        .collect()
}

fn dial_discovered_peer<B: NetworkBehaviour>(swarm: &mut Swarm<B>, peer_id: PeerId) {
    if swarm.is_connected(&peer_id) {
        return;
    }

    match swarm.dial(peer_id) {
        Ok(()) => println!("dialing discovered peer: {peer_id}"),
        Err(DialError::DialPeerConditionFalse(_)) => {}
        Err(err) => eprintln!("failed to dial discovered peer {peer_id}: {err}"),
    }
}

fn close_failed_heartbeat<B: NetworkBehaviour>(
    swarm: &mut Swarm<B>,
    peer: PeerId,
    connection: ConnectionId,
    error: &ping::Failure,
) {
    eprintln!("heartbeat failed for {peer} on {connection}: {error}; closing connection");
    if !swarm.close_connection(connection) {
        eprintln!("heartbeat connection {connection} for {peer} was already closed");
    }
}

fn track_connection(
    connected: &mut HashSet<PeerId>,
    peers: &mut HashMap<PeerId, PeerConnectionInfo>,
    peer_id: PeerId,
    connection_id: ConnectionId,
    address: String,
) {
    connected.insert(peer_id);
    let now = SystemTime::now();
    let info = peers.entry(peer_id).or_insert_with(|| PeerConnectionInfo {
        peer_id,
        connected_since: now,
        addresses: HashMap::new(),
        last_heartbeat: now,
        has_synced: false,
    });
    info.addresses.insert(connection_id, address);
}

fn untrack_connection(
    connected: &mut HashSet<PeerId>,
    peers: &mut HashMap<PeerId, PeerConnectionInfo>,
    peer_id: PeerId,
    connection_id: ConnectionId,
    remaining: u32,
) -> bool {
    if let Some(info) = peers.get_mut(&peer_id) {
        info.addresses.remove(&connection_id);
    }
    if remaining != 0 {
        return false;
    }
    connected.remove(&peer_id);
    peers.remove(&peer_id);
    true
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

pub fn peers_requiring_dial(
    discovered: &HashSet<PeerId>,
    connected: &HashSet<PeerId>,
    dialing: &HashSet<PeerId>,
) -> Vec<PeerId> {
    discovered
        .iter()
        .filter(|peer_id| !connected.contains(*peer_id) && !dialing.contains(*peer_id))
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_candidates_follow_live_mdns_addresses_even_if_already_connected() {
        let local = PeerId::random();
        let peer = PeerId::random();
        let other = PeerId::random();

        // mDNS returns a peer once per live address, including connected peers.
        let discovered = discovered_peer_ids([local, peer, peer, other], local);
        assert_eq!(discovered, [peer, other].into_iter().collect());
        // After one of the peer's addresses expires it is still discoverable;
        // after its last address expires it is no longer a reconnect candidate.
        let discovered = discovered_peer_ids([peer], local);
        assert_eq!(discovered, [peer].into_iter().collect());
        let discovered = discovered_peer_ids([], local);
        assert!(discovered.is_empty());
    }

    #[test]
    fn peer_remains_connected_until_its_last_connection_closes() {
        let peer = PeerId::random();
        let first = ConnectionId::new_unchecked(1);
        let second = ConnectionId::new_unchecked(2);
        let mut connected = HashSet::new();
        let mut peers = HashMap::new();

        track_connection(&mut connected, &mut peers, peer, first, "first".into());
        peers.get_mut(&peer).unwrap().has_synced = true;
        track_connection(&mut connected, &mut peers, peer, second, "second".into());
        assert!(peers[&peer].has_synced);
        assert!(!untrack_connection(
            &mut connected,
            &mut peers,
            peer,
            first,
            1
        ));
        assert!(connected.contains(&peer));
        assert_eq!(peers[&peer].addresses.get(&second).unwrap(), "second");
        assert!(untrack_connection(
            &mut connected,
            &mut peers,
            peer,
            second,
            0
        ));
        assert!(!connected.contains(&peer));
        assert!(!peers.contains_key(&peer));
    }

    #[derive(libp2p::swarm::NetworkBehaviour)]
    struct TestBehaviour {
        keep_alive: KeepAliveBehaviour,
        ping: ping::Behaviour,
        sync: request_response::cbor::Behaviour<SyncRequest, SyncResponse>,
    }

    fn test_swarm() -> Swarm<TestBehaviour> {
        SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| TestBehaviour {
                keep_alive: KeepAliveBehaviour,
                ping: ping::Behaviour::new(ping::Config::new()),
                sync: request_response::cbor::Behaviour::new(
                    [(
                        StreamProtocol::new(SYNC_PROTOCOL),
                        request_response::ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                ),
            })
            .unwrap()
            .build()
    }

    #[tokio::test]
    async fn heartbeat_failure_closes_connection_and_allows_redial() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut first = test_swarm();
            let mut second = test_swarm();
            let peer = *second.local_peer_id();
            second.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

            let address = loop {
                if let SwarmEvent::NewListenAddr { address, .. } = second.select_next_some().await {
                    break address;
                }
            };
            first.add_peer_address(peer, address);
            dial_discovered_peer(&mut first, peer);
            // A second discovery while the dial is pending must not poison future attempts.
            dial_discovered_peer(&mut first, peer);

            let mut connection = None;
            while connection.is_none() {
                tokio::select! {
                    event = first.select_next_some() => {
                        if let SwarmEvent::ConnectionEstablished { connection_id, .. } = event {
                            connection = Some(connection_id);
                        }
                    }
                    _ = second.select_next_some() => {}
                }
            }
            // Discovery while already connected must not prevent a later reconnect.
            dial_discovered_peer(&mut first, peer);
            close_failed_heartbeat(&mut first, peer, connection.unwrap(), &ping::Failure::Timeout);
            loop {
                tokio::select! {
                    event = first.select_next_some() => {
                        if let SwarmEvent::ConnectionClosed { peer_id, num_established, .. } = event {
                            assert_eq!(peer_id, peer);
                            assert_eq!(num_established, 0);
                            break;
                        }
                    }
                    _ = second.select_next_some() => {}
                }
            }
            assert!(!first.is_connected(&peer));
            dial_discovered_peer(&mut first, peer);
            loop {
                tokio::select! {
                    event = first.select_next_some() => {
                        if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                            assert_eq!(peer_id, peer);
                            break;
                        }
                    }
                    _ = second.select_next_some() => {}
                }
            }
            assert!(first.is_connected(&peer));
        })
        .await
        .expect("local peer should reconnect after a heartbeat failure");
    }

    #[tokio::test]
    async fn pending_checksum_does_not_block_status_or_other_requests() {
        use crate::db::{ASYNC_CHECKSUM_LIMIT, CHECKSUM_LOCK, CHECKSUM_TEST_LOCK};

        let _test_guard = CHECKSUM_TEST_LOCK.lock().await;
        let base = std::env::temp_dir().join(format!(
            "nacs-p2p-responsive-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let data_dir = base.join("data");
        tokio::fs::create_dir_all(&data_dir).await.unwrap();
        tokio::fs::write(data_dir.join("file.txt"), b"hello")
            .await
            .unwrap();
        let database = Database::open(base.join("sqlite"), &data_dir)
            .await
            .unwrap();

        let mut server =
            build_discovery_swarm(identity::Keypair::generate_ed25519(), false).unwrap();
        server
            .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
            .unwrap();
        let address = loop {
            if let SwarmEvent::NewListenAddr { address, .. } = server.select_next_some().await {
                break address;
            }
        };
        let server_peer = *server.local_peer_id();
        let (_announce_tx, announce_rx) = mpsc::unbounded_channel();
        let (query_tx, query_rx) = mpsc::channel(32);
        let server_task = tokio::spawn(run_swarm(
            server,
            data_dir.clone(),
            database,
            announce_rx,
            query_rx,
        ));

        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let lock_thread = std::thread::spawn(move || {
            let _guard = CHECKSUM_LOCK.lock().unwrap();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        locked_rx.recv().unwrap();

        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut client = test_swarm();
            client.dial(address).unwrap();
            loop {
                if let SwarmEvent::ConnectionEstablished { peer_id, .. } =
                    client.select_next_some().await
                    && peer_id == server_peer
                {
                    break;
                }
            }
            let fetch_id = client.behaviour_mut().sync.send_request(
                &server_peer,
                SyncRequest::FetchFile {
                    path: "/file.txt".into(),
                },
            );
            loop {
                tokio::select! {
                    _ = client.select_next_some() => {}
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if ASYNC_CHECKSUM_LIMIT.available_permits() == 0 {
                            break;
                        }
                    }
                }
            }

            let manifest_id = client
                .behaviour_mut()
                .sync
                .send_request(&server_peer, SyncRequest::Manifest);
            let (reply_tx, reply_rx) = oneshot::channel();
            query_tx.send(P2pQuery::GetStatus(reply_tx)).await.unwrap();
            let status = reply_rx.await.unwrap();
            assert_eq!(status.peers_connected, 1);

            let chunk_id = client.behaviour_mut().sync.send_request(
                &server_peer,
                SyncRequest::FetchFileChunk {
                    path: "/file.txt".into(),
                    offset: 0,
                },
            );
            loop {
                if let SwarmEvent::Behaviour(TestBehaviourEvent::Sync(
                    request_response::Event::Message {
                        message:
                            request_response::Message::Response {
                                request_id,
                                response,
                                ..
                            },
                        ..
                    },
                )) = client.select_next_some().await
                    && request_id == chunk_id
                {
                    assert!(
                        matches!(response, SyncResponse::FileChunk { data, .. } if data == b"hello")
                    );
                    break;
                }
            }

            release_tx.send(()).unwrap();
            lock_thread.join().unwrap();
            let mut fetch_received = false;
            let mut manifest_received = false;
            while !fetch_received || !manifest_received {
                if let SwarmEvent::Behaviour(TestBehaviourEvent::Sync(
                    request_response::Event::Message {
                        message:
                            request_response::Message::Response {
                                request_id,
                                response,
                                ..
                            },
                        ..
                    },
                )) = client.select_next_some().await
                {
                    if request_id == fetch_id {
                        assert!(matches!(
                            response,
                            SyncResponse::FileStart { total_size: 5, .. }
                        ));
                        fetch_received = true;
                    } else if request_id == manifest_id {
                        assert!(matches!(response, SyncResponse::Manifest(_)));
                        manifest_received = true;
                    }
                }
            }

            let (locked_tx, locked_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let lock_thread = std::thread::spawn(move || {
                let _guard = CHECKSUM_LOCK.lock().unwrap();
                locked_tx.send(()).unwrap();
                let _ = release_rx.recv();
            });
            locked_rx.recv().unwrap();
            client.behaviour_mut().sync.send_request(
                &server_peer,
                SyncRequest::FetchFile {
                    path: "/file.txt".into(),
                },
            );
            loop {
                tokio::select! {
                    _ = client.select_next_some() => {}
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if ASYNC_CHECKSUM_LIMIT.available_permits() == 0 {
                            break;
                        }
                    }
                }
            }
            client.disconnect_peer_id(server_peer).unwrap();
            loop {
                if let SwarmEvent::ConnectionClosed { peer_id, .. } =
                    client.select_next_some().await
                    && peer_id == server_peer
                {
                    break;
                }
            }
            loop {
                let (reply_tx, reply_rx) = oneshot::channel();
                query_tx.send(P2pQuery::GetStatus(reply_tx)).await.unwrap();
                if reply_rx.await.unwrap().peers_connected == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            release_tx.send(()).unwrap();
            lock_thread.join().unwrap();
            while ASYNC_CHECKSUM_LIMIT.available_permits() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        server_task.abort();
        let _ = server_task.await;
        tokio::fs::remove_dir_all(&base).await.unwrap();
        result.expect("status and another P2P request should complete while hashing is blocked");
    }
}
