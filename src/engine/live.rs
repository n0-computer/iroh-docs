#![allow(missing_docs)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use iroh::{address_lookup::memory::MemoryLookup, Endpoint, EndpointAddr, EndpointId, PublicKey};
use iroh_blobs::{
    api::{
        blobs::BlobStatus,
        downloader::{ContentDiscovery, DownloadRequest, Downloader, SplitStrategy},
        Store,
    },
    Hash, HashAndFormat,
};
use iroh_gossip::net::Gossip;
use n0_future::{task::JoinSet, time::SystemTime, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{self, mpsc, oneshot};
use tracing::{debug, error, info, instrument, trace, warn, Instrument, Span};

// use super::gossip::{GossipActor, ToGossipActor};
use super::state::{NamespaceStates, Origin, SyncReason};
use crate::{
    actor::{OpenOpts, SyncHandle},
    engine::gossip::GossipState,
    metrics::Metrics,
    net::{
        connect_and_sync, handle_connection, AbortReason, AcceptError, AcceptOutcome, ConnectError,
        SyncFinished,
    },
    store::Query,
    AuthorHeads, ContentStatus, NamespaceId, SignedEntry,
};

/// An iroh-docs operation
///
/// This is the message that is broadcast over iroh-gossip.
#[derive(Debug, Clone, Serialize, Deserialize, strum::Display)]
pub enum Op {
    /// A new entry was inserted into the document.
    Put(SignedEntry),
    /// A peer now has content available for a hash.
    ContentReady(Hash),
    /// We synced with another peer, here's the news.
    SyncReport(SyncReport),
}

/// Report of a successful sync with the new heads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReport {
    namespace: NamespaceId,
    /// Encoded [`AuthorHeads`]
    heads: Vec<u8>,
}

/// Messages to the sync actor
#[derive(derive_more::Debug, strum::Display)]
pub enum ToLiveActor {
    StartSync {
        namespace: NamespaceId,
        peers: Vec<EndpointAddr>,
        #[debug("onsehot::Sender")]
        reply: sync::oneshot::Sender<anyhow::Result<()>>,
    },
    Leave {
        namespace: NamespaceId,
        kill_subscribers: bool,
        #[debug("onsehot::Sender")]
        reply: sync::oneshot::Sender<anyhow::Result<()>>,
    },
    Shutdown {
        reply: sync::oneshot::Sender<()>,
    },
    Subscribe {
        namespace: NamespaceId,
        #[debug("sender")]
        sender: async_channel::Sender<Event>,
        #[debug("oneshot::Sender")]
        reply: sync::oneshot::Sender<Result<()>>,
    },
    HandleConnection {
        conn: iroh::endpoint::Connection,
    },
    AcceptSyncRequest {
        namespace: NamespaceId,
        peer: PublicKey,
        #[debug("oneshot::Sender")]
        reply: sync::oneshot::Sender<AcceptOutcome>,
    },

    IncomingSyncReport {
        from: PublicKey,
        report: SyncReport,
    },
    NeighborContentReady {
        namespace: NamespaceId,
        node: PublicKey,
        hash: Hash,
    },
    NeighborUp {
        namespace: NamespaceId,
        peer: PublicKey,
    },
    NeighborDown {
        namespace: NamespaceId,
        peer: PublicKey,
    },
}

/// Events informing about actions of the live sync progress.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, strum::Display)]
pub enum Event {
    /// The content of an entry was downloaded and is now available at the local node
    ContentReady {
        /// The content hash of the newly available entry content
        hash: Hash,
    },
    /// We have a new neighbor in the swarm.
    NeighborUp(PublicKey),
    /// We lost a neighbor in the swarm.
    NeighborDown(PublicKey),
    /// A set-reconciliation sync finished.
    SyncFinished(SyncEvent),
    /// All pending content is now ready.
    ///
    /// This event is only emitted after a sync completed and `Self::SyncFinished` was emitted at
    /// least once. It signals that all currently pending downloads have been completed.
    ///
    /// Receiving this event does not guarantee that all content in the document is available. If
    /// blobs failed to download, this event will still be emitted after all operations completed.
    PendingContentReady,
}

type SyncConnectRes = (
    NamespaceId,
    PublicKey,
    SyncReason,
    Result<SyncFinished, ConnectError>,
);
type SyncAcceptRes = Result<SyncFinished, AcceptError>;
type DownloadRes = (NamespaceId, Hash, Result<(), anyhow::Error>);

// Currently peers might double-sync in both directions.
pub struct LiveActor {
    /// Receiver for actor messages.
    inbox: mpsc::Receiver<ToLiveActor>,
    sync: SyncHandle,
    endpoint: Endpoint,
    bao_store: Store,
    downloader: Downloader,
    memory_lookup: MemoryLookup,
    replica_events_tx: async_channel::Sender<crate::Event>,
    replica_events_rx: async_channel::Receiver<crate::Event>,

    /// Send messages to self.
    /// Note: Must not be used in methods called from `Self::run` directly to prevent deadlocks.
    /// Only clone into newly spawned tasks.
    sync_actor_tx: mpsc::Sender<ToLiveActor>,
    gossip: GossipState,

    /// Running sync futures (from connect).
    running_sync_connect: JoinSet<SyncConnectRes>,
    /// Running sync futures (from accept).
    running_sync_accept: JoinSet<SyncAcceptRes>,
    /// Running download futures.
    download_tasks: JoinSet<DownloadRes>,
    /// Content hashes which are wanted but not yet queued because no provider was found.
    missing_hashes: HashSet<Hash>,
    /// Hashes for which a provider was signalled while a download for them was already
    /// running. The running download does not see providers added after it started, so a
    /// failure of that download is retried once with the providers known by then.
    retry_after_failure: HashSet<Hash>,
    /// Content hashes queued in downloader.
    queued_hashes: QueuedHashes,
    /// Nodes known to have a hash
    hash_providers: ProviderNodes,
    /// Per-blob backoff so a blob with no provider isn't re-requested on every re-scan.
    blob_recheck_backoff: HashMap<Hash, BlobRecheckBackoff>,
    /// Last incomplete-blob scan time per (namespace, peer), to debounce the scan.
    last_incomplete_check: HashMap<(NamespaceId, PublicKey), n0_future::time::Instant>,
    /// Minimum interval between incomplete-blob scans per (namespace, peer); zero scans every sync.
    incomplete_blob_check_interval: std::time::Duration,

    /// Subscribers to actor events
    subscribers: SubscribersMap,

    /// Sync state per replica and peer
    state: NamespaceStates,
    metrics: Arc<Metrics>,
}
impl LiveActor {
    /// Create the live actor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sync: SyncHandle,
        endpoint: Endpoint,
        gossip: Gossip,
        bao_store: Store,
        downloader: Downloader,
        inbox: mpsc::Receiver<ToLiveActor>,
        sync_actor_tx: mpsc::Sender<ToLiveActor>,
        metrics: Arc<Metrics>,
        incomplete_blob_check_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        let (replica_events_tx, replica_events_rx) = async_channel::bounded(1024);
        let gossip_state = GossipState::new(gossip, sync.clone(), sync_actor_tx.clone());
        let memory_lookup = MemoryLookup::new();
        endpoint.address_lookup()?.add(memory_lookup.clone());
        Ok(Self {
            inbox,
            sync,
            replica_events_rx,
            replica_events_tx,
            endpoint,
            memory_lookup,
            gossip: gossip_state,
            bao_store,
            downloader,
            sync_actor_tx,
            running_sync_connect: Default::default(),
            running_sync_accept: Default::default(),
            subscribers: Default::default(),
            download_tasks: Default::default(),
            state: Default::default(),
            missing_hashes: Default::default(),
            retry_after_failure: Default::default(),
            queued_hashes: Default::default(),
            hash_providers: Default::default(),
            blob_recheck_backoff: Default::default(),
            last_incomplete_check: Default::default(),
            incomplete_blob_check_interval: incomplete_blob_check_interval
                .unwrap_or(DEFAULT_INCOMPLETE_BLOB_CHECK_DEBOUNCE),
            metrics,
        })
    }

    /// Run the actor loop.
    pub async fn run(mut self) -> Result<()> {
        let shutdown_reply = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "Error during shutdown");
        }
        drop(self);
        match shutdown_reply {
            Ok(reply) => {
                reply.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        let mut i = 0;
        loop {
            i += 1;
            trace!(?i, "tick wait");
            self.metrics.doc_live_tick_main.inc();
            tokio::select! {
                biased;
                msg = self.inbox.recv() => {
                    let msg = msg.context("to_actor closed")?;
                    trace!(?i, %msg, "tick: to_actor");
                    self.metrics.doc_live_tick_actor.inc();
                    match msg {
                        ToLiveActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            self.on_actor_message(msg).await.context("on_actor_message")?;
                        }
                    }
                }
                event = self.replica_events_rx.recv() => {
                    trace!(?i, "tick: replica_event");
                    self.metrics.doc_live_tick_replica_event.inc();
                    let event = event.context("replica_events closed")?;
                    if let Err(err) = self.on_replica_event(event).await {
                        error!(?err, "Failed to process replica event");
                    }
                }
                Some(res) = self.running_sync_connect.join_next(), if !self.running_sync_connect.is_empty() => {
                    trace!(?i, "tick: running_sync_connect");
                    self.metrics.doc_live_tick_running_sync_connect.inc();
                    let (namespace, peer, reason, res) = res.context("running_sync_connect closed")?;
                    self.on_sync_via_connect_finished(namespace, peer, reason, res).await;

                }
                Some(res) = self.running_sync_accept.join_next(), if !self.running_sync_accept.is_empty() => {
                    trace!(?i, "tick: running_sync_accept");
                    self.metrics.doc_live_tick_running_sync_accept.inc();
                    let res = res.context("running_sync_accept closed")?;
                    self.on_sync_via_accept_finished(res).await;
                }
                Some(res) = self.download_tasks.join_next(), if !self.download_tasks.is_empty() => {
                    trace!(?i, "tick: pending_downloads");
                    self.metrics.doc_live_tick_pending_downloads.inc();
                    let (namespace, hash, res) = res.context("pending_downloads closed")?;
                    self.on_download_ready(namespace, hash, res).await;
                }
                res = self.gossip.progress(), if !self.gossip.is_empty() => {
                    if let Err(error) = res {
                        warn!(?error, "gossip state failed");
                    }
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToLiveActor) -> anyhow::Result<bool> {
        match msg {
            ToLiveActor::Shutdown { .. } => {
                unreachable!("handled in run");
            }
            ToLiveActor::IncomingSyncReport { from, report } => {
                self.on_sync_report(from, report).await
            }
            ToLiveActor::NeighborUp { namespace, peer } => {
                debug!(peer = %peer.fmt_short(), namespace = %namespace.fmt_short(), "neighbor up");
                self.sync_with_peer(namespace, peer, SyncReason::NewNeighbor);
                self.subscribers
                    .send(&namespace, Event::NeighborUp(peer))
                    .await;
            }
            ToLiveActor::NeighborDown { namespace, peer } => {
                debug!(peer = %peer.fmt_short(), namespace = %namespace.fmt_short(), "neighbor down");
                self.subscribers
                    .send(&namespace, Event::NeighborDown(peer))
                    .await;
            }
            ToLiveActor::StartSync {
                namespace,
                peers,
                reply,
            } => {
                let res = self.start_sync(namespace, peers).await;
                reply.send(res).ok();
            }
            ToLiveActor::Leave {
                namespace,
                kill_subscribers,
                reply,
            } => {
                let res = self.leave(namespace, kill_subscribers).await;
                reply.send(res).ok();
            }
            ToLiveActor::Subscribe {
                namespace,
                sender,
                reply,
            } => {
                self.subscribers.subscribe(namespace, sender);
                reply.send(Ok(())).ok();
            }
            ToLiveActor::HandleConnection { conn } => {
                self.handle_connection(conn).await;
            }
            ToLiveActor::AcceptSyncRequest {
                namespace,
                peer,
                reply,
            } => {
                let outcome = self.accept_sync_request(namespace, peer);
                reply.send(outcome).ok();
            }
            ToLiveActor::NeighborContentReady {
                namespace,
                node,
                hash,
            } => {
                self.on_neighbor_content_ready(namespace, node, hash).await;
            }
        };
        Ok(true)
    }

    #[instrument("connect", skip_all, fields(peer = %peer.fmt_short(), namespace = %namespace.fmt_short()))]
    fn sync_with_peer(&mut self, namespace: NamespaceId, peer: PublicKey, reason: SyncReason) {
        if !self.state.start_connect(&namespace, peer, reason) {
            return;
        }
        let endpoint = self.endpoint.clone();
        let sync = self.sync.clone();
        let metrics = self.metrics.clone();
        let fut = async move {
            let res = connect_and_sync(
                &endpoint,
                &sync,
                namespace,
                EndpointAddr::new(peer),
                Some(&metrics),
            )
            .await;
            (namespace, peer, reason, res)
        }
        .instrument(Span::current());
        self.running_sync_connect.spawn(fut);
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        // cancel all subscriptions
        self.subscribers.clear();
        let (gossip_shutdown_res, _store) = tokio::join!(
            // quit the gossip topics and task loops.
            self.gossip.shutdown(),
            // shutdown sync thread
            self.sync.shutdown()
        );
        gossip_shutdown_res?;
        // TODO: abort_all and join_next all JoinSets to catch panics
        // (they are aborted on drop, but that swallows panics)
        Ok(())
    }

    async fn start_sync(
        &mut self,
        namespace: NamespaceId,
        mut peers: Vec<EndpointAddr>,
    ) -> Result<()> {
        debug!(?namespace, peers = peers.len(), "start sync");
        // update state to allow sync
        if !self.state.is_syncing(&namespace) {
            let opts = OpenOpts::default()
                .sync()
                .subscribe(self.replica_events_tx.clone());
            self.sync.open(namespace, opts).await?;
            self.state.insert(namespace);
        }
        // add the peers stored for this document
        match self.sync.get_sync_peers(namespace).await {
            Ok(None) => {
                // no peers for this document
            }
            Ok(Some(known_useful_peers)) => {
                let as_node_addr = known_useful_peers.into_iter().filter_map(|peer_id_bytes| {
                    // peers are stored as bytes, don't fail the operation if they can't be
                    // decoded: simply ignore the peer
                    match PublicKey::from_bytes(&peer_id_bytes) {
                        Ok(public_key) => Some(EndpointAddr::new(public_key)),
                        Err(_signing_error) => {
                            warn!("potential db corruption: peers per doc can't be decoded");
                            None
                        }
                    }
                });
                peers.extend(as_node_addr);
            }
            Err(e) => {
                // try to continue if peers per doc can't be read since they are not vital for sync
                warn!(%e, "db error reading peers per document")
            }
        }
        self.join_peers(namespace, peers).await?;
        Ok(())
    }

    async fn leave(
        &mut self,
        namespace: NamespaceId,
        kill_subscribers: bool,
    ) -> anyhow::Result<()> {
        // self.subscribers.remove(&namespace);
        if self.state.remove(&namespace) {
            self.sync.set_sync(namespace, false).await?;
            self.sync
                .unsubscribe(namespace, self.replica_events_tx.clone())
                .await?;
            self.sync.close(namespace).await?;
            self.gossip.quit(&namespace);
        }
        if kill_subscribers {
            self.subscribers.remove(&namespace);
        }
        Ok(())
    }

    async fn join_peers(&mut self, namespace: NamespaceId, peers: Vec<EndpointAddr>) -> Result<()> {
        let mut peer_ids = Vec::new();

        // add addresses of peers to our endpoint address book
        for peer in peers.into_iter() {
            let peer_id = peer.id;
            // adding a node address without any addressing info fails with an error,
            // but we still want to include those peers because endpoint address lookup might find addresses for them
            if !peer.is_empty() {
                self.memory_lookup.add_endpoint_info(peer);
            }
            peer_ids.push(peer_id);
        }

        // tell gossip to join
        self.gossip.join(namespace, peer_ids.clone()).await?;

        if !peer_ids.is_empty() {
            // trigger initial sync with initial peers
            for peer in peer_ids {
                self.sync_with_peer(namespace, peer, SyncReason::DirectJoin);
            }
        }
        Ok(())
    }

    #[instrument("connect", skip_all, fields(peer = %peer.fmt_short(), namespace = %namespace.fmt_short()))]
    async fn on_sync_via_connect_finished(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
        reason: SyncReason,
        result: Result<SyncFinished, ConnectError>,
    ) {
        match result {
            Err(ConnectError::RemoteAbort(AbortReason::AlreadySyncing)) => {
                debug!(?reason, "remote abort, already syncing");
            }
            res => {
                self.on_sync_finished(
                    namespace,
                    peer,
                    Origin::Connect(reason),
                    res.map_err(Into::into),
                )
                .await
            }
        }
    }

    #[instrument("accept", skip_all, fields(peer = %fmt_accept_peer(&res), namespace = %fmt_accept_namespace(&res)))]
    async fn on_sync_via_accept_finished(&mut self, res: Result<SyncFinished, AcceptError>) {
        match res {
            Ok(state) => {
                self.on_sync_finished(state.namespace, state.peer, Origin::Accept, Ok(state))
                    .await
            }
            Err(AcceptError::Abort { reason, .. }) if reason == AbortReason::AlreadySyncing => {
                // In case we aborted the sync: do nothing (our outgoing sync is in progress)
                debug!(?reason, "aborted by us");
            }
            Err(err) => {
                if let (Some(peer), Some(namespace)) = (err.peer(), err.namespace()) {
                    self.on_sync_finished(
                        namespace,
                        peer,
                        Origin::Accept,
                        Err(anyhow::Error::from(err)),
                    )
                    .await;
                } else {
                    debug!(?err, "failed before reading the first message");
                }
            }
        }
    }

    async fn on_sync_finished(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
        origin: Origin,
        result: Result<SyncFinished>,
    ) {
        match &result {
            Err(ref err) => {
                warn!(?origin, ?err, "sync failed");
            }
            Ok(ref details) => {
                info!(
                    sent = %details.outcome.num_sent,
                    recv = %details.outcome.num_recv,
                    t_connect = ?details.timings.connect,
                    t_process = ?details.timings.process,
                    "sync finished",
                );

                // register the peer as useful for the document
                if let Err(e) = self
                    .sync
                    .register_useful_peer(namespace, *peer.as_bytes())
                    .await
                {
                    debug!(%e, "failed to register peer for document")
                }

                // broadcast a sync report to our neighbors, but only if we received new entries.
                if details.outcome.num_recv > 0 {
                    info!("broadcast sync report to neighbors");
                    match details
                        .outcome
                        .heads_received
                        .encode(Some(self.gossip.max_message_size()))
                    {
                        Err(err) => warn!(?err, "Failed to encode author heads for sync report"),
                        Ok(heads) => {
                            let report = SyncReport { namespace, heads };
                            self.broadcast_neighbors(namespace, &Op::SyncReport(report))
                                .await;
                        }
                    }
                }

                // Queue downloads for entries with incomplete blobs.
                self.check_incomplete_blobs(namespace, peer).await;
            }
        };

        let result_for_event = match &result {
            Ok(details) => Ok(details.into()),
            Err(err) => Err(err.to_string()),
        };

        let Some((started, resync)) = self.state.finish(&namespace, peer, &origin, result) else {
            return;
        };

        let ev = SyncEvent {
            peer,
            origin,
            result: result_for_event,
            finished: SystemTime::now(),
            started,
        };
        self.subscribers
            .send(&namespace, Event::SyncFinished(ev))
            .await;

        // Check if there are queued pending content hashes for this namespace.
        // If hashes are pending, mark this namespace to be eglible for a PendingContentReady event once all
        // pending hashes have completed downloading.
        // If no hashes are pending, emit the PendingContentReady event right away. The next
        // PendingContentReady event may then only be emitted after the next sync completes.
        if self.queued_hashes.contains_namespace(&namespace) {
            self.state.set_may_emit_ready(&namespace, true);
        } else {
            self.subscribers
                .send(&namespace, Event::PendingContentReady)
                .await;
            self.state.set_may_emit_ready(&namespace, false);
        }

        if resync {
            self.sync_with_peer(namespace, peer, SyncReason::Resync);
        }
    }

    async fn broadcast_neighbors(&mut self, namespace: NamespaceId, op: &Op) {
        if !self.state.is_syncing(&namespace) {
            return;
        }

        let msg = match postcard::to_stdvec(op) {
            Ok(msg) => msg,
            Err(err) => {
                error!(?err, ?op, "Failed to serialize message:");
                return;
            }
        };
        // TODO: We should debounce and merge these neighbor announcements likely.
        self.gossip
            .broadcast_neighbors(&namespace, msg.into())
            .await;
    }

    async fn on_download_ready(
        &mut self,
        namespace: NamespaceId,
        hash: Hash,
        res: Result<(), anyhow::Error>,
    ) {
        let completed_namespaces = self.queued_hashes.remove_hash(&hash);
        debug!(namespace=%namespace.fmt_short(), hash=%hash.fmt_short(), success=res.is_ok(), error=?res.as_ref().err(), completed_namespaces=completed_namespaces.len(), "download ready");
        if res.is_ok() {
            // Recovered: drop the re-request backoff.
            self.blob_recheck_backoff.remove(&hash);
            self.subscribers
                .send(&namespace, Event::ContentReady { hash })
                .await;
            // Inform our neighbors that we have new content ready.
            self.broadcast_neighbors(namespace, &Op::ContentReady(hash))
                .await;
        } else {
            self.missing_hashes.insert(hash);
            // The downloader snapshots its provider list when a download starts. If a
            // provider was signalled while this download was running (for example the
            // author's direct copy or a `ContentReady` arriving while the after-sync re-scan
            // was already trying a peer without the content), that provider was never asked.
            // Retry once with the providers known now; a further retry needs a further signal.
            if self.retry_after_failure.remove(&hash) {
                if let Some(provider) = self.hash_providers.any_provider(&hash) {
                    debug!(namespace=%namespace.fmt_short(), hash=%hash.fmt_short(), "retrying failed download with providers signalled meanwhile");
                    self.start_download(namespace, hash, provider, false).await;
                }
            }
        }
        for namespace in completed_namespaces.iter() {
            if let Some(true) = self.state.may_emit_ready(namespace) {
                self.subscribers
                    .send(namespace, Event::PendingContentReady)
                    .await;
            }
        }
    }

    async fn on_neighbor_content_ready(
        &mut self,
        namespace: NamespaceId,
        node: EndpointId,
        hash: Hash,
    ) {
        self.start_download(namespace, hash, node, true).await;
    }

    #[instrument("on_sync_report", skip_all, fields(peer = %from.fmt_short(), namespace = %report.namespace.fmt_short()))]
    async fn on_sync_report(&mut self, from: PublicKey, report: SyncReport) {
        let namespace = report.namespace;
        if !self.state.is_syncing(&namespace) {
            return;
        }
        let heads = match AuthorHeads::decode(&report.heads) {
            Ok(heads) => heads,
            Err(err) => {
                warn!(?err, "failed to decode AuthorHeads");
                return;
            }
        };
        match self.sync.has_news_for_us(report.namespace, heads).await {
            Ok(Some(updated_authors)) => {
                info!(%updated_authors, "news reported: sync now");
                self.sync_with_peer(report.namespace, from, SyncReason::SyncReport);
            }
            Ok(None) => {
                debug!("no news reported: nothing to do");
            }
            Err(err) => {
                warn!("sync actor error: {err:?}");
            }
        }
    }

    async fn on_replica_event(&mut self, event: crate::Event) -> Result<()> {
        match event {
            crate::Event::LocalInsert { namespace, entry } => {
                debug!(namespace=%namespace.fmt_short(), "replica event: LocalInsert");
                // A new entry was inserted locally. Broadcast a gossip message.
                if self.state.is_syncing(&namespace) {
                    let op = Op::Put(entry.clone());
                    let message = postcard::to_stdvec(&op)?.into();
                    self.gossip.broadcast(&namespace, message).await;
                }
            }
            crate::Event::RemoteInsert {
                namespace,
                entry,
                from,
                should_download,
                remote_content_status,
            } => {
                debug!(namespace=%namespace.fmt_short(), "replica event: RemoteInsert");
                // A new entry was inserted from initial sync or gossip. Queue downloading the
                // content.
                if should_download {
                    let hash = entry.content_hash();
                    if matches!(remote_content_status, ContentStatus::Complete) {
                        let node_id = PublicKey::from_bytes(&from)?;
                        self.start_download(namespace, hash, node_id, false).await;
                    } else {
                        self.missing_hashes.insert(hash);
                    }
                }
            }
        }

        Ok(())
    }

    /// Scan a namespace for entries whose blobs are not yet complete and queue downloads.
    ///
    /// This is called after a successful sync so that blobs that were missed on earlier
    /// syncs (e.g. because the first peer lacked the content, or due to a restart losing
    /// the in-memory `missing_hashes` set) get another download attempt with `peer` as a
    /// candidate provider.
    async fn check_incomplete_blobs(&mut self, namespace: NamespaceId, peer: PublicKey) {
        // Debounce per (namespace, peer): a newly-synced peer scans promptly, repeated syncs with
        // the same peer collapse.
        let debounce_key = (namespace, peer);
        if let Some(last) = self.last_incomplete_check.get(&debounce_key) {
            if last.elapsed() < self.incomplete_blob_check_interval {
                return;
            }
        }
        self.last_incomplete_check
            .insert(debounce_key, n0_future::time::Instant::now());

        let policy = match self.sync.get_download_policy(namespace).await {
            Ok(policy) => policy,
            Err(e) => {
                warn!(%e, "failed to get download policy for incomplete blob check");
                return;
            }
        };

        let (tx, mut rx) = irpc::channel::mpsc::channel::<crate::api::RpcResult<SignedEntry>>(64);
        if let Err(e) = self
            .sync
            .get_many(namespace, Query::all().build(), tx)
            .await
        {
            warn!(%e, "failed to get entries for incomplete blob check");
            return;
        }

        let mut queued = 0u64;
        while let Ok(Some(entry_result)) = rx.recv().await {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            if !policy.matches(entry.entry()) {
                continue;
            }
            if entry.content_len() == 0 {
                continue;
            }
            let hash = entry.content_hash();
            // Already complete: drop any stale backoff.
            if matches!(
                self.bao_store.blobs().status(hash).await,
                Ok(BlobStatus::Complete { .. })
            ) {
                self.blob_recheck_backoff.remove(&hash);
                continue;
            }
            // Always try a peer not yet offered for this hash ("retry from second peer"); only
            // back off re-requests to peers already tried.
            let untried_peer = !self.hash_providers.contains(&hash, &peer);
            if untried_peer
                || self
                    .blob_recheck_backoff
                    .entry(hash)
                    .or_default()
                    .should_request(MAX_BLOB_RECHECK_BACKOFF)
            {
                self.start_download(namespace, hash, peer, false).await;
                queued += 1;
            }
        }
        if queued > 0 {
            debug!(namespace=%namespace.fmt_short(), %queued, "queued incomplete blob downloads after sync");
        }
    }

    async fn start_download(
        &mut self,
        namespace: NamespaceId,
        hash: Hash,
        node: PublicKey,
        only_if_missing: bool,
    ) {
        let entry_status = self.bao_store.blobs().status(hash).await;
        if matches!(entry_status, Ok(BlobStatus::Complete { .. })) {
            self.missing_hashes.remove(&hash);
            return;
        }
        self.hash_providers.insert(hash, node);
        let queued = self.queued_hashes.contains_hash(&hash);
        debug!(namespace=%namespace.fmt_short(), hash=%hash.fmt_short(), node=%node.fmt_short(), only_if_missing, queued, missing=self.missing_hashes.contains(&hash), "start download");
        if queued {
            self.queued_hashes.insert(hash, namespace);
            // Any provider signal for a running download, even from a provider we already
            // knew, means the running attempt may be stale: arm a retry on failure.
            self.retry_after_failure.insert(hash);
        } else if !only_if_missing || self.missing_hashes.contains(&hash) {
            let req = DownloadRequest::new(
                HashAndFormat::raw(hash),
                self.hash_providers.clone(),
                SplitStrategy::None,
            );
            let handle = self.downloader.download_with_opts(req);

            self.queued_hashes.insert(hash, namespace);
            self.missing_hashes.remove(&hash);
            self.download_tasks.spawn(async move {
                (
                    namespace,
                    hash,
                    handle.await.map_err(|e| anyhow::anyhow!(e)),
                )
            });
        }
    }

    #[instrument("accept", skip_all)]
    pub async fn handle_connection(&mut self, conn: iroh::endpoint::Connection) {
        let to_actor_tx = self.sync_actor_tx.clone();
        let accept_request_cb = move |namespace, peer| {
            let to_actor_tx = to_actor_tx.clone();
            async move {
                let (reply_tx, reply_rx) = oneshot::channel();
                to_actor_tx
                    .send(ToLiveActor::AcceptSyncRequest {
                        namespace,
                        peer,
                        reply: reply_tx,
                    })
                    .await
                    .ok();
                match reply_rx.await {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        warn!(
                            "accept request callback failed to retrieve reply from actor: {err:?}"
                        );
                        AcceptOutcome::Reject(AbortReason::InternalServerError)
                    }
                }
            }
            .boxed()
        };
        debug!("incoming connection");
        let sync = self.sync.clone();
        let metrics = self.metrics.clone();
        self.running_sync_accept.spawn(
            async move { handle_connection(sync, conn, accept_request_cb, Some(&metrics)).await }
                .instrument(Span::current()),
        );
    }

    pub fn accept_sync_request(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
    ) -> AcceptOutcome {
        self.state
            .accept_request(&self.endpoint.id(), &namespace, peer)
    }
}

/// Event emitted when a sync operation completes
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncEvent {
    /// Peer we synced with
    pub peer: PublicKey,
    /// Origin of the sync exchange
    pub origin: Origin,
    /// Timestamp when the sync finished
    pub finished: SystemTime,
    /// Timestamp when the sync started
    pub started: SystemTime,
    /// Result of the sync operation
    pub result: std::result::Result<SyncDetails, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncDetails {
    /// Number of entries received
    pub entries_received: usize,
    /// Number of entries sent
    pub entries_sent: usize,
}

impl From<&SyncFinished> for SyncDetails {
    fn from(value: &SyncFinished) -> Self {
        Self {
            entries_received: value.outcome.num_recv,
            entries_sent: value.outcome.num_sent,
        }
    }
}

#[derive(Debug, Default)]
struct SubscribersMap(HashMap<NamespaceId, Subscribers>);

impl SubscribersMap {
    fn subscribe(&mut self, namespace: NamespaceId, sender: async_channel::Sender<Event>) {
        self.0.entry(namespace).or_default().subscribe(sender);
    }

    async fn send(&mut self, namespace: &NamespaceId, event: Event) -> bool {
        debug!(namespace=%namespace.fmt_short(), %event, "emit event");
        let Some(subscribers) = self.0.get_mut(namespace) else {
            return false;
        };

        if !subscribers.send(event).await {
            self.0.remove(namespace);
        }
        true
    }

    fn remove(&mut self, namespace: &NamespaceId) {
        self.0.remove(namespace);
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

#[derive(Debug, Default)]
struct QueuedHashes {
    by_hash: HashMap<Hash, HashSet<NamespaceId>>,
    by_namespace: HashMap<NamespaceId, HashSet<Hash>>,
}

/// Default interval for the after-sync incomplete-blob scan when the builder does not override it.
const DEFAULT_INCOMPLETE_BLOB_CHECK_DEBOUNCE: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Cap, in re-scan passes, on the per-blob re-request backoff window.
const MAX_BLOB_RECHECK_BACKOFF: u32 = 64;

/// Per-blob exponential backoff for the incomplete-blob re-scan: request, then skip a doubling
/// number of passes (capped at [`MAX_BLOB_RECHECK_BACKOFF`]) before the next request.
#[derive(Debug, Default)]
struct BlobRecheckBackoff {
    /// Remaining re-scan passes to skip before the next re-request.
    skip: u32,
    /// Current backoff window in passes; doubles after each re-request up to the cap.
    window: u32,
}

impl BlobRecheckBackoff {
    /// Advance one re-scan pass; return whether the blob should be re-requested now.
    fn should_request(&mut self, max_window: u32) -> bool {
        if self.skip > 0 {
            self.skip -= 1;
            return false;
        }
        self.window = self.window.saturating_mul(2).clamp(1, max_window);
        self.skip = self.window;
        true
    }
}

#[derive(Debug, Clone, Default)]
struct ProviderNodes(Arc<std::sync::Mutex<HashMap<Hash, HashSet<EndpointId>>>>);

impl ProviderNodes {
    /// Record `node` as a candidate provider for `hash`.
    fn insert(&self, hash: Hash, node: EndpointId) {
        self.0
            .lock()
            .expect("poisoned")
            .entry(hash)
            .or_default()
            .insert(node);
    }

    /// Any node recorded as a candidate provider for `hash`, if there is one.
    fn any_provider(&self, hash: &Hash) -> Option<EndpointId> {
        self.0
            .lock()
            .expect("poisoned")
            .get(hash)
            .and_then(|nodes| nodes.iter().next().copied())
    }

    /// Whether `node` has already been recorded as a candidate provider for `hash`.
    fn contains(&self, hash: &Hash, node: &EndpointId) -> bool {
        self.0
            .lock()
            .expect("poisoned")
            .get(hash)
            .is_some_and(|nodes| nodes.contains(node))
    }
}

impl ContentDiscovery for ProviderNodes {
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let nodes = self
            .0
            .lock()
            .expect("poisoned")
            .get(&hash.hash)
            .into_iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(n0_future::stream::iter(nodes))
    }
}

impl QueuedHashes {
    fn insert(&mut self, hash: Hash, namespace: NamespaceId) {
        self.by_hash.entry(hash).or_default().insert(namespace);
        self.by_namespace.entry(namespace).or_default().insert(hash);
    }

    /// Remove a hash from the set of queued hashes.
    ///
    /// Returns a list of namespaces that are now complete (have no queued hashes anymore).
    fn remove_hash(&mut self, hash: &Hash) -> Vec<NamespaceId> {
        let namespaces = self.by_hash.remove(hash).unwrap_or_default();
        let mut removed_namespaces = vec![];
        for namespace in namespaces {
            if let Some(hashes) = self.by_namespace.get_mut(&namespace) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.by_namespace.remove(&namespace);
                    removed_namespaces.push(namespace);
                }
            }
        }
        removed_namespaces
    }

    fn contains_hash(&self, hash: &Hash) -> bool {
        self.by_hash.contains_key(hash)
    }

    fn contains_namespace(&self, namespace: &NamespaceId) -> bool {
        self.by_namespace.contains_key(namespace)
    }
}

#[derive(Debug, Default)]
struct Subscribers(Vec<async_channel::Sender<Event>>);

impl Subscribers {
    fn subscribe(&mut self, sender: async_channel::Sender<Event>) {
        self.0.push(sender)
    }

    async fn send(&mut self, event: Event) -> bool {
        let futs = self.0.iter().map(|sender| sender.send(event.clone()));
        let res = futures_buffered::join_all(futs).await;
        // reverse the order so removing does not shift remaining indices
        for (i, res) in res.into_iter().enumerate().rev() {
            if res.is_err() {
                self.0.remove(i);
            }
        }
        !self.0.is_empty()
    }
}

fn fmt_accept_peer(res: &Result<SyncFinished, AcceptError>) -> String {
    match res {
        Ok(res) => res.peer.fmt_short().to_string(),
        Err(err) => err
            .peer()
            .map(|x| x.fmt_short().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}

fn fmt_accept_namespace(res: &Result<SyncFinished, AcceptError>) -> String {
    match res {
        Ok(res) => res.namespace.fmt_short(),
        Err(err) => err
            .namespace()
            .map(|x| x.fmt_short())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sync_remove() {
        let pk = PublicKey::from_bytes(&[1; 32]).unwrap();
        let (a_tx, a_rx) = async_channel::unbounded();
        let (b_tx, b_rx) = async_channel::unbounded();
        let mut subscribers = Subscribers::default();
        subscribers.subscribe(a_tx);
        subscribers.subscribe(b_tx);
        drop(a_rx);
        drop(b_rx);
        subscribers.send(Event::NeighborUp(pk)).await;
    }

    /// Build a `LiveActor` without running its loop, so `start_download` /
    /// `on_download_ready` can be driven directly and deterministically.
    async fn test_actor() -> Result<LiveActor> {
        let sync = SyncHandle::spawn(crate::store::Store::memory(), None, "test".into());
        let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let bao_store: Store = iroh_blobs::store::mem::MemStore::new().into();
        let downloader = bao_store.downloader(&endpoint);
        let (tx, rx) = mpsc::channel(8);
        LiveActor::new(
            sync.clone(),
            endpoint,
            gossip,
            bao_store,
            downloader,
            rx,
            tx,
            sync.metrics().clone(),
            None,
        )
    }

    /// A provider signalled while a download is already running is not seen by that download
    /// (the downloader snapshots its provider list). A failure must then be retried once with
    /// the providers known by then, and a failure without any new signal must park the hash.
    #[tokio::test]
    async fn failed_download_is_retried_with_providers_signalled_meanwhile() -> Result<()> {
        let mut actor = test_actor().await?;
        let namespace = crate::NamespaceSecret::new(&mut rand::rng()).id();
        let hash = Hash::new(b"content that nobody serves");
        let peer_a = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let peer_b = iroh::SecretKey::from_bytes(&[2; 32]).public();

        // Parked hash, first signal: one download starts, nothing armed.
        actor.missing_hashes.insert(hash);
        actor.start_download(namespace, hash, peer_a, true).await;
        assert_eq!(actor.download_tasks.len(), 1);
        assert!(!actor.retry_after_failure.contains(&hash));

        // Same provider signalled again while running: arms the retry (the running attempt
        // may already have given up on it).
        actor.start_download(namespace, hash, peer_a, true).await;
        assert_eq!(actor.download_tasks.len(), 1);
        assert!(actor.retry_after_failure.contains(&hash));
        // A second provider while running.
        actor.start_download(namespace, hash, peer_b, true).await;
        assert!(actor.hash_providers.contains(&hash, &peer_b));

        // The running download fails: retried once, flag consumed, hash still queued.
        actor
            .on_download_ready(namespace, hash, Err(anyhow::anyhow!("unable to download")))
            .await;
        assert_eq!(actor.download_tasks.len(), 2, "failure must be retried");
        assert!(!actor.retry_after_failure.contains(&hash));
        assert!(actor.queued_hashes.contains_hash(&hash));
        assert!(!actor.missing_hashes.contains(&hash));

        // The retry fails without any signal in between: parked, no third attempt.
        actor
            .on_download_ready(namespace, hash, Err(anyhow::anyhow!("unable to download")))
            .await;
        assert_eq!(
            actor.download_tasks.len(),
            2,
            "no retry without a new signal"
        );
        assert!(actor.missing_hashes.contains(&hash));
        assert!(!actor.queued_hashes.contains_hash(&hash));
        Ok(())
    }

    #[test]
    fn blob_recheck_backoff_is_exponential_and_capped() {
        let max = 8;
        let mut b = BlobRecheckBackoff::default();
        let requested: Vec<u32> = (0..30).filter(|_| b.should_request(max)).collect();
        // First pass requests, then gaps double (1, 2, 4, 8) and stay capped at 8.
        assert_eq!(requested, vec![0, 2, 5, 10, 19, 28]);
    }
}
