use std::collections::BTreeMap;

use anyhow::Result;
use iroh::EndpointId;
use n0_future::time::{Instant, SystemTime};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    net::{AbortReason, AcceptOutcome, SyncFinished},
    NamespaceId,
};

/// Why we started a sync request
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Copy)]
pub enum SyncReason {
    /// Direct join request via API
    DirectJoin,
    /// Peer showed up as new neighbor in the gossip swarm
    NewNeighbor,
    /// We synced after receiving a sync report that indicated news for us
    SyncReport,
    /// We received a sync report while a sync was running, so run again afterwars
    Resync,
}

/// Why we performed a sync exchange
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum Origin {
    /// We initiated the exchange
    Connect(SyncReason),
    /// A node connected to us and we accepted the exchange
    Accept,
}

/// The state we're in for a node and a namespace
#[derive(Debug, Clone, Default)]
pub enum SyncState {
    #[default]
    Idle,
    Running {
        start: SystemTime,
        origin: Origin,
    },
}

/// Contains an entry for each active (syncing) namespace, and in there an entry for each node we
/// synced with.
#[derive(Default)]
pub struct NamespaceStates(BTreeMap<NamespaceId, NamespaceState>);

#[derive(Default)]
struct NamespaceState {
    nodes: BTreeMap<EndpointId, PeerState>,
    may_emit_ready: bool,
}

impl NamespaceStates {
    /// Are we syncing this namespace?
    pub fn is_syncing(&self, namespace: &NamespaceId) -> bool {
        self.0.contains_key(namespace)
    }

    /// Insert a namespace into the set of syncing namespaces.
    pub fn insert(&mut self, namespace: NamespaceId) {
        self.0.entry(namespace).or_default();
    }

    /// Start a sync request.
    ///
    /// Returns true if the request should be performed, and false if it should be aborted.
    pub fn start_connect(
        &mut self,
        namespace: &NamespaceId,
        node: EndpointId,
        reason: SyncReason,
    ) -> bool {
        match self.entry(namespace, node) {
            None => {
                debug!("abort connect: namespace is not in sync set");
                false
            }
            Some(state) => state.start_connect(reason),
        }
    }

    /// Clear the running outgoing exchange with `node` after the remote aborted it as
    /// already-syncing. Nothing else will finish that exchange, so without this the pair would
    /// stay running forever and every later sync trigger for it would be silently dropped.
    /// Left untouched if a concurrent incoming exchange took the slot over (mutual-dial
    /// tie-break): that exchange finishes through the accept path.
    ///
    /// Returns true if the running outgoing exchange was cleared.
    pub fn abort_connect(
        &mut self,
        namespace: &NamespaceId,
        node: EndpointId,
        reason: SyncReason,
    ) -> bool {
        match self.entry(namespace, node) {
            None => false,
            Some(state) => state.abort_connect(reason),
        }
    }

    /// Accept a sync request.
    ///
    /// Returns the [`AcceptOutcome`] to be performed.
    pub fn accept_request(
        &mut self,
        me: &EndpointId,
        namespace: &NamespaceId,
        node: EndpointId,
    ) -> AcceptOutcome {
        let Some(state) = self.entry(namespace, node) else {
            return AcceptOutcome::Reject(AbortReason::NotFound);
        };
        state.accept_request(me, &node)
    }

    /// Insert a finished sync operation into the state.
    ///
    /// Returns the time when the operation was started, and a `bool` that is true if another sync
    /// request should be triggered right afterwards.
    ///
    /// Returns `None` if the namespace is not syncing or the sync state doesn't expect a finish
    /// event.
    pub fn finish(
        &mut self,
        namespace: &NamespaceId,
        node: EndpointId,
        origin: &Origin,
        result: Result<SyncFinished>,
    ) -> Option<(SystemTime, bool)> {
        let state = self.entry(namespace, node)?;
        state.finish(origin, result)
    }

    /// Set whether a [`super::live::Event::PendingContentReady`] may be emitted once the pending queue
    /// becomes empty.
    ///
    /// This should be set to `true` if there are pending content hashes after a sync finished, and
    /// to `false` whenever a `PendingContentReady` was emitted.
    pub fn set_may_emit_ready(&mut self, namespace: &NamespaceId, value: bool) -> Option<()> {
        let state = self.0.get_mut(namespace)?;
        state.may_emit_ready = value;
        Some(())
    }
    /// Returns whether a [`super::live::Event::PendingContentReady`] event may be emitted once the
    /// pending queue becomes empty.
    ///
    /// If this returns `false`, an event should not be emitted even if the queue becomes empty,
    /// because a currently running sync did not yet terminate. Once it terminates, the event will
    /// be emitted from the handler for finished syncs.
    pub fn may_emit_ready(&mut self, namespace: &NamespaceId) -> Option<bool> {
        let state = self.0.get_mut(namespace)?;
        if state.may_emit_ready {
            state.may_emit_ready = false;
            Some(true)
        } else {
            Some(false)
        }
    }

    /// Remove a namespace from the set of syncing namespaces.
    pub fn remove(&mut self, namespace: &NamespaceId) -> bool {
        self.0.remove(namespace).is_some()
    }

    /// Get the [`PeerState`] for a namespace and node.
    /// If the namespace is syncing and the node so far unknown, initialize and return a default [`PeerState`].
    /// If the namespace is not syncing return None.
    fn entry(&mut self, namespace: &NamespaceId, node: EndpointId) -> Option<&mut PeerState> {
        self.0
            .get_mut(namespace)
            .map(|n| n.nodes.entry(node).or_default())
    }
}

/// State of a node with regard to a namespace.
#[derive(Default)]
struct PeerState {
    state: SyncState,
    resync_requested: bool,
    last_sync: Option<(Instant, Result<SyncFinished>)>,
}

impl PeerState {
    fn finish(
        &mut self,
        origin: &Origin,
        result: Result<SyncFinished>,
    ) -> Option<(SystemTime, bool)> {
        let start = match &self.state {
            SyncState::Running {
                start,
                origin: origin2,
            } => {
                if origin2 != origin {
                    warn!(actual = ?origin, expected = ?origin2, "finished sync origin does not match state")
                }
                Some(*start)
            }
            SyncState::Idle => {
                warn!("sync state finish called but not in running state");
                None
            }
        };

        self.last_sync = Some((Instant::now(), result));
        self.state = SyncState::Idle;
        start.map(|s| (s, self.resync_requested))
    }

    fn start_connect(&mut self, reason: SyncReason) -> bool {
        debug!(?reason, "start connect");
        match self.state {
            // never run two syncs at the same time
            SyncState::Running { .. } => {
                debug!("abort connect: sync already running");
                if matches!(reason, SyncReason::SyncReport) {
                    debug!("resync queued");
                    self.resync_requested = true;
                }
                false
            }
            SyncState::Idle => {
                self.set_sync_running(Origin::Connect(reason));
                true
            }
        }
    }

    fn abort_connect(&mut self, reason: SyncReason) -> bool {
        match &self.state {
            SyncState::Running {
                origin: Origin::Connect(running_reason),
                ..
            } if *running_reason == reason => {
                self.state = SyncState::Idle;
                true
            }
            _ => false,
        }
    }

    fn accept_request(&mut self, me: &EndpointId, node: &EndpointId) -> AcceptOutcome {
        let outcome = match &self.state {
            SyncState::Idle => AcceptOutcome::Allow,
            SyncState::Running { origin, .. } => match origin {
                Origin::Accept => AcceptOutcome::Reject(AbortReason::AlreadySyncing),
                // Incoming sync request while we are dialing ourselves.
                // In this case, compare the binary representations of our and the other node's id
                // to deterministically decide which of the two concurrent connections will succeed.
                Origin::Connect(_reason) => match expected_sync_direction(me, node) {
                    SyncDirection::Accept => AcceptOutcome::Allow,
                    SyncDirection::Connect => AcceptOutcome::Reject(AbortReason::AlreadySyncing),
                },
            },
        };
        if let AcceptOutcome::Allow = outcome {
            self.set_sync_running(Origin::Accept);
        }
        outcome
    }

    fn set_sync_running(&mut self, origin: Origin) {
        self.state = SyncState::Running {
            origin,
            start: SystemTime::now(),
        };
        self.resync_requested = false;
    }
}

#[derive(Debug)]
enum SyncDirection {
    Accept,
    Connect,
}

fn expected_sync_direction(self_node_id: &EndpointId, other_node_id: &EndpointId) -> SyncDirection {
    if self_node_id.as_bytes() > other_node_id.as_bytes() {
        SyncDirection::Accept
    } else {
        SyncDirection::Connect
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    fn namespace() -> NamespaceId {
        NamespaceId::from(&[7u8; 32])
    }

    /// Two deterministic endpoint ids, returned as (lower, higher) by key bytes.
    fn node_pair() -> (EndpointId, EndpointId) {
        let a = SecretKey::from_bytes(&[1u8; 32]).public();
        let b = SecretKey::from_bytes(&[2u8; 32]).public();
        if a.as_bytes() < b.as_bytes() {
            (a, b)
        } else {
            (b, a)
        }
    }

    /// A dial the remote rejected as already-syncing leaves nothing running that could
    /// finish the exchange. `abort_connect` must return the pair to idle so the next
    /// trigger is not silently dropped — without it the pair stays `Running` forever
    /// and every later sync trigger for it is lost.
    #[test]
    fn abort_connect_returns_rejected_dial_to_idle() {
        let namespace = namespace();
        let (peer, _) = node_pair();
        let mut states = NamespaceStates::default();
        states.insert(namespace);

        assert!(states.start_connect(&namespace, peer, SyncReason::DirectJoin));
        // While the dial is in flight the pair is busy: triggers are dropped.
        assert!(!states.start_connect(&namespace, peer, SyncReason::NewNeighbor));

        // Remote answered AlreadySyncing: the dial is dead, clear it.
        assert!(states.abort_connect(&namespace, peer, SyncReason::DirectJoin));

        // The pair accepts sync triggers again.
        assert!(states.start_connect(&namespace, peer, SyncReason::NewNeighbor));
    }

    /// An abort whose reason does not match the running dial belongs to an older
    /// exchange; it must not clear the current one.
    #[test]
    fn stale_abort_does_not_clear_newer_dial() {
        let namespace = namespace();
        let (peer, _) = node_pair();
        let mut states = NamespaceStates::default();
        states.insert(namespace);

        assert!(states.start_connect(&namespace, peer, SyncReason::DirectJoin));
        assert!(states.abort_connect(&namespace, peer, SyncReason::DirectJoin));
        assert!(states.start_connect(&namespace, peer, SyncReason::NewNeighbor));

        // Late abort for the first dial: reason mismatch, nothing cleared.
        assert!(!states.abort_connect(&namespace, peer, SyncReason::DirectJoin));
        // The newer dial is still running.
        assert!(!states.start_connect(&namespace, peer, SyncReason::NewNeighbor));
    }

    /// Mutual dial where the incoming exchange won the tie-break and took the slot
    /// over: the remote abort of our own dial must leave the accept exchange
    /// untouched — it finishes through the accept path.
    #[test]
    fn abort_connect_spares_accept_takeover() {
        let namespace = namespace();
        // `me` compares higher, so the incoming request from `node` wins the tie-break.
        let (node, me) = node_pair();
        let mut states = NamespaceStates::default();
        states.insert(namespace);

        assert!(states.start_connect(&namespace, node, SyncReason::DirectJoin));
        let outcome = states.accept_request(&me, &namespace, node);
        assert!(matches!(outcome, AcceptOutcome::Allow));

        // Our dial comes back rejected; the slot now belongs to the accept exchange.
        assert!(!states.abort_connect(&namespace, node, SyncReason::DirectJoin));
        // Still busy until the accept exchange finishes.
        assert!(!states.start_connect(&namespace, node, SyncReason::DirectJoin));
    }
}
