//! Network implementation of the iroh-docs protocol

use std::{
    future::Future,
    time::{Duration, Instant},
};

use iroh::{endpoint::get_remote_node_id, key::PublicKey, Endpoint, NodeAddr};
#[cfg(feature = "metrics")]
use iroh_metrics::inc;
use serde::{Deserialize, Serialize};
use tracing::{debug, error_span, trace, Instrument};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::{
    actor::SyncHandle,
    net::codec::{run_alice, BobState},
    NamespaceId, SyncOutcome,
};

/// The ALPN identifier for the iroh-docs protocol
pub const DOCS_ALPN: &[u8] = b"/iroh-sync/1";

mod codec;

/// Connect to a peer and sync a replica
pub async fn connect_and_sync(
    endpoint: &Endpoint,
    sync: &SyncHandle,
    namespace: NamespaceId,
    peer: NodeAddr,
) -> Result<SyncFinished, ConnectError> {
    let t_start = Instant::now();
    let peer_id = peer.node_id;
    trace!("connect");
    let connection = endpoint
        .connect(peer, DOCS_ALPN)
        .await
        .map_err(ConnectError::connect)?;

    let (mut send_stream, mut recv_stream) =
        connection.open_bi().await.map_err(ConnectError::connect)?;

    let t_connect = t_start.elapsed();
    debug!(?t_connect, "connected");

    let res = run_alice(&mut send_stream, &mut recv_stream, sync, namespace, peer_id).await;

    send_stream.finish().map_err(ConnectError::close)?;
    send_stream.stopped().await.map_err(ConnectError::close)?;
    recv_stream
        .read_to_end(0)
        .await
        .map_err(ConnectError::close)?;

    #[cfg(feature = "metrics")]
    if res.is_ok() {
        inc!(Metrics, sync_via_connect_success);
    } else {
        inc!(Metrics, sync_via_connect_failure);
    }

    let t_process = t_start.elapsed() - t_connect;
    match &res {
        Ok(res) => {
            debug!(
                ?t_connect,
                ?t_process,
                sent = %res.num_sent,
                recv = %res.num_recv,
                "done, ok"
            );
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
        }
    }

    let outcome = res?;

    let timings = Timings {
        connect: t_connect,
        process: t_process,
    };

    let res = SyncFinished {
        namespace,
        peer: peer_id,
        outcome,
        timings,
    };

    Ok(res)
}

/// Whether we want to accept or reject an incoming sync request.
#[derive(Debug, Clone)]
pub enum AcceptOutcome {
    /// Accept the sync request.
    Allow,
    /// Decline the sync request
    Reject(AbortReason),
}

/// Handle an iroh-docs connection and sync all shared documents in the replica store.
pub async fn handle_connection<F, Fut>(
    sync: SyncHandle,
    connecting: iroh::endpoint::Connecting,
    accept_cb: F,
) -> Result<SyncFinished, AcceptError>
where
    F: Fn(NamespaceId, PublicKey) -> Fut,
    Fut: Future<Output = AcceptOutcome>,
{
    let t_start = Instant::now();
    let connection = connecting.await.map_err(AcceptError::connect)?;
    let peer = get_remote_node_id(&connection).map_err(AcceptError::connect)?;
    let (mut send_stream, mut recv_stream) = connection
        .accept_bi()
        .await
        .map_err(|e| AcceptError::open(peer, e))?;

    let t_connect = t_start.elapsed();
    let span = error_span!("accept", peer = %peer.fmt_short(), namespace = tracing::field::Empty);
    span.in_scope(|| {
        debug!(?t_connect, "connection established");
    });

    let mut state = BobState::new(peer);
    let res = state
        .run(&mut send_stream, &mut recv_stream, sync, accept_cb)
        .instrument(span.clone())
        .await;

    #[cfg(feature = "metrics")]
    if res.is_ok() {
        inc!(Metrics, sync_via_accept_success);
    } else {
        inc!(Metrics, sync_via_accept_failure);
    }

    let namespace = state.namespace();
    let outcome = state.into_outcome();

    send_stream
        .finish()
        .map_err(|error| AcceptError::close(peer, namespace, error))?;
    send_stream
        .stopped()
        .await
        .map_err(|error| AcceptError::close(peer, namespace, error))?;
    recv_stream
        .read_to_end(0)
        .await
        .map_err(|error| AcceptError::close(peer, namespace, error))?;

    let t_process = t_start.elapsed() - t_connect;
    span.in_scope(|| match &res {
        Ok(_res) => {
            debug!(
                ?t_connect,
                ?t_process,
                sent = %outcome.num_sent,
                recv = %outcome.num_recv,
                "done, ok"
            );
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
        }
    });

    let namespace = res?;

    let timings = Timings {
        connect: t_connect,
        process: t_process,
    };
    let res = SyncFinished {
        namespace,
        outcome,
        peer,
        timings,
    };

    Ok(res)
}

/// Details of a finished sync operation.
#[derive(Debug, Clone)]
pub struct SyncFinished {
    /// The namespace that was synced.
    pub namespace: NamespaceId,
    /// The peer we syned with.
    pub peer: PublicKey,
    /// The outcome of the sync operation
    pub outcome: SyncOutcome,
    /// The time this operation took
    pub timings: Timings,
}

/// Time a sync operation took
#[derive(Debug, Default, Clone)]
pub struct Timings {
    /// Time to establish connection
    pub connect: Duration,
    /// Time to run sync exchange
    pub process: Duration,
}

/// Errors that may occur on handling incoming sync connections.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum AcceptError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to open replica
    #[error("Failed to open replica with {peer:?}")]
    Open {
        peer: PublicKey,
        #[source]
        error: anyhow::Error,
    },
    /// We aborted the sync request.
    #[error("Aborted sync of {namespace:?} with {peer:?}: {reason:?}")]
    Abort {
        peer: PublicKey,
        namespace: NamespaceId,
        reason: AbortReason,
    },
    /// Failed to run sync
    #[error("Failed to sync {namespace:?} with {peer:?}")]
    Sync {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close {namespace:?} with {peer:?}")]
    Close {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
}

/// Errors that may occur on outgoing sync requests.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum ConnectError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// The remote peer aborted the sync request.
    #[error("Remote peer aborted sync: {0:?}")]
    RemoteAbort(AbortReason),
    /// Failed to run sync
    #[error("Failed to sync")]
    Sync {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close connection1")]
    Close {
        #[source]
        error: anyhow::Error,
    },
}

/// Reason why we aborted an incoming sync request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AbortReason {
    /// Namespace is not available.
    NotFound,
    /// We are already syncing this namespace.
    AlreadySyncing,
    /// We experienced an error while trying to provide the requested resource
    InternalServerError,
}

impl AcceptError {
    fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect {
            error: error.into(),
        }
    }
    fn open(peer: PublicKey, error: impl Into<anyhow::Error>) -> Self {
        Self::Open {
            peer,
            error: error.into(),
        }
    }
    pub(crate) fn sync(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Sync {
            peer,
            namespace,
            error: error.into(),
        }
    }
    fn close(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Close {
            peer,
            namespace,
            error: error.into(),
        }
    }
    /// Get the peer's node ID (if available)
    pub fn peer(&self) -> Option<PublicKey> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { peer, .. } => Some(*peer),
            AcceptError::Sync { peer, .. } => Some(*peer),
            AcceptError::Close { peer, .. } => Some(*peer),
            AcceptError::Abort { peer, .. } => Some(*peer),
        }
    }

    /// Get the namespace (if available)
    pub fn namespace(&self) -> Option<NamespaceId> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { .. } => None,
            AcceptError::Sync { namespace, .. } => namespace.to_owned(),
            AcceptError::Close { namespace, .. } => namespace.to_owned(),
            AcceptError::Abort { namespace, .. } => Some(*namespace),
        }
    }
}

impl ConnectError {
    fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect {
            error: error.into(),
        }
    }
    fn close(error: impl Into<anyhow::Error>) -> Self {
        Self::Close {
            error: error.into(),
        }
    }
    pub(crate) fn sync(error: impl Into<anyhow::Error>) -> Self {
        Self::Sync {
            error: error.into(),
        }
    }
    pub(crate) fn remote_abort(reason: AbortReason) -> Self {
        Self::RemoteAbort(reason)
    }
}
