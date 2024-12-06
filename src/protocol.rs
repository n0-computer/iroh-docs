//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use futures_lite::future::Boxed as BoxedFuture;
use iroh::{endpoint::Connecting, protocol::ProtocolHandler};
use iroh_blobs::net_protocol::{Blobs, ProtectCb};
use iroh_gossip::net::Gossip;
use quic_rpc::server::{ChannelTypes, RpcChannel};

use crate::{
    engine::{DefaultAuthorStorage, Engine},
    rpc::proto::{Request, RpcService},
    store::Store,
};

impl<S: iroh_blobs::store::Store> ProtocolHandler for Docs<S> {
    fn accept(&self, conn: Connecting) -> BoxedFuture<Result<()>> {
        let this = self.engine.clone();
        Box::pin(async move { this.handle_connection(conn).await })
    }

    fn shutdown(&self) -> BoxedFuture<()> {
        let this = self.engine.clone();
        Box::pin(async move {
            if let Err(err) = this.shutdown().await {
                tracing::warn!("shutdown error: {:?}", err);
            }
        })
    }
}

/// Docs protocol.
#[derive(Debug, Clone)]
pub struct Docs<S> {
    engine: Arc<Engine<S>>,
    #[cfg(feature = "rpc")]
    pub(crate) rpc_handler: Arc<std::sync::OnceLock<crate::rpc::RpcHandler>>,
}

impl Docs<()> {
    /// Create a new [`Builder`] for the docs protocol, using in memory replica and author storage.
    pub fn memory() -> Builder {
        Builder::default()
    }

    /// Create a new [`Builder`] for the docs protocol, using a persistent replica and author storage
    /// in the given directory.
    pub fn persistent(path: PathBuf) -> Builder {
        Builder { path: Some(path) }
    }
}

impl<S: iroh_blobs::store::Store> Docs<S> {
    /// Get an in memory client to interact with the docs engine.
    pub fn client(&self) -> &crate::rpc::client::docs::MemClient {
        &self
            .rpc_handler
            .get_or_init(|| crate::rpc::RpcHandler::new(&self.engine))
            .client
    }

    /// Create a new docs protocol with the given engine.
    ///
    /// Note that usually you would use the [`Builder`] to create a new docs protocol.
    pub fn new(engine: Engine<S>) -> Self {
        Self {
            engine: Arc::new(engine),
            rpc_handler: Default::default(),
        }
    }

    /// Handle a docs request from the RPC server.
    pub async fn handle_rpc_request<C: ChannelTypes<RpcService>>(
        self,
        msg: Request,
        chan: RpcChannel<RpcService, C>,
    ) -> Result<(), quic_rpc::server::RpcServerError<C>> {
        self.engine
            .as_ref()
            .clone()
            .handle_rpc_request(msg, chan)
            .await
    }

    /// Get the protect callback for the docs engine.
    pub fn protect_cb(&self) -> ProtectCb {
        self.engine.protect_cb()
    }
}

/// Builder for the docs protocol.
#[derive(Debug, Default)]
pub struct Builder {
    path: Option<PathBuf>,
}

impl Builder {
    /// Build a [`Docs`] protocol given a [`Blobs`] and [`Gossip`] protocol.
    pub async fn build<S: iroh_blobs::store::Store>(
        self,
        blobs: &Blobs<S>,
        gossip: &Gossip,
    ) -> anyhow::Result<Docs<S>> {
        let replica_store = match self.path {
            Some(ref path) => Store::persistent(path.join("docs.redb"))?,
            None => Store::memory(),
        };
        let author_store = match self.path {
            Some(ref path) => DefaultAuthorStorage::Persistent(path.join("default-author")),
            None => DefaultAuthorStorage::Mem,
        };
        let engine = Engine::spawn(
            blobs.endpoint().clone(),
            gossip.clone(),
            replica_store,
            blobs.store().clone(),
            blobs.downloader().clone(),
            author_store,
            blobs.rt().clone(),
        )
        .await?;
        Ok(Docs {
            engine: Arc::new(engine),
            rpc_handler: Default::default(),
        })
    }
}
