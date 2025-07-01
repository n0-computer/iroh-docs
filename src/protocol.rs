//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use iroh::{endpoint::Connection, protocol::ProtocolHandler, Endpoint};
use iroh_blobs::api::Store as BlobsStore;
use iroh_gossip::net::Gossip;

use crate::{
    engine::{DefaultAuthorStorage, Engine},
    store::Store,
};

impl ProtocolHandler for Docs {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        self.engine
            .handle_connection(connection)
            .await
            .map_err(|err| err.into_boxed_dyn_error())?;
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(err) = self.engine.shutdown().await {
            tracing::warn!("shutdown error: {:?}", err);
        }
    }
}

/// Docs protocol.
#[derive(Debug, Clone)]
pub struct Docs {
    engine: Arc<Engine>,
    #[cfg(feature = "rpc")]
    pub(crate) rpc_handler: Arc<std::sync::OnceLock<crate::rpc::RpcHandler>>,
}

impl Docs {
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

impl Docs {
    // /// Get an in memory client to interact with the docs engine.
    // #[cfg(feature = "rpc")]
    // pub fn client(&self) -> &crate::rpc::client::docs::MemClient {
    //     &self
    //         .rpc_handler
    //         .get_or_init(|| crate::rpc::RpcHandler::new(self.engine.clone()))
    //         .client
    // }

    /// Create a new docs protocol with the given engine.
    ///
    /// Note that usually you would use the [`Builder`] to create a new docs protocol.
    pub fn new(engine: Engine) -> Self {
        Self {
            engine: Arc::new(engine),
            #[cfg(feature = "rpc")]
            rpc_handler: Default::default(),
        }
    }

    // /// Handle a docs request from the RPC server.
    // #[cfg(feature = "rpc")]
    // pub async fn handle_rpc_request<
    //     C: quic_rpc::server::ChannelTypes<crate::rpc::proto::RpcService>,
    // >(
    //     self,
    //     msg: crate::rpc::proto::Request,
    //     chan: quic_rpc::server::RpcChannel<crate::rpc::proto::RpcService, C>,
    // ) -> Result<(), quic_rpc::server::RpcServerError<C>> {
    //     crate::rpc::Handler(self.engine.clone())
    //         .handle_rpc_request(msg, chan)
    //         .await
    // }

    // /// Get the protect callback for the docs engine.
    // pub fn protect_cb(&self) -> ProtectCb {
    //     self.engine.protect_cb()
    // }
}

/// Builder for the docs protocol.
#[derive(Debug, Default)]
pub struct Builder {
    path: Option<PathBuf>,
}

impl Builder {
    /// Build a [`Docs`] protocol given a [`Blobs`] and [`Gossip`] protocol.
    pub async fn spawn(
        self,
        endpoint: Endpoint,
        blobs: BlobsStore,
        gossip: Gossip,
    ) -> anyhow::Result<Docs> {
        let replica_store = match self.path {
            Some(ref path) => Store::persistent(path.join("docs.redb"))?,
            None => Store::memory(),
        };
        let author_store = match self.path {
            Some(ref path) => DefaultAuthorStorage::Persistent(path.join("default-author")),
            None => DefaultAuthorStorage::Mem,
        };
        let downloader = blobs.downloader(&endpoint);
        let engine = Engine::spawn(
            endpoint,
            gossip,
            replica_store,
            blobs,
            downloader,
            author_store,
        )
        .await?;
        Ok(Docs::new(engine))
    }
}
