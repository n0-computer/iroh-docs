//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use iroh::{endpoint::Connection, protocol::ProtocolHandler, Endpoint};
use iroh_blobs::api::Store as BlobsStore;
use iroh_gossip::net::Gossip;

use crate::{
    api::DocsApi,
    engine::{DefaultAuthorStorage, Engine, ProtectCallbackHandler},
    store::Store,
};

/// Docs protocol.
#[derive(Debug, Clone)]
pub struct Docs {
    engine: Arc<Engine>,
    api: DocsApi,
}

impl Docs {
    /// Create a new [`Builder`] for the docs protocol, using in memory replica and author storage.
    pub fn memory() -> Builder {
        Builder::default()
    }

    /// Create a new [`Builder`] for the docs protocol, using a persistent replica and author storage
    /// in the given directory.
    pub fn persistent(path: PathBuf) -> Builder {
        Builder {
            path: Some(path),
            protect_cb: None,
        }
    }

    /// Creates a new [`Docs`] from an [`Engine`].
    pub fn new(engine: Engine) -> Self {
        let engine = Arc::new(engine);
        let api = DocsApi::spawn(engine.clone());
        Self { engine, api }
    }

    /// Returns the API for this docs instance.
    pub fn api(&self) -> &DocsApi {
        &self.api
    }
}

impl std::ops::Deref for Docs {
    type Target = DocsApi;

    fn deref(&self) -> &Self::Target {
        &self.api
    }
}

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

/// Builder for the docs protocol.
#[derive(Debug, Default)]
pub struct Builder {
    path: Option<PathBuf>,
    protect_cb: Option<ProtectCallbackHandler>,
}

impl Builder {
    /// Set the garbage collection protection handler for blobs.
    ///
    /// See [`ProtectCallbackHandler::new`] for details.
    pub fn protect_handler(mut self, protect_handler: ProtectCallbackHandler) -> Self {
        self.protect_cb = Some(protect_handler);
        self
    }

    /// Build a [`Docs`] protocol given a [`BlobsStore`] and [`Gossip`] protocol.
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
            self.protect_cb,
        )
        .await?;
        Ok(Docs::new(engine))
    }
}
