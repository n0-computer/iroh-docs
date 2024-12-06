//! API for document management.
//!
//! The main entry point is the [`Client`].

use anyhow::Result;
use futures_lite::{Stream, StreamExt};
use quic_rpc::{client::BoxedConnector, Connector};

use super::flatten;
#[doc(inline)]
pub use crate::engine::{Origin, SyncEvent, SyncReason};
use crate::{
    actor::ImportAuthorAction,
    rpc::proto::{
        AuthorCreateRequest, AuthorDeleteRequest, AuthorExportRequest, AuthorGetDefaultRequest,
        AuthorListRequest, AuthorSetDefaultRequest, RpcService,
    },
    Author, AuthorId,
};

/// Iroh docs client.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client<C = BoxedConnector<RpcService>> {
    pub(super) rpc: quic_rpc::RpcClient<RpcService, C>,
}

impl<C: Connector<RpcService>> Client<C> {
    /// Creates a new docs client.
    pub fn new(rpc: quic_rpc::RpcClient<RpcService, C>) -> Self {
        Self { rpc }
    }

    /// Creates a new document author.
    ///
    /// You likely want to save the returned [`AuthorId`] somewhere so that you can use this author
    /// again.
    ///
    /// If you need only a single author, use [`Self::default`].
    pub async fn create(&self) -> Result<AuthorId> {
        let res = self.rpc.rpc(AuthorCreateRequest).await??;
        Ok(res.author_id)
    }

    /// Returns the default document author of this node.
    ///
    /// On persistent nodes, the author is created on first start and its public key is saved
    /// in the data directory.
    ///
    /// The default author can be set with [`Self::set_default`].
    pub async fn default(&self) -> Result<AuthorId> {
        let res = self.rpc.rpc(AuthorGetDefaultRequest).await??;
        Ok(res.author_id)
    }

    /// Sets the node-wide default author.
    ///
    /// If the author does not exist, an error is returned.
    ///
    /// On a persistent node, the author id will be saved to a file in the data directory and
    /// reloaded after a restart.
    pub async fn set_default(&self, author_id: AuthorId) -> Result<()> {
        self.rpc
            .rpc(AuthorSetDefaultRequest { author_id })
            .await??;
        Ok(())
    }

    /// Lists document authors for which we have a secret key.
    ///
    /// It's only possible to create writes from authors that we have the secret key of.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<AuthorId>>> {
        let stream = self.rpc.server_streaming(AuthorListRequest {}).await?;
        Ok(flatten(stream).map(|res| res.map(|res| res.author_id)))
    }

    /// Exports the given author.
    ///
    /// Warning: The [`Author`] struct contains sensitive data.
    pub async fn export(&self, author: AuthorId) -> Result<Option<Author>> {
        let res = self.rpc.rpc(AuthorExportRequest { author }).await??;
        Ok(res.author)
    }

    /// Imports the given author.
    ///
    /// Warning: The [`Author`] struct contains sensitive data.
    pub async fn import(&self, author: Author) -> Result<()> {
        self.rpc.rpc(ImportAuthorAction { author }).await??;
        Ok(())
    }

    /// Deletes the given author by id.
    ///
    /// Warning: This permanently removes this author.
    ///
    /// Returns an error if attempting to delete the default author.
    pub async fn delete(&self, author: AuthorId) -> Result<()> {
        self.rpc.rpc(AuthorDeleteRequest { author }).await??;
        Ok(())
    }
}
