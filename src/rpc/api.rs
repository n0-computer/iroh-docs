//! irpc-based RPC client implementation for docs.

use std::{path::Path, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures_lite::Stream;
use iroh::NodeAddr;
use iroh_blobs::{store::ExportMode, Hash};

use crate::{
    actor::OpenState,
    engine::LiveEvent,
    rpc2::{
        api::DocsApi as InnerDocsApi,
        protocol::{
            AddrInfoOptions, AuthorCreateRequest, AuthorDeleteRequest, AuthorExportRequest,
            AuthorGetDefaultRequest, AuthorImportRequest, AuthorListRequest,
            AuthorSetDefaultRequest, CloseRequest, CreateRequest, DelRequest, DropRequest,
            GetDownloadPolicyRequest, GetExactRequest, GetManyRequest, GetSyncPeersRequest,
            ImportRequest, LeaveRequest, ListRequest, OpenRequest, SetDownloadPolicyRequest,
            SetHashRequest, SetRequest, ShareMode, ShareRequest, StartSyncRequest, StatusRequest,
            SubscribeRequest,
        },
    },
    store::{DownloadPolicy, Query},
    Author, AuthorId, Capability, CapabilityKind, DocTicket, Entry, NamespaceId, PeerIdBytes,
};

/// Iroh docs API client.
#[derive(Debug, Clone)]
pub struct DocsApi {
    inner: InnerDocsApi,
}

impl DocsApi {
    /// Create a new docs API from an engine
    pub fn new(engine: Arc<crate::engine::Engine>) -> Self {
        Self {
            inner: InnerDocsApi::new(engine),
        }
    }

    /// Connect to a remote docs service
    pub fn connect(endpoint: quinn::Endpoint, addr: std::net::SocketAddr) -> Result<Self> {
        Ok(Self {
            inner: InnerDocsApi::connect(endpoint, addr)?,
        })
    }

    /// Listen for incoming RPC connections
    pub fn listen(
        &self,
        endpoint: quinn::Endpoint,
    ) -> Result<n0_future::task::AbortOnDropHandle<()>> {
        self.inner.listen(endpoint)
    }

    // Author methods (prefixed with author_)

    /// Creates a new document author.
    ///
    /// You likely want to save the returned [`AuthorId`] somewhere so that you can use this author
    /// again.
    ///
    /// If you need only a single author, use [`Self::author_default`].
    pub async fn author_create(&self) -> Result<AuthorId> {
        let response = self.inner.inner.rpc(AuthorCreateRequest).await?;
        Ok(response.author_id)
    }

    /// Returns the default document author of this node.
    ///
    /// On persistent nodes, the author is created on first start and its public key is saved
    /// in the data directory.
    ///
    /// The default author can be set with [`Self::author_set_default`].
    pub async fn author_default(&self) -> Result<AuthorId> {
        let response = self.inner.inner.rpc(AuthorGetDefaultRequest).await?;
        Ok(response.author_id)
    }

    /// Sets the node-wide default author.
    ///
    /// If the author does not exist, an error is returned.
    ///
    /// On a persistent node, the author id will be saved to a file in the data directory and
    /// reloaded after a restart.
    pub async fn author_set_default(&self, author_id: AuthorId) -> Result<()> {
        self.inner
            .inner
            .rpc(AuthorSetDefaultRequest { author_id })
            .await?;
        Ok(())
    }

    /// Lists document authors for which we have a secret key.
    ///
    /// It's only possible to create writes from authors that we have the secret key of.
    pub async fn author_list(&self) -> Result<impl Stream<Item = Result<AuthorId>>> {
        let stream = self.inner.inner.server_streaming(AuthorListRequest).await?;
        Ok(stream.map(|res| res.map(|res| res.author_id).map_err(Into::into)))
    }

    /// Exports the given author.
    ///
    /// Warning: The [`Author`] struct contains sensitive data.
    pub async fn author_export(&self, author: AuthorId) -> Result<Option<Author>> {
        let response = self.inner.inner.rpc(AuthorExportRequest { author }).await?;
        Ok(response.author)
    }

    /// Imports the given author.
    ///
    /// Warning: The [`Author`] struct contains sensitive data.
    pub async fn author_import(&self, author: Author) -> Result<()> {
        self.inner.inner.rpc(AuthorImportRequest { author }).await?;
        Ok(())
    }

    /// Deletes the given author by id.
    ///
    /// Warning: This permanently removes this author.
    ///
    /// Returns an error if attempting to delete the default author.
    pub async fn author_delete(&self, author: AuthorId) -> Result<()> {
        self.inner.inner.rpc(AuthorDeleteRequest { author }).await?;
        Ok(())
    }

    // Doc methods (no prefix)

    /// Creates a new document.
    pub async fn create(&self) -> Result<Doc> {
        let response = self.inner.inner.rpc(CreateRequest).await?;
        Ok(Doc {
            inner: self.inner.inner.clone(),
            namespace_id: response.id,
        })
    }

    /// Deletes a document from the local node.
    ///
    /// This is a destructive operation. Both the document secret key and all entries in the
    /// document will be permanently deleted from the node's storage. Content blobs will be deleted
    /// through garbage collection unless they are referenced from another document or tag.
    pub async fn drop_doc(&self, doc_id: NamespaceId) -> Result<()> {
        self.inner.inner.rpc(DropRequest { doc_id }).await?;
        Ok(())
    }

    /// Imports a document from a namespace capability.
    ///
    /// This does not start sync automatically. Use [`Doc::start_sync`] to start sync.
    pub async fn import_namespace(&self, capability: Capability) -> Result<Doc> {
        let response = self.inner.inner.rpc(ImportRequest { capability }).await?;
        Ok(Doc {
            inner: self.inner.inner.clone(),
            namespace_id: response.doc_id,
        })
    }

    /// Imports a document from a ticket and joins all peers in the ticket.
    pub async fn import(&self, ticket: DocTicket) -> Result<Doc> {
        let DocTicket { capability, nodes } = ticket;
        let doc = self.import_namespace(capability).await?;
        doc.start_sync(nodes).await?;
        Ok(doc)
    }

    /// Imports a document from a ticket, creates a subscription stream and joins all peers in the ticket.
    ///
    /// Returns the [`Doc`] and a [`Stream`] of [`LiveEvent`]s.
    ///
    /// The subscription stream is created before the sync is started, so the first call to this
    /// method after starting the node is guaranteed to not miss any sync events.
    pub async fn import_and_subscribe(
        &self,
        ticket: DocTicket,
    ) -> Result<(Doc, impl Stream<Item = Result<LiveEvent>>)> {
        let DocTicket { capability, nodes } = ticket;
        let response = self.inner.inner.rpc(ImportRequest { capability }).await?;
        let doc = Doc {
            inner: self.inner.inner.clone(),
            namespace_id: response.doc_id,
        };
        let events = doc.subscribe().await?;
        doc.start_sync(nodes).await?;
        Ok((doc, events))
    }

    /// Lists all documents.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<(NamespaceId, CapabilityKind)>>> {
        let stream = self.inner.inner.server_streaming(ListRequest).await?;
        Ok(stream.map(|res| res.map(|res| (res.id, res.capability)).map_err(Into::into)))
    }

    /// Returns a [`Doc`] client for a single document.
    ///
    /// Returns None if the document cannot be found.
    pub async fn open(&self, id: NamespaceId) -> Result<Option<Doc>> {
        self.inner.inner.rpc(OpenRequest { doc_id: id }).await?;
        Ok(Some(Doc {
            inner: self.inner.inner.clone(),
            namespace_id: id,
        }))
    }
}

/// Document handle
#[derive(Debug, Clone)]
pub struct Doc {
    inner: irpc::Client<
        crate::rpc2::protocol::DocsMessage,
        crate::rpc2::protocol::DocsProtocol,
        crate::rpc2::protocol::DocsService,
    >,
    namespace_id: NamespaceId,
}

impl Doc {
    /// Returns the document id of this doc.
    pub fn id(&self) -> NamespaceId {
        self.namespace_id
    }

    /// Closes the document.
    pub async fn close(&self) -> Result<()> {
        self.inner
            .rpc(CloseRequest {
                doc_id: self.namespace_id,
            })
            .await?;
        Ok(())
    }

    /// Sets the content of a key to a byte array.
    pub async fn set_bytes(
        &self,
        author_id: AuthorId,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<Hash> {
        let response = self
            .inner
            .rpc(SetRequest {
                doc_id: self.namespace_id,
                author_id,
                key: key.into(),
                value: value.into(),
            })
            .await?;
        Ok(response.entry.content_hash())
    }

    /// Sets an entries on the doc via its key, hash, and size.
    pub async fn set_hash(
        &self,
        author_id: AuthorId,
        key: impl Into<Bytes>,
        hash: Hash,
        size: u64,
    ) -> Result<()> {
        self.inner
            .rpc(SetHashRequest {
                doc_id: self.namespace_id,
                author_id,
                key: key.into(),
                hash,
                size,
            })
            .await?;
        Ok(())
    }

    /// Deletes entries that match the given `author` and key `prefix`.
    ///
    /// This inserts an empty entry with the key set to `prefix`, effectively clearing all other
    /// entries whose key starts with or is equal to the given `prefix`.
    ///
    /// Returns the number of entries deleted.
    pub async fn del(&self, author_id: AuthorId, prefix: impl Into<Bytes>) -> Result<usize> {
        let response = self
            .inner
            .rpc(DelRequest {
                doc_id: self.namespace_id,
                author_id,
                prefix: prefix.into(),
            })
            .await?;
        Ok(response.removed)
    }

    /// Returns an entry for a key and author.
    ///
    /// Optionally also returns the entry unless it is empty (i.e. a deletion marker).
    pub async fn get_exact(
        &self,
        author: AuthorId,
        key: impl AsRef<[u8]>,
        include_empty: bool,
    ) -> Result<Option<Entry>> {
        let response = self
            .inner
            .rpc(GetExactRequest {
                author,
                key: key.as_ref().to_vec().into(),
                doc_id: self.namespace_id,
                include_empty,
            })
            .await?;
        Ok(response.entry.map(|entry| entry.into()))
    }

    /// Returns all entries matching the query.
    pub async fn get_many(
        &self,
        query: impl Into<Query>,
    ) -> Result<impl Stream<Item = Result<Entry>>> {
        let stream = self
            .inner
            .server_streaming(GetManyRequest {
                doc_id: self.namespace_id,
                query: query.into(),
            })
            .await?;
        Ok(stream.map(|res| res.map(|entry| entry.into()).map_err(Into::into)))
    }

    /// Returns a single entry.
    pub async fn get_one(&self, query: impl Into<Query>) -> Result<Option<Entry>> {
        let mut stream = self.get_many(query).await?;
        futures_lite::StreamExt::next(&mut stream).await.transpose()
    }

    /// Shares this document with peers over a ticket.
    pub async fn share(&self, mode: ShareMode, addr_options: AddrInfoOptions) -> Result<DocTicket> {
        let response = self
            .inner
            .rpc(ShareRequest {
                doc_id: self.namespace_id,
                mode,
                addr_options,
            })
            .await?;
        Ok(response.0)
    }

    /// Starts to sync this document with a list of peers.
    pub async fn start_sync(&self, peers: Vec<NodeAddr>) -> Result<()> {
        self.inner
            .rpc(StartSyncRequest {
                doc_id: self.namespace_id,
                peers,
            })
            .await?;
        Ok(())
    }

    /// Stops the live sync for this document.
    pub async fn leave(&self) -> Result<()> {
        self.inner
            .rpc(LeaveRequest {
                doc_id: self.namespace_id,
            })
            .await?;
        Ok(())
    }

    /// Subscribes to events for this document.
    pub async fn subscribe(&self) -> Result<impl Stream<Item = Result<LiveEvent>>> {
        let stream = self
            .inner
            .server_streaming(SubscribeRequest {
                doc_id: self.namespace_id,
            })
            .await?;
        Ok(stream.map(|res| res.map(|res| res.event).map_err(Into::into)))
    }

    /// Returns status info for this document
    pub async fn status(&self) -> Result<OpenState> {
        let response = self
            .inner
            .rpc(StatusRequest {
                doc_id: self.namespace_id,
            })
            .await?;
        Ok(response.status)
    }

    /// Sets the download policy for this document
    pub async fn set_download_policy(&self, policy: DownloadPolicy) -> Result<()> {
        self.inner
            .rpc(SetDownloadPolicyRequest {
                doc_id: self.namespace_id,
                policy,
            })
            .await?;
        Ok(())
    }

    /// Returns the download policy for this document
    pub async fn get_download_policy(&self) -> Result<DownloadPolicy> {
        let response = self
            .inner
            .rpc(GetDownloadPolicyRequest {
                doc_id: self.namespace_id,
            })
            .await?;
        Ok(response.policy)
    }

    /// Returns sync peers for this document
    pub async fn get_sync_peers(&self) -> Result<Option<Vec<PeerIdBytes>>> {
        let response = self
            .inner
            .rpc(GetSyncPeersRequest {
                doc_id: self.namespace_id,
            })
            .await?;
        Ok(response.peers)
    }
}
