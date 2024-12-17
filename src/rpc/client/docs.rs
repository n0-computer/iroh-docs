//! API for document management.
//!
//! The main entry point is the [`Client`].
use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{anyhow, Context as _, Result};
use bytes::Bytes;
use derive_more::{Display, FromStr};
use futures_lite::{Stream, StreamExt};
use iroh::NodeAddr;
use iroh_blobs::{export::ExportProgress, store::ExportMode, Hash};
use portable_atomic::{AtomicBool, Ordering};
use quic_rpc::{
    client::BoxedConnector, message::RpcMsg, transport::flume::FlumeConnector, Connector,
};
use serde::{Deserialize, Serialize};

use super::{authors, flatten};
use crate::{
    actor::OpenState,
    rpc::{
        proto::{
            CloseRequest, CreateRequest, DelRequest, DelResponse, DocListRequest,
            DocSubscribeRequest, DropRequest, ExportFileRequest, GetDownloadPolicyRequest,
            GetExactRequest, GetManyRequest, GetSyncPeersRequest, ImportFileRequest, ImportRequest,
            LeaveRequest, OpenRequest, RpcService, SetDownloadPolicyRequest, SetHashRequest,
            SetRequest, ShareRequest, StartSyncRequest, StatusRequest,
        },
        AddrInfoOptions,
    },
    store::{DownloadPolicy, Query},
    AuthorId, Capability, CapabilityKind, DocTicket, NamespaceId, PeerIdBytes,
};
#[doc(inline)]
pub use crate::{
    engine::{LiveEvent, Origin, SyncEvent, SyncReason},
    Entry,
};

/// Type alias for a memory-backed client.
pub type MemClient =
    Client<FlumeConnector<crate::rpc::proto::Response, crate::rpc::proto::Request>>;

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

    /// Returns an authors client.
    pub fn authors(&self) -> authors::Client<C> {
        authors::Client::new(self.rpc.clone())
    }

    /// Creates a client.
    pub async fn create(&self) -> Result<Doc<C>> {
        let res = self.rpc.rpc(CreateRequest {}).await??;
        let doc = Doc::new(self.rpc.clone(), res.id);
        Ok(doc)
    }

    /// Deletes a document from the local node.
    ///
    /// This is a destructive operation. Both the document secret key and all entries in the
    /// document will be permanently deleted from the node's storage. Content blobs will be deleted
    /// through garbage collection unless they are referenced from another document or tag.
    pub async fn drop_doc(&self, doc_id: NamespaceId) -> Result<()> {
        self.rpc.rpc(DropRequest { doc_id }).await??;
        Ok(())
    }

    /// Imports a document from a namespace capability.
    ///
    /// This does not start sync automatically. Use [`Doc::start_sync`] to start sync.
    pub async fn import_namespace(&self, capability: Capability) -> Result<Doc<C>> {
        let res = self.rpc.rpc(ImportRequest { capability }).await??;
        let doc = Doc::new(self.rpc.clone(), res.doc_id);
        Ok(doc)
    }

    /// Imports a document from a ticket and joins all peers in the ticket.
    pub async fn import(&self, ticket: DocTicket) -> Result<Doc<C>> {
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
    ) -> Result<(Doc<C>, impl Stream<Item = anyhow::Result<LiveEvent>>)> {
        let DocTicket { capability, nodes } = ticket;
        let res = self.rpc.rpc(ImportRequest { capability }).await??;
        let doc = Doc::new(self.rpc.clone(), res.doc_id);
        let events = doc.subscribe().await?;
        doc.start_sync(nodes).await?;
        Ok((doc, events))
    }

    /// Lists all documents.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<(NamespaceId, CapabilityKind)>>> {
        let stream = self.rpc.server_streaming(DocListRequest {}).await?;
        Ok(flatten(stream).map(|res| res.map(|res| (res.id, res.capability))))
    }

    /// Returns a [`Doc`] client for a single document.
    ///
    /// Returns None if the document cannot be found.
    pub async fn open(&self, id: NamespaceId) -> Result<Option<Doc<C>>> {
        self.rpc.rpc(OpenRequest { doc_id: id }).await??;
        let doc = Doc::new(self.rpc.clone(), id);
        Ok(Some(doc))
    }
}

/// Document handle
#[derive(Debug, Clone)]
pub struct Doc<C: Connector<RpcService> = BoxedConnector<RpcService>>(Arc<DocInner<C>>)
where
    C: quic_rpc::Connector<RpcService>;

impl<C: Connector<RpcService>> PartialEq for Doc<C> {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id
    }
}

impl<C: Connector<RpcService>> Eq for Doc<C> {}

#[derive(Debug)]
struct DocInner<C: Connector<RpcService> = BoxedConnector<RpcService>> {
    id: NamespaceId,
    rpc: quic_rpc::RpcClient<RpcService, C>,
    closed: AtomicBool,
    rt: tokio::runtime::Handle,
}

impl<C> Drop for DocInner<C>
where
    C: quic_rpc::Connector<RpcService>,
{
    fn drop(&mut self) {
        let doc_id = self.id;
        let rpc = self.rpc.clone();
        if !self.closed.swap(true, Ordering::Relaxed) {
            self.rt.spawn(async move {
                rpc.rpc(CloseRequest { doc_id }).await.ok();
            });
        }
    }
}

impl<C: Connector<RpcService>> Doc<C> {
    fn new(rpc: quic_rpc::RpcClient<RpcService, C>, id: NamespaceId) -> Self {
        Self(Arc::new(DocInner {
            rpc,
            id,
            closed: AtomicBool::new(false),
            rt: tokio::runtime::Handle::current(),
        }))
    }

    async fn rpc<M>(&self, msg: M) -> Result<M::Response>
    where
        M: RpcMsg<RpcService>,
    {
        let res = self.0.rpc.rpc(msg).await?;
        Ok(res)
    }

    /// Returns the document id of this doc.
    pub fn id(&self) -> NamespaceId {
        self.0.id
    }

    /// Closes the document.
    pub async fn close(&self) -> Result<()> {
        if !self.0.closed.swap(true, Ordering::Relaxed) {
            self.rpc(CloseRequest { doc_id: self.id() }).await??;
        }
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.0.closed.load(Ordering::Relaxed) {
            Err(anyhow!("document is closed"))
        } else {
            Ok(())
        }
    }

    /// Sets the content of a key to a byte array.
    pub async fn set_bytes(
        &self,
        author_id: AuthorId,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<Hash> {
        self.ensure_open()?;
        let res = self
            .rpc(SetRequest {
                doc_id: self.id(),
                author_id,
                key: key.into(),
                value: value.into(),
            })
            .await??;
        Ok(res.entry.content_hash())
    }

    /// Sets an entries on the doc via its key, hash, and size.
    pub async fn set_hash(
        &self,
        author_id: AuthorId,
        key: impl Into<Bytes>,
        hash: Hash,
        size: u64,
    ) -> Result<()> {
        self.ensure_open()?;
        self.rpc(SetHashRequest {
            doc_id: self.id(),
            author_id,
            key: key.into(),
            hash,
            size,
        })
        .await??;
        Ok(())
    }

    /// Adds an entry from an absolute file path
    pub async fn import_file(
        &self,
        author: AuthorId,
        key: Bytes,
        path: impl AsRef<Path>,
        in_place: bool,
    ) -> Result<ImportFileProgress> {
        self.ensure_open()?;
        let stream = self
            .0
            .rpc
            .server_streaming(ImportFileRequest {
                doc_id: self.id(),
                author_id: author,
                path: path.as_ref().into(),
                key,
                in_place,
            })
            .await?;
        Ok(ImportFileProgress::new(stream))
    }

    /// Exports an entry as a file to a given absolute path.
    pub async fn export_file(
        &self,
        entry: Entry,
        path: impl AsRef<Path>,
        mode: ExportMode,
    ) -> Result<ExportFileProgress> {
        self.ensure_open()?;
        let stream = self
            .0
            .rpc
            .server_streaming(ExportFileRequest {
                entry,
                path: path.as_ref().into(),
                mode,
            })
            .await?;
        Ok(ExportFileProgress::new(stream))
    }

    /// Deletes entries that match the given `author` and key `prefix`.
    ///
    /// This inserts an empty entry with the key set to `prefix`, effectively clearing all other
    /// entries whose key starts with or is equal to the given `prefix`.
    ///
    /// Returns the number of entries deleted.
    pub async fn del(&self, author_id: AuthorId, prefix: impl Into<Bytes>) -> Result<usize> {
        self.ensure_open()?;
        let res = self
            .rpc(DelRequest {
                doc_id: self.id(),
                author_id,
                prefix: prefix.into(),
            })
            .await??;
        let DelResponse { removed } = res;
        Ok(removed)
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
        self.ensure_open()?;
        let res = self
            .rpc(GetExactRequest {
                author,
                key: key.as_ref().to_vec().into(),
                doc_id: self.id(),
                include_empty,
            })
            .await??;
        Ok(res.entry.map(|entry| entry.into()))
    }

    /// Returns all entries matching the query.
    pub async fn get_many(
        &self,
        query: impl Into<Query>,
    ) -> Result<impl Stream<Item = Result<Entry>>> {
        self.ensure_open()?;
        let stream = self
            .0
            .rpc
            .server_streaming(GetManyRequest {
                doc_id: self.id(),
                query: query.into(),
            })
            .await?;
        Ok(flatten(stream).map(|res| res.map(|res| res.entry.into())))
    }

    /// Returns a single entry.
    pub async fn get_one(&self, query: impl Into<Query>) -> Result<Option<Entry>> {
        self.get_many(query).await?.next().await.transpose()
    }

    /// Shares this document with peers over a ticket.
    pub async fn share(
        &self,
        mode: ShareMode,
        addr_options: AddrInfoOptions,
    ) -> anyhow::Result<DocTicket> {
        self.ensure_open()?;
        let res = self
            .rpc(ShareRequest {
                doc_id: self.id(),
                mode,
                addr_options,
            })
            .await??;
        Ok(res.0)
    }

    /// Starts to sync this document with a list of peers.
    pub async fn start_sync(&self, peers: Vec<NodeAddr>) -> Result<()> {
        self.ensure_open()?;
        let _res = self
            .rpc(StartSyncRequest {
                doc_id: self.id(),
                peers,
            })
            .await??;
        Ok(())
    }

    /// Stops the live sync for this document.
    pub async fn leave(&self) -> Result<()> {
        self.ensure_open()?;
        let _res = self.rpc(LeaveRequest { doc_id: self.id() }).await??;
        Ok(())
    }

    /// Subscribes to events for this document.
    pub async fn subscribe(&self) -> anyhow::Result<impl Stream<Item = anyhow::Result<LiveEvent>>> {
        self.ensure_open()?;
        let stream = self
            .0
            .rpc
            .try_server_streaming(DocSubscribeRequest { doc_id: self.id() })
            .await?;
        Ok(stream.map(|res| match res {
            Ok(res) => Ok(res.event),
            Err(err) => Err(err.into()),
        }))
    }

    /// Returns status info for this document
    pub async fn status(&self) -> anyhow::Result<OpenState> {
        self.ensure_open()?;
        let res = self.rpc(StatusRequest { doc_id: self.id() }).await??;
        Ok(res.status)
    }

    /// Sets the download policy for this document
    pub async fn set_download_policy(&self, policy: DownloadPolicy) -> Result<()> {
        self.rpc(SetDownloadPolicyRequest {
            doc_id: self.id(),
            policy,
        })
        .await??;
        Ok(())
    }

    /// Returns the download policy for this document
    pub async fn get_download_policy(&self) -> Result<DownloadPolicy> {
        let res = self
            .rpc(GetDownloadPolicyRequest { doc_id: self.id() })
            .await??;
        Ok(res.policy)
    }

    /// Returns sync peers for this document
    pub async fn get_sync_peers(&self) -> Result<Option<Vec<PeerIdBytes>>> {
        let res = self
            .rpc(GetSyncPeersRequest { doc_id: self.id() })
            .await??;
        Ok(res.peers)
    }
}

impl<'a, C> From<&'a Doc<C>> for &'a quic_rpc::RpcClient<RpcService, C>
where
    C: quic_rpc::Connector<RpcService>,
{
    fn from(doc: &'a Doc<C>) -> &'a quic_rpc::RpcClient<RpcService, C> {
        &doc.0.rpc
    }
}

/// Progress messages for an doc import operation
///
/// An import operation involves computing the outboard of a file, and then
/// either copying or moving the file into the database, then setting the author, hash, size, and tag of that
/// file as an entry in the doc.
#[derive(Debug, Serialize, Deserialize)]
pub enum ImportProgress {
    /// An item was found with name `name`, from now on referred to via `id`.
    Found {
        /// A new unique id for this entry.
        id: u64,
        /// The name of the entry.
        name: String,
        /// The size of the entry in bytes.
        size: u64,
    },
    /// We got progress ingesting item `id`.
    Progress {
        /// The unique id of the entry.
        id: u64,
        /// The offset of the progress, in bytes.
        offset: u64,
    },
    /// We are done adding `id` to the data store and the hash is `hash`.
    IngestDone {
        /// The unique id of the entry.
        id: u64,
        /// The hash of the entry.
        hash: Hash,
    },
    /// We are done setting the entry to the doc.
    AllDone {
        /// The key of the entry
        key: Bytes,
    },
    /// We got an error and need to abort.
    ///
    /// This will be the last message in the stream.
    Abort(serde_error::Error),
}

/// Intended capability for document share tickets
#[derive(Serialize, Deserialize, Debug, Clone, Display, FromStr)]
pub enum ShareMode {
    /// Read-only access
    Read,
    /// Write access
    Write,
}
/// Progress stream for [`Doc::import_file`].
#[derive(derive_more::Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct ImportFileProgress {
    #[debug(skip)]
    stream: Pin<Box<dyn Stream<Item = Result<ImportProgress>> + Send + Unpin + 'static>>,
}

impl ImportFileProgress {
    fn new(
        stream: (impl Stream<Item = Result<impl Into<ImportProgress>, impl Into<anyhow::Error>>>
             + Send
             + Unpin
             + 'static),
    ) -> Self {
        let stream = stream.map(|item| match item {
            Ok(item) => Ok(item.into()),
            Err(err) => Err(err.into()),
        });
        Self {
            stream: Box::pin(stream),
        }
    }

    /// Finishes writing the stream, ignoring all intermediate progress events.
    ///
    /// Returns a [`ImportFileOutcome`] which contains a tag, key, and hash and the size of the
    /// content.
    pub async fn finish(mut self) -> Result<ImportFileOutcome> {
        let mut entry_size = 0;
        let mut entry_hash = None;
        while let Some(msg) = self.next().await {
            match msg? {
                ImportProgress::Found { size, .. } => {
                    entry_size = size;
                }
                ImportProgress::AllDone { key } => {
                    let hash = entry_hash
                        .context("expected DocImportProgress::IngestDone event to occur")?;
                    let outcome = ImportFileOutcome {
                        hash,
                        key,
                        size: entry_size,
                    };
                    return Ok(outcome);
                }
                ImportProgress::Abort(err) => return Err(err.into()),
                ImportProgress::Progress { .. } => {}
                ImportProgress::IngestDone { hash, .. } => {
                    entry_hash = Some(hash);
                }
            }
        }
        Err(anyhow!("Response stream ended prematurely"))
    }
}

/// Outcome of a [`Doc::import_file`] operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportFileOutcome {
    /// The hash of the entry's content
    pub hash: Hash,
    /// The size of the entry
    pub size: u64,
    /// The key of the entry
    pub key: Bytes,
}

impl Stream for ImportFileProgress {
    type Item = Result<ImportProgress>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// Progress stream for [`Doc::export_file`].
#[derive(derive_more::Debug)]
pub struct ExportFileProgress {
    #[debug(skip)]
    stream: Pin<Box<dyn Stream<Item = Result<ExportProgress>> + Send + Unpin + 'static>>,
}
impl ExportFileProgress {
    fn new(
        stream: (impl Stream<Item = Result<impl Into<ExportProgress>, impl Into<anyhow::Error>>>
             + Send
             + Unpin
             + 'static),
    ) -> Self {
        let stream = stream.map(|item| match item {
            Ok(item) => Ok(item.into()),
            Err(err) => Err(err.into()),
        });
        Self {
            stream: Box::pin(stream),
        }
    }

    /// Iterates through the export progress stream, returning when the stream has completed.
    ///
    /// Returns a [`ExportFileOutcome`] which contains a file path the data was written to and the size of the content.
    pub async fn finish(mut self) -> Result<ExportFileOutcome> {
        let mut total_size = 0;
        let mut path = None;
        while let Some(msg) = self.next().await {
            match msg? {
                ExportProgress::Found { size, outpath, .. } => {
                    total_size = size.value();
                    path = Some(outpath);
                }
                ExportProgress::AllDone => {
                    let path = path.context("expected ExportProgress::Found event to occur")?;
                    let outcome = ExportFileOutcome {
                        size: total_size,
                        path,
                    };
                    return Ok(outcome);
                }
                ExportProgress::Done { .. } => {}
                ExportProgress::Abort(err) => return Err(anyhow!(err)),
                ExportProgress::Progress { .. } => {}
            }
        }
        Err(anyhow!("Response stream ended prematurely"))
    }
}

/// Outcome of a [`Doc::export_file`] operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportFileOutcome {
    /// The size of the entry
    pub size: u64,
    /// The path to which the entry was saved
    pub path: PathBuf,
}

impl Stream for ExportFileProgress {
    type Item = Result<ExportProgress>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}
