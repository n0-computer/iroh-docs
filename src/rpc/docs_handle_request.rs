use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use futures_lite::{Stream, StreamExt};
use iroh_blobs::{
    export::ExportProgress,
    store::{ExportFormat, ImportProgress},
    util::progress::{AsyncChannelProgressSender, ProgressSender},
    BlobFormat, HashAndFormat,
};

use super::{
    client::docs::ShareMode,
    proto::{
        AuthorCreateRequest, AuthorCreateResponse, AuthorDeleteRequest, AuthorDeleteResponse,
        AuthorExportRequest, AuthorExportResponse, AuthorGetDefaultRequest,
        AuthorGetDefaultResponse, AuthorImportRequest, AuthorImportResponse, AuthorListRequest,
        AuthorListResponse, AuthorSetDefaultRequest, AuthorSetDefaultResponse, CloseRequest,
        CloseResponse, CreateRequest as DocCreateRequest, CreateResponse as DocCreateResponse,
        DelRequest, DelResponse, DocListRequest, DocSubscribeRequest, DocSubscribeResponse,
        DropRequest, DropResponse, ExportFileRequest, ExportFileResponse, GetDownloadPolicyRequest,
        GetDownloadPolicyResponse, GetExactRequest, GetExactResponse, GetManyRequest,
        GetManyResponse, GetSyncPeersRequest, GetSyncPeersResponse, ImportFileRequest,
        ImportFileResponse, ImportRequest as DocImportRequest, ImportResponse as DocImportResponse,
        LeaveRequest, LeaveResponse, ListResponse as DocListResponse, OpenRequest, OpenResponse,
        SetDownloadPolicyRequest, SetDownloadPolicyResponse, SetHashRequest, SetHashResponse,
        SetRequest, SetResponse, ShareRequest, ShareResponse, StartSyncRequest, StartSyncResponse,
        StatusRequest, StatusResponse,
    },
    Handler, RpcError, RpcResult,
};
use crate::{Author, DocTicket, NamespaceSecret};

/// Capacity for the flume channels to forward sync store iterators to async RPC streams.
const ITER_CHANNEL_CAP: usize = 64;

impl<D: iroh_blobs::store::Store> Handler<D> {
    pub(super) async fn author_create(
        self,
        _req: AuthorCreateRequest,
    ) -> RpcResult<AuthorCreateResponse> {
        // TODO: pass rng
        let author = Author::new(&mut rand::rngs::OsRng {});
        self.sync
            .import_author(author.clone())
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(AuthorCreateResponse {
            author_id: author.id(),
        })
    }

    pub(super) async fn author_default(
        self,
        _req: AuthorGetDefaultRequest,
    ) -> RpcResult<AuthorGetDefaultResponse> {
        let author_id = self.default_author.get();
        Ok(AuthorGetDefaultResponse { author_id })
    }

    pub(super) async fn author_set_default(
        self,
        req: AuthorSetDefaultRequest,
    ) -> RpcResult<AuthorSetDefaultResponse> {
        self.default_author
            .set(req.author_id, &self.sync)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(AuthorSetDefaultResponse)
    }

    pub(super) fn author_list(
        self,
        _req: AuthorListRequest,
    ) -> impl Stream<Item = RpcResult<AuthorListResponse>> + Unpin {
        let (tx, rx) = async_channel::bounded(ITER_CHANNEL_CAP);
        let sync = self.sync.clone();
        // we need to spawn a task to send our request to the sync handle, because the method
        // itself must be sync.
        tokio::task::spawn(async move {
            let tx2 = tx.clone();
            if let Err(err) = sync.list_authors(tx).await {
                tx2.send(Err(err)).await.ok();
            }
        });
        rx.boxed().map(|r| {
            r.map(|author_id| AuthorListResponse { author_id })
                .map_err(|e| RpcError::new(&*e))
        })
    }

    pub(super) async fn author_import(
        self,
        req: AuthorImportRequest,
    ) -> RpcResult<AuthorImportResponse> {
        let author_id = self
            .sync
            .import_author(req.author)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(AuthorImportResponse { author_id })
    }

    pub(super) async fn author_export(
        self,
        req: AuthorExportRequest,
    ) -> RpcResult<AuthorExportResponse> {
        let author = self
            .sync
            .export_author(req.author)
            .await
            .map_err(|e| RpcError::new(&*e))?;

        Ok(AuthorExportResponse { author })
    }

    pub(super) async fn author_delete(
        self,
        req: AuthorDeleteRequest,
    ) -> RpcResult<AuthorDeleteResponse> {
        if req.author == self.default_author.get() {
            return Err(RpcError::new(&*anyhow!(
                "Deleting the default author is not supported"
            )));
        }
        self.sync
            .delete_author(req.author)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(AuthorDeleteResponse)
    }

    pub(super) async fn doc_create(self, _req: DocCreateRequest) -> RpcResult<DocCreateResponse> {
        let namespace = NamespaceSecret::new(&mut rand::rngs::OsRng {});
        let id = namespace.id();
        self.sync
            .import_namespace(namespace.into())
            .await
            .map_err(|e| RpcError::new(&*e))?;
        self.sync
            .open(id, Default::default())
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(DocCreateResponse { id })
    }

    pub(super) async fn doc_drop(self, req: DropRequest) -> RpcResult<DropResponse> {
        let DropRequest { doc_id } = req;
        self.leave(doc_id, true)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        self.sync
            .drop_replica(doc_id)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(DropResponse {})
    }

    pub(super) fn doc_list(
        self,
        _req: DocListRequest,
    ) -> impl Stream<Item = RpcResult<DocListResponse>> + Unpin {
        let (tx, rx) = async_channel::bounded(ITER_CHANNEL_CAP);
        let sync = self.sync.clone();
        // we need to spawn a task to send our request to the sync handle, because the method
        // itself must be sync.
        tokio::task::spawn(async move {
            let tx2 = tx.clone();
            if let Err(err) = sync.list_replicas(tx).await {
                tx2.send(Err(err)).await.ok();
            }
        });
        rx.boxed().map(|r| {
            r.map(|(id, capability)| DocListResponse { id, capability })
                .map_err(|e| RpcError::new(&*e))
        })
    }

    pub(super) async fn doc_open(self, req: OpenRequest) -> RpcResult<OpenResponse> {
        self.sync
            .open(req.doc_id, Default::default())
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(OpenResponse {})
    }

    pub(super) async fn doc_close(self, req: CloseRequest) -> RpcResult<CloseResponse> {
        self.sync
            .close(req.doc_id)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(CloseResponse {})
    }

    pub(super) async fn doc_status(self, req: StatusRequest) -> RpcResult<StatusResponse> {
        let status = self
            .sync
            .get_state(req.doc_id)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(StatusResponse { status })
    }

    pub(super) async fn doc_share(self, req: ShareRequest) -> RpcResult<ShareResponse> {
        let ShareRequest {
            doc_id,
            mode,
            addr_options,
        } = req;
        let me = self
            .endpoint
            .node_addr()
            .await
            .map_err(|e| RpcError::new(&*e))?;
        let me = addr_options.apply(&me);

        let capability = match mode {
            ShareMode::Read => crate::Capability::Read(doc_id),
            ShareMode::Write => {
                let secret = self
                    .sync
                    .export_secret_key(doc_id)
                    .await
                    .map_err(|e| RpcError::new(&*e))?;
                crate::Capability::Write(secret)
            }
        };
        self.start_sync(doc_id, vec![])
            .await
            .map_err(|e| RpcError::new(&*e))?;

        Ok(ShareResponse(DocTicket {
            capability,
            nodes: vec![me],
        }))
    }

    pub(super) async fn doc_subscribe(
        self,
        req: DocSubscribeRequest,
    ) -> RpcResult<impl Stream<Item = RpcResult<DocSubscribeResponse>>> {
        let stream = self
            .subscribe(req.doc_id)
            .await
            .map_err(|e| RpcError::new(&*e))?;

        Ok(stream.map(|el| {
            el.map(|event| DocSubscribeResponse { event })
                .map_err(|e| RpcError::new(&*e))
        }))
    }

    pub(super) async fn doc_import(self, req: DocImportRequest) -> RpcResult<DocImportResponse> {
        let DocImportRequest { capability } = req;
        let doc_id = self
            .sync
            .import_namespace(capability)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        self.sync
            .open(doc_id, Default::default())
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(DocImportResponse { doc_id })
    }

    pub(super) async fn doc_start_sync(
        self,
        req: StartSyncRequest,
    ) -> RpcResult<StartSyncResponse> {
        let StartSyncRequest { doc_id, peers } = req;
        self.start_sync(doc_id, peers)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(StartSyncResponse {})
    }

    pub(super) async fn doc_leave(self, req: LeaveRequest) -> RpcResult<LeaveResponse> {
        let LeaveRequest { doc_id } = req;
        self.leave(doc_id, false)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(LeaveResponse {})
    }

    pub(super) async fn doc_set(self, req: SetRequest) -> RpcResult<SetResponse> {
        let blobs_store = self.blob_store();
        let SetRequest {
            doc_id,
            author_id,
            key,
            value,
        } = req;
        let len = value.len();
        let tag = blobs_store
            .import_bytes(value, BlobFormat::Raw)
            .await
            .map_err(|e| RpcError::new(&e))?;
        self.sync
            .insert_local(doc_id, author_id, key.clone(), *tag.hash(), len as u64)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        let entry = self
            .sync
            .get_exact(doc_id, author_id, key, false)
            .await
            .map_err(|e| RpcError::new(&*e))?
            .ok_or_else(|| RpcError::new(&*anyhow!("failed to get entry after insertion")))?;
        Ok(SetResponse { entry })
    }

    pub(super) async fn doc_del(self, req: DelRequest) -> RpcResult<DelResponse> {
        let DelRequest {
            doc_id,
            author_id,
            prefix,
        } = req;
        let removed = self
            .sync
            .delete_prefix(doc_id, author_id, prefix)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(DelResponse { removed })
    }

    pub(super) async fn doc_set_hash(self, req: SetHashRequest) -> RpcResult<SetHashResponse> {
        let SetHashRequest {
            doc_id,
            author_id,
            key,
            hash,
            size,
        } = req;
        self.sync
            .insert_local(doc_id, author_id, key.clone(), hash, size)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(SetHashResponse {})
    }

    pub(super) fn doc_get_many(
        self,
        req: GetManyRequest,
    ) -> impl Stream<Item = RpcResult<GetManyResponse>> + Unpin {
        let GetManyRequest { doc_id, query } = req;
        let (tx, rx) = async_channel::bounded(ITER_CHANNEL_CAP);
        let sync = self.sync.clone();
        // we need to spawn a task to send our request to the sync handle, because the method
        // itself must be sync.
        tokio::task::spawn(async move {
            let tx2 = tx.clone();
            if let Err(err) = sync.get_many(doc_id, query, tx).await {
                tx2.send(Err(err)).await.ok();
            }
        });
        rx.boxed().map(|r| {
            r.map(|entry| GetManyResponse { entry })
                .map_err(|e| RpcError::new(&*e))
        })
    }

    pub(super) async fn doc_get_exact(self, req: GetExactRequest) -> RpcResult<GetExactResponse> {
        let GetExactRequest {
            doc_id,
            author,
            key,
            include_empty,
        } = req;
        let entry = self
            .sync
            .get_exact(doc_id, author, key, include_empty)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(GetExactResponse { entry })
    }

    pub(super) async fn doc_set_download_policy(
        self,
        req: SetDownloadPolicyRequest,
    ) -> RpcResult<SetDownloadPolicyResponse> {
        self.sync
            .set_download_policy(req.doc_id, req.policy)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(SetDownloadPolicyResponse {})
    }

    pub(super) async fn doc_get_download_policy(
        self,
        req: GetDownloadPolicyRequest,
    ) -> RpcResult<GetDownloadPolicyResponse> {
        let policy = self
            .sync
            .get_download_policy(req.doc_id)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(GetDownloadPolicyResponse { policy })
    }

    pub(super) async fn doc_get_sync_peers(
        self,
        req: GetSyncPeersRequest,
    ) -> RpcResult<GetSyncPeersResponse> {
        let peers = self
            .sync
            .get_sync_peers(req.doc_id)
            .await
            .map_err(|e| RpcError::new(&*e))?;
        Ok(GetSyncPeersResponse { peers })
    }

    pub(super) fn doc_import_file(
        self,
        msg: ImportFileRequest,
    ) -> impl Stream<Item = ImportFileResponse> {
        // provide a little buffer so that we don't slow down the sender
        let (tx, rx) = async_channel::bounded(32);
        let tx2 = tx.clone();
        let this = self.clone();
        self.local_pool_handle().spawn_detached(|| async move {
            if let Err(e) = this.doc_import_file0(msg, tx).await {
                tx2.send(super::client::docs::ImportProgress::Abort(RpcError::new(
                    &*e,
                )))
                .await
                .ok();
            }
        });
        rx.map(ImportFileResponse)
    }

    async fn doc_import_file0(
        self,
        msg: ImportFileRequest,
        progress: async_channel::Sender<super::client::docs::ImportProgress>,
    ) -> anyhow::Result<()> {
        use std::collections::BTreeMap;

        use iroh_blobs::store::ImportMode;

        use super::client::docs::ImportProgress as DocImportProgress;

        let progress = AsyncChannelProgressSender::new(progress);
        let names = Arc::new(Mutex::new(BTreeMap::new()));
        // convert import progress to provide progress
        let import_progress = progress.clone().with_filter_map(move |x| match x {
            ImportProgress::Found { id, name } => {
                names.lock().unwrap().insert(id, name);
                None
            }
            ImportProgress::Size { id, size } => {
                let name = names.lock().unwrap().remove(&id)?;
                Some(DocImportProgress::Found { id, name, size })
            }
            ImportProgress::OutboardProgress { id, offset } => {
                Some(DocImportProgress::Progress { id, offset })
            }
            ImportProgress::OutboardDone { hash, id } => {
                Some(DocImportProgress::IngestDone { hash, id })
            }
            _ => None,
        });
        let ImportFileRequest {
            doc_id,
            author_id,
            key,
            path: root,
            in_place,
        } = msg;
        // Check that the path is absolute and exists.
        anyhow::ensure!(root.is_absolute(), "path must be absolute");
        anyhow::ensure!(
            root.exists(),
            "trying to add missing path: {}",
            root.display()
        );

        let import_mode = match in_place {
            true => ImportMode::TryReference,
            false => ImportMode::Copy,
        };

        let blobs = self.blob_store();
        let (temp_tag, size) = blobs
            .import_file(root, import_mode, BlobFormat::Raw, import_progress)
            .await?;

        let hash_and_format = temp_tag.inner();
        let HashAndFormat { hash, .. } = *hash_and_format;
        self.doc_set_hash(SetHashRequest {
            doc_id,
            author_id,
            key: key.clone(),
            hash,
            size,
        })
        .await?;
        drop(temp_tag);
        progress.send(DocImportProgress::AllDone { key }).await?;
        Ok(())
    }

    pub(super) fn doc_export_file(
        self,
        msg: ExportFileRequest,
    ) -> impl Stream<Item = ExportFileResponse> {
        let (tx, rx) = async_channel::bounded(1024);
        let tx2 = tx.clone();
        let this = self.clone();
        self.local_pool_handle().spawn_detached(|| async move {
            if let Err(e) = this.doc_export_file0(msg, tx).await {
                tx2.send(ExportProgress::Abort(RpcError::new(&*e)))
                    .await
                    .ok();
            }
        });
        rx.map(ExportFileResponse)
    }

    async fn doc_export_file0(
        self,
        msg: ExportFileRequest,
        progress: async_channel::Sender<ExportProgress>,
    ) -> anyhow::Result<()> {
        let progress = AsyncChannelProgressSender::new(progress);
        let ExportFileRequest { entry, path, mode } = msg;
        let key = bytes::Bytes::from(entry.key().to_vec());
        let export_progress = progress.clone().with_map(move |mut x| {
            // assign the doc key to the `meta` field of the initial progress event
            if let ExportProgress::Found { meta, .. } = &mut x {
                *meta = Some(key.clone())
            }
            x
        });

        let blobs = self.blob_store();
        iroh_blobs::export::export(
            blobs,
            entry.content_hash(),
            path,
            ExportFormat::Blob,
            mode,
            export_progress,
        )
        .await?;
        progress.send(ExportProgress::AllDone).await?;
        Ok(())
    }
}
