use std::sync::Arc;

use irpc::LocalSender;
use irpc::WithChannels;
use n0_future::task::{self};
use tokio::sync::mpsc as tokio_mpsc;
use tracing::error;

use crate::engine::Engine;

use super::{
    protocol::{DocsMessage, DocsService},
    DocsApi,
};

/// The docs RPC actor that handles incoming messages
pub(crate) struct RpcActor {
    pub(crate) recv: tokio::sync::mpsc::Receiver<DocsMessage>,
    pub(crate) engine: Arc<Engine>,
}

impl RpcActor {
    pub(crate) fn spawn(engine: Arc<Engine>) -> DocsApi {
        let (tx, rx) = tokio_mpsc::channel(64);
        let actor = Self { recv: rx, engine };
        task::spawn(actor.run());
        let local = LocalSender::<DocsMessage, DocsService>::from(tx);
        DocsApi {
            inner: local.into(),
        }
    }

    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.recv.recv().await {
            tracing::trace!("handle rpc request: {msg:?}");
            self.handle(msg).await;
        }
    }

    pub(crate) async fn handle(&mut self, msg: DocsMessage) {
        match msg {
            DocsMessage::Open(open) => {
                let WithChannels { tx, inner, .. } = open;
                let result = self.doc_open(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Open response: {}", e);
                }
            }
            DocsMessage::Close(close) => {
                let WithChannels { tx, inner, .. } = close;
                let result = self.doc_close(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Close response: {}", e);
                }
            }
            DocsMessage::Status(status) => {
                let WithChannels { tx, inner, .. } = status;
                let result = self.doc_status(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Status response: {}", e);
                }
            }
            DocsMessage::List(list) => {
                let WithChannels { tx, inner, .. } = list;
                self.doc_list(inner, tx).await;
            }
            DocsMessage::Create(create) => {
                let WithChannels { tx, inner, .. } = create;
                let result = self.doc_create(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Create response: {}", e);
                }
            }
            DocsMessage::Drop(drop) => {
                let WithChannels { tx, inner, .. } = drop;
                let result = self.doc_drop(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Drop response: {}", e);
                }
            }
            DocsMessage::Import(import) => {
                let WithChannels { tx, inner, .. } = import;
                let result = self.doc_import(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Import response: {}", e);
                }
            }
            DocsMessage::Set(set) => {
                let WithChannels { tx, inner, .. } = set;
                let result = self.doc_set(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Set response: {}", e);
                }
            }
            DocsMessage::SetHash(set_hash) => {
                let WithChannels { tx, inner, .. } = set_hash;
                let result = self.doc_set_hash(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send SetHash response: {}", e);
                }
            }
            DocsMessage::Get(get) => {
                let WithChannels { tx, inner, .. } = get;
                self.doc_get_many(inner, tx).await;
            }
            DocsMessage::GetExact(get_exact) => {
                let WithChannels { tx, inner, .. } = get_exact;
                let result = self.doc_get_exact(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send GetExact response: {}", e);
                }
            }
            // DocsMessage::ImportFile(import_file) => {
            //     let WithChannels { tx, inner, .. } = import_file;
            //     self.doc_import_file(inner, tx).await;
            // }
            // DocsMessage::ExportFile(export_file) => {
            //     let WithChannels { tx, inner, .. } = export_file;
            //     self.doc_export_file(inner, tx).await;
            // }
            DocsMessage::Del(del) => {
                let WithChannels { tx, inner, .. } = del;
                let result = self.doc_del(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Del response: {}", e);
                }
            }
            DocsMessage::StartSync(start_sync) => {
                let WithChannels { tx, inner, .. } = start_sync;
                let result = self.doc_start_sync(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send StartSync response: {}", e);
                }
            }
            DocsMessage::Leave(leave) => {
                let WithChannels { tx, inner, .. } = leave;
                let result = self.doc_leave(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Leave response: {}", e);
                }
            }
            DocsMessage::Share(share) => {
                let WithChannels { tx, inner, .. } = share;
                let result = self.doc_share(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send Share response: {}", e);
                }
            }
            DocsMessage::Subscribe(subscribe) => {
                let WithChannels { tx, inner, .. } = subscribe;
                self.doc_subscribe(inner, tx).await;
            }
            DocsMessage::GetDownloadPolicy(get_policy) => {
                let WithChannels { tx, inner, .. } = get_policy;
                let result = self.doc_get_download_policy(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send GetDownloadPolicy response: {}", e);
                }
            }
            DocsMessage::SetDownloadPolicy(set_policy) => {
                let WithChannels { tx, inner, .. } = set_policy;
                let result = self.doc_set_download_policy(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send SetDownloadPolicy response: {}", e);
                }
            }
            DocsMessage::GetSyncPeers(get_peers) => {
                let WithChannels { tx, inner, .. } = get_peers;
                let result = self.doc_get_sync_peers(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send GetSyncPeers response: {}", e);
                }
            }
            DocsMessage::AuthorList(author_list) => {
                let WithChannels { tx, inner, .. } = author_list;
                self.author_list(inner, tx).await;
            }
            DocsMessage::AuthorCreate(author_create) => {
                let WithChannels { tx, inner, .. } = author_create;
                let result = self.author_create(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send AuthorCreate response: {}", e);
                }
            }
            DocsMessage::AuthorGetDefault(author_get_default) => {
                let WithChannels { tx, inner, .. } = author_get_default;
                let result = self.author_default(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send AuthorGetDefault response: {}", e);
                }
            }
            DocsMessage::AuthorSetDefault(author_set_default) => {
                let WithChannels { tx, inner, .. } = author_set_default;
                let result = self.author_set_default(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send AuthorSetDefault response: {}", e);
                }
            }
            DocsMessage::AuthorImport(author_import) => {
                let WithChannels { tx, inner, .. } = author_import;
                let result = self.author_import(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send AuthorImport response: {}", e);
                }
            }
            DocsMessage::AuthorExport(author_export) => {
                let WithChannels { tx, inner, .. } = author_export;
                let result = self.author_export(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send AuthorExport response: {}", e);
                }
            }
            DocsMessage::AuthorDelete(author_delete) => {
                let WithChannels { tx, inner, .. } = author_delete;
                let result = self.author_delete(inner).await;
                if let Err(e) = tx.send(result).await {
                    error!("Failed to send AuthorDelete response: {}", e);
                }
            }
        }
    }
}

impl std::ops::Deref for RpcActor {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}
