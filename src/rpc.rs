//! Quic RPC implemenation for docs.

use crate::engine::Engine;

pub mod client;
pub mod proto;

mod docs_handle_request;

type RpcError = serde_error::Error;
type RpcResult<T> = std::result::Result<T, RpcError>;

impl Engine {
    /// Handle a docs request from the RPC server.
    pub async fn handle_rpc_request<S: quic_rpc::Service, C: quic_rpc::ServiceEndpoint<S>>(
        &self,
        msg: crate::rpc::proto::Request,
        chan: quic_rpc::server::RpcChannel<crate::rpc::proto::RpcService, C, S>,
    ) -> Result<(), quic_rpc::server::RpcServerError<C>> {
        use crate::rpc::proto::Request::*;

        let this = self.clone();
        match msg {
            Open(msg) => chan.rpc(msg, this, Self::doc_open).await,
            Close(msg) => chan.rpc(msg, this, Self::doc_close).await,
            Status(msg) => chan.rpc(msg, this, Self::doc_status).await,
            List(msg) => chan.server_streaming(msg, this, Self::doc_list).await,
            Create(msg) => chan.rpc(msg, this, Self::doc_create).await,
            Drop(msg) => chan.rpc(msg, this, Self::doc_drop).await,
            Import(msg) => chan.rpc(msg, this, Self::doc_import).await,
            Set(msg) => chan.rpc(msg, this, Self::doc_set).await,
            ImportFile(msg) => {
                chan.server_streaming(msg, this, Self::doc_import_file)
                    .await
            }
            ExportFile(msg) => {
                chan.server_streaming(msg, this, Self::doc_export_file)
                    .await
            }
            Del(msg) => chan.rpc(msg, this, Self::doc_del).await,
            SetHash(msg) => chan.rpc(msg, this, Self::doc_set_hash).await,
            Get(msg) => chan.server_streaming(msg, this, Self::doc_get_many).await,
            GetExact(msg) => chan.rpc(msg, this, Self::doc_get_exact).await,
            StartSync(msg) => chan.rpc(msg, this, Self::doc_start_sync).await,
            Leave(msg) => chan.rpc(msg, this, Self::doc_leave).await,
            Share(msg) => chan.rpc(msg, this, Self::doc_share).await,
            Subscribe(msg) => {
                chan.try_server_streaming(msg, this, Self::doc_subscribe)
                    .await
            }
            SetDownloadPolicy(msg) => chan.rpc(msg, this, Self::doc_set_download_policy).await,
            GetDownloadPolicy(msg) => chan.rpc(msg, this, Self::doc_get_download_policy).await,
            GetSyncPeers(msg) => chan.rpc(msg, this, Self::doc_get_sync_peers).await,

            AuthorList(msg) => chan.server_streaming(msg, this, Self::author_list).await,
            AuthorCreate(msg) => chan.rpc(msg, this, Self::author_create).await,
            AuthorImport(msg) => chan.rpc(msg, this, Self::author_import).await,
            AuthorExport(msg) => chan.rpc(msg, this, Self::author_export).await,
            AuthorDelete(msg) => chan.rpc(msg, this, Self::author_delete).await,
            AuthorGetDefault(msg) => chan.rpc(msg, this, Self::author_default).await,
            AuthorSetDefault(msg) => chan.rpc(msg, this, Self::author_set_default).await,
        }
    }
}
