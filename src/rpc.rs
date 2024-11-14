//! Quic RPC implementation for docs.

use proto::{Request, Response, RpcService};
use quic_rpc::{
    server::{ChannelTypes, RpcChannel},
    transport::flume::FlumeConnector,
    RpcClient, RpcServer,
};
use tokio_util::task::AbortOnDropHandle;

use crate::engine::Engine;

pub mod client;
pub mod proto;

mod docs_handle_request;

type RpcError = serde_error::Error;
type RpcResult<T> = std::result::Result<T, RpcError>;

impl<D: iroh_blobs::store::Store> Engine<D> {
    /// Get an in memory client to interact with the docs engine.
    pub fn client(&self) -> &client::docs::Client<FlumeConnector<Response, Request>> {
        &self
            .rpc_handler
            .get_or_init(|| RpcHandler::new(self))
            .client
    }

    /// Handle a docs request from the RPC server.
    pub async fn handle_rpc_request<C: ChannelTypes<RpcService>>(
        self,
        msg: Request,
        chan: RpcChannel<RpcService, C>,
    ) -> Result<(), quic_rpc::server::RpcServerError<C>> {
        use Request::*;
        let this = self;
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

#[derive(Debug)]
pub(crate) struct RpcHandler {
    /// Client to hand out
    client: client::docs::Client<FlumeConnector<Response, Request>>,
    /// Handler task
    _handler: AbortOnDropHandle<()>,
}

impl RpcHandler {
    fn new<D: iroh_blobs::store::Store>(engine: &Engine<D>) -> Self {
        let engine = engine.clone();
        let (listener, connector) = quic_rpc::transport::flume::channel(1);
        let listener = RpcServer::new(listener);
        let client = client::docs::Client::new(RpcClient::new(connector));
        let _handler = listener
            .spawn_accept_loop(move |req, chan| engine.clone().handle_rpc_request(req, chan));
        Self { client, _handler }
    }
}
