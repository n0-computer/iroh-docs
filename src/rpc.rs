//! Quic RPC implementation for docs.

use proto::RpcService;
use quic_rpc::{
    server::{ChannelTypes, RpcChannel},
    transport::flume::FlumeConnector,
    RpcClient, RpcServer,
};
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, warn};

use crate::engine::Engine;

pub mod client;
pub mod proto;

mod docs_handle_request;

type RpcError = serde_error::Error;
type RpcResult<T> = std::result::Result<T, RpcError>;

impl<D: iroh_blobs::store::Store> Engine<D> {
    /// Get an in memory client to interact with the docs engine.
    pub fn client(
        &self,
    ) -> &crate::rpc::client::docs::Client<
        FlumeConnector<crate::rpc::proto::Response, crate::rpc::proto::Request>,
    > {
        &self
            .rpc_handler
            .get_or_init(|| RpcHandler::new(self))
            .client
    }

    /// Handle a docs request from the RPC server.
    pub async fn handle_rpc_request<C: ChannelTypes<RpcService>>(
        &self,
        msg: crate::rpc::proto::Request,
        chan: RpcChannel<RpcService, C>,
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

#[derive(Debug)]
pub(crate) struct RpcHandler {
    /// Client to hand out
    client: crate::rpc::client::docs::Client<
        FlumeConnector<crate::rpc::proto::Response, crate::rpc::proto::Request>,
    >,
    /// Handler task
    _handler: AbortOnDropHandle<()>,
}

impl RpcHandler {
    fn new<D: iroh_blobs::store::Store>(engine: &Engine<D>) -> Self {
        let engine = engine.clone();
        let (listener, connector) = quic_rpc::transport::flume::channel(1);
        let listener = RpcServer::new(listener);
        let client = crate::rpc::client::docs::Client::new(RpcClient::new(connector));
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    Some(res) = tasks.join_next(), if !tasks.is_empty() => {
                        if let Err(e) = res {
                            if e.is_panic() {
                                error!("Panic handling RPC request: {e}");
                            }
                        }
                    }
                    req = listener.accept() => {
                        let req = match req {
                            Ok(req) => req,
                            Err(e) => {
                                warn!("Error accepting RPC request: {e}");
                                continue;
                            }
                        };
                        let engine = engine.clone();
                        tasks.spawn(async move {
                            let (req, client) = match req.read_first().await {
                                Ok((req, client)) => (req, client),
                                Err(e) => {
                                    warn!("Error reading first message: {e}");
                                    return;
                                }
                            };
                            if let Err(cause) = engine.handle_rpc_request(req, client).await {
                                warn!("Error handling RPC request: {:?}", cause);
                            }
                        });
                    }
                }
            }
        });
        Self {
            client,
            _handler: AbortOnDropHandle::new(task),
        }
    }
}
