//! An iroh node that just has the blobs transport
use std::{collections::BTreeSet, sync::Arc};

use iroh_blobs::{
    store::{GcConfig, Store as BlobStore},
    util::local_pool::{LocalPool, Run},
};
use iroh_net::{key::SecretKey, relay::RelayMode, NodeId};
use nested_enum_utils::enum_conversions;
use quic_rpc::transport::{Connector, Listener};
use serde::{Deserialize, Serialize};
use tokio_util::task::AbortOnDropHandle;

/// An iroh node that just has the blobs transport
#[derive(Debug)]
pub struct Node<S> {
    router: iroh_router::Router,
    client: Client,
    _store: S,
    _local_pool: LocalPool,
    _rpc_task: AbortOnDropHandle<()>,
    _gc_task: Option<Run<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[enum_conversions]
enum Request {
    BlobsOrTags(iroh_blobs::rpc::proto::Request),
    Docs(iroh_docs::rpc::proto::Request),
}

#[derive(Debug, Serialize, Deserialize)]
#[enum_conversions]
enum Response {
    BlobsOrTags(iroh_blobs::rpc::proto::Response),
    Docs(iroh_docs::rpc::proto::Response),
}

#[derive(Debug, Clone, Copy)]
struct Service;

impl quic_rpc::Service for Service {
    type Req = Request;
    type Res = Response;
}

#[derive(Debug, Clone)]
pub struct Client {
    blobs: iroh_blobs::rpc::client::blobs::Client,
    docs: iroh_docs::rpc::client::Client,
}

impl Client {
    fn new(client: quic_rpc::RpcClient<Service>) -> Self {
        Self {
            blobs: iroh_blobs::rpc::client::blobs::Client::new(client.clone().map().boxed()),
            docs: iroh_docs::rpc::client::Client::new(client.map().boxed()),
        }
    }

    pub fn blobs(&self) -> &iroh_blobs::rpc::client::blobs::Client {
        &self.blobs
    }

    pub fn docs(&self) -> &iroh_docs::rpc::client::Client {
        &self.docs
    }
}

/// An iroh node builder
#[derive(Debug)]
pub struct Builder<S> {
    store: S,
    secret_key: Option<SecretKey>,
    relay_mode: RelayMode,
    gc_interval: Option<std::time::Duration>,
}

impl<S: BlobStore> Builder<S> {
    /// Spawns the node
    pub async fn spawn(self) -> anyhow::Result<Node<S>> {
        let (client, router, rpc_task, docs, local_pool) =
            setup_router(self.store.clone(), self.relay_mode).await?;
        let _gc_task = if let Some(period) = self.gc_interval {
            let store = self.store.clone();
            let local_pool = local_pool.clone();
            let docs = docs.clone();
            let protected_cb = move || {
                let docs = docs.clone();
                async move {
                    let mut live = BTreeSet::default();
                    let doc_hashes = match docs.sync.content_hashes().await {
                        Ok(hashes) => hashes,
                        Err(err) => {
                            tracing::warn!("Error getting doc hashes: {}", err);
                            return live;
                        }
                    };
                    for hash in doc_hashes {
                        match hash {
                            Ok(hash) => {
                                live.insert(hash);
                            }
                            Err(err) => {
                                tracing::error!("Error getting doc hash: {}", err);
                            }
                        }
                    }
                    live
                }
            };
            Some(local_pool.spawn(move || async move {
                store
                    .gc_run(
                        GcConfig {
                            period,
                            done_callback: None,
                        },
                        protected_cb,
                    )
                    .await
            }))
        } else {
            None
        };
        let client = Client::new(client);
        Ok(Node {
            router,
            client,
            _store: self.store,
            _rpc_task: AbortOnDropHandle::new(rpc_task),
            _gc_task,
            _local_pool: local_pool,
        })
    }

    pub fn secret_key(mut self, value: SecretKey) -> Self {
        self.secret_key = Some(value);
        self
    }

    pub fn relay_mode(mut self, value: RelayMode) -> Self {
        self.relay_mode = value;
        self
    }

    pub fn gc_interval(mut self, value: Option<std::time::Duration>) -> Self {
        self.gc_interval = value;
        self
    }
}

impl Node<iroh_blobs::store::mem::Store> {
    /// Creates a new node with memory storage
    pub fn memory() -> Builder<iroh_blobs::store::mem::Store> {
        Builder {
            store: iroh_blobs::store::mem::Store::new(),
            secret_key: None,
            relay_mode: RelayMode::Default,
            gc_interval: None,
        }
    }
}

impl<S> Node<S> {
    /// Returns the node id
    pub fn node_id(&self) -> NodeId {
        self.router.endpoint().node_id()
    }

    /// Shuts down the node
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.router.shutdown().await
    }

    /// Returns the client
    pub fn client(&self) -> &Client {
        &self.client
    }
}

async fn setup_router<S: iroh_blobs::store::Store>(
    store: S,
    relay_mode: RelayMode,
) -> anyhow::Result<(
    quic_rpc::RpcClient<Service>,
    iroh_router::Router,
    tokio::task::JoinHandle<()>,
    iroh_docs::engine::Engine<S>,
    LocalPool,
)> {
    let endpoint = iroh_net::Endpoint::builder()
        .discovery_n0()
        .relay_mode(relay_mode)
        .bind()
        .await?;
    let addr = endpoint.node_addr().await?;
    let local_pool = LocalPool::single();
    let mut router = iroh_router::Router::builder(endpoint.clone());

    // Setup blobs
    let downloader = iroh_blobs::downloader::Downloader::new(
        store.clone(),
        endpoint.clone(),
        local_pool.handle().clone(),
    );
    let blobs = Arc::new(iroh_blobs::net_protocol::Blobs::new_with_events(
        store.clone(),
        local_pool.handle().clone(),
        Default::default(),
        downloader.clone(),
        endpoint.clone(),
    ));
    let gossip =
        iroh_gossip::net::Gossip::from_endpoint(endpoint.clone(), Default::default(), &addr.info);
    let docs = iroh_docs::engine::Engine::spawn(
        endpoint,
        gossip.clone(),
        iroh_docs::store::Store::memory(),
        store.clone(),
        downloader,
        iroh_docs::engine::DefaultAuthorStorage::Mem,
        local_pool.handle().clone(),
    )
    .await?;
    router = router.accept(iroh_blobs::protocol::ALPN.to_vec(), blobs.clone());
    router = router.accept(iroh_docs::net::DOCS_ALPN.to_vec(), Arc::new(docs.clone()));
    router = router.accept(
        iroh_gossip::net::GOSSIP_ALPN.to_vec(),
        Arc::new(gossip.clone()),
    );

    // Build the router
    let router = router.spawn().await?;

    // Setup RPC
    let (internal_rpc, controller) = quic_rpc::transport::flume::channel::<Request, Response>(32);
    let controller = controller.boxed();
    let internal_rpc = internal_rpc.boxed();
    let internal_rpc = quic_rpc::RpcServer::<Service>::new(internal_rpc);

    let docs2 = docs.clone();
    let rpc_server_task: tokio::task::JoinHandle<()> = tokio::task::spawn(async move {
        loop {
            let request = internal_rpc.accept().await;
            match request {
                Ok(accepting) => {
                    let blobs = blobs.clone();
                    let docs = docs2.clone();
                    tokio::task::spawn(async move {
                        let (msg, chan) = accepting.read_first().await?;
                        match msg {
                            Request::BlobsOrTags(msg) => {
                                blobs.handle_rpc_request(msg, chan.map().boxed()).await?;
                            }
                            Request::Docs(msg) => {
                                docs.handle_rpc_request(msg, chan.map().boxed()).await?;
                            }
                        }
                        anyhow::Ok(())
                    });
                }
                Err(err) => {
                    tracing::warn!("rpc error: {:?}", err);
                }
            }
        }
    });

    let client = quic_rpc::RpcClient::new(controller);

    Ok((client, router, rpc_server_task, docs, local_pool))
}
