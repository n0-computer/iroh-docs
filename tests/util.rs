#![cfg(feature = "rpc")]
#![allow(dead_code)]
use std::{
    collections::BTreeSet,
    marker::PhantomData,
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use iroh_blobs::{
    store::{GcConfig, Store as BlobStore},
    util::local_pool::{LocalPool, Run},
};
use iroh_net::{discovery::Discovery, dns::DnsResolver, key::SecretKey, relay::RelayMode, NodeId};
use nested_enum_utils::enum_conversions;
use quic_rpc::transport::{Connector, Listener};
use serde::{Deserialize, Serialize};
use tokio_util::task::AbortOnDropHandle;

/// Default bind address for the node.
/// 11204 is "iroh" in leetspeak <https://simple.wikipedia.org/wiki/Leet>
pub const DEFAULT_BIND_PORT: u16 = 11204;

/// The default bind address for the iroh IPv4 socket.
pub const DEFAULT_BIND_ADDR_V4: SocketAddrV4 =
    SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DEFAULT_BIND_PORT);

/// The default bind address for the iroh IPv6 socket.
pub const DEFAULT_BIND_ADDR_V6: SocketAddrV6 =
    SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, DEFAULT_BIND_PORT + 1, 0, 0);

/// An iroh node that just has the blobs transport
#[derive(Debug)]
pub struct Node<S> {
    router: iroh_router::Router,
    client: Client,
    store: S,
    local_pool: LocalPool,
    rpc_task: AbortOnDropHandle<()>,
    gc_task: Option<Run<()>>,
}

impl<S> Deref for Node<S> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
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
#[derive(derive_more::Debug)]
pub struct Builder<S> {
    path: Option<PathBuf>,
    secret_key: Option<SecretKey>,
    relay_mode: RelayMode,
    dns_resolver: Option<DnsResolver>,
    node_discovery: Option<Box<dyn Discovery>>,
    gc_interval: Option<std::time::Duration>,
    #[debug(skip)]
    register_gc_done_cb: Option<Box<dyn Fn() + Send + 'static>>,
    insecure_skip_relay_cert_verify: bool,
    bind_random_port: bool,
    _p: PhantomData<S>,
}

impl<S: BlobStore> Builder<S> {
    /// Spawns the node
    async fn spawn0(self, store: S) -> anyhow::Result<Node<S>> {
        let mut addr_v4 = DEFAULT_BIND_ADDR_V4;
        let mut addr_v6 = DEFAULT_BIND_ADDR_V6;
        if self.bind_random_port {
            addr_v4.set_port(0);
            addr_v6.set_port(0);
        }
        let mut builder = iroh_net::Endpoint::builder()
            .bind_addr_v4(addr_v4)
            .bind_addr_v6(addr_v6)
            .discovery_n0()
            .relay_mode(self.relay_mode.clone())
            .insecure_skip_relay_cert_verify(self.insecure_skip_relay_cert_verify);
        if let Some(dns_resolver) = self.dns_resolver.clone() {
            builder = builder.dns_resolver(dns_resolver);
        }
        let endpoint = builder.bind().await?;
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
        let gossip = iroh_gossip::net::Gossip::from_endpoint(
            endpoint.clone(),
            Default::default(),
            &addr.info,
        );
        let replica_store = match self.path {
            Some(ref path) => iroh_docs::store::Store::persistent(path.join("docs.redb"))?,
            None => iroh_docs::store::Store::memory(),
        };
        let author_store = match self.path {
            Some(ref path) => {
                iroh_docs::engine::DefaultAuthorStorage::Persistent(path.join("default-author"))
            }
            None => iroh_docs::engine::DefaultAuthorStorage::Mem,
        };
        let docs = match iroh_docs::engine::Engine::spawn(
            endpoint,
            gossip.clone(),
            replica_store,
            store.clone(),
            downloader,
            author_store,
            local_pool.handle().clone(),
        )
        .await
        {
            Ok(docs) => docs,
            Err(err) => {
                store.shutdown().await;
                return Err(err);
            }
        };
        router = router.accept(iroh_blobs::protocol::ALPN.to_vec(), blobs.clone());
        router = router.accept(iroh_docs::net::DOCS_ALPN.to_vec(), Arc::new(docs.clone()));
        router = router.accept(
            iroh_gossip::net::GOSSIP_ALPN.to_vec(),
            Arc::new(gossip.clone()),
        );

        // Build the router
        let router = router.spawn().await?;

        // Setup RPC
        let (internal_rpc, controller) =
            quic_rpc::transport::flume::channel::<Request, Response>(1);
        let controller = controller.boxed();
        let internal_rpc = internal_rpc.boxed();
        let internal_rpc = quic_rpc::RpcServer::<Service>::new(internal_rpc);

        let docs2 = docs.clone();
        let rpc_task: tokio::task::JoinHandle<()> = tokio::task::spawn(async move {
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

        let _gc_task = if let Some(period) = self.gc_interval {
            let store = store.clone();
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
                            done_callback: self.register_gc_done_cb,
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
            store,
            rpc_task: AbortOnDropHandle::new(rpc_task),
            gc_task: _gc_task,
            local_pool,
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

    pub fn dns_resolver(mut self, value: DnsResolver) -> Self {
        self.dns_resolver = Some(value);
        self
    }

    pub fn node_discovery(mut self, value: Box<dyn Discovery>) -> Self {
        self.node_discovery = Some(value);
        self
    }

    pub fn gc_interval(mut self, value: Option<std::time::Duration>) -> Self {
        self.gc_interval = value;
        self
    }

    pub fn register_gc_done_cb(mut self, value: Box<dyn Fn() + Send + Sync>) -> Self {
        self.register_gc_done_cb = Some(value);
        self
    }

    pub fn insecure_skip_relay_cert_verify(mut self, value: bool) -> Self {
        self.insecure_skip_relay_cert_verify = value;
        self
    }

    pub fn bind_random_port(mut self) -> Self {
        self.bind_random_port = true;
        self
    }

    fn new(path: Option<PathBuf>) -> Self {
        Self {
            path,
            secret_key: None,
            relay_mode: RelayMode::Default,
            gc_interval: None,
            insecure_skip_relay_cert_verify: false,
            bind_random_port: false,
            dns_resolver: None,
            node_discovery: None,
            register_gc_done_cb: None,
            _p: PhantomData,
        }
    }
}

impl Node<iroh_blobs::store::mem::Store> {
    /// Creates a new node with memory storage
    pub fn memory() -> Builder<iroh_blobs::store::mem::Store> {
        Builder::new(None)
    }
}

impl Builder<iroh_blobs::store::mem::Store> {
    /// Spawns the node
    pub async fn spawn(self) -> anyhow::Result<Node<iroh_blobs::store::mem::Store>> {
        let store = iroh_blobs::store::mem::Store::new();
        self.spawn0(store).await
    }
}

impl Node<iroh_blobs::store::fs::Store> {
    /// Creates a new node with persistent storage
    pub fn persistent(path: impl AsRef<Path>) -> Builder<iroh_blobs::store::fs::Store> {
        let path = Some(path.as_ref().to_owned());
        Builder::new(path)
    }
}

impl Builder<iroh_blobs::store::fs::Store> {
    /// Spawns the node
    pub async fn spawn(self) -> anyhow::Result<Node<iroh_blobs::store::fs::Store>> {
        let store = iroh_blobs::store::fs::Store::load(self.path.clone().unwrap()).await?;
        self.spawn0(store).await
    }
}

impl<S> Node<S> {
    /// Returns the node id
    pub fn node_id(&self) -> NodeId {
        self.router.endpoint().node_id()
    }

    /// Returns the blob store
    pub fn blob_store(&self) -> &S {
        &self.store
    }

    /// Shuts down the node
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.router.shutdown().await?;
        self.local_pool.shutdown().await;
        self.rpc_task.abort();
        if let Some(mut task) = self.gc_task.take() {
            task.abort();
        }
        Ok(())
    }

    /// Returns the client
    pub fn client(&self) -> &Client {
        &self.client
    }
}
