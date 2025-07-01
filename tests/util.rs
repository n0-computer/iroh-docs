#![cfg(feature = "rpc")]
#![allow(dead_code)]
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
    ops::Deref,
    path::{Path, PathBuf},
};

use iroh::{discovery::IntoDiscovery, dns::DnsResolver, NodeId, RelayMode, SecretKey};
use iroh_docs::protocol::Docs;
use iroh_gossip::net::Gossip;

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
pub struct Node {
    router: iroh::protocol::Router,
    client: Client,
    // store: iroh_blobs::api::Store,
    // rpc_task: AbortOnDropHandle<()>,
}

impl Deref for Node {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    blobs: iroh_blobs::api::Store,
    docs: iroh_docs::rpc2::api::DocsApi,
}

impl Client {
    fn new(blobs: iroh_blobs::api::Store, docs: iroh_docs::rpc2::api::DocsApi) -> Self {
        Self { blobs, docs }
    }

    pub fn blobs(&self) -> &iroh_blobs::api::Store {
        &self.blobs
    }

    pub fn docs(&self) -> &iroh_docs::rpc2::api::DocsApi {
        &self.docs
    }
}

/// An iroh node builder
#[derive(derive_more::Debug)]
pub struct Builder {
    endpoint: iroh::endpoint::Builder,
    use_n0_discovery: bool,
    path: Option<PathBuf>,
    // node_discovery: Option<Box<dyn Discovery>>,
    gc_interval: Option<std::time::Duration>,
    #[debug(skip)]
    register_gc_done_cb: Option<Box<dyn Fn() + Send + 'static>>,
    bind_random_port: bool,
}

impl Builder {
    /// Spawns the node
    async fn spawn0(self, blobs: iroh_blobs::api::Store) -> anyhow::Result<Node> {
        let mut addr_v4 = DEFAULT_BIND_ADDR_V4;
        let mut addr_v6 = DEFAULT_BIND_ADDR_V6;
        if self.bind_random_port {
            addr_v4.set_port(0);
            addr_v6.set_port(0);
        }
        let mut builder = self.endpoint.bind_addr_v4(addr_v4).bind_addr_v6(addr_v6);
        if self.use_n0_discovery {
            builder = builder.discovery_n0();
        }
        builder = builder.discovery_n0();
        let endpoint = builder.bind().await?;
        let mut router = iroh::protocol::Router::builder(endpoint.clone());
        // let blobs = Blobs::builder(store.clone()).build(&endpoint);
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let docs_builder = match self.path {
            Some(ref path) => Docs::persistent(path.to_path_buf()),
            None => Docs::memory(),
        };
        let docs = match docs_builder
            .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
            .await
        {
            Ok(docs) => docs,
            Err(err) => {
                blobs.shutdown().await.ok();
                return Err(err);
            }
        };
        router = router.accept(
            iroh_blobs::ALPN,
            iroh_blobs::net_protocol::Blobs::new(&blobs, endpoint.clone(), None),
        );
        router = router.accept(iroh_docs::ALPN, docs.clone());
        router = router.accept(iroh_gossip::ALPN, gossip.clone());

        // Build the router
        let router = router.spawn();

        // Setup RPC
        // let (internal_rpc, controller) =
        //     quic_rpc::transport::flume::channel::<Request, Response>(1);
        // let controller = controller.boxed();
        // let internal_rpc = internal_rpc.boxed();
        // let internal_rpc = quic_rpc::RpcServer::<Service>::new(internal_rpc);

        // let docs2 = docs.clone();
        // let blobs2 = blobs.clone();
        // let rpc_task: tokio::task::JoinHandle<()> = tokio::task::spawn(async move {
        //     loop {
        //         let request = internal_rpc.accept().await;
        //         match request {
        //             Ok(accepting) => {
        //                 let blobs = blobs2.clone();
        //                 let docs = docs2.clone();
        //                 tokio::task::spawn(async move {
        //                     let (msg, chan) = accepting.read_first().await?;
        //                     match msg {
        //                         Request::BlobsOrTags(msg) => {
        //                             blobs.handle_rpc_request(msg, chan.map().boxed()).await?;
        //                         }
        //                         Request::Docs(msg) => {
        //                             docs.handle_rpc_request(msg, chan.map().boxed()).await?;
        //                         }
        //                     }
        //                     anyhow::Ok(())
        //                 });
        //             }
        //             Err(err) => {
        //                 tracing::warn!("rpc error: {:?}", err);
        //             }
        //         }
        //     }
        // });

        // let client = quic_rpc::RpcClient::new(controller);

        // TODO: Make this work again.
        // if let Some(period) = self.gc_interval {
        //     blobs.add_protected(docs.protect_cb())?;
        //     blobs.start_gc(GcConfig {
        //         period,
        //         done_callback: self.register_gc_done_cb,
        //     })?;
        // }

        let client = Client::new(blobs.clone(), docs.api().clone());
        Ok(Node {
            router,
            client,
            // store: blobs,
            // rpc_task: AbortOnDropHandle::new(rpc_task),
        })
    }

    pub fn secret_key(mut self, value: SecretKey) -> Self {
        self.endpoint = self.endpoint.secret_key(value);
        self
    }

    pub fn relay_mode(mut self, value: RelayMode) -> Self {
        self.endpoint = self.endpoint.relay_mode(value);
        self
    }

    pub fn dns_resolver(mut self, value: DnsResolver) -> Self {
        self.endpoint = self.endpoint.dns_resolver(value);
        self
    }

    pub fn node_discovery(mut self, value: impl IntoDiscovery) -> Self {
        self.use_n0_discovery = false;
        self.endpoint = self.endpoint.discovery(value);
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
        self.endpoint = self.endpoint.insecure_skip_relay_cert_verify(value);
        self
    }

    pub fn bind_random_port(mut self) -> Self {
        self.bind_random_port = true;
        self
    }

    fn new(path: Option<PathBuf>) -> Self {
        Self {
            endpoint: iroh::Endpoint::builder(),
            use_n0_discovery: true,
            path,
            gc_interval: None,
            bind_random_port: false,
            // node_discovery: None,
            register_gc_done_cb: None,
            // _p: PhantomData,
        }
    }
}

impl Node {
    /// Creates a new node with memory storage
    pub fn memory() -> Builder {
        Builder::new(None)
    }

    /// Creates a new node with persistent storage
    pub fn persistent(path: impl AsRef<Path>) -> Builder {
        let path = Some(path.as_ref().to_owned());
        Builder::new(path)
    }
}

impl Builder {
    /// Spawns the node
    pub async fn spawn(self) -> anyhow::Result<Node> {
        let store = match self.path {
            None => {
                let store = iroh_blobs::store::mem::MemStore::new();
                (&*store).clone()
            }
            Some(ref path) => {
                let store = iroh_blobs::store::fs::FsStore::load(path.clone()).await?;
                (&*store).clone()
            }
        };
        self.spawn0(store).await
    }
}

impl Node {
    /// Returns the node id
    pub fn node_id(&self) -> NodeId {
        self.router.endpoint().node_id()
    }

    // /// Returns the blob store
    // pub fn blob_store(&self) -> &S {
    //     &self.store
    // }

    /// Shuts down the node
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.router.shutdown().await?;
        // self.rpc_task.abort();
        Ok(())
    }

    /// Returns the client
    pub fn client(&self) -> &Client {
        &self.client
    }
}
