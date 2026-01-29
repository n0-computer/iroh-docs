#![allow(dead_code)]
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6},
    ops::Deref,
    path::{Path, PathBuf},
};

use iroh::{address_lookup::IntoAddressLookup, dns::DnsResolver, EndpointId, RelayMode, SecretKey};
use iroh_blobs::store::{fs::options::Options, GcConfig};
use iroh_docs::{engine::ProtectCallbackHandler, protocol::Docs};
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
    docs: iroh_docs::api::DocsApi,
}

impl Client {
    fn new(blobs: iroh_blobs::api::Store, docs: iroh_docs::api::DocsApi) -> Self {
        Self { blobs, docs }
    }

    pub fn blobs(&self) -> &iroh_blobs::api::Store {
        &self.blobs
    }

    pub fn docs(&self) -> &iroh_docs::api::DocsApi {
        &self.docs
    }
}

/// An iroh node builder
#[derive(derive_more::Debug)]
pub struct Builder {
    endpoint: iroh::endpoint::Builder,
    use_n0_address_lookup: bool,
    path: Option<PathBuf>,
    // node_address_lookup: Option<Box<dyn AddressLookup>>,
    gc_interval: Option<std::time::Duration>,
    #[debug(skip)]
    register_gc_done_cb: Option<Box<dyn Fn() + Send + 'static>>,
    bind_random_port: bool,
}

impl Builder {
    /// Spawns the node
    async fn spawn0(
        self,
        blobs: iroh_blobs::api::Store,
        protect_cb: Option<ProtectCallbackHandler>,
    ) -> anyhow::Result<Node> {
        let mut addr_v4 = DEFAULT_BIND_ADDR_V4;
        let mut addr_v6 = DEFAULT_BIND_ADDR_V6;
        if self.bind_random_port {
            addr_v4.set_port(0);
            addr_v6.set_port(0);
        }
        let mut builder = self.endpoint.bind_addr(addr_v4)?.bind_addr(addr_v6)?;
        if self.use_n0_address_lookup {
            builder = builder.address_lookup(iroh::address_lookup::pkarr::PkarrPublisher::n0_dns());
            // Resolve using HTTPS requests to our DNS server's /pkarr path in browsers
            builder = builder.address_lookup(iroh::address_lookup::pkarr::PkarrResolver::n0_dns());
            // Resolve using DNS queries outside browsers.
            builder = builder.address_lookup(iroh::address_lookup::dns::DnsAddressLookup::n0_dns());
        }

        let endpoint = builder.bind().await?;
        let mut router = iroh::protocol::Router::builder(endpoint.clone());
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let mut docs_builder = match self.path {
            Some(ref path) => Docs::persistent(path.to_path_buf()),
            None => Docs::memory(),
        };
        if let Some(protect_cb) = protect_cb {
            docs_builder = docs_builder.protect_handler(protect_cb);
        }
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
            iroh_blobs::BlobsProtocol::new(&blobs, None),
        );
        router = router.accept(iroh_docs::ALPN, docs.clone());
        router = router.accept(iroh_gossip::ALPN, gossip.clone());

        // Build the router
        let router = router.spawn();

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

    pub fn node_address_lookup(mut self, value: impl IntoAddressLookup) -> Self {
        self.use_n0_address_lookup = false;
        self.endpoint = self.endpoint.address_lookup(value);
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

    pub fn bind_random_port(mut self, bind_random_port: bool) -> Self {
        self.bind_random_port = bind_random_port;
        self
    }

    fn new(path: Option<PathBuf>) -> Self {
        Self {
            endpoint: iroh::Endpoint::empty_builder(RelayMode::Disabled),
            use_n0_address_lookup: true,
            path,
            gc_interval: None,
            bind_random_port: true,
            // node_address_lookup: None,
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
        let (store, protect_handler) = match self.path {
            None => {
                let store = iroh_blobs::store::mem::MemStore::new();
                ((*store).clone(), None)
            }
            Some(ref path) => {
                let db_path = path.join("blobs.db");
                let mut opts = Options::new(path);
                let protect_handler = if let Some(interval) = self.gc_interval {
                    let (handler, cb) = ProtectCallbackHandler::new();
                    opts.gc = Some(GcConfig {
                        interval,
                        add_protected: Some(cb),
                    });
                    Some(handler)
                } else {
                    None
                };
                let store = iroh_blobs::store::fs::FsStore::load_with_opts(db_path, opts).await?;
                ((*store).clone(), protect_handler)
            }
        };
        self.spawn0(store, protect_handler).await
    }
}

impl Node {
    /// Returns the node id
    pub fn id(&self) -> EndpointId {
        self.router.endpoint().id()
    }

    // /// Returns the blob store
    // pub fn blob_store(&self) -> &S {
    //     &self.store
    // }

    /// Ensure the node is "online", aka, is connected to a relay and
    /// has a direct addresses
    pub async fn online(&self) {
        self.router.endpoint().online().await
    }

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

pub mod path {
    use std::path::{Component, Path, PathBuf};

    use anyhow::Context;
    use bytes::Bytes;

    /// Helper function that translates a key that was derived from the [`path_to_key`] function back
    /// into a path.
    ///
    /// If `prefix` exists, it will be stripped before converting back to a path
    /// If `root` exists, will add the root as a parent to the created path
    /// Removes any null byte that has been appended to the key
    pub fn key_to_path(
        key: impl AsRef<[u8]>,
        prefix: Option<String>,
        root: Option<PathBuf>,
    ) -> anyhow::Result<PathBuf> {
        let mut key = key.as_ref();
        if key.is_empty() {
            return Ok(PathBuf::new());
        }
        // if the last element is the null byte, remove it
        if b'\0' == key[key.len() - 1] {
            key = &key[..key.len() - 1]
        }

        let key = if let Some(prefix) = prefix {
            let prefix = prefix.into_bytes();
            if prefix[..] == key[..prefix.len()] {
                &key[prefix.len()..]
            } else {
                anyhow::bail!("key {:?} does not begin with prefix {:?}", key, prefix);
            }
        } else {
            key
        };

        let mut path = if key[0] == b'/' {
            PathBuf::from("/")
        } else {
            PathBuf::new()
        };
        for component in key
            .split(|c| c == &b'/')
            .map(|c| String::from_utf8(c.into()).context("key contains invalid data"))
        {
            let component = component?;
            path = path.join(component);
        }

        // add root if it exists
        let path = if let Some(root) = root {
            root.join(path)
        } else {
            path
        };

        Ok(path)
    }

    /// Helper function that creates a document key from a canonicalized path, removing the `root` and adding the `prefix`, if they exist
    ///
    /// Appends the null byte to the end of the key.
    pub fn path_to_key(
        path: impl AsRef<Path>,
        prefix: Option<String>,
        root: Option<PathBuf>,
    ) -> anyhow::Result<Bytes> {
        let path = path.as_ref();
        let path = if let Some(root) = root {
            path.strip_prefix(root)?
        } else {
            path
        };
        let suffix = canonicalized_path_to_string(path, false)?.into_bytes();
        let mut key = if let Some(prefix) = prefix {
            prefix.into_bytes().to_vec()
        } else {
            Vec::new()
        };
        key.extend(suffix);
        key.push(b'\0');
        Ok(key.into())
    }

    /// This function converts an already canonicalized path to a string.
    ///
    /// If `must_be_relative` is true, the function will fail if any component of the path is
    /// `Component::RootDir`
    ///
    /// This function will also fail if the path is non canonical, i.e. contains
    /// `..` or `.`, or if the path components contain any windows or unix path
    /// separators.
    pub fn canonicalized_path_to_string(
        path: impl AsRef<Path>,
        must_be_relative: bool,
    ) -> anyhow::Result<String> {
        let mut path_str = String::new();
        let parts = path
            .as_ref()
            .components()
            .filter_map(|c| match c {
                Component::Normal(x) => {
                    let c = match x.to_str() {
                        Some(c) => c,
                        None => return Some(Err(anyhow::anyhow!("invalid character in path"))),
                    };

                    if !c.contains('/') && !c.contains('\\') {
                        Some(Ok(c))
                    } else {
                        Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                    }
                }
                Component::RootDir => {
                    if must_be_relative {
                        Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                    } else {
                        path_str.push('/');
                        None
                    }
                }
                _ => Some(Err(anyhow::anyhow!("invalid path component {:?}", c))),
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let parts = parts.join("/");
        path_str.push_str(&parts);
        Ok(path_str)
    }
}
