use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{net_protocol::Blobs, util::local_pool::LocalPool, ALPN as BLOBS_ALPN};
use iroh_docs::{protocol::Docs, ALPN as DOCS_ALPN};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // create an iroh endpoint that includes the standard discovery mechanisms
    // we've built at number0
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;

    // create a router builder, we will add the
    // protocols to this builder and then spawn
    // the router
    let builder = Router::builder(endpoint);

    // build the blobs protocol
    let local_pool = LocalPool::default();
    let blobs = Blobs::memory().build(local_pool.handle(), builder.endpoint());

    // build the gossip protocol
    let gossip = Gossip::builder().spawn(builder.endpoint().clone()).await?;

    // build the docs protocol
    let docs = Docs::memory().spawn(&blobs, &gossip).await?;

    // setup router
    let router = builder
        .accept(BLOBS_ALPN, blobs)
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs)
        .spawn()
        .await?;

    // do fun stuff with docs!
    drop(router);

    Ok(())
}
