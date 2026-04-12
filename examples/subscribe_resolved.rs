//! Subscribe to the **latest entry per key** under a **key prefix**, including **blob bytes**, on **one stream**.
//!
//! [`iroh_docs::api::Doc::subscribe_resolved`] uses a [`Query::single_latest_per_key`] scope (here
//! [`QueryBuilder::key_prefix`](iroh_docs::store::QueryBuilder::key_prefix)). Each stream item is
//! the current winning entry for some key in range, emitted only once its blob is locally
//! [`BlobStatus::Complete`](iroh_blobs::api::blobs::BlobStatus::Complete) (or the record is empty).
//!
//! With `initial_snapshot: true`, the resolver first emits the latest entry for every matching key
//! that already exists; then it continues with live updates.
//!
//! The `main` body uses the usual pattern for long-lived subscriptions: `while let Some(result) =
//! stream.next().await`, handle each `Ok`/`Err`, and exit when the stream yields `None` (sender
//! dropped or subscription closed). This demo **breaks** after three items so the process can exit:
//! (1) snapshot for an existing key, (2) live insert for a **new** key under the prefix, (3) live
//! update when **`app/foo` is written again**—same key, new latest entry. Omit that `break` in a
//! real app, or drive the loop with `tokio::select!` alongside shutdown.

use anyhow::Result;
use futures_util::StreamExt;
use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{store::mem::MemStore, BlobsProtocol, ALPN as BLOBS_ALPN};
use iroh_docs::{
    protocol::Docs, store::Query, subscribe_resolved::ResolvedSubscribeOpts, ALPN as DOCS_ALPN,
};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};

#[tokio::main]
async fn main() -> Result<()> {
    let endpoint = Endpoint::empty_builder().bind().await?;

    let blobs = MemStore::default();
    let blob_store = (*blobs).clone();

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let docs = Docs::memory()
        .spawn(endpoint.clone(), blob_store.clone(), gossip.clone())
        .await?;

    let router = Router::builder(endpoint.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    let author = docs.author_create().await?;
    let doc = docs.create().await?;

    doc.set_bytes(author, b"app/foo".to_vec(), b"hello".to_vec())
        .await?;

    let scope = Query::single_latest_per_key().key_prefix(b"app/").build();

    let mut stream = doc
        .subscribe_resolved(
            &blob_store,
            scope,
            ResolvedSubscribeOpts {
                initial_snapshot: true,
                include_content: true,
                resolution_delay: None,
            },
        )
        .await?;

    let mut updates = 0u32;

    while let Some(result) = stream.next().await {
        let kv = result?;
        updates += 1;
        println!(
            "update #{updates}: key={:?} content={:?}",
            String::from_utf8_lossy(&kv.key),
            kv.content.as_deref().map(String::from_utf8_lossy)
        );

        match updates {
            // Snapshot was `app/foo`; add another key under the same prefix.
            1 => {
                doc.set_bytes(author, b"app/bar".to_vec(), b"world".to_vec())
                    .await?;
            }
            // Show that the stream also fires when an existing key gets a new latest entry.
            2 => {
                doc.set_bytes(author, b"app/foo".to_vec(), b"hello again".to_vec())
                    .await?;
            }
            _ => {}
        }

        if updates >= 3 {
            break;
        }
    }

    drop(stream);

    router.shutdown().await?;
    Ok(())
}
