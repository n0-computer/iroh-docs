//! Integration tests for [`iroh_docs::subscribe_resolved`].

use std::collections::HashSet;

use anyhow::Result;
use futures_util::StreamExt;
use iroh_docs::{store::Query, subscribe_resolved::ResolvedSubscribeOpts};
use n0_future::time::Duration;
use tracing_test::traced_test;

mod util;
use util::Node;

use crate::util::empty_endpoint;

fn scope_key_exact(key: &[u8]) -> Query {
    Query::single_latest_per_key().key_exact(key).build()
}

fn scope_prefix(prefix: &[u8]) -> Query {
    Query::single_latest_per_key().key_prefix(prefix).build()
}

fn scope_key_exact_include_empty(key: &[u8]) -> Query {
    Query::single_latest_per_key()
        .key_exact(key)
        .include_empty()
        .build()
}

fn scope_whole_replica() -> Query {
    Query::single_latest_per_key().build()
}

#[tokio::test]
#[traced_test]
async fn subscribe_resolved_single_latest_two_authors_local() -> Result<()> {
    let ep = empty_endpoint().await?;
    let node = Node::memory(ep).spawn().await?;
    let client = node.client();
    let author_a = client.docs().author_create().await?;
    let author_b = client.docs().author_create().await?;
    let doc = client.docs().create().await?;
    let blobs = client.blobs();

    let mut sub = doc
        .subscribe_resolved(
            blobs,
            scope_key_exact(b"k"),
            ResolvedSubscribeOpts {
                include_content: true,
                initial_snapshot: false,
                resolution_delay: None,
            },
        )
        .await?;

    doc.set_bytes(author_a, b"k".to_vec(), b"a".to_vec())
        .await?;
    let first = tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await?
        .expect("stream ended")
        .expect("outer err");
    assert_eq!(first.content.as_deref(), Some(&b"a"[..]));

    doc.set_bytes(author_b, b"k".to_vec(), b"b".to_vec())
        .await?;
    let second = tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await?
        .expect("stream ended")
        .expect("outer err");
    assert_eq!(second.content.as_deref(), Some(&b"b"[..]));
    assert_eq!(second.entry.author(), author_b);

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn subscribe_resolved_include_empty_tombstone_after_del() -> Result<()> {
    let ep = empty_endpoint().await?;
    let node = Node::memory(ep).spawn().await?;
    let client = node.client();
    let author = client.docs().author_create().await?;
    let doc = client.docs().create().await?;
    let blobs = client.blobs();

    let mut sub = doc
        .subscribe_resolved(
            blobs,
            scope_key_exact_include_empty(b"k"),
            ResolvedSubscribeOpts {
                include_content: true,
                initial_snapshot: false,
                resolution_delay: None,
            },
        )
        .await?;

    doc.set_bytes(author, b"k".to_vec(), b"payload".to_vec())
        .await?;
    let first = tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await?
        .expect("stream ended")
        .expect("outer err");
    assert_eq!(first.content.as_deref(), Some(&b"payload"[..]));
    assert!(!first.entry.is_empty());

    doc.del(author, b"k".to_vec()).await?;
    let second = tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await?
        .expect("stream ended")
        .expect("outer err");
    assert!(second.entry.is_empty());
    assert_eq!(second.content.as_deref(), Some(&[][..]));

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn subscribe_resolved_whole_replica_two_keys() -> Result<()> {
    let ep = empty_endpoint().await?;
    let node = Node::memory(ep).spawn().await?;
    let client = node.client();
    let author = client.docs().author_default().await?;
    let doc = client.docs().create().await?;
    let blobs = client.blobs();

    let mut sub = doc
        .subscribe_resolved(
            blobs,
            scope_whole_replica(),
            ResolvedSubscribeOpts {
                include_content: false,
                initial_snapshot: false,
                resolution_delay: None,
            },
        )
        .await?;

    doc.set_bytes(author, b"a".to_vec(), b"1".to_vec()).await?;
    doc.set_bytes(author, b"b".to_vec(), b"2".to_vec()).await?;

    let mut seen = HashSet::new();
    for _ in 0..2 {
        let up = tokio::time::timeout(Duration::from_secs(10), sub.next())
            .await?
            .expect("stream ended")
            .expect("outer err");
        assert!(seen.insert(up.key.to_vec()));
    }
    assert_eq!(seen, HashSet::from([b"a".to_vec(), b"b".to_vec()]));

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn subscribe_resolved_initial_snapshot() -> Result<()> {
    let ep = empty_endpoint().await?;
    let node = Node::memory(ep).spawn().await?;
    let client = node.client();
    let author = client.docs().author_create().await?;
    let doc = client.docs().create().await?;
    let blobs = client.blobs();

    doc.set_bytes(author, b"k".to_vec(), b"before_sub".to_vec())
        .await?;

    let mut sub = doc
        .subscribe_resolved(
            blobs,
            scope_key_exact(b"k"),
            ResolvedSubscribeOpts {
                include_content: true,
                initial_snapshot: true,
                resolution_delay: None,
            },
        )
        .await?;

    let first = tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await?
        .expect("stream ended")
        .expect("outer err");
    assert_eq!(first.content.as_deref(), Some(&b"before_sub"[..]));

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn subscribe_resolved_author_filter_after_grouping() -> Result<()> {
    let ep = empty_endpoint().await?;
    let node = Node::memory(ep).spawn().await?;
    let client = node.client();
    let author_a = client.docs().author_create().await?;
    let author_b = client.docs().author_create().await?;
    let doc = client.docs().create().await?;
    let blobs = client.blobs();

    let scope = Query::single_latest_per_key()
        .key_exact(b"k")
        .author(author_b)
        .build();

    let mut sub = doc
        .subscribe_resolved(
            blobs,
            scope,
            ResolvedSubscribeOpts {
                include_content: true,
                initial_snapshot: false,
                resolution_delay: None,
            },
        )
        .await?;

    doc.set_bytes(author_a, b"k".to_vec(), b"from_a".to_vec())
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(800), sub.next())
            .await
            .is_err(),
        "author B scope should not emit when only A has written"
    );

    doc.set_bytes(author_b, b"k".to_vec(), b"from_b".to_vec())
        .await?;
    let got = tokio::time::timeout(Duration::from_secs(10), sub.next())
        .await?
        .expect("stream ended")
        .expect("outer err");
    assert_eq!(got.entry.author(), author_b);
    assert_eq!(got.content.as_deref(), Some(&b"from_b"[..]));

    node.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn subscribe_resolved_burst_inserts_do_not_deadlock() -> Result<()> {
    let ep = empty_endpoint().await?;
    let node = Node::memory(ep).spawn().await?;
    let client = node.client();
    let author = client.docs().author_default().await?;
    let doc = client.docs().create().await?;
    let blobs = client.blobs();

    let _sub = doc
        .subscribe_resolved(
            blobs,
            scope_prefix(b"k"),
            ResolvedSubscribeOpts {
                include_content: false,
                initial_snapshot: false,
                resolution_delay: Some(Duration::from_millis(2)),
            },
        )
        .await?;

    let n = 800u32;
    let insert_fut = async {
        for i in 0..n {
            let key = format!("k{i}");
            doc.set_bytes(author, key.into_bytes(), format!("v{i}").into_bytes())
                .await?;
        }
        anyhow::Ok(())
    };

    tokio::time::timeout(Duration::from_secs(120), insert_fut)
        .await
        .expect("insert loop should not hang (deadlock)")?;

    node.shutdown().await?;
    Ok(())
}
