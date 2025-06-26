#![cfg(feature = "rpc")]
use std::{
    io::{Cursor, Write},
    path::PathBuf,
    time::Duration,
};

use anyhow::Result;
use bao_tree::{blake3, io::sync::Outboard, ChunkRanges};
use bytes::Bytes;
use futures_lite::StreamExt;
use iroh_blobs::{
    store::{bao_tree, Map},
    IROH_BLOCK_SIZE,
};
use iroh_io::AsyncSliceReaderExt;
use rand::RngCore;
use testdir::testdir;
use util::Node;

mod util;

pub fn create_test_data(size: usize) -> Bytes {
    let mut rand = rand::thread_rng();
    let mut res = vec![0u8; size];
    rand.fill_bytes(&mut res);
    res.into()
}

/// Take some data and encode it
pub fn simulate_remote(data: &[u8]) -> (blake3::Hash, Cursor<Bytes>) {
    let outboard = bao_tree::io::outboard::PostOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
    let mut encoded = Vec::new();
    encoded
        .write_all(outboard.tree.size().to_le_bytes().as_ref())
        .unwrap();
    bao_tree::io::sync::encode_ranges_validated(data, &outboard, &ChunkRanges::all(), &mut encoded)
        .unwrap();
    let hash = outboard.root();
    (hash, Cursor::new(encoded.into()))
}

/// Wrap a bao store in a node that has gc enabled.
async fn persistent_node(
    path: PathBuf,
    gc_period: Duration,
) -> (
    Node<iroh_blobs::store::fs::Store>,
    async_channel::Receiver<()>,
) {
    let (gc_send, gc_recv) = async_channel::unbounded();
    let node = Node::persistent(path)
        .gc_interval(Some(gc_period))
        .register_gc_done_cb(Box::new(move || {
            gc_send.send_blocking(()).ok();
        }))
        .spawn()
        .await
        .unwrap();
    (node, gc_recv)
}

#[tokio::test]
async fn redb_doc_import_stress() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let dir = testdir!();
    let (node, _) = persistent_node(dir.join("store"), Duration::from_secs(10)).await;
    let bao_store = node.blob_store().clone();
    let client = node.client();
    let doc = client.docs().create().await?;
    let author = client.authors().create().await?;
    let temp_path = dir.join("temp");
    tokio::fs::create_dir_all(&temp_path).await?;
    let mut to_import = Vec::new();
    for i in 0..100 {
        let data = create_test_data(16 * 1024 * 3 + 1);
        let path = temp_path.join(format!("file{i}"));
        tokio::fs::write(&path, &data).await?;
        let key = Bytes::from(format!("{}", path.display()));
        to_import.push((key, path, data));
    }
    for (key, path, _) in to_import.iter() {
        let mut progress = doc.import_file(author, key.clone(), path, true).await?;
        while let Some(msg) = progress.next().await {
            tracing::info!("import progress {:?}", msg);
        }
    }
    for (i, (key, _, expected)) in to_import.iter().enumerate() {
        let Some(entry) = doc.get_exact(author, key.clone(), true).await? else {
            anyhow::bail!("doc entry not found {}", i);
        };
        let hash = entry.content_hash();
        let Some(content) = bao_store.get(&hash).await? else {
            anyhow::bail!("content not found {} {}", i, &hash.to_hex()[..8]);
        };
        let data = content.data_reader().read_to_end().await?;
        assert_eq!(data, expected);
    }
    Ok(())
}
