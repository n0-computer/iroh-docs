use anyhow::{Context, Result};
use iroh_blobs::api::blobs::{ExportMode, ImportMode};
use iroh_docs::store::Query;
use n0_future::StreamExt;
use rand::Rng;
use testresult::TestResult;
use tokio::io::AsyncWriteExt;
use tracing_test::traced_test;

use self::util::{
    path::{key_to_path, path_to_key},
    Node,
};
use crate::util::empty_endpoint;

mod util;

/// Test that closing a doc does not close other instances.
#[tokio::test]
#[traced_test]
async fn test_doc_close() -> Result<()> {
    let node = Node::memory(empty_endpoint().await?).spawn().await?;
    // let author = node.authors().default().await?;
    let author = node.docs().author_default().await?;
    // open doc two times
    let doc1 = node.docs().create().await?;
    let doc2 = node.docs().open(doc1.id()).await?.expect("doc to exist");
    // close doc1 instance
    doc1.close().await?;
    // operations on doc1 now fail.
    assert!(doc1.set_bytes(author, "foo", "bar").await.is_err());
    // dropping doc1 will close the doc if not already closed
    // wait a bit because the close-on-drop spawns a task for which we cannot track completion.
    drop(doc1);
    n0_future::time::sleep(n0_future::time::Duration::from_millis(100)).await;

    // operations on doc2 still succeed
    doc2.set_bytes(author, "foo", "bar").await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_doc_import_export() -> TestResult<()> {
    let node = Node::memory(empty_endpoint().await?).spawn().await?;

    // create temp file
    let temp_dir = tempfile::tempdir().context("tempdir")?;

    let in_root = temp_dir.path().join("in");
    tokio::fs::create_dir_all(in_root.clone())
        .await
        .context("create dir all")?;
    let out_root = temp_dir.path().join("out");

    let path = in_root.join("test");

    let size = 100;
    let mut buf = vec![0u8; size];
    rand::rng().fill_bytes(&mut buf);
    let mut file = tokio::fs::File::create(path.clone())
        .await
        .context("create file")?;
    file.write_all(&buf.clone()).await.context("write_all")?;
    file.flush().await.context("flush")?;

    // create doc & author
    let client = node.client();
    let blobs = client.blobs();
    let docs_client = client.docs();
    let doc = docs_client.create().await.context("doc create")?;
    // let author = client.authors().create().await.context("author create")?;
    let author = client
        .docs()
        .author_create()
        .await
        .context("author create")?;

    // import file
    let import_outcome = doc
        .import_file(
            blobs,
            author,
            path_to_key(path.clone(), None, Some(in_root))?,
            path,
            ImportMode::TryReference,
        )
        .await
        .context("import file")?
        .await
        .context("import finish")?;

    // export file
    let entry = doc
        .get_one(Query::author(author).key_exact(import_outcome.key))
        .await
        .context("get one")?
        .unwrap();
    let key = entry.key().to_vec();
    let path = key_to_path(key, None, Some(out_root))?;
    // TODO(Frando): iroh-blobs should do this IMO.
    // tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let progress = doc
        .export_file(blobs, entry, path.clone(), ExportMode::Copy)
        .await?;
    let mut progress = progress.stream().await;
    while let Some(msg) = progress.next().await {
        println!("MSG {msg:?}");
    }
    // let _export_outcome = doc
    //     .export_file(blobs, entry, path.clone(), ExportMode::Copy)
    //     .await
    //     .context("export file")?
    //     .finish()
    //     .await
    //     .context("export finish")?;

    let got_bytes = tokio::fs::read(path).await.context("tokio read")?;
    assert_eq!(buf, got_bytes);

    Ok(())
}

#[tokio::test]
async fn test_authors() -> Result<()> {
    let node = Node::memory(empty_endpoint().await?).spawn().await?;

    // default author always exists
    let authors: Vec<_> = node.docs().author_list().await?.try_collect().await?;
    assert_eq!(authors.len(), 1);
    let default_author = node.docs().author_default().await?;
    assert_eq!(authors, vec![default_author]);

    let author_id = node.docs().author_create().await?;

    let authors: Vec<_> = node.docs().author_list().await?.try_collect().await?;
    assert_eq!(authors.len(), 2);

    let author = node
        .docs()
        .author_export(author_id)
        .await?
        .expect("should have author");
    node.docs().author_delete(author_id).await?;
    let authors: Vec<_> = node.docs().author_list().await?.try_collect().await?;
    assert_eq!(authors.len(), 1);

    node.docs().author_import(author).await?;

    let authors: Vec<_> = node.docs().author_list().await?.try_collect().await?;
    assert_eq!(authors.len(), 2);

    assert!(node.docs().author_default().await? != author_id);
    node.docs().author_set_default(author_id).await?;
    assert_eq!(node.docs().author_default().await?, author_id);

    Ok(())
}

#[tokio::test]
async fn test_default_author_memory() -> Result<()> {
    let iroh = Node::memory(empty_endpoint().await?).spawn().await?;
    let author = iroh.docs().author_default().await?;
    assert!(iroh.docs().author_export(author).await?.is_some());
    assert!(iroh.docs().author_delete(author).await.is_err());
    Ok(())
}

#[tokio::test]
#[traced_test]
#[cfg(feature = "fs-store")]
async fn test_default_author_persist() -> TestResult<()> {
    let iroh_root_dir = tempfile::TempDir::new()?;
    let iroh_root = iroh_root_dir.path();

    // check that the default author exists and cannot be deleted.
    let default_author = {
        let iroh = Node::persistent(iroh_root, empty_endpoint().await?)
            .spawn()
            .await?;
        let author = iroh.docs().author_default().await?;
        assert!(iroh.docs().author_export(author).await?.is_some());
        assert!(iroh.docs().author_delete(author).await.is_err());
        iroh.shutdown().await?;
        author
    };

    // check that the default author is persisted across restarts.
    {
        let iroh = Node::persistent(iroh_root, empty_endpoint().await?)
            .spawn()
            .await?;
        let author = iroh.docs().author_default().await?;
        assert_eq!(author, default_author);
        assert!(iroh.docs().author_export(author).await?.is_some());
        assert!(iroh.docs().author_delete(author).await.is_err());
        iroh.shutdown().await?;
    };

    // check that the default author can be set manually and is persisted.
    let default_author = {
        let iroh = Node::persistent(iroh_root, empty_endpoint().await?)
            .spawn()
            .await?;
        let author = iroh.docs().author_create().await?;
        iroh.docs().author_set_default(author).await?;
        assert_eq!(iroh.docs().author_default().await?, author);
        iroh.shutdown().await?;
        author
    };
    {
        let iroh = Node::persistent(iroh_root, empty_endpoint().await?)
            .spawn()
            .await?;
        assert_eq!(iroh.docs().author_default().await?, default_author);
        iroh.shutdown().await?;
    }

    Ok(())
}

#[tokio::test]
#[cfg(feature = "fs-store")]
async fn test_default_author_sidecar_migration() -> TestResult<()> {
    let root = tempfile::TempDir::new()?;
    let database_path = root.path().join("docs.redb");
    let author = iroh_docs::Author::new(&mut rand::rng());
    let author_id = author.id();

    let mut store = iroh_docs::store::fs::Store::persistent(&database_path)?;
    store.import_author(author)?;
    store.flush()?;
    drop(store);
    tokio::fs::write(root.path().join("default-author"), author_id.to_string()).await?;

    let iroh = Node::persistent(root.path(), empty_endpoint().await?)
        .spawn()
        .await?;
    assert_eq!(iroh.docs().author_default().await?, author_id);
    iroh.shutdown().await?;

    Ok(())
}
