// use std::time::Duration;

// use bytes::Bytes;
// use testresult::TestResult;

// #[tokio::test]
// async fn redb_doc_import_stress() -> TestResult<()> {
//     let _ = tracing_subscriber::fmt::try_init();
//     let dir = testdir!();
//     let bao_store = iroh_blobs::store::fs::Store::load(dir.join("store")).await?;
//     let (node, _) = wrap_in_node(bao_store.clone(), Duration::from_secs(10)).await;
//     let client = node.client();
//     let doc = client.docs().create().await?;
//     let author = client.authors().create().await?;
//     let temp_path = dir.join("temp");
//     tokio::fs::create_dir_all(&temp_path).await?;
//     let mut to_import = Vec::new();
//     for i in 0..100 {
//         let data = create_test_data(16 * 1024 * 3 + 1);
//         let path = temp_path.join(format!("file{}", i));
//         tokio::fs::write(&path, &data).await?;
//         let key = Bytes::from(format!("{}", path.display()));
//         to_import.push((key, path, data));
//     }
//     for (key, path, _) in to_import.iter() {
//         let mut progress = doc.import_file(author, key.clone(), path, true).await?;
//         while let Some(msg) = progress.next().await {
//             tracing::info!("import progress {:?}", msg);
//         }
//     }
//     for (i, (key, _, expected)) in to_import.iter().enumerate() {
//         let Some(entry) = doc.get_exact(author, key.clone(), true).await? else {
//             anyhow::bail!("doc entry not found {}", i);
//         };
//         let hash = entry.content_hash();
//         let Some(content) = bao_store.get(&hash).await? else {
//             anyhow::bail!("content not found {} {}", i, &hash.to_hex()[..8]);
//         };
//         let data = content.data_reader().read_to_end().await?;
//         assert_eq!(data, expected);
//     }
//     Ok(())
// }
