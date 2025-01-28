use std::{path::PathBuf, str::FromStr, sync::Arc, thread, time::Duration};

use dialoguer::{theme::ColorfulTheme, Input};
use futures_lite::StreamExt;
use iroh::protocol::Router;
use iroh_blobs::util::local_pool::LocalPool;
use iroh_docs::{
    engine::LiveEvent,
    rpc::{
        client::docs::{Doc, ShareMode},
        proto::{Request, Response},
    },
    DocTicket,
};
use quic_rpc::transport::flume::FlumeConnector;

pub(crate) type BlobsClient = iroh_blobs::rpc::client::blobs::Client<
    FlumeConnector<iroh_blobs::rpc::proto::Response, iroh_blobs::rpc::proto::Request>,
>;
pub(crate) type DocsClient = iroh_docs::rpc::client::docs::Client<
    FlumeConnector<iroh_docs::rpc::proto::Response, iroh_docs::rpc::proto::Request>,
>;
pub(crate) type GossipClient = iroh_gossip::rpc::client::Client<
    FlumeConnector<iroh_gossip::rpc::proto::Response, iroh_gossip::rpc::proto::Request>,
>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let i = Iroh::new(".iroh-dir".into()).await?;
    let author = i.docs.authors().default().await?;

    println!("author: {}", author.fmt_short());

    let _ = tokio::spawn(menu_loop(i.clone())).await;

    Ok(())
}

async fn menu_loop(node: Iroh) {
    loop {
        let input: String = Input::with_theme(&ColorfulTheme::default())
            .with_prompt("Commmand:")
            .interact_text()
            .unwrap();
        match input.as_str() {
            "create" => {
                let temp_doc = node.docs.create().await.unwrap();

                let ticket = temp_doc
                    .share(
                        ShareMode::Write,
                        iroh_docs::rpc::AddrInfoOptions::RelayAndAddresses,
                    )
                    .await
                    .unwrap();

                println!("share this ticket to your friend: {}", ticket.to_string());

                let chat = node.docs.import(ticket.clone()).await.unwrap();
                let _ = chat.set_bytes(
                    node.docs.authors().default().await.unwrap(),
                    "chat-ticket",
                    ticket.to_string(),
                );

                tokio::spawn(chat_loop(chat.clone(), node.clone()));
                loop {
                    let line: String = Input::with_theme(&ColorfulTheme::default())
                        .with_prompt("Chat:")
                        .interact_text()
                        .unwrap();
                    let timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_micros();
                    let _ = chat
                        .set_bytes(
                            node.docs.authors().default().await.unwrap(),
                            timestamp.to_string(),
                            line,
                        )
                        .await
                        .unwrap();
                }
            }
            "join" => {
                let join_ticket: String = Input::with_theme(&ColorfulTheme::default())
                    .with_prompt("ticket:")
                    .interact_text()
                    .unwrap();
                // trim extra spaces
                let join_ticket = join_ticket.trim();

                let chat = node
                    .docs
                    .import(DocTicket::from_str(&join_ticket).unwrap())
                    .await
                    .unwrap();
                tokio::spawn(chat_loop(chat.clone(), node.clone()));
                loop {
                    let line: String = Input::with_theme(&ColorfulTheme::default())
                        .with_prompt("Chat:")
                        .interact_text()
                        .unwrap();

                    let timestamp = std::time::UNIX_EPOCH.elapsed().unwrap().as_micros();
                    let _ = chat
                        .set_bytes(
                            node.docs.authors().default().await.unwrap(),
                            timestamp.to_string(),
                            line,
                        )
                        .await
                        .unwrap();
                }
            }
            _ => {}
        }
    }
}

async fn chat_loop(chat: Doc<FlumeConnector<Response, Request>>, iroh: Iroh) {
    let mut sub = chat.subscribe().await.unwrap();
    let blobs = iroh.blobs.clone();
    while let Ok(event) = sub.try_next().await {
        if let Some(e) = event {
            if let LiveEvent::InsertRemote { from, entry, .. } = e {
                let message_content = blobs.read_to_bytes(entry.content_hash()).await;
                match message_content {
                    Ok(a) => {
                        println!(
                            "{}: {}",
                            from.fmt_short(),
                            String::from_utf8(a.into()).unwrap()
                        );
                    }
                    Err(_) => {
                        // may still be syncing so
                        for _ in 0..3 {
                            thread::sleep(Duration::from_secs(1));
                            let message_content = blobs.read_to_bytes(entry.content_hash()).await;
                            if let Ok(a) = message_content {
                                println!(
                                    "{}: {}",
                                    from.fmt_short(),
                                    String::from_utf8(a.into()).unwrap()
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct Iroh {
    _local_pool: Arc<LocalPool>,
    router: Router,
    pub(crate) gossip: GossipClient,
    pub(crate) blobs: BlobsClient,
    pub(crate) docs: DocsClient,
}

impl Iroh {
    pub async fn new(path: PathBuf) -> Result<Self, anyhow::Error> {
        // create dir if it doesn't already exist
        tokio::fs::create_dir_all(&path).await?;

        let key = iroh_blobs::util::fs::load_secret_key(path.clone().join("keypair")).await?;

        // local thread pool manager for blobs
        let local_pool = LocalPool::default();

        // create endpoint
        let endpoint = iroh::Endpoint::builder()
            .discovery_n0()
            .secret_key(key)
            .bind()
            .await?;

        // build the protocol router
        let mut builder = iroh::protocol::Router::builder(endpoint);

        // add iroh gossip
        let gossip = iroh_gossip::net::Gossip::builder()
            .spawn(builder.endpoint().clone())
            .await?;

        // add iroh blobs
        let blobs = iroh_blobs::net_protocol::Blobs::persistent(&path)
            .await?
            .build(&local_pool.handle(), builder.endpoint());

        // add docs
        let docs = iroh_docs::protocol::Docs::persistent(path)
            .spawn(&blobs, &gossip)
            .await?;

        builder = builder
            .accept(iroh_gossip::ALPN, Arc::new(gossip.clone()))
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_docs::ALPN, Arc::new(docs.clone()));

        Ok(Self {
            _local_pool: Arc::new(local_pool),
            router: builder.spawn().await?,
            gossip: gossip.client().clone(),
            blobs: blobs.client().clone(),
            docs: docs.client().clone(),
        })
    }
}
