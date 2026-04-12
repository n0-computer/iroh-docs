//! Subscription to the latest entry per key ([`Query::single_latest_per_key`]) with blob availability.
//!
//! Entry points: [`subscribe_resolved_with`], [`ResolvedFetcher`],
//! [`Engine::subscribe_resolved`](crate::engine::Engine::subscribe_resolved), and
//! [`Doc::subscribe_resolved`](crate::api::Doc::subscribe_resolved).
//!
//! Live [`crate::engine::LiveEvent`] values are forwarded on a dedicated task so slow resolution
//! work does not fill the bounded replica subscribe channel (see [`Engine::subscribe`](crate::engine::Engine::subscribe)).
//!
//! ## WebAssembly
//!
//! This module is intended to work on `wasm32-unknown-unknown` under the same **Tokio-based**
//! runtime as the rest of iroh (for example via `wasm-bindgen-futures` driving the executor).
//! Internal work is scheduled with [`n0_future::task::spawn`]; timers use [`n0_future::time`], not
//! Tokio-only clocks. The merged [`Engine::subscribe`] stream may be `!Send` in the browser; the
//! [`subscribe_resolved_with`] bounds reflect that via [`SendUnlessWasmBrowser`]. Custom
//! [`ResolvedFetcher`] closures still use `Send` futures on all targets (see [`FetchLatestBox`]).

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use iroh_blobs::{
    api::{blobs::BlobStatus, Store},
    Hash,
};
use irpc::channel::mpsc as irpc_mpsc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    actor::SyncHandle,
    api::RpcResult,
    engine::LiveEvent,
    store::{KeyFilter, Query},
    AuthorId, Entry, NamespaceId, SignedEntry,
};

// On native targets we require `Send` on the live stream; on `wasm32-unknown-unknown` (see
// `wasm_browser` in `build.rs`), `n0_future::boxed::BoxFuture` is `!Send` (see that crate's
// `boxed` module), matching `Engine::subscribe`'s merged stream.
mod live_stream_bound {
    #[cfg(not(wasm_browser))]
    pub trait SendUnlessWasmBrowser: Send {}
    #[cfg(not(wasm_browser))]
    impl<T: Send + ?Sized> SendUnlessWasmBrowser for T {}

    #[cfg(wasm_browser)]
    pub trait SendUnlessWasmBrowser {}
    #[cfg(wasm_browser)]
    impl<T: ?Sized> SendUnlessWasmBrowser for T {}
}

/// Blob reads used by [`subscribe_resolved_with`] so tests can inject faults without a full store.
#[async_trait]
pub(crate) trait ResolveBlobs: Send + Sync {
    async fn blob_status(&self, hash: Hash) -> anyhow::Result<BlobStatus>;
    async fn blob_get_bytes(&self, hash: Hash) -> anyhow::Result<Bytes>;
}

#[async_trait]
impl ResolveBlobs for iroh_blobs::api::blobs::Blobs {
    async fn blob_status(&self, hash: Hash) -> anyhow::Result<BlobStatus> {
        self.status(hash).await.map_err(|e| anyhow::anyhow!(e))
    }

    async fn blob_get_bytes(&self, hash: Hash) -> anyhow::Result<Bytes> {
        self.get_bytes(hash).await.map_err(|e| anyhow::anyhow!(e))
    }
}

/// Boxed future returned by the “latest entry” callback in [`ResolvedFetcher`].
pub type FetchLatestBox =
    Pin<Box<dyn Future<Output = anyhow::Result<Option<Entry>>> + Send + 'static>>;
/// Boxed future returned by the “all matching entries” callback in [`ResolvedFetcher`].
pub type FetchAllBox = Pin<Box<dyn Future<Output = anyhow::Result<Vec<Entry>>> + Send + 'static>>;

/// Fetches the latest entry per key (see [`Query::single_latest_per_key`]) for [`subscribe_resolved_with`].
#[derive(Clone, derive_more::Debug)]
#[debug("ResolvedFetcher")]
pub struct ResolvedFetcher {
    latest: Arc<dyn Fn(Query) -> FetchLatestBox + Send + Sync>,
    all: Arc<dyn Fn(Query) -> FetchAllBox + Send + Sync>,
}

impl ResolvedFetcher {
    /// Builds a fetcher backed by the docs actor ([`SyncHandle`]).
    pub fn for_sync_engine(sync: SyncHandle, namespace: NamespaceId) -> Self {
        let sync_latest = sync.clone();
        let sync_all = sync.clone();
        let latest = Arc::new(move |q: Query| {
            let sync = sync_latest.clone();
            Box::pin(async move { fetch_latest_sync(&sync, namespace, q).await }) as FetchLatestBox
        });
        let all = Arc::new(move |q: Query| {
            let sync = sync_all.clone();
            Box::pin(async move { fetch_all_sync(&sync, namespace, q).await }) as FetchAllBox
        });
        Self { latest, all }
    }

    /// Custom fetcher (e.g. RPC-backed [`crate::api::Doc`]).
    pub fn new(
        latest: Arc<dyn Fn(Query) -> FetchLatestBox + Send + Sync>,
        all: Arc<dyn Fn(Query) -> FetchAllBox + Send + Sync>,
    ) -> Self {
        Self { latest, all }
    }

    async fn fetch_latest(&self, q: Query) -> anyhow::Result<Option<Entry>> {
        (self.latest)(q).await
    }

    async fn fetch_all(&self, q: Query) -> anyhow::Result<Vec<Entry>> {
        (self.all)(q).await
    }
}

/// Latest entry for a key (per [`Query::single_latest_per_key`]) once the blob is locally complete (or the record is empty).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedKeyValue {
    /// Document key.
    pub key: Bytes,
    /// Latest entry for this key under the subscription scope ([`Query::single_latest_per_key`]).
    pub entry: Entry,
    /// Raw blob bytes when [`ResolvedSubscribeOpts::include_content`] is true; for empty
    /// records this is `None`.
    pub content: Option<Bytes>,
}

/// Options for [`subscribe_resolved_with`].
#[derive(Debug, Clone, Default)]
pub struct ResolvedSubscribeOpts {
    /// Emit the current latest entry for each key in scope before processing live events.
    pub initial_snapshot: bool,
    /// Read blob bytes into [`ResolvedKeyValue::content`] (requires a complete blob).
    pub include_content: bool,
    /// Injected delay after each flush (for tests that exercise subscribe backpressure).
    pub resolution_delay: Option<std::time::Duration>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct EntryFp {
    author: AuthorId,
    ts: u64,
    hash: Hash,
}

fn fingerprint(entry: &Entry) -> EntryFp {
    EntryFp {
        author: entry.author(),
        ts: entry.timestamp(),
        hash: entry.content_hash(),
    }
}

/// Capacity between the bounded [`crate::engine::Engine::subscribe`] merge stream and the resolver.
const FORWARD_CAP: usize = 2048;

/// Subscribe to updates for the latest entry for each key matching `scope`, emitting only when that
/// entry’s blob is [`BlobStatus::Complete`] (or the record is empty).
///
/// `scope` must be built with [`Query::single_latest_per_key`]. Live events are forwarded on a
/// separate task so slow resolution does not fill the bounded replica subscribe channel.
///
/// The live stream must be [`Send`] on native targets; when building for the browser Wasm target
/// (`wasm_browser`), it may be `!Send` (local boxed futures from blob status checks in the subscribe pipeline).
pub fn subscribe_resolved_with<LS>(
    live_stream: LS,
    fetcher: ResolvedFetcher,
    blobs: Store,
    scope: Query,
    opts: ResolvedSubscribeOpts,
) -> anyhow::Result<ReceiverStream<anyhow::Result<ResolvedKeyValue>>>
where
    LS: Stream<Item = anyhow::Result<LiveEvent>>
        + live_stream_bound::SendUnlessWasmBrowser
        + 'static,
{
    subscribe_resolved_spawn(
        live_stream,
        fetcher,
        Arc::new(blobs.blobs().clone()),
        scope,
        opts,
    )
}

fn subscribe_resolved_spawn<LS>(
    live_stream: LS,
    fetcher: ResolvedFetcher,
    blobs: Arc<dyn ResolveBlobs>,
    scope: Query,
    opts: ResolvedSubscribeOpts,
) -> anyhow::Result<ReceiverStream<anyhow::Result<ResolvedKeyValue>>>
where
    LS: Stream<Item = anyhow::Result<LiveEvent>>
        + live_stream_bound::SendUnlessWasmBrowser
        + 'static,
{
    if !scope.is_single_latest_per_key() {
        anyhow::bail!("subscribe_resolved requires Query::single_latest_per_key()");
    }
    let (out_tx, out_rx) = mpsc::channel(64);
    let (fwd_tx, fwd_rx) = mpsc::channel::<anyhow::Result<LiveEvent>>(FORWARD_CAP);

    n0_future::task::spawn(async move {
        use futures_util::{pin_mut, StreamExt};
        pin_mut!(live_stream);
        while let Some(item) = live_stream.next().await {
            if fwd_tx.send(item).await.is_err() {
                break;
            }
        }
    });

    n0_future::task::spawn(async move {
        if let Err(e) = run_resolver(fwd_rx, out_tx, fetcher, blobs, scope, opts).await {
            tracing::warn!("subscribe_resolved resolver ended: {e:#}");
        }
    });

    Ok(ReceiverStream::new(out_rx))
}

#[allow(clippy::too_many_arguments)]
async fn handle_live_event(
    fetcher: &ResolvedFetcher,
    blobs: &dyn ResolveBlobs,
    scope: &Query,
    opts: &ResolvedSubscribeOpts,
    out_tx: &mpsc::Sender<anyhow::Result<ResolvedKeyValue>>,
    pending_blob: &mut HashMap<Bytes, Hash>,
    last_emitted: &mut HashMap<Bytes, EntryFp>,
    dirty_keys: &mut HashSet<Bytes>,
    dirty_all: &mut bool,
    ev: &LiveEvent,
) -> anyhow::Result<()> {
    match ev {
        LiveEvent::InsertLocal { entry } | LiveEvent::InsertRemote { entry, .. } => {
            let key = entry.key();
            if scope.filter_key().matches(key) {
                if matches!(scope.filter_key(), KeyFilter::Any) {
                    *dirty_all = true;
                } else {
                    dirty_keys.insert(Bytes::copy_from_slice(key));
                }
            }
        }
        LiveEvent::ContentReady { hash } => {
            let keys: Vec<Bytes> = pending_blob
                .iter()
                .filter(|(_, h)| **h == *hash)
                .map(|(k, _)| k.clone())
                .collect();
            for k in keys {
                try_resolve_key(
                    fetcher,
                    blobs,
                    scope,
                    opts,
                    out_tx,
                    pending_blob,
                    last_emitted,
                    k,
                )
                .await?;
            }
        }
        LiveEvent::PendingContentReady => {
            let keys: Vec<Bytes> = pending_blob.keys().cloned().collect();
            for k in keys {
                try_resolve_key(
                    fetcher,
                    blobs,
                    scope,
                    opts,
                    out_tx,
                    pending_blob,
                    last_emitted,
                    k,
                )
                .await?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn run_resolver(
    mut fwd_rx: mpsc::Receiver<anyhow::Result<LiveEvent>>,
    out_tx: mpsc::Sender<anyhow::Result<ResolvedKeyValue>>,
    fetcher: ResolvedFetcher,
    blobs: Arc<dyn ResolveBlobs>,
    scope: Query,
    opts: ResolvedSubscribeOpts,
) -> anyhow::Result<()> {
    let mut dirty_keys: HashSet<Bytes> = HashSet::new();
    let mut dirty_all = false;
    let mut pending_blob: HashMap<Bytes, Hash> = HashMap::new();
    let mut last_emitted: HashMap<Bytes, EntryFp> = HashMap::new();

    if opts.initial_snapshot {
        let entries = fetcher.fetch_all(scope.clone()).await?;
        for e in entries {
            try_emit(
                blobs.as_ref(),
                &scope,
                &opts,
                &out_tx,
                &mut pending_blob,
                &mut last_emitted,
                e,
            )
            .await?;
        }
        if let Some(d) = opts.resolution_delay {
            n0_future::time::sleep(d).await;
        }
    }

    loop {
        let msg = match fwd_rx.recv().await {
            Some(m) => m,
            None => break,
        };

        match msg {
            Err(e) => {
                if out_tx.send(Err(e)).await.is_err() {
                    break;
                }
            }
            Ok(ev) => {
                handle_live_event(
                    &fetcher,
                    blobs.as_ref(),
                    &scope,
                    &opts,
                    &out_tx,
                    &mut pending_blob,
                    &mut last_emitted,
                    &mut dirty_keys,
                    &mut dirty_all,
                    &ev,
                )
                .await?;

                while let Ok(more) = fwd_rx.try_recv() {
                    match more {
                        Err(e) => {
                            if out_tx.send(Err(e)).await.is_err() {
                                return Ok(());
                            }
                        }
                        Ok(ev2) => {
                            handle_live_event(
                                &fetcher,
                                blobs.as_ref(),
                                &scope,
                                &opts,
                                &out_tx,
                                &mut pending_blob,
                                &mut last_emitted,
                                &mut dirty_keys,
                                &mut dirty_all,
                                &ev2,
                            )
                            .await?;
                        }
                    }
                }

                flush_dirty(
                    &fetcher,
                    blobs.as_ref(),
                    &scope,
                    &opts,
                    &out_tx,
                    &mut dirty_keys,
                    &mut dirty_all,
                    &mut pending_blob,
                    &mut last_emitted,
                )
                .await?;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_dirty(
    fetcher: &ResolvedFetcher,
    blobs: &dyn ResolveBlobs,
    scope: &Query,
    opts: &ResolvedSubscribeOpts,
    out_tx: &mpsc::Sender<anyhow::Result<ResolvedKeyValue>>,
    dirty_keys: &mut HashSet<Bytes>,
    dirty_all: &mut bool,
    pending_blob: &mut HashMap<Bytes, Hash>,
    last_emitted: &mut HashMap<Bytes, EntryFp>,
) -> anyhow::Result<()> {
    if *dirty_all {
        *dirty_all = false;
        dirty_keys.clear();
        let entries = fetcher.fetch_all(scope.clone()).await?;
        for e in entries {
            try_emit(blobs, scope, opts, out_tx, pending_blob, last_emitted, e).await?;
        }
    } else if !dirty_keys.is_empty() {
        let keys: Vec<Bytes> = dirty_keys.drain().collect();
        for key in keys {
            try_resolve_key(
                fetcher,
                blobs,
                scope,
                opts,
                out_tx,
                pending_blob,
                last_emitted,
                key,
            )
            .await?;
        }
    }

    if let Some(d) = opts.resolution_delay {
        n0_future::time::sleep(d).await;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn try_resolve_key(
    fetcher: &ResolvedFetcher,
    blobs: &dyn ResolveBlobs,
    scope: &Query,
    opts: &ResolvedSubscribeOpts,
    out_tx: &mpsc::Sender<anyhow::Result<ResolvedKeyValue>>,
    pending_blob: &mut HashMap<Bytes, Hash>,
    last_emitted: &mut HashMap<Bytes, EntryFp>,
    key: Bytes,
) -> anyhow::Result<()> {
    let Some(q) = scope.single_latest_for_exact_key(&key) else {
        return Ok(());
    };
    let Some(entry) = fetcher.fetch_latest(q).await? else {
        pending_blob.remove(&key);
        last_emitted.remove(&key);
        return Ok(());
    };
    try_emit(
        blobs,
        scope,
        opts,
        out_tx,
        pending_blob,
        last_emitted,
        entry,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn try_emit(
    blobs: &dyn ResolveBlobs,
    scope: &Query,
    opts: &ResolvedSubscribeOpts,
    out_tx: &mpsc::Sender<anyhow::Result<ResolvedKeyValue>>,
    pending_blob: &mut HashMap<Bytes, Hash>,
    last_emitted: &mut HashMap<Bytes, EntryFp>,
    entry: Entry,
) -> anyhow::Result<()> {
    let key = Bytes::copy_from_slice(entry.key());
    if !scope.filter_key().matches(entry.key()) {
        return Ok(());
    }

    let fp = fingerprint(&entry);
    if entry.is_empty() {
        pending_blob.remove(&key);
        if last_emitted.get(&key) == Some(&fp) {
            return Ok(());
        }
        last_emitted.insert(key.clone(), fp);
        let content = if opts.include_content {
            Some(Bytes::new())
        } else {
            None
        };
        if out_tx
            .send(Ok(ResolvedKeyValue {
                key,
                entry,
                content,
            }))
            .await
            .is_err()
        {
            return Ok(());
        }
        return Ok(());
    }

    let hash = entry.content_hash();
    match blobs.blob_status(hash).await {
        Ok(BlobStatus::Complete { .. }) => {}
        Ok(_status) => {
            pending_blob.insert(key.clone(), hash);
            return Ok(());
        }
        Err(e) => {
            tracing::debug!(
                hash = %hash,
                key = ?key,
                error = %e,
                "subscribe_resolved: blobs_api.status error; treating as not BlobStatus::Complete, pending key"
            );
            pending_blob.insert(key.clone(), hash);
            return Ok(());
        }
    }

    pending_blob.remove(&key);
    if last_emitted.get(&key) == Some(&fp) {
        return Ok(());
    }
    last_emitted.insert(key.clone(), fp);

    let content = if opts.include_content {
        match blobs.blob_get_bytes(hash).await {
            Ok(b) => Some(b),
            Err(e) => {
                let _ = out_tx.send(Err(e)).await;
                return Ok(());
            }
        }
    } else {
        None
    };

    if out_tx
        .send(Ok(ResolvedKeyValue {
            key,
            entry,
            content,
        }))
        .await
        .is_err()
    {
        return Ok(());
    }
    Ok(())
}

pub(crate) async fn fetch_latest_sync(
    sync: &SyncHandle,
    namespace: NamespaceId,
    query: Query,
) -> anyhow::Result<Option<Entry>> {
    let (tx, mut rx) = irpc_mpsc::channel::<RpcResult<SignedEntry>>(64);
    sync.get_many(namespace, query, tx).await?;
    match rx.recv().await {
        Ok(Some(Ok(se))) => Ok(Some(se.into())),
        Ok(Some(Err(e))) => Err(anyhow::anyhow!(e)),
        Ok(None) => Ok(None),
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

pub(crate) async fn fetch_all_sync(
    sync: &SyncHandle,
    namespace: NamespaceId,
    query: Query,
) -> anyhow::Result<Vec<Entry>> {
    let (tx, mut rx) = irpc_mpsc::channel::<RpcResult<SignedEntry>>(64);
    sync.get_many(namespace, query, tx).await?;
    let mut v = Vec::new();
    loop {
        match rx.recv().await {
            Ok(Some(Ok(se))) => v.push(se.into()),
            Ok(Some(Err(e))) => return Err(anyhow::anyhow!(e)),
            Ok(None) => break,
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::stream::{self, StreamExt};

    use super::*;
    use crate::{
        sync::{Record, RecordIdentifier},
        AuthorId, NamespaceId,
    };

    fn test_entry() -> Entry {
        let ns = NamespaceId::from(&[11u8; 32]);
        let author = AuthorId::from(&[22u8; 32]);
        let id = RecordIdentifier::new(ns, author, b"k");
        let record = Record::new(Hash::new(b"blob-bytes"), 10, 99);
        Entry::new(id, record)
    }

    fn test_fetcher(entry: Entry) -> ResolvedFetcher {
        let e1 = entry.clone();
        let latest: Arc<dyn Fn(Query) -> FetchLatestBox + Send + Sync> = Arc::new(move |_q| {
            let e = e1.clone();
            Box::pin(async move { Ok(Some(e)) }) as FetchLatestBox
        });
        let e2 = entry.clone();
        let all: Arc<dyn Fn(Query) -> FetchAllBox + Send + Sync> = Arc::new(move |_q| {
            let e = e2.clone();
            Box::pin(async move { Ok(vec![e]) }) as FetchAllBox
        });
        ResolvedFetcher::new(latest, all)
    }

    /// First `blob_status` fails; later calls succeed (exercises pending + retry path).
    struct StatusFailOnceThenComplete {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ResolveBlobs for StatusFailOnceThenComplete {
        async fn blob_status(&self, _hash: Hash) -> anyhow::Result<BlobStatus> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Err(anyhow::anyhow!("mock status error"))
            } else {
                Ok(BlobStatus::Complete { size: 10 })
            }
        }

        async fn blob_get_bytes(&self, _hash: Hash) -> anyhow::Result<Bytes> {
            Ok(Bytes::from_static(b"blob-bytes"))
        }
    }

    #[tokio::test]
    async fn blob_status_err_then_content_ready_emits_ok() {
        let entry = test_entry();
        let hash = entry.content_hash();
        let fetcher = test_fetcher(entry.clone());
        let scope = Query::single_latest_per_key().key_exact(b"k").build();
        let opts = ResolvedSubscribeOpts {
            include_content: true,
            initial_snapshot: false,
            resolution_delay: None,
        };
        // Yield Insert first so flush runs (status fails → pending) before ContentReady is read;
        // otherwise try_recv batches both events and pending_blob is still empty for ContentReady.
        let entry_a = entry.clone();
        let live = stream::unfold(0u8, move |step| {
            let entry = entry_a.clone();
            async move {
                match step {
                    0 => Some((Ok(LiveEvent::InsertLocal { entry }), 1u8)),
                    1 => {
                        n0_future::time::sleep(std::time::Duration::from_millis(50)).await;
                        Some((Ok(LiveEvent::ContentReady { hash }), 2u8))
                    }
                    _ => None,
                }
            }
        });
        let mut sub = subscribe_resolved_spawn(
            live,
            fetcher,
            Arc::new(StatusFailOnceThenComplete {
                calls: AtomicUsize::new(0),
            }),
            scope,
            opts,
        )
        .expect("spawn");

        let item = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
            .await
            .expect("wall timeout")
            .expect("stream ended");
        let v = item.expect("outer err");
        assert_eq!(v.key.as_ref(), b"k");
        assert_eq!(v.content.as_deref(), Some(&b"blob-bytes"[..]));
    }

    struct CompleteThenGetBytesFail;

    #[async_trait]
    impl ResolveBlobs for CompleteThenGetBytesFail {
        async fn blob_status(&self, _hash: Hash) -> anyhow::Result<BlobStatus> {
            Ok(BlobStatus::Complete { size: 10 })
        }

        async fn blob_get_bytes(&self, _hash: Hash) -> anyhow::Result<Bytes> {
            Err(anyhow::anyhow!("mock get_bytes error"))
        }
    }

    #[tokio::test]
    async fn blob_get_bytes_err_surfaces_on_stream() {
        let entry = test_entry();
        let fetcher = test_fetcher(entry.clone());
        let scope = Query::single_latest_per_key().key_exact(b"k").build();
        let opts = ResolvedSubscribeOpts {
            include_content: true,
            initial_snapshot: false,
            resolution_delay: None,
        };
        let live = stream::iter([Ok(LiveEvent::InsertLocal { entry })]);
        let mut sub = subscribe_resolved_spawn(
            live,
            fetcher,
            Arc::new(CompleteThenGetBytesFail),
            scope,
            opts,
        )
        .expect("spawn");

        let item = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
            .await
            .expect("wall timeout")
            .expect("stream ended");
        let err = item.expect_err("expected Err from get_bytes");
        assert!(err.to_string().contains("mock get_bytes error"), "{err}");
    }

    #[tokio::test]
    async fn blob_status_err_only_no_ok_before_stream_end() {
        let entry = test_entry();
        let fetcher = test_fetcher(entry.clone());
        let scope = Query::single_latest_per_key().key_exact(b"k").build();
        let opts = ResolvedSubscribeOpts {
            include_content: true,
            initial_snapshot: false,
            resolution_delay: None,
        };
        let live = stream::iter([Ok(LiveEvent::InsertLocal { entry })]);
        let mut sub = subscribe_resolved_spawn(
            live,
            fetcher,
            Arc::new(StatusFailOnceThenComplete {
                calls: AtomicUsize::new(0),
            }),
            scope,
            opts,
        )
        .expect("spawn");

        match tokio::time::timeout(std::time::Duration::from_millis(400), sub.next()).await {
            Err(_) => {}
            Ok(None) => {}
            Ok(Some(Ok(_))) => panic!("unexpected Ok(ResolvedKeyValue) while blob status errors"),
            Ok(Some(Err(e))) => panic!("unexpected stream err: {e:#}"),
        }
    }
}
