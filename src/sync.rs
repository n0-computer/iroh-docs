//! API for iroh-docs replicas

// Names and concepts are roughly based on Willows design at the moment:
//
// https://hackmd.io/DTtck8QOQm6tZaQBBtTf7w
//
// This is going to change!

use std::{
    cmp::Ordering,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::{Bytes, BytesMut};
use ed25519_dalek::{Signature, SignatureError};
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

pub use crate::heads::AuthorHeads;
use crate::{
    keys::{Author, AuthorId, AuthorPublicKey, NamespaceId, NamespacePublicKey, NamespaceSecret},
    ranger::{self, Fingerprint, InsertOutcome, RangeEntry, RangeKey, RangeValue, Store},
    store::{self, fs::StoreInstance, DownloadPolicyStore, PublicKeyStore},
};

/// Protocol message for the set reconciliation protocol.
///
/// Can be serialized to bytes with [serde] to transfer between peers.
pub type ProtocolMessage = crate::ranger::Message<SignedEntry>;

/// Byte representation of a `PeerId` from `iroh-net`.
// TODO: PeerId is in iroh-net which iroh-docs doesn't depend on. Add iroh-base crate with `PeerId`.
pub type PeerIdBytes = [u8; 32];

/// Max time in the future from our wall clock time that we accept entries for.
/// Value is 10 minutes.
pub const MAX_TIMESTAMP_FUTURE_SHIFT: u64 = 10 * 60 * Duration::from_secs(1).as_millis() as u64;

/// Callback that may be set on a replica to determine the availability status for a content hash.
pub type ContentStatusCallback =
    Arc<dyn Fn(Hash) -> n0_future::boxed::BoxFuture<ContentStatus> + Send + Sync + 'static>;

/// Event emitted by sync when entries are added.
#[derive(Debug, Clone)]
pub enum Event {
    /// A local entry has been added.
    LocalInsert {
        /// Document in which the entry was inserted.
        namespace: NamespaceId,
        /// Inserted entry.
        entry: SignedEntry,
    },
    /// A remote entry has been added.
    RemoteInsert {
        /// Document in which the entry was inserted.
        namespace: NamespaceId,
        /// Inserted entry.
        entry: SignedEntry,
        /// Peer that provided the inserted entry.
        from: PeerIdBytes,
        /// Whether download policies require the content to be downloaded.
        should_download: bool,
        /// [`ContentStatus`] for this entry in the remote's replica.
        remote_content_status: ContentStatus,
    },
}

/// Whether an entry was inserted locally or by a remote peer.
#[derive(Debug, Clone)]
pub enum InsertOrigin {
    /// The entry was inserted locally.
    Local,
    /// The entry was received from the remote node identified by [`PeerIdBytes`].
    Sync {
        /// The peer from which we received this entry.
        from: PeerIdBytes,
        /// Whether the peer claims to have the content blob for this entry.
        remote_content_status: ContentStatus,
    },
}

/// Whether the content status is available on a node.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ContentStatus {
    /// The content is completely available.
    Complete,
    /// The content is partially available.
    Incomplete,
    /// The content is missing.
    Missing,
}

/// Outcome of a sync operation.
#[derive(Debug, Clone, Default)]
pub struct SyncOutcome {
    /// Timestamp of the latest entry for each author in the set we received.
    pub heads_received: AuthorHeads,
    /// Number of entries we received.
    pub num_recv: usize,
    /// Number of entries we sent.
    pub num_sent: usize,
}

fn get_as_ptr<T>(value: &T) -> Option<usize> {
    use std::mem;
    if mem::size_of::<T>() == std::mem::size_of::<usize>()
        && mem::align_of::<T>() == mem::align_of::<usize>()
    {
        // Safe only if size and alignment requirements are met
        unsafe { Some(mem::transmute_copy(value)) }
    } else {
        None
    }
}

fn same_channel<T>(a: &async_channel::Sender<T>, b: &async_channel::Sender<T>) -> bool {
    get_as_ptr(a).unwrap() == get_as_ptr(b).unwrap()
}

#[derive(Debug, Default)]
struct Subscribers(Vec<async_channel::Sender<Event>>);
impl Subscribers {
    pub fn subscribe(&mut self, sender: async_channel::Sender<Event>) {
        self.0.push(sender)
    }
    pub fn unsubscribe(&mut self, sender: &async_channel::Sender<Event>) {
        self.0.retain(|s| !same_channel(s, sender));
    }
    pub fn send(&mut self, event: Event) {
        self.0
            .retain(|sender| sender.send_blocking(event.clone()).is_ok())
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn send_with(&mut self, f: impl FnOnce() -> Event) {
        if !self.0.is_empty() {
            self.send(f())
        }
    }
}

/// Kind of capability of the namespace.
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    num_enum::IntoPrimitive,
    num_enum::TryFromPrimitive,
    strum::Display,
)]
#[repr(u8)]
#[strum(serialize_all = "snake_case")]
pub enum CapabilityKind {
    /// A writable replica.
    Write = 1,
    /// A readable replica.
    Read = 2,
}

/// The capability of the namespace.
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum Capability {
    /// Write access to the namespace.
    Write(NamespaceSecret),
    /// Read only access to the namespace.
    Read(NamespaceId),
}

impl Capability {
    /// Get the [`NamespaceId`] for this [`Capability`].
    pub fn id(&self) -> NamespaceId {
        match self {
            Capability::Write(secret) => secret.id(),
            Capability::Read(id) => *id,
        }
    }

    /// Get the [`NamespaceSecret`] of this [`Capability`].
    /// Will fail if the [`Capability`] is read only.
    pub fn secret_key(&self) -> Result<&NamespaceSecret, ReadOnly> {
        match self {
            Capability::Write(secret) => Ok(secret),
            Capability::Read(_) => Err(ReadOnly),
        }
    }

    /// Get the kind of capability.
    pub fn kind(&self) -> CapabilityKind {
        match self {
            Capability::Write(_) => CapabilityKind::Write,
            Capability::Read(_) => CapabilityKind::Read,
        }
    }

    /// Get the raw representation of this namespace capability.
    pub fn raw(&self) -> (u8, [u8; 32]) {
        let capability_repr: u8 = self.kind().into();
        let bytes = match self {
            Capability::Write(secret) => secret.to_bytes(),
            Capability::Read(id) => id.to_bytes(),
        };
        (capability_repr, bytes)
    }

    /// Create a [`Capability`] from its raw representation.
    pub fn from_raw(kind: u8, bytes: &[u8; 32]) -> anyhow::Result<Self> {
        let kind: CapabilityKind = kind.try_into()?;
        let capability = match kind {
            CapabilityKind::Write => {
                let secret = NamespaceSecret::from_bytes(bytes);
                Capability::Write(secret)
            }
            CapabilityKind::Read => {
                let id = NamespaceId::from(bytes);
                Capability::Read(id)
            }
        };
        Ok(capability)
    }

    /// Merge this capability with another capability.
    ///
    /// Will return an error if `other` is not a capability for the same namespace.
    ///
    /// Returns `true` if the capability was changed, `false` otherwise.
    pub fn merge(&mut self, other: Capability) -> Result<bool, CapabilityError> {
        if other.id() != self.id() {
            return Err(CapabilityError::NamespaceMismatch);
        }

        // the only capability upgrade is from read-only (self) to writable (other)
        if matches!(self, Capability::Read(_)) && matches!(other, Capability::Write(_)) {
            let _ = std::mem::replace(self, other);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Errors for capability operations
#[derive(Debug, thiserror::Error)]
pub enum CapabilityError {
    /// Namespaces are not the same
    #[error("Namespaces are not the same")]
    NamespaceMismatch,
}

/// In memory information about an open replica.
#[derive(derive_more::Debug)]
pub struct ReplicaInfo {
    pub(crate) capability: Capability,
    subscribers: Subscribers,
    #[debug("ContentStatusCallback")]
    content_status_cb: Option<ContentStatusCallback>,
    closed: bool,
}

impl ReplicaInfo {
    /// Create a new replica.
    pub fn new(capability: Capability) -> Self {
        Self {
            capability,
            subscribers: Default::default(),
            // on_insert_sender: RwLock::new(None),
            content_status_cb: None,
            closed: false,
        }
    }

    /// Subscribe to insert events.
    ///
    /// When subscribing to a replica, you must ensure that the corresponding [`async_channel::Receiver`] is
    /// received from in a loop. If not receiving, local and remote inserts will hang waiting for
    /// the receiver to be received from.
    pub fn subscribe(&mut self, sender: async_channel::Sender<Event>) {
        self.subscribers.subscribe(sender)
    }

    /// Explicitly unsubscribe a sender.
    ///
    /// Simply dropping the receiver is fine too. If you cloned a single sender to subscribe to
    /// multiple replicas, you can use this method to explicitly unsubscribe the sender from
    /// this replica without having to drop the receiver.
    pub fn unsubscribe(&mut self, sender: &async_channel::Sender<Event>) {
        self.subscribers.unsubscribe(sender)
    }

    /// Get the number of current event subscribers.
    pub fn subscribers_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Set the content status callback.
    ///
    /// Only one callback can be active at a time. If a previous callback was registered, this
    /// will return `false`.
    pub fn set_content_status_callback(&mut self, cb: ContentStatusCallback) -> bool {
        if self.content_status_cb.is_some() {
            false
        } else {
            self.content_status_cb = Some(cb);
            true
        }
    }

    fn ensure_open(&self) -> Result<(), InsertError> {
        if self.closed() {
            Err(InsertError::Closed)
        } else {
            Ok(())
        }
    }

    /// Returns true if the replica is closed.
    ///
    /// If a replica is closed, no further operations can be performed. A replica cannot be closed
    /// manually, it must be closed via [`store::Store::close_replica`] or
    /// [`store::Store::remove_replica`]
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Merge a capability.
    ///
    /// The capability must refer to the the same namespace, otherwise an error will be returned.
    ///
    /// This will upgrade the replica's capability when passing a `Capability::Write`.
    /// It is a no-op if `capability` is a Capability::Read`.
    pub fn merge_capability(&mut self, capability: Capability) -> Result<bool, CapabilityError> {
        self.capability.merge(capability)
    }
}

/// Local representation of a mutable, synchronizable key-value store.
#[derive(derive_more::Debug)]
pub struct Replica<'a, I = Box<ReplicaInfo>> {
    pub(crate) store: StoreInstance<'a>,
    pub(crate) info: I,
}

impl<'a, I> Replica<'a, I>
where
    I: Deref<Target = ReplicaInfo> + DerefMut,
{
    /// Create a new replica.
    pub fn new(store: StoreInstance<'a>, info: I) -> Self {
        Replica { info, store }
    }

    /// Insert a new record at the given key.
    ///
    /// The entry will by signed by the provided `author`.
    /// The `len` must be the byte length of the data identified by `hash`.
    ///
    /// Returns the number of entries removed as a consequence of this insertion,
    /// or an error either if the entry failed to validate or if a store operation failed.
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        author: &Author,
        hash: Hash,
        len: u64,
    ) -> Result<usize, InsertError> {
        if len == 0 || hash == Hash::EMPTY {
            return Err(InsertError::EntryIsEmpty);
        }
        self.info.ensure_open()?;
        let id = RecordIdentifier::new(self.id(), author.id(), key);
        let record = Record::new_current(hash, len);
        let entry = Entry::new(id, record);
        let secret = self.secret_key()?;
        let signed_entry = entry.sign(secret, author);
        self.insert_entry(signed_entry, InsertOrigin::Local)
    }

    /// Delete entries that match the given `author` and key `prefix`.
    ///
    /// This inserts an empty entry with the key set to `prefix`, effectively clearing all other
    /// entries whose key starts with or is equal to the given `prefix`.
    ///
    /// Returns the number of entries deleted.
    pub fn delete_prefix(
        &mut self,
        prefix: impl AsRef<[u8]>,
        author: &Author,
    ) -> Result<usize, InsertError> {
        self.info.ensure_open()?;
        let id = RecordIdentifier::new(self.id(), author.id(), prefix);
        let entry = Entry::new_empty(id);
        let signed_entry = entry.sign(self.secret_key()?, author);
        self.insert_entry(signed_entry, InsertOrigin::Local)
    }

    /// Insert an entry into this replica which was received from a remote peer.
    ///
    /// This will verify both the namespace and author signatures of the entry, emit an `on_insert`
    /// event, and insert the entry into the replica store.
    ///
    /// Returns the number of entries removed as a consequence of this insertion,
    /// or an error if the entry failed to validate or if a store operation failed.
    pub fn insert_remote_entry(
        &mut self,
        entry: SignedEntry,
        received_from: PeerIdBytes,
        content_status: ContentStatus,
    ) -> Result<usize, InsertError> {
        self.info.ensure_open()?;
        entry.validate_empty()?;
        let origin = InsertOrigin::Sync {
            from: received_from,
            remote_content_status: content_status,
        };
        self.insert_entry(entry, origin)
    }

    /// Insert a signed entry into the database.
    ///
    /// Returns the number of entries removed as a consequence of this insertion.
    fn insert_entry(
        &mut self,
        entry: SignedEntry,
        origin: InsertOrigin,
    ) -> Result<usize, InsertError> {
        let namespace = self.id();

        let store = &self.store;
        validate_entry(system_time_now(), store, namespace, &entry, &origin)?;

        let outcome = self.store.put(entry.clone()).map_err(InsertError::Store)?;
        tracing::debug!(?origin, hash = %entry.content_hash(), ?outcome, "insert");

        let removed_count = match outcome {
            InsertOutcome::Inserted { removed } => removed,
            InsertOutcome::NotInserted => return Err(InsertError::NewerEntryExists),
        };

        let insert_event = match origin {
            InsertOrigin::Local => Event::LocalInsert { namespace, entry },
            InsertOrigin::Sync {
                from,
                remote_content_status,
            } => {
                let download_policy = self
                    .store
                    .get_download_policy(&self.id())
                    .unwrap_or_default();
                let should_download = download_policy.matches(entry.entry());
                Event::RemoteInsert {
                    namespace,
                    entry,
                    from,
                    should_download,
                    remote_content_status,
                }
            }
        };

        self.info.subscribers.send(insert_event);

        Ok(removed_count)
    }

    /// Hashes the given data and inserts it.
    ///
    /// This does not store the content, just the record of it.
    /// Returns the calculated hash.
    pub fn hash_and_insert(
        &mut self,
        key: impl AsRef<[u8]>,
        author: &Author,
        data: impl AsRef<[u8]>,
    ) -> Result<Hash, InsertError> {
        self.info.ensure_open()?;
        let len = data.as_ref().len() as u64;
        let hash = Hash::new(data);
        self.insert(key, author, hash, len)?;
        Ok(hash)
    }

    /// Get the identifier for an entry in this replica.
    pub fn record_id(&self, key: impl AsRef<[u8]>, author: &Author) -> RecordIdentifier {
        RecordIdentifier::new(self.info.capability.id(), author.id(), key)
    }

    /// Create the initial message for the set reconciliation flow with a remote peer.
    pub fn sync_initial_message(&mut self) -> anyhow::Result<crate::ranger::Message<SignedEntry>> {
        self.info.ensure_open().map_err(anyhow::Error::from)?;
        self.store.initial_message()
    }

    /// Process a set reconciliation message from a remote peer.
    ///
    /// Returns the next message to be sent to the peer, if any.
    pub async fn sync_process_message(
        &mut self,
        message: crate::ranger::Message<SignedEntry>,
        from_peer: PeerIdBytes,
        state: &mut SyncOutcome,
    ) -> Result<Option<crate::ranger::Message<SignedEntry>>, anyhow::Error> {
        self.info.ensure_open()?;
        let my_namespace = self.id();
        let now = system_time_now();

        // update state with incoming data.
        state.num_recv += message.value_count();
        for (entry, _content_status) in message.values() {
            state
                .heads_received
                .insert(entry.author(), entry.timestamp());
        }

        // let subscribers = std::rc::Rc::new(&mut self.subscribers);
        // l
        let cb = self.info.content_status_cb.clone();
        let download_policy = self
            .store
            .get_download_policy(&my_namespace)
            .unwrap_or_default();
        let reply = self
            .store
            .process_message(
                &Default::default(),
                message,
                // validate callback: validate incoming entries, and send to on_insert channel
                |store, entry, content_status| {
                    let origin = InsertOrigin::Sync {
                        from: from_peer,
                        remote_content_status: content_status,
                    };
                    validate_entry(now, store, my_namespace, entry, &origin).is_ok()
                },
                // on_insert callback: is called when an entry was actually inserted in the store
                |_store, entry, content_status| {
                    // We use `send_with` to only clone the entry if we have active subscriptions.
                    self.info.subscribers.send_with(|| {
                        let should_download = download_policy.matches(entry.entry());
                        Event::RemoteInsert {
                            from: from_peer,
                            namespace: my_namespace,
                            entry: entry.clone(),
                            should_download,
                            remote_content_status: content_status,
                        }
                    })
                },
                // content_status callback: get content status for outgoing entries
                move |entry| {
                    let cb = cb.clone();
                    Box::pin(async move {
                        if let Some(cb) = cb.as_ref() {
                            cb(entry.content_hash()).await
                        } else {
                            ContentStatus::Missing
                        }
                    })
                },
            )
            .await?;

        // update state with outgoing data.
        if let Some(ref reply) = reply {
            state.num_sent += reply.value_count();
        }

        Ok(reply)
    }

    /// Get the namespace identifier for this [`Replica`].
    pub fn id(&self) -> NamespaceId {
        self.info.capability.id()
    }

    /// Get the [`Capability`] of this [`Replica`].
    pub fn capability(&self) -> &Capability {
        &self.info.capability
    }

    /// Get the byte representation of the [`NamespaceSecret`] key for this replica. Will fail if
    /// the replica is read only
    pub fn secret_key(&self) -> Result<&NamespaceSecret, ReadOnly> {
        self.info.capability.secret_key()
    }
}

/// Error that occurs trying to access the [`NamespaceSecret`] of a read-only [`Capability`].
#[derive(Debug, thiserror::Error)]
#[error("Replica allows read access only.")]
pub struct ReadOnly;

/// Validate a [`SignedEntry`] if it's fit to be inserted.
///
/// This validates that
/// * the entry's author and namespace signatures are correct
/// * the entry's namespace matches the current replica
/// * the entry's timestamp is not more than 10 minutes in the future of our system time
/// * the entry is newer than an existing entry for the same key and author, if such exists.
fn validate_entry<S: ranger::Store<SignedEntry> + PublicKeyStore>(
    now: u64,
    store: &S,
    expected_namespace: NamespaceId,
    entry: &SignedEntry,
    origin: &InsertOrigin,
) -> Result<(), ValidationFailure> {
    // Verify the namespace
    if entry.namespace() != expected_namespace {
        return Err(ValidationFailure::InvalidNamespace);
    }

    // Verify signature for non-local entries.
    if !matches!(origin, InsertOrigin::Local) && entry.verify(store).is_err() {
        return Err(ValidationFailure::BadSignature);
    }

    // Verify that the timestamp of the entry is not too far in the future.
    if entry.timestamp() > now + MAX_TIMESTAMP_FUTURE_SHIFT {
        return Err(ValidationFailure::TooFarInTheFuture);
    }
    Ok(())
}

/// Error emitted when inserting entries into a [`Replica`] failed
#[derive(thiserror::Error, derive_more::Debug, derive_more::From)]
pub enum InsertError {
    /// Storage error
    #[error("storage error")]
    Store(anyhow::Error),
    /// Validation failure
    #[error("validation failure")]
    Validation(#[from] ValidationFailure),
    /// A newer entry exists for either this entry's key or a prefix of the key.
    #[error("A newer entry exists for either this entry's key or a prefix of the key.")]
    NewerEntryExists,
    /// Attempted to insert an empty entry.
    #[error("Attempted to insert an empty entry")]
    EntryIsEmpty,
    /// Replica is read only.
    #[error("Attempted to insert to read only replica")]
    #[from(ReadOnly)]
    ReadOnly,
    /// The replica is closed, no operations may be performed.
    #[error("replica is closed")]
    Closed,
}

/// Reason why entry validation failed
#[derive(thiserror::Error, Debug)]
pub enum ValidationFailure {
    /// Entry namespace does not match the current replica.
    #[error("Entry namespace does not match the current replica")]
    InvalidNamespace,
    /// Entry signature is invalid.
    #[error("Entry signature is invalid")]
    BadSignature,
    /// Entry timestamp is too far in the future.
    #[error("Entry timestamp is too far in the future.")]
    TooFarInTheFuture,
    /// Entry has length 0 but not the empty hash, or the empty hash but not length 0.
    #[error("Entry has length 0 but not the empty hash, or the empty hash but not length 0")]
    InvalidEmptyEntry,
}

/// A signed entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedEntry {
    signature: EntrySignature,
    entry: Entry,
}

impl From<SignedEntry> for Entry {
    fn from(value: SignedEntry) -> Self {
        value.entry
    }
}

impl PartialOrd for SignedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SignedEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry.cmp(&other.entry)
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| self.record.cmp(&other.record))
    }
}

impl SignedEntry {
    pub(crate) fn new(signature: EntrySignature, entry: Entry) -> Self {
        SignedEntry { signature, entry }
    }

    /// Create a new signed entry by signing an entry with the `namespace` and `author`.
    pub fn from_entry(entry: Entry, namespace: &NamespaceSecret, author: &Author) -> Self {
        let signature = EntrySignature::from_entry(&entry, namespace, author);
        SignedEntry { signature, entry }
    }

    /// Create a new signed entries from its parts.
    pub fn from_parts(
        namespace: &NamespaceSecret,
        author: &Author,
        key: impl AsRef<[u8]>,
        record: Record,
    ) -> Self {
        let id = RecordIdentifier::new(namespace.id(), author.id(), key);
        let entry = Entry::new(id, record);
        Self::from_entry(entry, namespace, author)
    }

    /// Verify the signatures on this entry.
    pub fn verify<S: store::PublicKeyStore>(&self, store: &S) -> Result<(), SignatureError> {
        self.signature.verify(
            &self.entry,
            &self.entry.namespace().public_key(store)?,
            &self.entry.author().public_key(store)?,
        )
    }

    /// Get the signature.
    pub fn signature(&self) -> &EntrySignature {
        &self.signature
    }

    /// Validate that the entry has the empty hash if the length is 0, or a non-zero length.
    pub fn validate_empty(&self) -> Result<(), ValidationFailure> {
        self.entry().validate_empty()
    }

    /// Get the [`Entry`].
    pub fn entry(&self) -> &Entry {
        &self.entry
    }

    /// Get the content [`struct@Hash`] of the entry.
    pub fn content_hash(&self) -> Hash {
        self.entry().content_hash()
    }

    /// Get the content length of the entry.
    pub fn content_len(&self) -> u64 {
        self.entry().content_len()
    }

    /// Get the author bytes of this entry.
    pub fn author_bytes(&self) -> AuthorId {
        self.entry().id().author()
    }

    /// Get the key of the entry.
    pub fn key(&self) -> &[u8] {
        self.entry().id().key()
    }

    /// Get the timestamp of the entry.
    pub fn timestamp(&self) -> u64 {
        self.entry().timestamp()
    }
}

impl RangeEntry for SignedEntry {
    type Key = RecordIdentifier;
    type Value = Record;

    fn key(&self) -> &Self::Key {
        &self.entry.id
    }

    fn value(&self) -> &Self::Value {
        &self.entry.record
    }

    fn as_fingerprint(&self) -> crate::ranger::Fingerprint {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.namespace().as_ref());
        hasher.update(self.author_bytes().as_ref());
        hasher.update(self.key());
        hasher.update(&self.timestamp().to_be_bytes());
        hasher.update(self.content_hash().as_bytes());
        Fingerprint(hasher.finalize().into())
    }
}

/// Signature over an entry.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntrySignature {
    author_signature: Signature,
    namespace_signature: Signature,
}

impl Debug for EntrySignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntrySignature")
            .field(
                "namespace_signature",
                &hex::encode(self.namespace_signature.to_bytes()),
            )
            .field(
                "author_signature",
                &hex::encode(self.author_signature.to_bytes()),
            )
            .finish()
    }
}

impl EntrySignature {
    /// Create a new signature by signing an entry with the `namespace` and `author`.
    pub fn from_entry(entry: &Entry, namespace: &NamespaceSecret, author: &Author) -> Self {
        // TODO: this should probably include a namespace prefix
        // namespace in the cryptographic sense.
        let bytes = entry.to_vec();
        let namespace_signature = namespace.sign(&bytes);
        let author_signature = author.sign(&bytes);

        EntrySignature {
            author_signature,
            namespace_signature,
        }
    }

    /// Verify that this signature was created by signing the `entry` with the
    /// secret keys of the specified `author` and `namespace`.
    pub fn verify(
        &self,
        entry: &Entry,
        namespace: &NamespacePublicKey,
        author: &AuthorPublicKey,
    ) -> Result<(), SignatureError> {
        let bytes = entry.to_vec();
        namespace.verify(&bytes, &self.namespace_signature)?;
        author.verify(&bytes, &self.author_signature)?;

        Ok(())
    }

    pub(crate) fn from_parts(namespace_sig: &[u8; 64], author_sig: &[u8; 64]) -> Self {
        let namespace_signature = Signature::from_bytes(namespace_sig);
        let author_signature = Signature::from_bytes(author_sig);

        EntrySignature {
            author_signature,
            namespace_signature,
        }
    }

    pub(crate) fn author(&self) -> &Signature {
        &self.author_signature
    }

    pub(crate) fn namespace(&self) -> &Signature {
        &self.namespace_signature
    }
}

/// A single entry in a [`Replica`]
///
/// An entry is identified by a key, its [`Author`], and the [`Replica`]'s
/// [`NamespaceSecret`]. Its value is the [32-byte BLAKE3 hash](iroh_blobs::Hash)
/// of the entry's content data, the size of this content data, and a timestamp.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    id: RecordIdentifier,
    record: Record,
}

impl Entry {
    /// Create a new entry
    pub fn new(id: RecordIdentifier, record: Record) -> Self {
        Entry { id, record }
    }

    /// Create a new empty entry with the current timestamp.
    pub fn new_empty(id: RecordIdentifier) -> Self {
        Entry {
            id,
            record: Record::empty_current(),
        }
    }

    /// Validate that the entry has the empty hash if the length is 0, or a non-zero length.
    pub fn validate_empty(&self) -> Result<(), ValidationFailure> {
        match (self.content_hash() == Hash::EMPTY, self.content_len() == 0) {
            (true, true) => Ok(()),
            (false, false) => Ok(()),
            (true, false) => Err(ValidationFailure::InvalidEmptyEntry),
            (false, true) => Err(ValidationFailure::InvalidEmptyEntry),
        }
    }

    /// Get the [`RecordIdentifier`] for this entry.
    pub fn id(&self) -> &RecordIdentifier {
        &self.id
    }

    /// Get the [`NamespaceId`] of this entry.
    pub fn namespace(&self) -> NamespaceId {
        self.id.namespace()
    }

    /// Get the [`AuthorId`] of this entry.
    pub fn author(&self) -> AuthorId {
        self.id.author()
    }

    /// Get the key of this entry.
    pub fn key(&self) -> &[u8] {
        self.id.key()
    }

    /// Get the [`Record`] contained in this entry.
    pub fn record(&self) -> &Record {
        &self.record
    }

    /// Get the content hash of the record.
    pub fn content_hash(&self) -> Hash {
        self.record.hash
    }

    /// Get the content length of the record.
    pub fn content_len(&self) -> u64 {
        self.record.len
    }

    /// Get the timestamp of the record.
    pub fn timestamp(&self) -> u64 {
        self.record.timestamp
    }

    /// Serialize this entry into its canonical byte representation used for signing.
    pub fn encode(&self, out: &mut Vec<u8>) {
        self.id.encode(out);
        self.record.encode(out);
    }

    /// Serialize this entry into a new vector with its canonical byte representation.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }

    /// Sign this entry with a [`NamespaceSecret`] and [`Author`].
    pub fn sign(self, namespace: &NamespaceSecret, author: &Author) -> SignedEntry {
        SignedEntry::from_entry(self, namespace, author)
    }
}

const NAMESPACE_BYTES: std::ops::Range<usize> = 0..32;
const AUTHOR_BYTES: std::ops::Range<usize> = 32..64;
const KEY_BYTES: std::ops::RangeFrom<usize> = 64..;

/// The identifier of a record.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordIdentifier(Bytes);

impl Default for RecordIdentifier {
    fn default() -> Self {
        Self::new(NamespaceId::default(), AuthorId::default(), b"")
    }
}

impl Debug for RecordIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordIdentifier")
            .field("namespace", &self.namespace())
            .field("author", &self.author())
            .field("key", &std::string::String::from_utf8_lossy(self.key()))
            .finish()
    }
}

impl RangeKey for RecordIdentifier {
    #[cfg(test)]
    fn is_prefix_of(&self, other: &Self) -> bool {
        other.as_ref().starts_with(self.as_ref())
    }
}

fn system_time_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("time drift")
        .as_micros() as u64
}

impl RecordIdentifier {
    /// Create a new [`RecordIdentifier`].
    pub fn new(
        namespace: impl Into<NamespaceId>,
        author: impl Into<AuthorId>,
        key: impl AsRef<[u8]>,
    ) -> Self {
        let mut bytes = BytesMut::with_capacity(32 + 32 + key.as_ref().len());
        bytes.extend_from_slice(namespace.into().as_bytes());
        bytes.extend_from_slice(author.into().as_bytes());
        bytes.extend_from_slice(key.as_ref());
        Self(bytes.freeze())
    }

    /// Serialize this [`RecordIdentifier`] into a mutable byte array.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.0);
    }

    /// Get this [`RecordIdentifier`] as [Bytes].
    pub fn as_bytes(&self) -> Bytes {
        self.0.clone()
    }

    /// Get this [`RecordIdentifier`] as a tuple of byte slices.
    pub fn as_byte_tuple(&self) -> (&[u8; 32], &[u8; 32], &[u8]) {
        (
            self.0[NAMESPACE_BYTES].try_into().unwrap(),
            self.0[AUTHOR_BYTES].try_into().unwrap(),
            &self.0[KEY_BYTES],
        )
    }

    /// Get this [`RecordIdentifier`] as a tuple of bytes.
    pub fn to_byte_tuple(&self) -> ([u8; 32], [u8; 32], Bytes) {
        (
            self.0[NAMESPACE_BYTES].try_into().unwrap(),
            self.0[AUTHOR_BYTES].try_into().unwrap(),
            self.0.slice(KEY_BYTES),
        )
    }

    /// Get the key of this record.
    pub fn key(&self) -> &[u8] {
        &self.0[KEY_BYTES]
    }

    /// Get the key of this record as [`Bytes`].
    pub fn key_bytes(&self) -> Bytes {
        self.0.slice(KEY_BYTES)
    }

    /// Get the [`NamespaceId`] of this record as byte array.
    pub fn namespace(&self) -> NamespaceId {
        let value: &[u8; 32] = &self.0[NAMESPACE_BYTES].try_into().unwrap();
        value.into()
    }

    /// Get the [`AuthorId`] of this record as byte array.
    pub fn author(&self) -> AuthorId {
        let value: &[u8; 32] = &self.0[AUTHOR_BYTES].try_into().unwrap();
        value.into()
    }
}

impl AsRef<[u8]> for RecordIdentifier {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for SignedEntry {
    type Target = Entry;
    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl Deref for Entry {
    type Target = Record;
    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

/// The data part of an entry in a [`Replica`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Record {
    /// Length of the data referenced by `hash`.
    len: u64,
    /// Hash of the content data.
    hash: Hash,
    /// Record creation timestamp. Counted as micros since the Unix epoch.
    timestamp: u64,
}

impl RangeValue for Record {}

/// Ordering for entry values.
///
/// Compares first the timestamp, then the content hash.
impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.hash.cmp(&other.hash))
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Record {
    /// Create a new record.
    pub fn new(hash: Hash, len: u64, timestamp: u64) -> Self {
        debug_assert!(
            len != 0 || hash == Hash::EMPTY,
            "if `len` is 0 then `hash` must be the hash of the empty byte range"
        );
        Record {
            hash,
            len,
            timestamp,
        }
    }

    /// Create a tombstone record (empty content)
    pub fn empty(timestamp: u64) -> Self {
        Self::new(Hash::EMPTY, 0, timestamp)
    }

    /// Create a tombstone record with the timestamp set to now.
    pub fn empty_current() -> Self {
        Self::new_current(Hash::EMPTY, 0)
    }

    /// Return `true` if the entry is empty.
    pub fn is_empty(&self) -> bool {
        self.hash == Hash::EMPTY
    }

    /// Create a new [`Record`] with the timestamp set to now.
    pub fn new_current(hash: Hash, len: u64) -> Self {
        let timestamp = system_time_now();
        Self::new(hash, len, timestamp)
    }

    /// Get the length of the data addressed by this record's content hash.
    pub fn content_len(&self) -> u64 {
        self.len
    }

    /// Get the [`struct@Hash`] of the content data of this record.
    pub fn content_hash(&self) -> Hash {
        self.hash
    }

    /// Get the timestamp of this record.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    #[cfg(test)]
    pub(crate) fn current_from_data(data: impl AsRef<[u8]>) -> Self {
        let len = data.as_ref().len() as u64;
        let hash = Hash::new(data);
        Self::new_current(hash, len)
    }

    #[cfg(test)]
    pub(crate) fn from_data(data: impl AsRef<[u8]>, timestamp: u64) -> Self {
        let len = data.as_ref().len() as u64;
        let hash = Hash::new(data);
        Self::new(hash, len, timestamp)
    }

    /// Serialize this record into a mutable byte array.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.len.to_be_bytes());
        out.extend_from_slice(self.hash.as_ref());
        out.extend_from_slice(&self.timestamp.to_be_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use anyhow::Result;
    use rand_core::SeedableRng;

    use super::*;
    use crate::{
        actor::SyncHandle,
        ranger::{Range, Store as _},
        store::{OpenError, Query, SortBy, SortDirection, Store},
    };

    #[test]
    fn test_basics_memory() -> Result<()> {
        let store = store::Store::memory();
        test_basics(store)?;

        Ok(())
    }

    #[test]
    fn test_basics_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_basics(store)?;
        Ok(())
    }

    fn test_basics(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let alice = Author::new(&mut rng);
        let bob = Author::new(&mut rng);
        let myspace = NamespaceSecret::new(&mut rng);

        let record_id = RecordIdentifier::new(myspace.id(), alice.id(), "/my/key");
        let record = Record::current_from_data(b"this is my cool data");
        let entry = Entry::new(record_id, record);
        let signed_entry = entry.sign(&myspace, &alice);
        signed_entry.verify(&()).expect("failed to verify");

        let mut my_replica = store.new_replica(myspace.clone())?;
        for i in 0..10 {
            my_replica.hash_and_insert(
                format!("/{i}"),
                &alice,
                format!("{i}: hello from alice"),
            )?;
        }

        for i in 0..10 {
            let res = store
                .get_exact(myspace.id(), alice.id(), format!("/{i}"), false)?
                .unwrap();
            let len = format!("{i}: hello from alice").as_bytes().len() as u64;
            assert_eq!(res.entry().record().content_len(), len);
            res.verify(&())?;
        }

        // Test multiple records for the same key
        let mut my_replica = store.new_replica(myspace.clone())?;
        my_replica.hash_and_insert("/cool/path", &alice, "round 1")?;
        let _entry = store
            .get_exact(myspace.id(), alice.id(), "/cool/path", false)?
            .unwrap();
        // Second
        let mut my_replica = store.new_replica(myspace.clone())?;
        my_replica.hash_and_insert("/cool/path", &alice, "round 2")?;
        let _entry = store
            .get_exact(myspace.id(), alice.id(), "/cool/path", false)?
            .unwrap();

        // Get All by author
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::author(alice.id()))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 11);

        // Get All by author
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::author(bob.id()))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 0);

        // Get All by key
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::key_exact(b"/cool/path"))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        // Get All
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::all())?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 11);

        // insert record from different author
        let mut my_replica = store.new_replica(myspace.clone())?;
        let _entry = my_replica.hash_and_insert("/cool/path", &bob, "bob round 1")?;

        // Get All by author
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::author(alice.id()))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 11);

        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::author(bob.id()))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        // Get All by key
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::key_exact(b"/cool/path"))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        // Get all by prefix
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::key_prefix(b"/cool"))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        // Get All by author and prefix
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::author(alice.id()).key_prefix(b"/cool"))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::author(bob.id()).key_prefix(b"/cool"))?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        // Get All
        let entries: Vec<_> = store
            .get_many(myspace.id(), Query::all())?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 12);

        // Get Range of all should return all latest
        let mut my_replica = store.new_replica(myspace.clone())?;
        let entries_second: Vec<_> = my_replica
            .store
            .get_range(Range::new(
                RecordIdentifier::default(),
                RecordIdentifier::default(),
            ))?
            .collect::<Result<_, _>>()?;

        assert_eq!(entries_second.len(), 12);
        assert_eq!(entries, entries_second.into_iter().collect::<Vec<_>>());

        test_lru_cache_like_behaviour(&mut store, myspace.id())?;
        store.flush()?;
        Ok(())
    }

    /// Test that [`Store::register_useful_peer`] behaves like a LRUCache of size
    /// [`super::store::PEERS_PER_DOC_CACHE_SIZE`].
    fn test_lru_cache_like_behaviour(store: &mut Store, namespace: NamespaceId) -> Result<()> {
        /// Helper to verify the store returns the expected peers for the namespace.
        #[track_caller]
        fn verify_peers(store: &mut Store, namespace: NamespaceId, expected_peers: &Vec<[u8; 32]>) {
            assert_eq!(
                expected_peers,
                &store
                    .get_sync_peers(&namespace)
                    .unwrap()
                    .unwrap()
                    .collect::<Vec<_>>(),
                "sync peers differ"
            );
        }

        let count = super::store::PEERS_PER_DOC_CACHE_SIZE.get();
        // expected peers: newest peers are to the front, oldest to the back
        let mut expected_peers = Vec::with_capacity(count);
        for i in 0..count as u8 {
            let peer = [i; 32];
            expected_peers.insert(0, peer);
            store.register_useful_peer(namespace, peer)?;
        }
        verify_peers(store, namespace, &expected_peers);

        // one more peer should evict the last peer
        expected_peers.pop();
        let newer_peer = [count as u8; 32];
        expected_peers.insert(0, newer_peer);
        store.register_useful_peer(namespace, newer_peer)?;
        verify_peers(store, namespace, &expected_peers);

        // move one existing peer up
        let refreshed_peer = expected_peers.remove(2);
        expected_peers.insert(0, refreshed_peer);
        store.register_useful_peer(namespace, refreshed_peer)?;
        verify_peers(store, namespace, &expected_peers);
        Ok(())
    }

    #[test]
    fn test_content_hashes_iterator_memory() -> Result<()> {
        let store = store::Store::memory();
        test_content_hashes_iterator(store)
    }

    #[test]
    fn test_content_hashes_iterator_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_content_hashes_iterator(store)
    }

    fn test_content_hashes_iterator(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let mut expected = HashSet::new();
        let n_replicas = 3;
        let n_entries = 4;
        for i in 0..n_replicas {
            let namespace = NamespaceSecret::new(&mut rng);
            let author = store.new_author(&mut rng)?;
            let mut replica = store.new_replica(namespace)?;
            for j in 0..n_entries {
                let key = format!("{j}");
                let data = format!("{i}:{j}");
                let hash = replica.hash_and_insert(key, &author, data)?;
                expected.insert(hash);
            }
        }
        assert_eq!(expected.len(), n_replicas * n_entries);
        let actual = store.content_hashes()?.collect::<Result<HashSet<Hash>>>()?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_multikey() {
        let mut rng = rand::thread_rng();

        let k = ["a", "c", "z"];

        let mut n: Vec<_> = (0..3).map(|_| NamespaceSecret::new(&mut rng)).collect();
        n.sort_by_key(|n| n.id());

        let mut a: Vec<_> = (0..3).map(|_| Author::new(&mut rng)).collect();
        a.sort_by_key(|a| a.id());

        // Just key
        {
            let ri0 = RecordIdentifier::new(n[0].id(), a[0].id(), k[0]);
            let ri1 = RecordIdentifier::new(n[0].id(), a[0].id(), k[1]);
            let ri2 = RecordIdentifier::new(n[0].id(), a[0].id(), k[2]);

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(range.contains(&ri0), "start");
            assert!(range.contains(&ri1), "inside");
            assert!(!range.contains(&ri2), "end");

            assert!(ri0 < ri1);
            assert!(ri1 < ri2);
        }

        // Just namespace
        {
            let ri0 = RecordIdentifier::new(n[0].id(), a[0].id(), k[0]);
            let ri1 = RecordIdentifier::new(n[1].id(), a[0].id(), k[1]);
            let ri2 = RecordIdentifier::new(n[2].id(), a[0].id(), k[2]);

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(range.contains(&ri0), "start");
            assert!(range.contains(&ri1), "inside");
            assert!(!range.contains(&ri2), "end");

            assert!(ri0 < ri1);
            assert!(ri1 < ri2);
        }

        // Just author
        {
            let ri0 = RecordIdentifier::new(n[0].id(), a[0].id(), k[0]);
            let ri1 = RecordIdentifier::new(n[0].id(), a[1].id(), k[0]);
            let ri2 = RecordIdentifier::new(n[0].id(), a[2].id(), k[0]);

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(range.contains(&ri0), "start");
            assert!(range.contains(&ri1), "inside");
            assert!(!range.contains(&ri2), "end");

            assert!(ri0 < ri1);
            assert!(ri1 < ri2);
        }

        // Just key and namespace
        {
            let ri0 = RecordIdentifier::new(n[0].id(), a[0].id(), k[0]);
            let ri1 = RecordIdentifier::new(n[1].id(), a[0].id(), k[1]);
            let ri2 = RecordIdentifier::new(n[2].id(), a[0].id(), k[2]);

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(range.contains(&ri0), "start");
            assert!(range.contains(&ri1), "inside");
            assert!(!range.contains(&ri2), "end");

            assert!(ri0 < ri1);
            assert!(ri1 < ri2);
        }

        // Mixed
        {
            // Ord should prioritize namespace - author - key

            let a0 = a[0].id();
            let a1 = a[1].id();
            let n0 = n[0].id();
            let n1 = n[1].id();
            let k0 = k[0];
            let k1 = k[1];

            assert!(RecordIdentifier::new(n0, a0, k0) < RecordIdentifier::new(n1, a1, k1));
            assert!(RecordIdentifier::new(n0, a0, k1) < RecordIdentifier::new(n1, a0, k0));
            assert!(RecordIdentifier::new(n0, a1, k0) < RecordIdentifier::new(n0, a1, k1));
            assert!(RecordIdentifier::new(n1, a1, k0) < RecordIdentifier::new(n1, a1, k1));
        }
    }

    #[test]
    fn test_timestamps_memory() -> Result<()> {
        let store = store::Store::memory();
        test_timestamps(store)?;

        Ok(())
    }

    #[test]
    fn test_timestamps_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_timestamps(store)?;
        Ok(())
    }

    fn test_timestamps(mut store: Store) -> Result<()> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let namespace = NamespaceSecret::new(&mut rng);
        let _replica = store.new_replica(namespace.clone())?;
        let author = store.new_author(&mut rng)?;
        store.close_replica(namespace.id());
        let mut replica = store.open_replica(&namespace.id())?;

        let key = b"hello";
        let value = b"world";
        let entry = {
            let timestamp = 2;
            let id = RecordIdentifier::new(namespace.id(), author.id(), key);
            let record = Record::from_data(value, timestamp);
            Entry::new(id, record).sign(&namespace, &author)
        };

        replica
            .insert_entry(entry.clone(), InsertOrigin::Local)
            .unwrap();
        store.close_replica(namespace.id());
        let res = store
            .get_exact(namespace.id(), author.id(), key, false)?
            .unwrap();
        assert_eq!(res, entry);

        let entry2 = {
            let timestamp = 1;
            let id = RecordIdentifier::new(namespace.id(), author.id(), key);
            let record = Record::from_data(value, timestamp);
            Entry::new(id, record).sign(&namespace, &author)
        };

        let mut replica = store.open_replica(&namespace.id())?;
        let res = replica.insert_entry(entry2, InsertOrigin::Local);
        store.close_replica(namespace.id());
        assert!(matches!(res, Err(InsertError::NewerEntryExists)));
        let res = store
            .get_exact(namespace.id(), author.id(), key, false)?
            .unwrap();
        assert_eq!(res, entry);
        store.flush()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replica_sync_memory() -> Result<()> {
        let alice_store = store::Store::memory();
        let bob_store = store::Store::memory();

        test_replica_sync(alice_store, bob_store).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replica_sync_fs() -> Result<()> {
        let alice_dbfile = tempfile::NamedTempFile::new()?;
        let alice_store = store::fs::Store::persistent(alice_dbfile.path())?;
        let bob_dbfile = tempfile::NamedTempFile::new()?;
        let bob_store = store::fs::Store::persistent(bob_dbfile.path())?;
        test_replica_sync(alice_store, bob_store).await?;

        Ok(())
    }

    async fn test_replica_sync(mut alice_store: Store, mut bob_store: Store) -> Result<()> {
        let alice_set = ["ape", "eel", "fox", "gnu"];
        let bob_set = ["bee", "cat", "doe", "eel", "fox", "hog"];

        let mut rng = rand::thread_rng();
        let author = Author::new(&mut rng);
        let myspace = NamespaceSecret::new(&mut rng);
        let mut alice = alice_store.new_replica(myspace.clone())?;
        for el in &alice_set {
            alice.hash_and_insert(el, &author, el.as_bytes())?;
        }

        let mut bob = bob_store.new_replica(myspace.clone())?;
        for el in &bob_set {
            bob.hash_and_insert(el, &author, el.as_bytes())?;
        }

        let (alice_out, bob_out) = sync(&mut alice, &mut bob).await?;

        assert_eq!(alice_out.num_sent, 2);
        assert_eq!(bob_out.num_recv, 2);
        assert_eq!(alice_out.num_recv, 6);
        assert_eq!(bob_out.num_sent, 6);

        check_entries(&mut alice_store, &myspace.id(), &author, &alice_set)?;
        check_entries(&mut alice_store, &myspace.id(), &author, &bob_set)?;
        check_entries(&mut bob_store, &myspace.id(), &author, &alice_set)?;
        check_entries(&mut bob_store, &myspace.id(), &author, &bob_set)?;
        alice_store.flush()?;
        bob_store.flush()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replica_timestamp_sync_memory() -> Result<()> {
        let alice_store = store::Store::memory();
        let bob_store = store::Store::memory();

        test_replica_timestamp_sync(alice_store, bob_store).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replica_timestamp_sync_fs() -> Result<()> {
        let alice_dbfile = tempfile::NamedTempFile::new()?;
        let alice_store = store::fs::Store::persistent(alice_dbfile.path())?;
        let bob_dbfile = tempfile::NamedTempFile::new()?;
        let bob_store = store::fs::Store::persistent(bob_dbfile.path())?;
        test_replica_timestamp_sync(alice_store, bob_store).await?;

        Ok(())
    }

    async fn test_replica_timestamp_sync(
        mut alice_store: Store,
        mut bob_store: Store,
    ) -> Result<()> {
        let mut rng = rand::thread_rng();
        let author = Author::new(&mut rng);
        let namespace = NamespaceSecret::new(&mut rng);
        let mut alice = alice_store.new_replica(namespace.clone())?;
        let mut bob = bob_store.new_replica(namespace.clone())?;

        let key = b"key";
        let alice_value = b"alice";
        let bob_value = b"bob";
        let _alice_hash = alice.hash_and_insert(key, &author, alice_value)?;
        // system time increased - sync should overwrite
        let bob_hash = bob.hash_and_insert(key, &author, bob_value)?;
        sync(&mut alice, &mut bob).await?;
        assert_eq!(
            get_content_hash(&mut alice_store, namespace.id(), author.id(), key)?,
            Some(bob_hash)
        );
        assert_eq!(
            get_content_hash(&mut alice_store, namespace.id(), author.id(), key)?,
            Some(bob_hash)
        );

        let mut alice = alice_store.new_replica(namespace.clone())?;
        let mut bob = bob_store.new_replica(namespace.clone())?;

        let alice_value_2 = b"alice2";
        // system time increased - sync should overwrite
        let _bob_hash_2 = bob.hash_and_insert(key, &author, bob_value)?;
        let alice_hash_2 = alice.hash_and_insert(key, &author, alice_value_2)?;
        sync(&mut alice, &mut bob).await?;
        assert_eq!(
            get_content_hash(&mut alice_store, namespace.id(), author.id(), key)?,
            Some(alice_hash_2)
        );
        assert_eq!(
            get_content_hash(&mut alice_store, namespace.id(), author.id(), key)?,
            Some(alice_hash_2)
        );
        alice_store.flush()?;
        bob_store.flush()?;
        Ok(())
    }

    #[test]
    fn test_future_timestamp() -> Result<()> {
        let mut rng = rand::thread_rng();
        let mut store = store::Store::memory();
        let author = Author::new(&mut rng);
        let namespace = NamespaceSecret::new(&mut rng);

        let mut replica = store.new_replica(namespace.clone())?;
        let key = b"hi";
        let t = system_time_now();
        let record = Record::from_data(b"1", t);
        let entry0 = SignedEntry::from_parts(&namespace, &author, key, record);
        replica.insert_entry(entry0.clone(), InsertOrigin::Local)?;

        assert_eq!(
            get_entry(&mut store, namespace.id(), author.id(), key)?,
            entry0
        );

        let mut replica = store.new_replica(namespace.clone())?;
        let t = system_time_now() + MAX_TIMESTAMP_FUTURE_SHIFT - 10000;
        let record = Record::from_data(b"2", t);
        let entry1 = SignedEntry::from_parts(&namespace, &author, key, record);
        replica.insert_entry(entry1.clone(), InsertOrigin::Local)?;
        assert_eq!(
            get_entry(&mut store, namespace.id(), author.id(), key)?,
            entry1
        );

        let mut replica = store.new_replica(namespace.clone())?;
        let t = system_time_now() + MAX_TIMESTAMP_FUTURE_SHIFT;
        let record = Record::from_data(b"2", t);
        let entry2 = SignedEntry::from_parts(&namespace, &author, key, record);
        replica.insert_entry(entry2.clone(), InsertOrigin::Local)?;
        assert_eq!(
            get_entry(&mut store, namespace.id(), author.id(), key)?,
            entry2
        );

        let mut replica = store.new_replica(namespace.clone())?;
        let t = system_time_now() + MAX_TIMESTAMP_FUTURE_SHIFT + 10000;
        let record = Record::from_data(b"2", t);
        let entry3 = SignedEntry::from_parts(&namespace, &author, key, record);
        let res = replica.insert_entry(entry3, InsertOrigin::Local);
        assert!(matches!(
            res,
            Err(InsertError::Validation(
                ValidationFailure::TooFarInTheFuture
            ))
        ));
        assert_eq!(
            get_entry(&mut store, namespace.id(), author.id(), key)?,
            entry2
        );
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_insert_empty() -> Result<()> {
        let mut store = store::Store::memory();
        let mut rng = rand::thread_rng();
        let alice = Author::new(&mut rng);
        let myspace = NamespaceSecret::new(&mut rng);
        let mut replica = store.new_replica(myspace.clone())?;
        let hash = Hash::new(b"");
        let res = replica.insert(b"foo", &alice, hash, 0);
        assert!(matches!(res, Err(InsertError::EntryIsEmpty)));
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_prefix_delete_memory() -> Result<()> {
        let store = store::Store::memory();
        test_prefix_delete(store)?;
        Ok(())
    }

    #[test]
    fn test_prefix_delete_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_prefix_delete(store)?;
        Ok(())
    }

    fn test_prefix_delete(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let alice = Author::new(&mut rng);
        let myspace = NamespaceSecret::new(&mut rng);
        let mut replica = store.new_replica(myspace.clone())?;
        let hash1 = replica.hash_and_insert(b"foobar", &alice, b"hello")?;
        let hash2 = replica.hash_and_insert(b"fooboo", &alice, b"world")?;

        // sanity checks
        assert_eq!(
            get_content_hash(&mut store, myspace.id(), alice.id(), b"foobar")?,
            Some(hash1)
        );
        assert_eq!(
            get_content_hash(&mut store, myspace.id(), alice.id(), b"fooboo")?,
            Some(hash2)
        );

        // delete
        let mut replica = store.new_replica(myspace.clone())?;
        let deleted = replica.delete_prefix(b"foo", &alice)?;
        assert_eq!(deleted, 2);
        assert_eq!(
            store.get_exact(myspace.id(), alice.id(), b"foobar", false)?,
            None
        );
        assert_eq!(
            store.get_exact(myspace.id(), alice.id(), b"fooboo", false)?,
            None
        );
        assert_eq!(
            store.get_exact(myspace.id(), alice.id(), b"foo", false)?,
            None
        );
        store.flush()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replica_sync_delete_memory() -> Result<()> {
        let alice_store = store::Store::memory();
        let bob_store = store::Store::memory();

        test_replica_sync_delete(alice_store, bob_store).await
    }

    #[tokio::test]
    async fn test_replica_sync_delete_fs() -> Result<()> {
        let alice_dbfile = tempfile::NamedTempFile::new()?;
        let alice_store = store::fs::Store::persistent(alice_dbfile.path())?;
        let bob_dbfile = tempfile::NamedTempFile::new()?;
        let bob_store = store::fs::Store::persistent(bob_dbfile.path())?;
        test_replica_sync_delete(alice_store, bob_store).await
    }

    async fn test_replica_sync_delete(mut alice_store: Store, mut bob_store: Store) -> Result<()> {
        let alice_set = ["foot"];
        let bob_set = ["fool", "foo", "fog"];

        let mut rng = rand::thread_rng();
        let author = Author::new(&mut rng);
        let myspace = NamespaceSecret::new(&mut rng);
        let mut alice = alice_store.new_replica(myspace.clone())?;
        for el in &alice_set {
            alice.hash_and_insert(el, &author, el.as_bytes())?;
        }

        let mut bob = bob_store.new_replica(myspace.clone())?;
        for el in &bob_set {
            bob.hash_and_insert(el, &author, el.as_bytes())?;
        }

        sync(&mut alice, &mut bob).await?;

        check_entries(&mut alice_store, &myspace.id(), &author, &alice_set)?;
        check_entries(&mut alice_store, &myspace.id(), &author, &bob_set)?;
        check_entries(&mut bob_store, &myspace.id(), &author, &alice_set)?;
        check_entries(&mut bob_store, &myspace.id(), &author, &bob_set)?;

        let mut alice = alice_store.new_replica(myspace.clone())?;
        let mut bob = bob_store.new_replica(myspace.clone())?;
        alice.delete_prefix("foo", &author)?;
        bob.hash_and_insert("fooz", &author, "fooz".as_bytes())?;
        sync(&mut alice, &mut bob).await?;
        check_entries(&mut alice_store, &myspace.id(), &author, &["fog", "fooz"])?;
        check_entries(&mut bob_store, &myspace.id(), &author, &["fog", "fooz"])?;
        alice_store.flush()?;
        bob_store.flush()?;
        Ok(())
    }

    #[test]
    fn test_replica_remove_memory() -> Result<()> {
        let alice_store = store::Store::memory();
        test_replica_remove(alice_store)
    }

    #[test]
    fn test_replica_remove_fs() -> Result<()> {
        let alice_dbfile = tempfile::NamedTempFile::new()?;
        let alice_store = store::fs::Store::persistent(alice_dbfile.path())?;
        test_replica_remove(alice_store)
    }

    fn test_replica_remove(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let namespace = NamespaceSecret::new(&mut rng);
        let author = Author::new(&mut rng);
        let mut replica = store.new_replica(namespace.clone())?;

        // insert entry
        let hash = replica.hash_and_insert(b"foo", &author, b"bar")?;
        let res = store
            .get_many(namespace.id(), Query::all())?
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 1);

        // remove replica
        let res = store.remove_replica(&namespace.id());
        // may not remove replica while still open;
        assert!(res.is_err());
        store.close_replica(namespace.id());
        store.remove_replica(&namespace.id())?;
        let res = store
            .get_many(namespace.id(), Query::all())?
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 0);

        // may not reopen removed replica
        let res = store.load_replica_info(&namespace.id());
        assert!(matches!(res, Err(OpenError::NotFound)));

        // may recreate replica
        let mut replica = store.new_replica(namespace.clone())?;
        replica.insert(b"foo", &author, hash, 3)?;
        let res = store
            .get_many(namespace.id(), Query::all())?
            .collect::<Vec<_>>();
        assert_eq!(res.len(), 1);
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_replica_delete_edge_cases_memory() -> Result<()> {
        let store = store::Store::memory();
        test_replica_delete_edge_cases(store)
    }

    #[test]
    fn test_replica_delete_edge_cases_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_replica_delete_edge_cases(store)
    }

    fn test_replica_delete_edge_cases(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let author = Author::new(&mut rng);
        let namespace = NamespaceSecret::new(&mut rng);

        let edgecases = [0u8, 1u8, 255u8];
        let prefixes = [0u8, 255u8];
        let hash = Hash::new(b"foo");
        let len = 3;
        for prefix in prefixes {
            let mut expected = vec![];
            let mut replica = store.new_replica(namespace.clone())?;
            for suffix in edgecases {
                let key = [prefix, suffix].to_vec();
                expected.push(key.clone());
                replica.insert(&key, &author, hash, len)?;
            }
            assert_keys(&mut store, namespace.id(), expected);
            let mut replica = store.new_replica(namespace.clone())?;
            replica.delete_prefix([prefix], &author)?;
            assert_keys(&mut store, namespace.id(), vec![]);
        }

        let mut replica = store.new_replica(namespace.clone())?;
        let key = vec![1u8, 0u8];
        replica.insert(key, &author, hash, len)?;
        let key = vec![1u8, 1u8];
        replica.insert(key, &author, hash, len)?;
        let key = vec![1u8, 2u8];
        replica.insert(key, &author, hash, len)?;
        let prefix = vec![1u8, 1u8];
        replica.delete_prefix(prefix, &author)?;
        assert_keys(
            &mut store,
            namespace.id(),
            vec![vec![1u8, 0u8], vec![1u8, 2u8]],
        );

        let mut replica = store.new_replica(namespace.clone())?;
        let key = vec![0u8, 255u8];
        replica.insert(key, &author, hash, len)?;
        let key = vec![0u8, 0u8];
        replica.insert(key, &author, hash, len)?;
        let prefix = vec![0u8];
        replica.delete_prefix(prefix, &author)?;
        assert_keys(
            &mut store,
            namespace.id(),
            vec![vec![1u8, 0u8], vec![1u8, 2u8]],
        );
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_latest_iter_memory() -> Result<()> {
        let store = store::Store::memory();
        test_latest_iter(store)
    }

    #[test]
    fn test_latest_iter_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_latest_iter(store)
    }

    fn test_latest_iter(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let author0 = Author::new(&mut rng);
        let author1 = Author::new(&mut rng);
        let namespace = NamespaceSecret::new(&mut rng);
        let mut replica = store.new_replica(namespace.clone())?;

        replica.hash_and_insert(b"a0.1", &author0, b"hi")?;
        let latest = store
            .get_latest_for_each_author(namespace.id())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].2, b"a0.1".to_vec());

        let mut replica = store.new_replica(namespace.clone())?;
        replica.hash_and_insert(b"a1.1", &author1, b"hi")?;
        replica.hash_and_insert(b"a0.2", &author0, b"hi")?;
        let latest = store
            .get_latest_for_each_author(namespace.id())?
            .collect::<Result<Vec<_>>>()?;
        let mut latest_keys: Vec<Vec<u8>> = latest.iter().map(|r| r.2.to_vec()).collect();
        latest_keys.sort();
        assert_eq!(latest_keys, vec![b"a0.2".to_vec(), b"a1.1".to_vec()]);
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_replica_byte_keys_memory() -> Result<()> {
        let store = store::Store::memory();

        test_replica_byte_keys(store)?;
        Ok(())
    }

    #[test]
    fn test_replica_byte_keys_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_replica_byte_keys(store)?;

        Ok(())
    }

    fn test_replica_byte_keys(mut store: Store) -> Result<()> {
        let mut rng = rand::thread_rng();
        let author = Author::new(&mut rng);
        let namespace = NamespaceSecret::new(&mut rng);

        let hash = Hash::new(b"foo");
        let len = 3;

        let key = vec![1u8, 0u8];
        let mut replica = store.new_replica(namespace.clone())?;
        replica.insert(key, &author, hash, len)?;
        assert_keys(&mut store, namespace.id(), vec![vec![1u8, 0u8]]);
        let key = vec![1u8, 2u8];
        let mut replica = store.new_replica(namespace.clone())?;
        replica.insert(key, &author, hash, len)?;
        assert_keys(
            &mut store,
            namespace.id(),
            vec![vec![1u8, 0u8], vec![1u8, 2u8]],
        );

        let key = vec![0u8, 255u8];
        let mut replica = store.new_replica(namespace.clone())?;
        replica.insert(key, &author, hash, len)?;
        assert_keys(
            &mut store,
            namespace.id(),
            vec![vec![1u8, 0u8], vec![1u8, 2u8], vec![0u8, 255u8]],
        );
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_replica_capability_memory() -> Result<()> {
        let store = store::Store::memory();
        test_replica_capability(store)
    }

    #[test]
    fn test_replica_capability_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_replica_capability(store)
    }

    #[allow(clippy::redundant_pattern_matching)]
    fn test_replica_capability(mut store: Store) -> Result<()> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let author = store.new_author(&mut rng)?;
        let namespace = NamespaceSecret::new(&mut rng);

        // import read capability - insert must fail
        let capability = Capability::Read(namespace.id());
        store.import_namespace(capability)?;
        let mut replica = store.open_replica(&namespace.id())?;
        let res = replica.hash_and_insert(b"foo", &author, b"bar");
        assert!(matches!(res, Err(InsertError::ReadOnly)));

        // import write capability - insert must succeed
        let capability = Capability::Write(namespace.clone());
        store.import_namespace(capability)?;
        let mut replica = store.open_replica(&namespace.id())?;
        let res = replica.hash_and_insert(b"foo", &author, b"bar");
        assert!(matches!(res, Ok(_)));
        store.close_replica(namespace.id());
        let mut replica = store.open_replica(&namespace.id())?;
        let res = replica.hash_and_insert(b"foo", &author, b"bar");
        assert!(res.is_ok());

        // import read capability again - insert must still succeed
        let capability = Capability::Read(namespace.id());
        store.import_namespace(capability)?;
        store.close_replica(namespace.id());
        let mut replica = store.open_replica(&namespace.id())?;
        let res = replica.hash_and_insert(b"foo", &author, b"bar");
        assert!(res.is_ok());
        store.flush()?;
        Ok(())
    }

    #[tokio::test]
    async fn test_actor_capability_memory() -> Result<()> {
        let store = store::Store::memory();
        test_actor_capability(store).await
    }

    #[tokio::test]
    async fn test_actor_capability_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_actor_capability(store).await
    }

    async fn test_actor_capability(store: Store) -> Result<()> {
        // test with actor
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let author = Author::new(&mut rng);
        let handle = SyncHandle::spawn(store, None, "test".into());
        let author = handle.import_author(author).await?;
        let namespace = NamespaceSecret::new(&mut rng);
        let id = namespace.id();

        // import read capability - insert must fail
        let capability = Capability::Read(namespace.id());
        handle.import_namespace(capability).await?;
        handle.open(namespace.id(), Default::default()).await?;
        let res = handle
            .insert_local(id, author, b"foo".to_vec().into(), Hash::new(b"bar"), 3)
            .await;
        assert!(res.is_err());

        // import write capability - insert must succeed
        let capability = Capability::Write(namespace.clone());
        handle.import_namespace(capability).await?;
        let res = handle
            .insert_local(id, author, b"foo".to_vec().into(), Hash::new(b"bar"), 3)
            .await;
        assert!(res.is_ok());

        // close and reopen - must still succeed
        handle.close(namespace.id()).await?;
        let res = handle
            .insert_local(id, author, b"foo".to_vec().into(), Hash::new(b"bar"), 3)
            .await;
        assert!(res.is_err());
        handle.open(namespace.id(), Default::default()).await?;
        let res = handle
            .insert_local(id, author, b"foo".to_vec().into(), Hash::new(b"bar"), 3)
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    fn drain(events: async_channel::Receiver<Event>) -> Vec<Event> {
        let mut res = vec![];
        while let Ok(ev) = events.try_recv() {
            res.push(ev);
        }
        res
    }

    /// This tests that no events are emitted for entries received during sync which are obsolete
    /// (too old) by the time they are actually inserted in the store.
    #[tokio::test]
    async fn test_replica_no_wrong_remote_insert_events() -> Result<()> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let mut store1 = store::Store::memory();
        let mut store2 = store::Store::memory();
        let peer1 = [1u8; 32];
        let peer2 = [2u8; 32];
        let mut state1 = SyncOutcome::default();
        let mut state2 = SyncOutcome::default();

        let author = Author::new(&mut rng);
        let namespace = NamespaceSecret::new(&mut rng);
        let mut replica1 = store1.new_replica(namespace.clone())?;
        let mut replica2 = store2.new_replica(namespace.clone())?;

        let (events1_sender, events1) = async_channel::bounded(32);
        let (events2_sender, events2) = async_channel::bounded(32);

        replica1.info.subscribe(events1_sender);
        replica2.info.subscribe(events2_sender);

        replica1.hash_and_insert(b"foo", &author, b"init")?;

        let from1 = replica1.sync_initial_message()?;
        let from2 = replica2
            .sync_process_message(from1, peer1, &mut state2)
            .await
            .unwrap()
            .unwrap();
        let from1 = replica1
            .sync_process_message(from2, peer2, &mut state1)
            .await
            .unwrap()
            .unwrap();
        // now we will receive the entry from rpelica1. we will insert a newer entry now, while the
        // sync is already running. this means the entry from replica1 will be rejected. we make
        // sure that no InsertRemote event is emitted for this entry.
        replica2.hash_and_insert(b"foo", &author, b"update")?;
        let from2 = replica2
            .sync_process_message(from1, peer1, &mut state2)
            .await
            .unwrap();
        assert!(from2.is_none());
        let events1 = drain(events1);
        let events2 = drain(events2);
        assert_eq!(events1.len(), 1);
        assert_eq!(events2.len(), 1);
        assert!(matches!(events1[0], Event::LocalInsert { .. }));
        assert!(matches!(events2[0], Event::LocalInsert { .. }));
        assert_eq!(state1.num_sent, 1);
        assert_eq!(state1.num_recv, 0);
        assert_eq!(state2.num_sent, 0);
        assert_eq!(state2.num_recv, 1);
        store1.flush()?;
        store2.flush()?;
        Ok(())
    }

    #[test]
    fn test_replica_queries_mem() -> Result<()> {
        let store = store::Store::memory();

        test_replica_queries(store)?;
        Ok(())
    }

    #[test]
    fn test_replica_queries_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::persistent(dbfile.path())?;
        test_replica_queries(store)?;

        Ok(())
    }

    fn test_replica_queries(mut store: Store) -> Result<()> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let namespace = NamespaceSecret::new(&mut rng);
        let namespace_id = namespace.id();

        let a1 = store.new_author(&mut rng)?;
        let a2 = store.new_author(&mut rng)?;
        let a3 = store.new_author(&mut rng)?;
        println!(
            "a1 {} a2 {} a3 {}",
            a1.id().fmt_short(),
            a2.id().fmt_short(),
            a3.id().fmt_short()
        );

        let mut replica = store.new_replica(namespace.clone())?;
        replica.hash_and_insert("hi/world", &a2, "a2")?;
        replica.hash_and_insert("hi/world", &a1, "a1")?;
        replica.hash_and_insert("hi/moon", &a2, "a1")?;
        replica.hash_and_insert("hi", &a3, "a3")?;

        struct QueryTester<'a> {
            store: &'a mut Store,
            namespace: NamespaceId,
        }
        impl QueryTester<'_> {
            fn assert(&mut self, query: impl Into<Query>, expected: Vec<(&'static str, &Author)>) {
                let query = query.into();
                let actual = self
                    .store
                    .get_many(self.namespace, query.clone())
                    .unwrap()
                    .map(|e| e.map(|e| (String::from_utf8(e.key().to_vec()).unwrap(), e.author())))
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                let expected = expected
                    .into_iter()
                    .map(|(key, author)| (key.to_string(), author.id()))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "query: {query:#?}")
            }
        }

        let mut qt = QueryTester {
            store: &mut store,
            namespace: namespace_id,
        };

        qt.assert(
            Query::all(),
            vec![
                ("hi/world", &a1),
                ("hi/moon", &a2),
                ("hi/world", &a2),
                ("hi", &a3),
            ],
        );

        qt.assert(
            Query::single_latest_per_key(),
            vec![("hi", &a3), ("hi/moon", &a2), ("hi/world", &a1)],
        );

        qt.assert(
            Query::single_latest_per_key().sort_direction(SortDirection::Desc),
            vec![("hi/world", &a1), ("hi/moon", &a2), ("hi", &a3)],
        );

        qt.assert(
            Query::single_latest_per_key().key_prefix("hi/"),
            vec![("hi/moon", &a2), ("hi/world", &a1)],
        );

        qt.assert(
            Query::single_latest_per_key()
                .key_prefix("hi/")
                .sort_direction(SortDirection::Desc),
            vec![("hi/world", &a1), ("hi/moon", &a2)],
        );

        qt.assert(
            Query::all().sort_by(SortBy::KeyAuthor, SortDirection::Asc),
            vec![
                ("hi", &a3),
                ("hi/moon", &a2),
                ("hi/world", &a1),
                ("hi/world", &a2),
            ],
        );

        qt.assert(
            Query::all().sort_by(SortBy::KeyAuthor, SortDirection::Desc),
            vec![
                ("hi/world", &a2),
                ("hi/world", &a1),
                ("hi/moon", &a2),
                ("hi", &a3),
            ],
        );

        qt.assert(
            Query::all().key_prefix("hi/"),
            vec![("hi/world", &a1), ("hi/moon", &a2), ("hi/world", &a2)],
        );

        qt.assert(
            Query::all().key_prefix("hi/").offset(1).limit(1),
            vec![("hi/moon", &a2)],
        );

        qt.assert(
            Query::all()
                .key_prefix("hi/")
                .sort_by(SortBy::KeyAuthor, SortDirection::Desc),
            vec![("hi/world", &a2), ("hi/world", &a1), ("hi/moon", &a2)],
        );

        qt.assert(
            Query::all()
                .key_prefix("hi/")
                .sort_by(SortBy::KeyAuthor, SortDirection::Desc)
                .offset(1)
                .limit(1),
            vec![("hi/world", &a1)],
        );

        qt.assert(
            Query::all()
                .key_prefix("hi/")
                .sort_by(SortBy::AuthorKey, SortDirection::Asc),
            vec![("hi/world", &a1), ("hi/moon", &a2), ("hi/world", &a2)],
        );

        qt.assert(
            Query::all()
                .key_prefix("hi/")
                .sort_by(SortBy::AuthorKey, SortDirection::Desc),
            vec![("hi/world", &a2), ("hi/moon", &a2), ("hi/world", &a1)],
        );

        qt.assert(
            Query::all()
                .sort_by(SortBy::KeyAuthor, SortDirection::Asc)
                .limit(2)
                .offset(1),
            vec![("hi/moon", &a2), ("hi/world", &a1)],
        );

        let mut replica = store.new_replica(namespace)?;
        replica.delete_prefix("hi/world", &a2)?;
        let mut qt = QueryTester {
            store: &mut store,
            namespace: namespace_id,
        };

        qt.assert(
            Query::all(),
            vec![("hi/world", &a1), ("hi/moon", &a2), ("hi", &a3)],
        );

        qt.assert(
            Query::all().include_empty(),
            vec![
                ("hi/world", &a1),
                ("hi/moon", &a2),
                ("hi/world", &a2),
                ("hi", &a3),
            ],
        );
        store.flush()?;
        Ok(())
    }

    #[test]
    fn test_dl_policies_mem() -> Result<()> {
        let mut store = store::Store::memory();
        test_dl_policies(&mut store)
    }

    #[test]
    fn test_dl_policies_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = store::fs::Store::persistent(dbfile.path())?;
        test_dl_policies(&mut store)
    }

    fn test_dl_policies(store: &mut Store) -> Result<()> {
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let namespace = NamespaceSecret::new(&mut rng);
        let id = namespace.id();

        let filter = store::FilterKind::Exact("foo".into());
        let policy = store::DownloadPolicy::NothingExcept(vec![filter]);
        store
            .set_download_policy(&id, policy.clone())
            .expect_err("document dos not exist");

        // now create the document
        store.new_replica(namespace)?;

        store.set_download_policy(&id, policy.clone())?;
        let retrieved_policy = store.get_download_policy(&id)?;
        assert_eq!(retrieved_policy, policy);
        store.flush()?;
        Ok(())
    }

    fn assert_keys(store: &mut Store, namespace: NamespaceId, mut expected: Vec<Vec<u8>>) {
        expected.sort();
        assert_eq!(expected, get_keys_sorted(store, namespace));
    }

    fn get_keys_sorted(store: &mut Store, namespace: NamespaceId) -> Vec<Vec<u8>> {
        let mut res = store
            .get_many(namespace, Query::all())
            .unwrap()
            .map(|e| e.map(|e| e.key().to_vec()))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        res.sort();
        res
    }

    fn get_entry(
        store: &mut Store,
        namespace: NamespaceId,
        author: AuthorId,
        key: &[u8],
    ) -> anyhow::Result<SignedEntry> {
        let entry = store
            .get_exact(namespace, author, key, true)?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        Ok(entry)
    }

    fn get_content_hash(
        store: &mut Store,
        namespace: NamespaceId,
        author: AuthorId,
        key: &[u8],
    ) -> anyhow::Result<Option<Hash>> {
        let hash = store
            .get_exact(namespace, author, key, false)?
            .map(|e| e.content_hash());
        Ok(hash)
    }

    async fn sync<'a>(
        alice: &'a mut Replica<'a>,
        bob: &'a mut Replica<'a>,
    ) -> Result<(SyncOutcome, SyncOutcome)> {
        let alice_peer_id = [1u8; 32];
        let bob_peer_id = [2u8; 32];
        let mut alice_state = SyncOutcome::default();
        let mut bob_state = SyncOutcome::default();
        // Sync alice - bob
        let mut next_to_bob = Some(alice.sync_initial_message()?);
        let mut rounds = 0;
        while let Some(msg) = next_to_bob.take() {
            assert!(rounds < 100, "too many rounds");
            rounds += 1;
            println!("round {rounds}");
            if let Some(msg) = bob
                .sync_process_message(msg, alice_peer_id, &mut bob_state)
                .await?
            {
                next_to_bob = alice
                    .sync_process_message(msg, bob_peer_id, &mut alice_state)
                    .await?
            }
        }
        assert_eq!(alice_state.num_sent, bob_state.num_recv);
        assert_eq!(alice_state.num_recv, bob_state.num_sent);
        Ok((alice_state, bob_state))
    }

    fn check_entries(
        store: &mut Store,
        namespace: &NamespaceId,
        author: &Author,
        set: &[&str],
    ) -> Result<()> {
        for el in set {
            store.get_exact(*namespace, author.id(), el, false)?;
        }
        Ok(())
    }
}
