//! Implementation of Set Reconcilliation based on
//! "Range-Based Set Reconciliation" by Aljoscha Meyer.

use std::{cmp::Ordering, fmt::Debug, pin::Pin};

use n0_future::StreamExt;
use serde::{Deserialize, Serialize};

use crate::ContentStatus;

/// Store entries that can be fingerprinted and put into ranges.
pub trait RangeEntry: Debug + Clone {
    /// The key type for this entry.
    ///
    /// This type must implement [`Ord`] to define the range ordering used in the set
    /// reconciliation algorithm.
    ///
    /// See [`RangeKey`] for details.
    type Key: RangeKey;

    /// The value type for this entry. See
    ///
    /// The type must implement [`Ord`] to define the time ordering of entries used in the prefix
    /// deletion algorithm.
    ///
    /// See [`RangeValue`] for details.
    type Value: RangeValue;

    /// Get the key for this entry.
    fn key(&self) -> &Self::Key;

    /// Get the value for this entry.
    fn value(&self) -> &Self::Value;

    /// Get the fingerprint for this entry.
    fn as_fingerprint(&self) -> Fingerprint;
}

/// A trait constraining types that are valid entry keys.
pub trait RangeKey: Sized + Debug + Ord + PartialEq + Clone + 'static {
    /// Returns `true` if `self` is a prefix of `other`.
    #[cfg(test)]
    fn is_prefix_of(&self, other: &Self) -> bool;

    /// Returns true if `other` is a prefix of `self`.
    #[cfg(test)]
    fn is_prefixed_by(&self, other: &Self) -> bool {
        other.is_prefix_of(self)
    }
}

/// A trait constraining types that are valid entry values.
pub trait RangeValue: Sized + Debug + Ord + PartialEq + Clone + 'static {}

/// Stores a range.
///
/// There are three possibilities
///
/// - x, x: All elements in a set, denoted with
/// - [x, y): x < y: Includes x, but not y
/// - S \ [y, x) y < x: Includes x, but not y.
///
/// This means that ranges are "wrap around" conceptually.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct Range<K> {
    x: K,
    y: K,
}

impl<K> Range<K> {
    pub fn x(&self) -> &K {
        &self.x
    }

    pub fn y(&self) -> &K {
        &self.y
    }

    pub fn new(x: K, y: K) -> Self {
        Range { x, y }
    }

    pub fn map<X>(self, f: impl FnOnce(K, K) -> (X, X)) -> Range<X> {
        let (x, y) = f(self.x, self.y);
        Range { x, y }
    }
}

impl<K: Ord> Range<K> {
    pub fn is_all(&self) -> bool {
        self.x() == self.y()
    }

    pub fn contains(&self, t: &K) -> bool {
        match self.x().cmp(self.y()) {
            Ordering::Equal => true,
            Ordering::Less => self.x() <= t && t < self.y(),
            Ordering::Greater => self.x() <= t || t < self.y(),
        }
    }
}

impl<K> From<(K, K)> for Range<K> {
    fn from((x, y): (K, K)) -> Self {
        Range { x, y }
    }
}

#[derive(Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fingerprint(pub [u8; 32]);

impl Debug for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fp({})", blake3::Hash::from(self.0).to_hex())
    }
}

impl Fingerprint {
    /// The fingerprint of the empty set
    pub fn empty() -> Self {
        Fingerprint(*blake3::hash(&[]).as_bytes())
    }

    pub fn new<T: RangeEntry>(val: T) -> Self {
        val.as_fingerprint()
    }
}

impl std::ops::BitXorAssign for Fingerprint {
    fn bitxor_assign(&mut self, rhs: Self) {
        for (a, b) in self.0.iter_mut().zip(rhs.0.iter()) {
            *a ^= b;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeFingerprint<K> {
    #[serde(bound(
        serialize = "Range<K>: Serialize",
        deserialize = "Range<K>: Deserialize<'de>"
    ))]
    pub range: Range<K>,
    /// The fingerprint of `range`.
    pub fingerprint: Fingerprint,
}

/// Transfers items inside a range to the other participant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RangeItem<E: RangeEntry> {
    /// The range out of which the elements are.
    #[serde(bound(
        serialize = "Range<E::Key>: Serialize",
        deserialize = "Range<E::Key>: Deserialize<'de>"
    ))]
    pub range: Range<E::Key>,
    #[serde(bound(serialize = "E: Serialize", deserialize = "E: Deserialize<'de>"))]
    pub values: Vec<(E, ContentStatus)>,
    /// If false, requests to send local items in the range.
    /// Otherwise not.
    pub have_local: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MessagePart<E: RangeEntry> {
    #[serde(bound(
        serialize = "RangeFingerprint<E::Key>: Serialize",
        deserialize = "RangeFingerprint<E::Key>: Deserialize<'de>"
    ))]
    RangeFingerprint(RangeFingerprint<E::Key>),
    #[serde(bound(
        serialize = "RangeItem<E>: Serialize",
        deserialize = "RangeItem<E>: Deserialize<'de>"
    ))]
    RangeItem(RangeItem<E>),
}

impl<E: RangeEntry> MessagePart<E> {
    pub fn is_range_fingerprint(&self) -> bool {
        matches!(self, MessagePart::RangeFingerprint(_))
    }

    pub fn is_range_item(&self) -> bool {
        matches!(self, MessagePart::RangeItem(_))
    }

    pub fn values(&self) -> Option<&[(E, ContentStatus)]> {
        match self {
            MessagePart::RangeFingerprint(_) => None,
            MessagePart::RangeItem(RangeItem { values, .. }) => Some(values),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message<E: RangeEntry> {
    #[serde(bound(
        serialize = "MessagePart<E>: Serialize",
        deserialize = "MessagePart<E>: Deserialize<'de>"
    ))]
    parts: Vec<MessagePart<E>>,
}

impl<E: RangeEntry> Message<E> {
    /// Construct the initial message.
    fn init<S: Store<E>>(store: &mut S) -> Result<Self, S::Error> {
        let x = store.get_first()?;
        let range = Range::new(x.clone(), x);
        let fingerprint = store.get_fingerprint(&range)?;
        let part = MessagePart::RangeFingerprint(RangeFingerprint { range, fingerprint });
        Ok(Message { parts: vec![part] })
    }

    pub fn parts(&self) -> &[MessagePart<E>] {
        &self.parts
    }

    pub fn values(&self) -> impl Iterator<Item = &(E, ContentStatus)> {
        self.parts().iter().filter_map(|p| p.values()).flatten()
    }

    pub fn value_count(&self) -> usize {
        self.values().count()
    }
}

pub trait Store<E: RangeEntry>: Sized {
    type Error: Debug + Send + Sync + Into<anyhow::Error> + 'static;

    type RangeIterator<'a>: Iterator<Item = Result<E, Self::Error>>
    where
        Self: 'a,
        E: 'a;

    type ParentIterator<'a>: Iterator<Item = Result<E, Self::Error>>
    where
        Self: 'a,
        E: 'a;

    /// Get a the first key (or the default if none is available).
    fn get_first(&mut self) -> Result<E::Key, Self::Error>;

    /// Get a single entry.
    fn get(&mut self, key: &E::Key) -> Result<Option<E>, Self::Error>;

    /// Get the number of entries in the store.
    fn len(&mut self) -> Result<usize, Self::Error>;

    /// Returns `true` if the vector contains no elements.
    fn is_empty(&mut self) -> Result<bool, Self::Error>;

    /// Calculate the fingerprint of the given range.
    fn get_fingerprint(&mut self, range: &Range<E::Key>) -> Result<Fingerprint, Self::Error>;

    /// Insert just the given key value pair.
    ///
    /// This will replace just the existing entry, but will not perform prefix
    /// deletion.
    fn entry_put(&mut self, entry: E) -> Result<(), Self::Error>;

    /// Returns all entries in the given range.
    fn get_range(&mut self, range: Range<E::Key>) -> Result<Self::RangeIterator<'_>, Self::Error>;

    /// Returns the number of entries in the range.
    ///
    /// Default impl is not optimized, but does avoid excessive memory usage.
    fn get_range_len(&mut self, range: Range<E::Key>) -> Result<usize, Self::Error> {
        let mut count = 0;
        for el in self.get_range(range)? {
            let _el = el?;
            count += 1;
        }
        Ok(count)
    }

    /// Returns all entries whose key starts with the given `prefix`.
    fn prefixed_by(&mut self, prefix: &E::Key) -> Result<Self::RangeIterator<'_>, Self::Error>;

    /// Returns all entries that share a prefix with `key`, including the entry for `key` itself.
    fn prefixes_of(&mut self, key: &E::Key) -> Result<Self::ParentIterator<'_>, Self::Error>;

    /// Get all entries in the store
    fn all(&mut self) -> Result<Self::RangeIterator<'_>, Self::Error>;

    /// Remove an entry from the store.
    ///
    /// This will remove just the entry with the given key, but will not perform prefix deletion.
    fn entry_remove(&mut self, key: &E::Key) -> Result<Option<E>, Self::Error>;

    /// Remove all entries whose key start with a prefix and for which the `predicate` callback
    /// returns true.
    ///
    /// Returns the number of elements removed.
    // TODO: We might want to return an iterator with the removed elements instead to emit as
    // events to the application potentially.
    fn remove_prefix_filtered(
        &mut self,
        prefix: &E::Key,
        predicate: impl Fn(&E::Value) -> bool,
    ) -> Result<usize, Self::Error>;

    /// Generates the initial message.
    fn initial_message(&mut self) -> Result<Message<E>, Self::Error> {
        Message::init(self)
    }

    /// Processes an incoming message and produces a response.
    /// If terminated, returns `None`
    ///
    /// `validate_cb` is called for each incoming entry received from the remote.
    /// It must return true if the entry is valid and should be stored, and false otherwise
    /// (which means the entry will be dropped and not stored).
    ///
    /// `on_insert_cb` is called for each entry that was actually inserted into the store (so not
    /// for entries which validated, but are not inserted because they are older than one of their
    /// prefixes).
    ///
    /// `content_status_cb` is called for each outgoing entry about to be sent to the remote.
    /// It must return a [`ContentStatus`], which will be sent to the remote with the entry.
    async fn process_message<F, F2, F3>(
        &mut self,
        config: &SyncConfig,
        message: Message<E>,
        validate_cb: F,
        mut on_insert_cb: F2,
        content_status_cb: F3,
    ) -> Result<Option<Message<E>>, Self::Error>
    where
        F: Fn(&Self, &E, ContentStatus) -> bool,
        F2: FnMut(&Self, E, ContentStatus),
        F3: for<'a> Fn(
            &'a E,
        )
            -> Pin<Box<dyn std::future::Future<Output = ContentStatus> + Send + 'a>>,
    {
        let mut out = Vec::new();

        // TODO: can these allocs be avoided?
        let mut items = Vec::new();
        let mut fingerprints = Vec::new();
        for part in message.parts {
            match part {
                MessagePart::RangeItem(item) => {
                    items.push(item);
                }
                MessagePart::RangeFingerprint(fp) => {
                    fingerprints.push(fp);
                }
            }
        }

        // Process item messages
        for RangeItem {
            range,
            values,
            have_local,
        } in items
        {
            let diff: Option<Vec<_>> = if have_local {
                None
            } else {
                Some({
                    // we get the range of the item form our store. from this set, we remove all
                    // entries that whose key is contained in the peer's set and where our value is
                    // lower than the peer entry's value.
                    let items = self
                        .get_range(range.clone())?
                        .filter_map(|our_entry| match our_entry {
                            Ok(our_entry) => {
                                if !values.iter().any(|(their_entry, _)| {
                                    our_entry.key() == their_entry.key()
                                        && their_entry.value() >= our_entry.value()
                                }) {
                                    Some(Ok(our_entry))
                                } else {
                                    None
                                }
                            }
                            Err(err) => Some(Err(err)),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    // add the content status in a second pass
                    let values = items.into_iter().map(|entry| async {
                        let content_status = content_status_cb(&entry).await;
                        (entry, content_status)
                    });
                    n0_future::FuturesOrdered::from_iter(values).collect().await
                })
            };

            // Store incoming values
            for (entry, content_status) in values {
                if validate_cb(self, &entry, content_status) {
                    // TODO: Get rid of the clone?
                    let outcome = self.put(entry.clone())?;
                    if let InsertOutcome::Inserted { .. } = outcome {
                        on_insert_cb(self, entry, content_status);
                    }
                }
            }

            if let Some(diff) = diff {
                if !diff.is_empty() {
                    out.push(MessagePart::RangeItem(RangeItem {
                        range,
                        values: diff,
                        have_local: true,
                    }));
                }
            }
        }

        // Process fingerprint messages
        for RangeFingerprint { range, fingerprint } in fingerprints {
            let local_fingerprint = self.get_fingerprint(&range)?;
            // Case1 Match, nothing to do
            if local_fingerprint == fingerprint {
                continue;
            }

            // Case2 Recursion Anchor
            let num_local_values = self.get_range_len(range.clone())?;
            if num_local_values <= 1 || fingerprint == Fingerprint::empty() {
                let values = self
                    .get_range(range.clone())?
                    .collect::<Result<Vec<_>, _>>()?;
                let values = values.into_iter().map(|entry| async {
                    let content_status = content_status_cb(&entry).await;
                    (entry, content_status)
                });
                let values = n0_future::FuturesOrdered::from_iter(values).collect().await;
                out.push(MessagePart::RangeItem(RangeItem {
                    range,
                    values,
                    have_local: false,
                }));
            } else {
                // Case3 Recurse
                // Create partition
                // m0 = x < m1 < .. < mk = y, with k>= 2
                // such that [ml, ml+1) is nonempty
                let mut ranges = Vec::with_capacity(config.split_factor);

                // Select the first index, for which the key is larger or equal than the x of the range.
                let mut start_index = 0;
                for el in self.get_range(range.clone())? {
                    let el = el?;
                    if el.key() >= range.x() {
                        break;
                    }
                    start_index += 1;
                }

                // select a pivot value. pivots repeat every split_factor, so pivot(i) == pivot(i + self.split_factor * x)
                // it is guaranteed that pivot(0) != x if local_values.len() >= 2
                let mut pivot = |i: usize| {
                    // ensure that pivots wrap around
                    let i = i % config.split_factor;
                    // choose an offset. this will be
                    // 1/2, 1 in case of split_factor == 2
                    // 1/3, 2/3, 1 in case of split_factor == 3
                    // etc.
                    let offset = (num_local_values * (i + 1)) / config.split_factor;
                    let offset = (start_index + offset) % num_local_values;
                    self.get_range(range.clone())
                        .map(|mut i| i.nth(offset))
                        .and_then(|e| e.expect("missing entry"))
                        .map(|e| e.key().clone())
                };
                if range.is_all() {
                    // the range is the whole set, so range.x and range.y should not matter
                    // just add all ranges as normal ranges. Exactly one of the ranges will
                    // wrap around, so we cover the entire set.
                    for i in 0..config.split_factor {
                        let (x, y) = (pivot(i)?, pivot(i + 1)?);
                        // don't push empty ranges
                        if x != y {
                            ranges.push(Range { x, y })
                        }
                    }
                } else {
                    // guaranteed to be non-empty because
                    // - pivot(0) is guaranteed to be != x for local_values.len() >= 2
                    // - local_values.len() < 2 gets handled by the recursion anchor
                    // - x != y (regular range)
                    ranges.push(Range {
                        x: range.x().clone(),
                        y: pivot(0)?,
                    });
                    // this will only be executed for split_factor > 2
                    for i in 0..config.split_factor - 2 {
                        // don't push empty ranges
                        let (x, y) = (pivot(i)?, pivot(i + 1)?);
                        if x != y {
                            ranges.push(Range { x, y })
                        }
                    }
                    // guaranteed to be non-empty because
                    // - pivot is a value in the range
                    // - y is the exclusive end of the range
                    // - x != y (regular range)
                    ranges.push(Range {
                        x: pivot(config.split_factor - 2)?,
                        y: range.y().clone(),
                    });
                }

                let mut non_empty = 0;
                for range in ranges {
                    let chunk: Vec<_> = self.get_range(range.clone())?.collect();
                    if !chunk.is_empty() {
                        non_empty += 1;
                    }
                    // Add either the fingerprint or the item set
                    let fingerprint = self.get_fingerprint(&range)?;
                    if chunk.len() > config.max_set_size {
                        out.push(MessagePart::RangeFingerprint(RangeFingerprint {
                            range: range.clone(),
                            fingerprint,
                        }));
                    } else {
                        // let content_status_cb = content_status_cb.clone();
                        let values = chunk.into_iter().filter_map(|entry| match entry {
                            Err(_err) => None,
                            Ok(entry) => Some(async {
                                let content_status = content_status_cb(&entry).await;
                                (entry, content_status)
                            }),
                        });
                        let values = n0_future::FuturesOrdered::from_iter(values).collect().await;
                        out.push(MessagePart::RangeItem(RangeItem {
                            range,
                            values,
                            have_local: false,
                        }));
                    }
                }
                debug_assert!(non_empty > 1);
            }
        }

        // If we have any parts, return a message
        if !out.is_empty() {
            Ok(Some(Message { parts: out }))
        } else {
            Ok(None)
        }
    }

    /// Insert a key value pair.
    ///
    /// Entries are inserted if they compare strictly greater than all entries in the set of
    /// entries which have the same key as `entry` or have a key which is a prefix of `entry`.
    ///
    /// Additionally, entries that have a key which is a prefix of the entry's key and whose
    /// timestamp is not strictly greater than that of the new entry are deleted
    ///
    /// Note: The deleted entries are simply dropped right now. We might want to make this return
    /// an iterator, to potentially log or expose the deleted entries.
    ///
    /// Returns `true` if the entry was inserted.
    /// Returns `false` if it was not inserted.
    fn put(&mut self, entry: E) -> Result<InsertOutcome, Self::Error> {
        let prefix_entry = self.prefixes_of(entry.key())?;
        // First we check if our entry is strictly greater than all parent elements.
        // From the willow spec:
        // "Remove all entries whose timestamp is strictly less than the timestamp of any other entry [..]
        // whose path is a prefix of p." and then "remove all but those whose record has the greatest hash component".
        // This is the contract of the `Ord` impl for `E::Value`.
        for prefix_entry in prefix_entry {
            let prefix_entry = prefix_entry?;
            if entry.value() <= prefix_entry.value() {
                return Ok(InsertOutcome::NotInserted);
            }
        }

        // Now we remove all entries that have our key as a prefix and are older than our entry.
        let removed = self.remove_prefix_filtered(entry.key(), |value| entry.value() >= value)?;

        // Insert our new entry.
        self.entry_put(entry)?;
        Ok(InsertOutcome::Inserted { removed })
    }
}

impl<E: RangeEntry, S: Store<E>> Store<E> for &mut S {
    type Error = S::Error;

    type RangeIterator<'a>
        = S::RangeIterator<'a>
    where
        Self: 'a,
        E: 'a;

    type ParentIterator<'a>
        = S::ParentIterator<'a>
    where
        Self: 'a,
        E: 'a;

    fn get_first(&mut self) -> Result<<E as RangeEntry>::Key, Self::Error> {
        (**self).get_first()
    }

    fn get(&mut self, key: &<E as RangeEntry>::Key) -> Result<Option<E>, Self::Error> {
        (**self).get(key)
    }

    fn len(&mut self) -> Result<usize, Self::Error> {
        (**self).len()
    }

    fn is_empty(&mut self) -> Result<bool, Self::Error> {
        (**self).is_empty()
    }

    fn get_fingerprint(
        &mut self,
        range: &Range<<E as RangeEntry>::Key>,
    ) -> Result<Fingerprint, Self::Error> {
        (**self).get_fingerprint(range)
    }

    fn entry_put(&mut self, entry: E) -> Result<(), Self::Error> {
        (**self).entry_put(entry)
    }

    fn get_range(
        &mut self,
        range: Range<<E as RangeEntry>::Key>,
    ) -> Result<Self::RangeIterator<'_>, Self::Error> {
        (**self).get_range(range)
    }

    fn prefixed_by(
        &mut self,
        prefix: &<E as RangeEntry>::Key,
    ) -> Result<Self::RangeIterator<'_>, Self::Error> {
        (**self).prefixed_by(prefix)
    }

    fn prefixes_of(
        &mut self,
        key: &<E as RangeEntry>::Key,
    ) -> Result<Self::ParentIterator<'_>, Self::Error> {
        (**self).prefixes_of(key)
    }

    fn all(&mut self) -> Result<Self::RangeIterator<'_>, Self::Error> {
        (**self).all()
    }

    fn entry_remove(&mut self, key: &<E as RangeEntry>::Key) -> Result<Option<E>, Self::Error> {
        (**self).entry_remove(key)
    }

    fn remove_prefix_filtered(
        &mut self,
        prefix: &<E as RangeEntry>::Key,
        predicate: impl Fn(&<E as RangeEntry>::Value) -> bool,
    ) -> Result<usize, Self::Error> {
        (**self).remove_prefix_filtered(prefix, predicate)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SyncConfig {
    /// Up to how many values to send immediately, before sending only a fingerprint.
    max_set_size: usize,
    /// `k` in the protocol, how many splits to generate. at least 2
    split_factor: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        SyncConfig {
            max_set_size: 1,
            split_factor: 2,
        }
    }
}

/// The outcome of a [`Store::put`] operation.
#[derive(Debug)]
pub enum InsertOutcome {
    /// The entry was not inserted because a newer entry for its key or a
    /// prefix of its key exists.
    NotInserted,
    /// The entry was inserted.
    Inserted {
        /// Number of entries that were removed as a consequence of this insert operation.
        /// The removed entries had a key that starts with the new entry's key and a lower value.
        removed: usize,
    },
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::BTreeMap, convert::Infallible, fmt::Debug, rc::Rc};

    use proptest::prelude::*;
    use test_strategy::proptest;

    use super::*;

    #[derive(Debug)]
    struct SimpleStore<K, V> {
        data: BTreeMap<K, V>,
    }

    impl<K, V> Default for SimpleStore<K, V> {
        fn default() -> Self {
            SimpleStore {
                data: BTreeMap::default(),
            }
        }
    }

    impl<K, V> RangeEntry for (K, V)
    where
        K: RangeKey,
        V: RangeValue,
    {
        type Key = K;
        type Value = V;

        fn key(&self) -> &Self::Key {
            &self.0
        }

        fn value(&self) -> &Self::Value {
            &self.1
        }

        fn as_fingerprint(&self) -> Fingerprint {
            let mut hasher = blake3::Hasher::new();
            hasher.update(format!("{:?}", self.0).as_bytes());
            hasher.update(format!("{:?}", self.1).as_bytes());
            Fingerprint(hasher.finalize().into())
        }
    }

    impl RangeKey for &'static str {
        fn is_prefix_of(&self, other: &Self) -> bool {
            other.starts_with(self)
        }
    }
    impl RangeKey for String {
        fn is_prefix_of(&self, other: &Self) -> bool {
            other.starts_with(self)
        }
    }

    impl RangeValue for &'static [u8] {}
    impl RangeValue for i32 {}
    impl RangeValue for u8 {}
    impl RangeValue for () {}

    impl<K, V> Store<(K, V)> for SimpleStore<K, V>
    where
        K: RangeKey + Default,
        V: RangeValue,
    {
        type Error = Infallible;
        type ParentIterator<'a> = std::vec::IntoIter<Result<(K, V), Infallible>>;

        fn get_first(&mut self) -> Result<K, Self::Error> {
            if let Some((k, _)) = self.data.first_key_value() {
                Ok(k.clone())
            } else {
                Ok(Default::default())
            }
        }

        fn get(&mut self, key: &K) -> Result<Option<(K, V)>, Self::Error> {
            Ok(self.data.get(key).cloned().map(|v| (key.clone(), v)))
        }

        fn len(&mut self) -> Result<usize, Self::Error> {
            Ok(self.data.len())
        }

        fn is_empty(&mut self) -> Result<bool, Self::Error> {
            Ok(self.data.is_empty())
        }

        /// Calculate the fingerprint of the given range.
        fn get_fingerprint(&mut self, range: &Range<K>) -> Result<Fingerprint, Self::Error> {
            let elements = self.get_range(range.clone())?;
            let mut fp = Fingerprint::empty();
            for el in elements {
                let el = el?;
                fp ^= el.as_fingerprint();
            }

            Ok(fp)
        }

        /// Insert the given key value pair.
        fn entry_put(&mut self, e: (K, V)) -> Result<(), Self::Error> {
            self.data.insert(e.0, e.1);
            Ok(())
        }

        type RangeIterator<'a>
            = SimpleRangeIterator<'a, K, V>
        where
            K: 'a,
            V: 'a;
        /// Returns all items in the given range
        fn get_range(&mut self, range: Range<K>) -> Result<Self::RangeIterator<'_>, Self::Error> {
            // TODO: this is not very efficient, optimize depending on data structure
            let iter = self.data.iter();

            Ok(SimpleRangeIterator {
                iter,
                filter: SimpleFilter::Range(range),
            })
        }

        fn entry_remove(&mut self, key: &K) -> Result<Option<(K, V)>, Self::Error> {
            let res = self.data.remove(key).map(|v| (key.clone(), v));
            Ok(res)
        }

        fn all(&mut self) -> Result<Self::RangeIterator<'_>, Self::Error> {
            let iter = self.data.iter();
            Ok(SimpleRangeIterator {
                iter,
                filter: SimpleFilter::None,
            })
        }

        // TODO: Not horrible.
        fn prefixes_of(&mut self, key: &K) -> Result<Self::ParentIterator<'_>, Self::Error> {
            let mut res = vec![];
            for (k, v) in self.data.iter() {
                if k.is_prefix_of(key) {
                    res.push(Ok((k.clone(), v.clone())));
                }
            }
            Ok(res.into_iter())
        }

        fn prefixed_by(&mut self, prefix: &K) -> Result<Self::RangeIterator<'_>, Self::Error> {
            let iter = self.data.iter();
            Ok(SimpleRangeIterator {
                iter,
                filter: SimpleFilter::Prefix(prefix.clone()),
            })
        }

        fn remove_prefix_filtered(
            &mut self,
            prefix: &K,
            predicate: impl Fn(&V) -> bool,
        ) -> Result<usize, Self::Error> {
            let old_len = self.data.len();
            self.data.retain(|k, v| {
                let remove = prefix.is_prefix_of(k) && predicate(v);
                !remove
            });
            Ok(old_len - self.data.len())
        }
    }

    #[derive(Debug)]
    pub struct SimpleRangeIterator<'a, K, V> {
        iter: std::collections::btree_map::Iter<'a, K, V>,
        filter: SimpleFilter<K>,
    }

    #[derive(Debug)]
    enum SimpleFilter<K> {
        None,
        Range(Range<K>),
        Prefix(K),
    }

    impl<K, V> Iterator for SimpleRangeIterator<'_, K, V>
    where
        K: RangeKey + Default,
        V: Clone,
    {
        type Item = Result<(K, V), Infallible>;

        fn next(&mut self) -> Option<Self::Item> {
            let mut next = self.iter.next()?;

            let filter = |x: &K| match &self.filter {
                SimpleFilter::None => true,
                SimpleFilter::Range(range) => range.contains(x),
                SimpleFilter::Prefix(prefix) => prefix.is_prefix_of(x),
            };

            loop {
                if filter(next.0) {
                    return Some(Ok((next.0.clone(), next.1.clone())));
                }

                next = self.iter.next()?;
            }
        }
    }

    #[tokio::test]
    async fn test_paper_1() {
        let alice_set = [("ape", 1), ("eel", 1), ("fox", 1), ("gnu", 1)];
        let bob_set = [
            ("bee", 1),
            ("cat", 1),
            ("doe", 1),
            ("eel", 1),
            ("fox", 1),
            ("hog", 1),
        ];

        let res = sync(&alice_set, &bob_set).await;
        res.print_messages();
        assert_eq!(res.alice_to_bob.len(), 3, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 2, "B -> A message count");

        // Initial message
        assert_eq!(res.alice_to_bob[0].parts.len(), 1);
        assert!(res.alice_to_bob[0].parts[0].is_range_fingerprint());

        // Response from Bob - recurse once
        assert_eq!(res.bob_to_alice[0].parts.len(), 2);
        assert!(res.bob_to_alice[0].parts[0].is_range_fingerprint());
        assert!(res.bob_to_alice[0].parts[1].is_range_fingerprint());
        // Last response from Alice
        assert_eq!(res.alice_to_bob[1].parts.len(), 3);
        assert!(res.alice_to_bob[1].parts[0].is_range_fingerprint());
        assert!(res.alice_to_bob[1].parts[1].is_range_fingerprint());
        assert!(res.alice_to_bob[1].parts[2].is_range_item());

        // Last response from Bob
        assert_eq!(res.bob_to_alice[1].parts.len(), 2);
        assert!(res.bob_to_alice[1].parts[0].is_range_item());
        assert!(res.bob_to_alice[1].parts[1].is_range_item());
    }

    #[tokio::test]
    async fn test_paper_2() {
        let alice_set = [
            ("ape", 1),
            ("bee", 1),
            ("cat", 1),
            ("doe", 1),
            ("eel", 1),
            ("fox", 1), // the only value being sent
            ("gnu", 1),
            ("hog", 1),
        ];
        let bob_set = [
            ("ape", 1),
            ("bee", 1),
            ("cat", 1),
            ("doe", 1),
            ("eel", 1),
            ("gnu", 1),
            ("hog", 1),
        ];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 3, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 2, "B -> A message count");
    }

    #[tokio::test]
    async fn test_paper_3() {
        let alice_set = [
            ("ape", 1),
            ("bee", 1),
            ("cat", 1),
            ("doe", 1),
            ("eel", 1),
            ("fox", 1),
            ("gnu", 1),
            ("hog", 1),
        ];
        let bob_set = [("ape", 1), ("cat", 1), ("eel", 1), ("gnu", 1)];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 3, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 2, "B -> A message count");
    }

    #[tokio::test]
    async fn test_limits() {
        let alice_set = [("ape", 1), ("bee", 1), ("cat", 1)];
        let bob_set = [("ape", 1), ("cat", 1), ("doe", 1)];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 2, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 2, "B -> A message count");
    }

    #[tokio::test]
    async fn test_prefixes_simple() {
        let alice_set = [("/foo/bar", 1), ("/foo/baz", 1), ("/foo/cat", 1)];
        let bob_set = [("/foo/bar", 1), ("/alice/bar", 1), ("/alice/baz", 1)];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 2, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 2, "B -> A message count");
    }

    #[tokio::test]
    async fn test_prefixes_empty_alice() {
        let alice_set = [];
        let bob_set = [("/foo/bar", 1), ("/alice/bar", 1), ("/alice/baz", 1)];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 1, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 1, "B -> A message count");
    }

    #[tokio::test]
    async fn test_prefixes_empty_bob() {
        let alice_set = [("/foo/bar", 1), ("/foo/baz", 1), ("/foo/cat", 1)];
        let bob_set = [];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 2, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 1, "B -> A message count");
    }

    #[tokio::test]
    async fn test_equal_key_higher_value() {
        let alice_set = [("foo", 2)];
        let bob_set = [("foo", 1)];

        let res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 2, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 1, "B -> A message count");
    }

    #[tokio::test]
    async fn test_multikey() {
        /// Uses the blanket impl of [`RangeKey]` for `T: AsRef<[u8]>` in this module.
        #[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
        struct Multikey {
            author: [u8; 4],
            key: Vec<u8>,
        }

        impl RangeKey for Multikey {
            fn is_prefix_of(&self, other: &Self) -> bool {
                self.author == other.author && self.key.starts_with(&other.key)
            }
        }

        impl Debug for Multikey {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let key = if let Ok(key) = std::str::from_utf8(&self.key) {
                    key.to_string()
                } else {
                    hex::encode(&self.key)
                };
                f.debug_struct("Multikey")
                    .field("author", &hex::encode(self.author))
                    .field("key", &key)
                    .finish()
            }
        }

        impl Multikey {
            fn new(author: [u8; 4], key: impl AsRef<[u8]>) -> Self {
                Multikey {
                    author,
                    key: key.as_ref().to_vec(),
                }
            }
        }
        let author_a = [1u8; 4];
        let author_b = [2u8; 4];
        let alice_set = [
            (Multikey::new(author_a, "ape"), 1),
            (Multikey::new(author_a, "bee"), 1),
            (Multikey::new(author_b, "bee"), 1),
            (Multikey::new(author_a, "doe"), 1),
        ];
        let bob_set = [
            (Multikey::new(author_a, "ape"), 1),
            (Multikey::new(author_a, "bee"), 1),
            (Multikey::new(author_a, "cat"), 1),
            (Multikey::new(author_b, "cat"), 1),
        ];

        // No limit
        let mut res = sync(&alice_set, &bob_set).await;
        assert_eq!(res.alice_to_bob.len(), 2, "A -> B message count");
        assert_eq!(res.bob_to_alice.len(), 2, "B -> A message count");
        res.assert_alice_set(
            "no limit",
            &[
                (Multikey::new(author_a, "ape"), 1),
                (Multikey::new(author_a, "bee"), 1),
                (Multikey::new(author_b, "bee"), 1),
                (Multikey::new(author_a, "doe"), 1),
                (Multikey::new(author_a, "cat"), 1),
                (Multikey::new(author_b, "cat"), 1),
            ],
        );

        res.assert_bob_set(
            "no limit",
            &[
                (Multikey::new(author_a, "ape"), 1),
                (Multikey::new(author_a, "bee"), 1),
                (Multikey::new(author_b, "bee"), 1),
                (Multikey::new(author_a, "doe"), 1),
                (Multikey::new(author_a, "cat"), 1),
                (Multikey::new(author_b, "cat"), 1),
            ],
        );
    }

    // This tests two things:
    // 1) validate cb returning false leads to no changes on both sides after sync
    // 2) validate cb receives expected entries
    #[tokio::test]
    async fn test_validate_cb() {
        let alice_set = [("alice1", 1), ("alice2", 2)];
        let bob_set = [("bob1", 3), ("bob2", 4), ("bob3", 5)];
        let alice_validate_set = Rc::new(RefCell::new(vec![]));
        let bob_validate_set = Rc::new(RefCell::new(vec![]));

        let validate_alice: ValidateCb<&str, i32> = Box::new({
            let alice_validate_set = alice_validate_set.clone();
            move |_, e, _| {
                alice_validate_set.borrow_mut().push(*e);
                false
            }
        });
        let validate_bob: ValidateCb<&str, i32> = Box::new({
            let bob_validate_set = bob_validate_set.clone();
            move |_, e, _| {
                bob_validate_set.borrow_mut().push(*e);
                false
            }
        });

        let mut alice = SimpleStore::default();
        for (k, v) in alice_set {
            alice.put((k, v)).unwrap();
        }

        let mut bob = SimpleStore::default();
        for (k, v) in bob_set {
            bob.put((k, v)).unwrap();
        }

        // run sync with a validate callback returning false, so no new entries are stored on either side
        let mut res = sync_exchange_messages(alice, bob, &validate_alice, &validate_bob, 100).await;
        res.assert_alice_set("unchanged", &alice_set);
        res.assert_bob_set("unchanged", &bob_set);

        // assert that the validate callbacks received all expected entries
        assert_eq!(alice_validate_set.take(), bob_set);
        assert_eq!(bob_validate_set.take(), alice_set);
    }

    struct SyncResult<K, V>
    where
        K: RangeKey + Default,
        V: RangeValue,
    {
        alice: SimpleStore<K, V>,
        bob: SimpleStore<K, V>,
        alice_to_bob: Vec<Message<(K, V)>>,
        bob_to_alice: Vec<Message<(K, V)>>,
    }

    impl<K, V> SyncResult<K, V>
    where
        K: RangeKey + Default,
        V: RangeValue,
    {
        fn print_messages(&self) {
            let len = std::cmp::max(self.alice_to_bob.len(), self.bob_to_alice.len());
            for i in 0..len {
                if let Some(msg) = self.alice_to_bob.get(i) {
                    println!("A -> B:");
                    print_message(msg);
                }
                if let Some(msg) = self.bob_to_alice.get(i) {
                    println!("B -> A:");
                    print_message(msg);
                }
            }
        }

        fn assert_alice_set(&mut self, ctx: &str, expected: &[(K, V)]) {
            dbg!(self.alice.all().unwrap().collect::<Vec<_>>());
            for e in expected {
                assert_eq!(
                    self.alice.get(e.key()).unwrap().as_ref(),
                    Some(e),
                    "{}: (alice) missing key {:?}",
                    ctx,
                    e.key()
                );
            }
            assert_eq!(expected.len(), self.alice.len().unwrap(), "{ctx}: (alice)");
        }

        fn assert_bob_set(&mut self, ctx: &str, expected: &[(K, V)]) {
            dbg!(self.bob.all().unwrap().collect::<Vec<_>>());

            for e in expected {
                assert_eq!(
                    self.bob.get(e.key()).unwrap().as_ref(),
                    Some(e),
                    "{ctx}: (bob) missing key {e:?}"
                );
            }
            assert_eq!(expected.len(), self.bob.len().unwrap(), "{ctx}: (bob)");
        }
    }

    fn print_message<E: RangeEntry>(msg: &Message<E>) {
        for part in &msg.parts {
            match part {
                MessagePart::RangeFingerprint(RangeFingerprint { range, fingerprint }) => {
                    println!(
                        "  RangeFingerprint({:?}, {:?}, {:?})",
                        range.x(),
                        range.y(),
                        fingerprint
                    );
                }
                MessagePart::RangeItem(RangeItem {
                    range,
                    values,
                    have_local,
                }) => {
                    println!(
                        "  RangeItem({:?} | {:?}) (local?: {})\n  {:?}",
                        range.x(),
                        range.y(),
                        have_local,
                        values,
                    );
                }
            }
        }
    }

    type ValidateCb<K, V> = Box<dyn Fn(&SimpleStore<K, V>, &(K, V), ContentStatus) -> bool>;

    async fn sync<K, V>(alice_set: &[(K, V)], bob_set: &[(K, V)]) -> SyncResult<K, V>
    where
        K: RangeKey + Default,
        V: RangeValue,
    {
        let alice_validate_cb: ValidateCb<K, V> = Box::new(|_, _, _| true);
        let bob_validate_cb: ValidateCb<K, V> = Box::new(|_, _, _| true);
        sync_with_validate_cb_and_assert(alice_set, bob_set, &alice_validate_cb, &bob_validate_cb)
            .await
    }

    fn insert_if_larger<K: RangeKey, V: RangeValue>(map: &mut BTreeMap<K, V>, key: K, value: V) {
        let mut insert = true;
        for (k, v) in map.iter() {
            if k.is_prefix_of(&key) && v >= &value {
                insert = false;
            }
        }
        if insert {
            #[allow(clippy::needless_bool)]
            map.retain(|k, v| {
                if key.is_prefix_of(k) && value >= *v {
                    false
                } else {
                    true
                }
            });
            map.insert(key, value);
        }
    }

    async fn sync_with_validate_cb_and_assert<K, V, F1, F2>(
        alice_set: &[(K, V)],
        bob_set: &[(K, V)],
        alice_validate_cb: F1,
        bob_validate_cb: F2,
    ) -> SyncResult<K, V>
    where
        K: RangeKey + Default,
        V: RangeValue,
        F1: Fn(&SimpleStore<K, V>, &(K, V), ContentStatus) -> bool,
        F2: Fn(&SimpleStore<K, V>, &(K, V), ContentStatus) -> bool,
    {
        let mut alice = SimpleStore::<K, V>::default();
        let mut bob = SimpleStore::<K, V>::default();

        let expected_set = {
            let mut expected_set = BTreeMap::new();
            let mut alice_expected = BTreeMap::new();
            for e in alice_set {
                alice.put(e.clone()).unwrap();
                insert_if_larger(&mut expected_set, e.0.clone(), e.1.clone());
                insert_if_larger(&mut alice_expected, e.0.clone(), e.1.clone());
            }
            let alice_expected = alice_expected.into_iter().collect::<Vec<_>>();
            let alice_now: Vec<_> = alice.all().unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(
                alice_expected, alice_now,
                "alice initial set does not match"
            );

            let mut bob_expected = BTreeMap::new();
            for e in bob_set {
                bob.put(e.clone()).unwrap();
                insert_if_larger(&mut expected_set, e.0.clone(), e.1.clone());
                insert_if_larger(&mut bob_expected, e.0.clone(), e.1.clone());
            }
            let bob_expected = bob_expected.into_iter().collect::<Vec<_>>();
            let bob_now: Vec<_> = bob.all().unwrap().collect::<Result<_, _>>().unwrap();
            assert_eq!(bob_expected, bob_now, "bob initial set does not match");

            expected_set.into_iter().collect::<Vec<_>>()
        };

        let mut res =
            sync_exchange_messages(alice, bob, alice_validate_cb, bob_validate_cb, 100).await;

        let alice_now: Vec<_> = res.alice.all().unwrap().collect::<Result<_, _>>().unwrap();
        if alice_now != expected_set {
            res.print_messages();
            println!("alice_init: {alice_set:?}");
            println!("bob_init:   {bob_set:?}");
            println!("expected:   {expected_set:?}");
            println!("alice_now:  {alice_now:?}");
            panic!("alice_now does not match expected");
        }

        let bob_now: Vec<_> = res.bob.all().unwrap().collect::<Result<_, _>>().unwrap();
        if bob_now != expected_set {
            res.print_messages();
            println!("alice_init: {alice_set:?}");
            println!("bob_init:   {bob_set:?}");
            println!("expected:   {expected_set:?}");
            println!("bob_now:    {bob_now:?}");
            panic!("bob_now does not match expected");
        }

        // Check that values were never sent twice
        let mut alice_sent = BTreeMap::new();
        for msg in &res.alice_to_bob {
            for part in &msg.parts {
                if let Some(values) = part.values() {
                    for (e, _) in values {
                        assert!(
                            alice_sent.insert(e.key(), e).is_none(),
                            "alice: duplicate {e:?}"
                        );
                    }
                }
            }
        }

        let mut bob_sent = BTreeMap::new();
        for msg in &res.bob_to_alice {
            for part in &msg.parts {
                if let Some(values) = part.values() {
                    for (e, _) in values {
                        assert!(
                            bob_sent.insert(e.key(), e).is_none(),
                            "bob: duplicate {e:?}"
                        );
                    }
                }
            }
        }

        res
    }

    async fn sync_exchange_messages<K, V, F1, F2>(
        mut alice: SimpleStore<K, V>,
        mut bob: SimpleStore<K, V>,
        alice_validate_cb: F1,
        bob_validate_cb: F2,
        max_rounds: usize,
    ) -> SyncResult<K, V>
    where
        K: RangeKey + Default,
        V: RangeValue,
        F1: Fn(&SimpleStore<K, V>, &(K, V), ContentStatus) -> bool,
        F2: Fn(&SimpleStore<K, V>, &(K, V), ContentStatus) -> bool,
    {
        let mut alice_to_bob = Vec::new();
        let mut bob_to_alice = Vec::new();
        let initial_message = alice.initial_message().unwrap();

        let mut next_to_bob = Some(initial_message);
        let mut rounds = 0;
        while let Some(msg) = next_to_bob.take() {
            assert!(rounds < max_rounds, "too many rounds");
            rounds += 1;
            alice_to_bob.push(msg.clone());

            if let Some(msg) = bob
                .process_message(
                    &Default::default(),
                    msg,
                    &bob_validate_cb,
                    |_, _, _| (),
                    |_| Box::pin(async move { ContentStatus::Complete }),
                )
                .await
                .unwrap()
            {
                bob_to_alice.push(msg.clone());
                next_to_bob = alice
                    .process_message(
                        &Default::default(),
                        msg,
                        &alice_validate_cb,
                        |_, _, _| (),
                        |_| Box::pin(async move { ContentStatus::Complete }),
                    )
                    .await
                    .unwrap();
            }
        }
        SyncResult {
            alice,
            bob,
            alice_to_bob,
            bob_to_alice,
        }
    }

    #[test]
    fn store_get_range() {
        let mut store = SimpleStore::<&'static str, i32>::default();
        let set = [
            ("bee", 1),
            ("cat", 1),
            ("doe", 1),
            ("eel", 1),
            ("fox", 1),
            ("hog", 1),
        ];
        for (k, v) in &set {
            store.entry_put((*k, *v)).unwrap();
        }

        let all: Vec<_> = store
            .get_range(Range::new("", ""))
            .unwrap()
            .collect::<Result<_, Infallible>>()
            .unwrap();
        assert_eq!(&all, &set[..]);

        let regular: Vec<_> = store
            .get_range(("bee", "eel").into())
            .unwrap()
            .collect::<Result<_, Infallible>>()
            .unwrap();
        assert_eq!(&regular, &set[..3]);

        // empty start
        let regular: Vec<_> = store
            .get_range(("", "eel").into())
            .unwrap()
            .collect::<Result<_, Infallible>>()
            .unwrap();
        assert_eq!(&regular, &set[..3]);

        let regular: Vec<_> = store
            .get_range(("cat", "hog").into())
            .unwrap()
            .collect::<Result<_, Infallible>>()
            .unwrap();

        assert_eq!(&regular, &set[1..5]);

        let excluded: Vec<_> = store
            .get_range(("fox", "bee").into())
            .unwrap()
            .collect::<Result<_, Infallible>>()
            .unwrap();

        assert_eq!(excluded[0].0, "fox");
        assert_eq!(excluded[1].0, "hog");
        assert_eq!(excluded.len(), 2);

        let excluded: Vec<_> = store
            .get_range(("fox", "doe").into())
            .unwrap()
            .collect::<Result<_, Infallible>>()
            .unwrap();

        assert_eq!(excluded.len(), 4);
        assert_eq!(excluded[0].0, "bee");
        assert_eq!(excluded[1].0, "cat");
        assert_eq!(excluded[2].0, "fox");
        assert_eq!(excluded[3].0, "hog");
    }

    type TestSetStringUnit = BTreeMap<String, ()>;
    type TestSetStringU8 = BTreeMap<String, u8>;

    fn test_key() -> impl Strategy<Value = String> {
        "[a-z0-9]{0,5}"
    }

    fn test_set_string_unit() -> impl Strategy<Value = TestSetStringUnit> {
        prop::collection::btree_map(test_key(), Just(()), 0..10)
    }

    fn test_set_string_u8() -> impl Strategy<Value = TestSetStringU8> {
        prop::collection::btree_map(test_key(), test_value_u8(), 0..10)
    }

    fn test_value_u8() -> impl Strategy<Value = u8> {
        0u8..u8::MAX
    }

    fn test_vec_string_unit() -> impl Strategy<Value = Vec<(String, ())>> {
        test_set_string_unit().prop_map(|m| m.into_iter().collect::<Vec<_>>())
    }
    fn test_vec_string_u8() -> impl Strategy<Value = Vec<(String, u8)>> {
        test_set_string_u8().prop_map(|m| m.into_iter().collect::<Vec<_>>())
    }

    fn test_range() -> impl Strategy<Value = Range<String>> {
        // ranges with x > y are explicitly allowed - they wrap around
        (test_key(), test_key()).prop_map(|(x, y)| Range::new(x, y))
    }

    fn mk_test_set(values: impl IntoIterator<Item = impl AsRef<str>>) -> TestSetStringUnit {
        values
            .into_iter()
            .map(|v| v.as_ref().to_string())
            .map(|k| (k, ()))
            .collect()
    }

    fn mk_test_vec(values: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<(String, ())> {
        mk_test_set(values).into_iter().collect()
    }

    #[tokio::test]
    async fn simple_store_sync_1() {
        let alice = mk_test_vec(["3"]);
        let bob = mk_test_vec(["2", "3", "4", "5", "6", "7", "8"]);
        let _res = sync(&alice, &bob).await;
    }

    #[tokio::test]
    async fn simple_store_sync_x() {
        let alice = mk_test_vec(["1", "3"]);
        let bob = mk_test_vec(["2"]);
        let _res = sync(&alice, &bob).await;
    }

    #[tokio::test]
    async fn simple_store_sync_2() {
        let alice = mk_test_vec(["1", "3"]);
        let bob = mk_test_vec(["0", "2", "3"]);
        let _res = sync(&alice, &bob).await;
    }

    #[tokio::test]
    async fn simple_store_sync_3() {
        let alice = mk_test_vec(["8", "9"]);
        let bob = mk_test_vec(["1", "2", "3"]);
        let _res = sync(&alice, &bob).await;
    }

    #[proptest]
    fn simple_store_sync(
        #[strategy(test_vec_string_unit())] alice: Vec<(String, ())>,
        #[strategy(test_vec_string_unit())] bob: Vec<(String, ())>,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let _res = sync(&alice, &bob).await;
        });
    }

    #[proptest]
    fn simple_store_sync_u8(
        #[strategy(test_vec_string_u8())] alice: Vec<(String, u8)>,
        #[strategy(test_vec_string_u8())] bob: Vec<(String, u8)>,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let _res = sync(&alice, &bob).await;
        });
    }

    /// A generic fn to make a test for the get_range fn of a store.
    #[allow(clippy::type_complexity)]
    fn store_get_ranges_test<S, E>(
        elems: impl IntoIterator<Item = E>,
        range: Range<E::Key>,
    ) -> (Vec<E>, Vec<E>)
    where
        S: Store<E> + Default,
        E: RangeEntry,
    {
        let mut store = S::default();
        let elems = elems.into_iter().collect::<Vec<_>>();
        for e in elems.iter().cloned() {
            store.entry_put(e).unwrap();
        }
        let mut actual = store
            .get_range(range.clone())
            .unwrap()
            .collect::<std::result::Result<Vec<_>, S::Error>>()
            .unwrap();
        let mut expected = elems
            .into_iter()
            .filter(|e| range.contains(e.key()))
            .collect::<Vec<_>>();

        actual.sort_by(|a, b| a.key().cmp(b.key()));
        expected.sort_by(|a, b| a.key().cmp(b.key()));
        (expected, actual)
    }

    #[proptest]
    fn simple_store_get_ranges(
        #[strategy(test_set_string_unit())] contents: BTreeMap<String, ()>,
        #[strategy(test_range())] range: Range<String>,
    ) {
        let (expected, actual) = store_get_ranges_test::<SimpleStore<_, _>, _>(contents, range);
        prop_assert_eq!(expected, actual);
    }
}
