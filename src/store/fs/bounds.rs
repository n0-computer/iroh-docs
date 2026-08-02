use std::ops::{Bound, RangeBounds};

use bytes::Bytes;

use super::tables::{RecordsByKeyId, RecordsByKeyIdOwned, RecordsId, RecordsIdOwned};
use crate::{store::KeyFilter, AuthorId, NamespaceId};

/// Bounds on the records table.
///
/// Supports bounds by author, key
pub struct RecordsBounds(Bound<RecordsIdOwned>, Bound<RecordsIdOwned>);

impl RecordsBounds {
    pub fn new(start: Bound<RecordsIdOwned>, end: Bound<RecordsIdOwned>) -> Self {
        Self(start, end)
    }

    pub fn author_key(ns: NamespaceId, author: AuthorId, key_matcher: KeyFilter) -> Self {
        let key_is_exact = matches!(key_matcher, KeyFilter::Exact(_));
        let key = match key_matcher {
            KeyFilter::Any => Bytes::new(),
            KeyFilter::Exact(key) => key,
            KeyFilter::Prefix(prefix) => prefix,
        };
        let author = author.to_bytes();
        let ns = ns.to_bytes();
        let mut author_end = author;
        let mut ns_end = ns;
        let mut key_end = key.to_vec();

        let start = (ns, author, key);

        let end = if key_is_exact {
            Bound::Included(start.clone())
        } else if increment_by_one(&mut key_end) {
            Bound::Excluded((ns, author, key_end.into()))
        } else if increment_by_one(&mut author_end) {
            Bound::Excluded((ns, author_end, Bytes::new()))
        } else if increment_by_one(&mut ns_end) {
            Bound::Excluded((ns_end, [0u8; 32], Bytes::new()))
        } else {
            Bound::Unbounded
        };

        Self(Bound::Included(start), end)
    }

    pub fn author_prefix(ns: NamespaceId, author: AuthorId, prefix: Bytes) -> Self {
        RecordsBounds::author_key(ns, author, KeyFilter::Prefix(prefix))
    }

    pub fn namespace(ns: NamespaceId) -> Self {
        Self::new(Self::namespace_start(&ns), Self::namespace_end(&ns))
    }

    pub fn from_start(ns: &NamespaceId, end: Bound<RecordsIdOwned>) -> Self {
        Self::new(Self::namespace_start(ns), end)
    }

    pub fn to_end(ns: &NamespaceId, start: Bound<RecordsIdOwned>) -> Self {
        Self::new(start, Self::namespace_end(ns))
    }

    /// Intersect these bounds with `ns`.
    ///
    /// Every namespace shares the same records table, so the bounds are the only thing
    /// keeping documents apart. Sync range endpoints arrive from the remote peer
    /// unvalidated and may name any namespace, so they have to be intersected with the
    /// namespace the session is pinned to: a range reaching into another document then
    /// selects nothing instead of reading it.
    pub fn clamp_to_namespace(self, ns: &NamespaceId) -> Self {
        let Self(start, end) = self;
        // Both `namespace_start` and every caller's start are `Included`; the tighter of
        // two lower bounds is the greater one. Unexpected shapes fall back to the
        // namespace bound, which is never wider than what was asked for.
        let start = match (start, Self::namespace_start(ns)) {
            (Bound::Included(remote), Bound::Included(ns_start)) => {
                Bound::Included(remote.max(ns_start))
            }
            (_, ns_start) => ns_start,
        };
        // `namespace_end` is `Excluded`, or `Unbounded` for the last namespace, in which
        // case the remote's own end is already within it.
        let end = match (end, Self::namespace_end(ns)) {
            (Bound::Excluded(remote), Bound::Excluded(ns_end)) => {
                Bound::Excluded(remote.min(ns_end))
            }
            (Bound::Excluded(remote), Bound::Unbounded) => Bound::Excluded(remote),
            (_, ns_end) => ns_end,
        };
        // The intersection is empty whenever the range covers only foreign namespaces.
        // Normalize that to a range selecting nothing, as inverted bounds are not a
        // valid query.
        if let (Bound::Included(s), Bound::Excluded(e)) = (&start, &end) {
            if s >= e {
                let empty = (ns.to_bytes(), [0u8; 32], Bytes::new());
                return Self(Bound::Included(empty.clone()), Bound::Excluded(empty));
            }
        }
        Self(start, end)
    }

    pub fn as_ref(&self) -> (Bound<RecordsId<'_>>, Bound<RecordsId<'_>>) {
        fn map(id: &RecordsIdOwned) -> RecordsId<'_> {
            (&id.0, &id.1, &id.2[..])
        }
        (map_bound(&self.0, map), map_bound(&self.1, map))
    }

    fn namespace_start(namespace: &NamespaceId) -> Bound<RecordsIdOwned> {
        Bound::Included((namespace.to_bytes(), [0u8; 32], Bytes::new()))
    }

    fn namespace_end(namespace: &NamespaceId) -> Bound<RecordsIdOwned> {
        let mut ns_end = namespace.to_bytes();
        if increment_by_one(&mut ns_end) {
            Bound::Excluded((ns_end, [0u8; 32], Bytes::new()))
        } else {
            Bound::Unbounded
        }
    }
}

impl RangeBounds<RecordsIdOwned> for RecordsBounds {
    fn start_bound(&self) -> Bound<&RecordsIdOwned> {
        map_bound(&self.0, |s| s)
    }

    fn end_bound(&self) -> Bound<&RecordsIdOwned> {
        map_bound(&self.1, |s| s)
    }
}

impl From<(Bound<RecordsIdOwned>, Bound<RecordsIdOwned>)> for RecordsBounds {
    fn from(value: (Bound<RecordsIdOwned>, Bound<RecordsIdOwned>)) -> Self {
        Self::new(value.0, value.1)
    }
}

/// Bounds for the by-key index table.
///
/// Supports bounds by key.
pub struct ByKeyBounds(Bound<RecordsByKeyIdOwned>, Bound<RecordsByKeyIdOwned>);
impl ByKeyBounds {
    pub fn new(ns: NamespaceId, matcher: &KeyFilter) -> Self {
        match matcher {
            KeyFilter::Any => Self::namespace(ns),
            KeyFilter::Exact(key) => {
                let start = (ns.to_bytes(), key.clone(), [0u8; 32]);
                let end = (ns.to_bytes(), key.clone(), [255u8; 32]);
                Self(Bound::Included(start), Bound::Included(end))
            }
            KeyFilter::Prefix(ref prefix) => {
                let start = Bound::Included((ns.to_bytes(), prefix.clone(), [0u8; 32]));

                let mut ns_end = ns.to_bytes();
                let mut key_end = prefix.to_vec();
                let end = if increment_by_one(&mut key_end) {
                    Bound::Excluded((ns.to_bytes(), key_end.into(), [0u8; 32]))
                } else if increment_by_one(&mut ns_end) {
                    Bound::Excluded((ns_end, Bytes::new(), [0u8; 32]))
                } else {
                    Bound::Unbounded
                };
                Self(start, end)
            }
        }
    }

    pub fn namespace(ns: NamespaceId) -> Self {
        let start = Bound::Included((ns.to_bytes(), Bytes::new(), [0u8; 32]));
        let mut ns_end = ns.to_bytes();
        let end = if increment_by_one(&mut ns_end) {
            Bound::Excluded((ns_end, Bytes::new(), [0u8; 32]))
        } else {
            Bound::Unbounded
        };
        Self(start, end)
    }

    pub fn as_ref(&self) -> (Bound<RecordsByKeyId<'_>>, Bound<RecordsByKeyId<'_>>) {
        fn map(id: &RecordsByKeyIdOwned) -> RecordsByKeyId<'_> {
            (&id.0, &id.1[..], &id.2)
        }
        (map_bound(&self.0, map), map_bound(&self.1, map))
    }
}

impl RangeBounds<RecordsByKeyIdOwned> for ByKeyBounds {
    fn start_bound(&self) -> Bound<&RecordsByKeyIdOwned> {
        map_bound(&self.0, |s| s)
    }

    fn end_bound(&self) -> Bound<&RecordsByKeyIdOwned> {
        map_bound(&self.1, |s| s)
    }
}

/// Increment a byte string by one, by incrementing the last byte that is not 255 by one.
///
/// Returns false if all bytes are 255.
fn increment_by_one(value: &mut [u8]) -> bool {
    for char in value.iter_mut().rev() {
        if *char != 255 {
            *char += 1;
            return true;
        } else {
            *char = 0;
        }
    }
    false
}

fn map_bound<'a, T, U: 'a>(bound: &'a Bound<T>, f: impl Fn(&'a T) -> U) -> Bound<U> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(t) => Bound::Included(f(t)),
        Bound::Excluded(t) => Bound::Excluded(f(t)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_bounds() {
        let ns = NamespaceId::from(&[255u8; 32]);

        let bounds = RecordsBounds::namespace(ns);
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), [0u8; 32], Bytes::new()))
        );
        assert_eq!(bounds.end_bound(), Bound::Unbounded);

        let a = AuthorId::from(&[255u8; 32]);

        let bounds = RecordsBounds::author_key(ns, a, KeyFilter::Any);
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), a.to_bytes(), Bytes::new()))
        );
        assert_eq!(bounds.end_bound(), Bound::Unbounded);

        let a = AuthorId::from(&[0u8; 32]);
        let mut a_end = a.to_bytes();
        a_end[31] = 1;
        let bounds = RecordsBounds::author_key(ns, a, KeyFilter::Any);
        assert_eq!(
            bounds.end_bound(),
            Bound::Excluded(&(ns.to_bytes(), a_end, Default::default()))
        );

        let bounds = RecordsBounds::author_key(ns, a, KeyFilter::Prefix(vec![1u8].into()));
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), a.to_bytes(), vec![1u8].into()))
        );
        assert_eq!(
            bounds.end_bound(),
            Bound::Excluded(&(ns.to_bytes(), a.to_bytes(), vec![2u8].into()))
        );

        let bounds = RecordsBounds::author_key(ns, a, KeyFilter::Exact(vec![1u8].into()));
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), a.to_bytes(), vec![1u8].into()))
        );
        assert_eq!(
            bounds.end_bound(),
            Bound::Included(&(ns.to_bytes(), a.to_bytes(), vec![1u8].into()))
        );
    }

    #[test]
    fn by_key_bounds() {
        let ns = NamespaceId::from(&[255u8; 32]);

        let bounds = ByKeyBounds::namespace(ns);
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), Bytes::new(), [0u8; 32]))
        );
        assert_eq!(bounds.end_bound(), Bound::Unbounded);

        let bounds = ByKeyBounds::new(ns, &KeyFilter::Any);
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), Bytes::new(), [0u8; 32]))
        );
        assert_eq!(bounds.end_bound(), Bound::Unbounded);

        let bounds = ByKeyBounds::new(ns, &KeyFilter::Prefix(vec![1u8].into()));
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), vec![1u8].into(), [0u8; 32]))
        );
        assert_eq!(
            bounds.end_bound(),
            Bound::Excluded(&(ns.to_bytes(), vec![2u8].into(), [0u8; 32]))
        );

        let bounds = ByKeyBounds::new(ns, &KeyFilter::Prefix(vec![255u8].into()));
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), vec![255u8].into(), [0u8; 32]))
        );
        assert_eq!(bounds.end_bound(), Bound::Unbounded);

        let ns = NamespaceId::from(&[2u8; 32]);
        let mut ns_end = ns.to_bytes();
        ns_end[31] = 3u8;
        let bounds = ByKeyBounds::new(ns, &KeyFilter::Prefix(vec![255u8].into()));
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), vec![255u8].into(), [0u8; 32]))
        );
        assert_eq!(
            bounds.end_bound(),
            Bound::Excluded(&(ns_end, Bytes::new(), [0u8; 32]))
        );

        let bounds = ByKeyBounds::new(ns, &KeyFilter::Exact(vec![1u8].into()));
        assert_eq!(
            bounds.start_bound(),
            Bound::Included(&(ns.to_bytes(), vec![1u8].into(), [0u8; 32]))
        );
        assert_eq!(
            bounds.end_bound(),
            Bound::Included(&(ns.to_bytes(), vec![1u8].into(), [255u8; 32]))
        );
    }
}
