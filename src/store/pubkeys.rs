use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use ed25519_dalek::{SignatureError, VerifyingKey};
use lru::LruCache;

use crate::{AuthorId, AuthorPublicKey, NamespaceId, NamespacePublicKey};

/// Store trait for expanded public keys for authors and namespaces.
///
/// Used to cache [`ed25519_dalek::VerifyingKey`].
///
/// This trait is implemented for the unit type `()`, where no caching is used.
pub trait PublicKeyStore {
    /// Convert a byte array into a  [`VerifyingKey`].
    ///
    /// New keys are inserted into the [`PublicKeyStore ] and reused on subsequent calls.
    fn public_key(&self, id: &[u8; 32]) -> Result<VerifyingKey, SignatureError>;

    /// Convert a [`NamespaceId`] into a [`NamespacePublicKey`].
    ///
    /// New keys are inserted into the [`PublicKeyStore ] and reused on subsequent calls.
    fn namespace_key(&self, bytes: &NamespaceId) -> Result<NamespacePublicKey, SignatureError> {
        self.public_key(bytes.as_bytes()).map(Into::into)
    }

    /// Convert a [`AuthorId`] into a [`AuthorPublicKey`].
    ///
    /// New keys are inserted into the [`PublicKeyStore ] and reused on subsequent calls.
    fn author_key(&self, bytes: &AuthorId) -> Result<AuthorPublicKey, SignatureError> {
        self.public_key(bytes.as_bytes()).map(Into::into)
    }
}

impl<T: PublicKeyStore> PublicKeyStore for &T {
    fn public_key(&self, id: &[u8; 32]) -> Result<VerifyingKey, SignatureError> {
        (*self).public_key(id)
    }
}

impl<T: PublicKeyStore> PublicKeyStore for &mut T {
    fn public_key(&self, id: &[u8; 32]) -> Result<VerifyingKey, SignatureError> {
        PublicKeyStore::public_key(*self, id)
    }
}

impl PublicKeyStore for () {
    fn public_key(&self, id: &[u8; 32]) -> Result<VerifyingKey, SignatureError> {
        VerifyingKey::from_bytes(id)
    }
}

/// Maximum number of public keys cached in [`MemPublicKeyStore`].
///
/// The cache is populated by [`PublicKeyStore::public_key`], which is called
/// during signature verification of entries received from sync peers (see
/// `validate_entry` in `sync.rs`). Without a bound, a peer that sends entries
/// signed by many distinct authors causes unbounded memory growth — one cache
/// entry per distinct 32-byte public key, forever.
///
/// When the cache is full, the least-recently-used entry is evicted.
const MAX_CACHED_KEYS: usize = 10_000;

/// In-memory key storage with a bounded LRU cache.
#[derive(Debug, Clone)]
pub struct MemPublicKeyStore {
    keys: Arc<Mutex<LruCache<[u8; 32], VerifyingKey>>>,
}

impl Default for MemPublicKeyStore {
    fn default() -> Self {
        let cap = NonZeroUsize::new(MAX_CACHED_KEYS).expect("MAX_CACHED_KEYS is non-zero");
        Self {
            keys: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }
}

impl PublicKeyStore for MemPublicKeyStore {
    fn public_key(&self, bytes: &[u8; 32]) -> Result<VerifyingKey, SignatureError> {
        let mut guard = self.keys.lock().expect("MemPublicKeyStore mutex poisoned");
        if let Some(vk) = guard.get(bytes) {
            return Ok(*vk);
        }
        let vk = VerifyingKey::from_bytes(bytes)?;
        guard.put(*bytes, vk);
        Ok(vk)
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;

    /// Regression test for unbounded cache growth.
    ///
    /// Before the [`MAX_CACHED_KEYS`] bound was added, a peer that sent entries
    /// signed by many distinct authors could grow [`MemPublicKeyStore`]'s
    /// internal map without limit via the
    /// `insert_remote_entry → validate_entry → entry.verify → public_key` call
    /// chain. This test inserts more distinct keys than the bound and asserts
    /// that the cache size stays bounded.
    #[test]
    fn cache_is_bounded_under_unique_key_flood() {
        let store = MemPublicKeyStore::default();
        let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);

        let n = MAX_CACHED_KEYS + MAX_CACHED_KEYS / 2;
        for _ in 0..n {
            let sk = SigningKey::generate(&mut rng);
            let bytes = sk.verifying_key().to_bytes();
            store.public_key(&bytes).expect("valid key");
        }

        let cache_size = store.keys.lock().unwrap().len();
        assert!(
            cache_size <= MAX_CACHED_KEYS,
            "cache should be bounded by MAX_CACHED_KEYS ({MAX_CACHED_KEYS}) but contains {cache_size} entries after {n} distinct keys",
        );
    }

    /// Recently-used keys survive eviction when the cache overflows.
    #[test]
    fn recently_used_keys_survive_eviction() {
        let store = MemPublicKeyStore::default();
        let mut rng = StdRng::seed_from_u64(0xBEEF_CAFE);

        // Fill the cache exactly to capacity.
        let keys: Vec<[u8; 32]> = (0..MAX_CACHED_KEYS)
            .map(|_| SigningKey::generate(&mut rng).verifying_key().to_bytes())
            .collect();
        for k in &keys {
            store.public_key(k).expect("valid key");
        }

        // Touch the first inserted key to promote it to most-recently-used.
        store.public_key(&keys[0]).expect("valid key");

        // Insert 100 more distinct keys, forcing 100 LRU evictions.
        for _ in 0..100 {
            let bytes = SigningKey::generate(&mut rng).verifying_key().to_bytes();
            store.public_key(&bytes).expect("valid key");
        }

        let guard = store.keys.lock().unwrap();
        assert!(
            guard.contains(&keys[0]),
            "touched key should survive LRU eviction"
        );
        assert!(
            !guard.contains(&keys[1]),
            "oldest untouched key should have been evicted"
        );
    }
}
