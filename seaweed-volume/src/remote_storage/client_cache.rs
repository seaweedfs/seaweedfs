//! One remote storage client per configuration. Mirrors Go's
//! weed/remote_storage/client_cache.go.
//!
//! A client built per request keeps its connector's idle connections for the
//! pool's idle timeout and never reuses them, so a steady stream of cold reads
//! (fetch_and_write_needle) holds one open connection per request. Clients are
//! cached instead, keyed by the configuration's protobuf encoding (RemoteConf
//! holds no maps, so the encoding is deterministic), in an LRU of
//! WEED_REMOTE_CLIENT_CACHE_MAX entries (default 64). An evicted entry drops
//! the client's Arc; dropping its last Arc drops the connector and its idle
//! connections. The Go cache's retain hook and credentials version do not
//! apply here: the volume server never loads /etc/remote, and Rust
//! credentials come only from the configuration itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use prost::Message;

use crate::pb::remote_pb::RemoteConf;

use super::{RemoteStorageClient, RemoteStorageError, make_remote_storage_client};

struct CachedClient {
    client: Arc<dyn RemoteStorageClient>,
    last_used: u64,
}

struct CacheInner {
    max: usize,
    ticks: u64,
    entries: HashMap<Vec<u8>, CachedClient>,
}

impl CacheInner {
    fn new(max: usize) -> Self {
        CacheInner {
            max,
            ticks: 0,
            entries: HashMap::new(),
        }
    }

    fn get(
        &mut self,
        conf: &RemoteConf,
    ) -> Result<Arc<dyn RemoteStorageClient>, RemoteStorageError> {
        self.ticks += 1;
        let tick = self.ticks;
        let key = conf.encode_to_vec();
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.last_used = tick;
            return Ok(entry.client.clone());
        }
        // The build is a pure allocation (the connector dials lazily), so it
        // runs under the lock; the lock itself serializes concurrent builds of
        // one key.
        let client: Arc<dyn RemoteStorageClient> = Arc::from(make_remote_storage_client(conf)?);
        self.entries.insert(
            key,
            CachedClient {
                client: client.clone(),
                last_used: tick,
            },
        );
        while self.entries.len() > self.max {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        Ok(client)
    }
}

fn lock_inner(cache: &Mutex<CacheInner>) -> MutexGuard<'_, CacheInner> {
    cache.lock().unwrap_or_else(|e| e.into_inner())
}

static CACHE: OnceLock<Mutex<CacheInner>> = OnceLock::new();

fn client_cache_max() -> usize {
    match std::env::var("WEED_REMOTE_CLIENT_CACHE_MAX") {
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                tracing::warn!(
                    "WEED_REMOTE_CLIENT_CACHE_MAX={:?} is not a positive number; using 64",
                    v
                );
                64
            }
        },
        Err(_) => 64,
    }
}

/// Return the cached client of a configuration, building and caching it on a
/// miss. Mirrors Go's remote_storage.GetRemoteStorage.
pub fn get_remote_storage_client(
    conf: &RemoteConf,
) -> Result<Arc<dyn RemoteStorageClient>, RemoteStorageError> {
    lock_inner(CACHE.get_or_init(|| Mutex::new(CacheInner::new(client_cache_max())))).get(conf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_conf(name: &str, secret: &str) -> RemoteConf {
        RemoteConf {
            r#type: "s3".to_string(),
            name: name.to_string(),
            s3_endpoint: "https://s3.example".to_string(),
            s3_access_key: "k".to_string(),
            s3_secret_key: secret.to_string(),
            s3_region: "us-east-1".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn same_configuration_one_client() {
        let mut cache = CacheInner::new(64);
        let first = cache.get(&s3_conf("a", "x")).unwrap();
        for _ in 0..5 {
            let again = cache.get(&s3_conf("a", "x")).unwrap();
            assert!(Arc::ptr_eq(&first, &again));
        }
    }

    #[test]
    fn changed_configuration_new_client() {
        let mut cache = CacheInner::new(64);
        let first = cache.get(&s3_conf("a", "x")).unwrap();
        let next = cache.get(&s3_conf("a", "y")).unwrap();
        assert!(!Arc::ptr_eq(&first, &next));
    }

    #[test]
    fn lru_bound_evicts_least_used() {
        let mut cache = CacheInner::new(2);
        let a = cache.get(&s3_conf("a", "x")).unwrap();
        let b = cache.get(&s3_conf("b", "x")).unwrap();
        // touch a so b is the least recently used
        cache.get(&s3_conf("a", "x")).unwrap();
        cache.get(&s3_conf("c", "x")).unwrap();
        assert_eq!(cache.entries.len(), 2);
        let a2 = cache.get(&s3_conf("a", "x")).unwrap();
        assert!(Arc::ptr_eq(&a, &a2));
        let b2 = cache.get(&s3_conf("b", "x")).unwrap();
        assert!(!Arc::ptr_eq(&b, &b2));
    }
}
