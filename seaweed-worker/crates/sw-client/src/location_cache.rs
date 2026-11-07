//! Volume id -> volume server locations, cached for reads.
//!
//! Mirrors the Go client's vid map (`weed/wdclient/vid_map.go`): one flat map
//! whose entries remember the generation they were learned in. [`reset`] starts
//! a new generation when the metadata source has to be relearned, entries stay
//! readable while that happens, and any not relearned within
//! `retain_generations` resets are dropped. Nothing is chained, so there is no
//! history to walk and none to sever underneath a reader.
//!
//! [`reset`]: LocationCache::reset

use std::collections::HashMap;
use std::sync::Mutex;

/// How many resets a location survives without being relearned. Matches the Go
/// client's `DefaultVidMapCacheSize`.
pub const DEFAULT_RETAIN_GENERATIONS: u64 = 5;

#[derive(Debug)]
struct Entry {
    urls: Vec<String>,
    generation: u64,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<u32, Entry>,
    generation: u64,
}

#[derive(Debug)]
pub struct LocationCache {
    inner: Mutex<Inner>,
    retain_generations: u64,
}

impl Default for LocationCache {
    fn default() -> Self {
        Self::new()
    }
}

impl LocationCache {
    pub fn new() -> Self {
        Self::with_retain_generations(DEFAULT_RETAIN_GENERATIONS)
    }

    /// Keep locations for `retain_generations` resets after they were last
    /// learned. Zero is raised to one: an entry always survives the reset that
    /// immediately follows it, which is what keeps in-flight reads resolving.
    pub fn with_retain_generations(retain_generations: u64) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            retain_generations: retain_generations.max(1),
        }
    }

    /// Locations for `vid`, or `None` when it is unknown or has none. There is
    /// nothing older to fall back to: a volume's history lives in its own entry.
    pub fn locations(&self, vid: u32) -> Option<Vec<String>> {
        let inner = self.inner.lock().unwrap();
        inner.entries.get(&vid).map(|entry| entry.urls.clone())
    }

    /// Record what the lookup returned for `vid`, replacing whatever an earlier
    /// generation held. A volume that moved must not keep answering with the
    /// server it moved off, and one with no locations left is dropped rather
    /// than kept as an empty entry.
    pub fn learn(&self, vid: u32, urls: Vec<String>) {
        let mut inner = self.inner.lock().unwrap();
        if urls.is_empty() {
            inner.entries.remove(&vid);
            return;
        }
        let generation = inner.generation;
        inner.entries.insert(vid, Entry { urls, generation });
    }

    /// Forget `vid` so the next read looks it up again.
    pub fn invalidate(&self, vid: u32) {
        self.inner.lock().unwrap().entries.remove(&vid);
    }

    /// Forget `vid` only while it still holds exactly `urls`, and report
    /// whether it did. The counterpart of a read that failed against the
    /// servers we had cached: a slow read failing against servers that a newer
    /// lookup has already replaced must not throw the fresh entry away.
    pub fn invalidate_stale(&self, vid: u32, urls: &[String]) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let stale = inner
            .entries
            .get(&vid)
            .is_some_and(|entry| entry.urls == urls);
        if stale {
            inner.entries.remove(&vid);
        }
        stale
    }

    /// Start a new generation, as when the metadata subscription reconnects and
    /// what we were told may no longer hold. Entries stay readable until they
    /// are relearned or fall out of the retained window.
    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.generation += 1;
        if inner.generation <= self.retain_generations {
            return;
        }
        let oldest = inner.generation - self.retain_generations;
        inner.entries.retain(|_, entry| entry.generation >= oldest);
    }

    /// Number of volumes currently held, retained generations included.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn urls(urls: &[&str]) -> Vec<String> {
        urls.iter().map(|u| u.to_string()).collect()
    }

    #[test]
    fn unknown_volume_is_a_miss() {
        let cache = LocationCache::new();
        assert_eq!(cache.locations(1), None);
    }

    #[test]
    fn learned_locations_are_served() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["h1:8080", "h2:8080"]));
        assert_eq!(cache.locations(1), Some(urls(&["h1:8080", "h2:8080"])));
    }

    #[test]
    fn empty_learn_is_a_definitive_miss() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["h1:8080"]));
        cache.learn(1, Vec::new());
        assert_eq!(
            cache.locations(1),
            None,
            "a volume known to have no locations must not serve what it had before"
        );
        assert!(cache.is_empty(), "an emptied volume is dropped, not kept");
    }

    // The Go side's addLocation rule: the first write of a generation replaces
    // the entry rather than merging into it.
    #[test]
    fn learn_replaces_earlier_generation() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["moved-from:8080"]));
        cache.reset();
        cache.learn(1, urls(&["moved-to:8080"]));
        assert_eq!(cache.locations(1), Some(urls(&["moved-to:8080"])));
    }

    #[test]
    fn locations_survive_a_reset() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["h1:8080"]));
        cache.reset();
        assert_eq!(
            cache.locations(1),
            Some(urls(&["h1:8080"])),
            "a reset must not strand reads that are already in flight"
        );
    }

    #[test]
    fn entries_expire_after_the_retained_window() {
        let retain = 2;
        let cache = LocationCache::with_retain_generations(retain);
        cache.learn(1, urls(&["h1:8080"]));

        for i in 0..retain {
            cache.reset();
            assert!(
                cache.locations(1).is_some(),
                "location should still be retained after {} reset(s)",
                i + 1
            );
        }

        cache.reset();
        assert_eq!(cache.locations(1), None);
        assert!(
            cache.is_empty(),
            "expired entries must be dropped, not kept"
        );
    }

    #[test]
    fn relearning_restarts_the_window() {
        let cache = LocationCache::with_retain_generations(2);
        for _ in 0..5 {
            cache.learn(1, urls(&["h1:8080"]));
            cache.reset();
        }
        assert!(cache.locations(1).is_some());
    }

    #[test]
    fn zero_retention_still_survives_one_reset() {
        let cache = LocationCache::with_retain_generations(0);
        cache.learn(1, urls(&["h1:8080"]));
        cache.reset();
        assert!(cache.locations(1).is_some());
        cache.reset();
        assert_eq!(cache.locations(1), None);
    }

    #[test]
    fn invalidate_drops_a_retained_entry() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["h1:8080"]));
        cache.reset();
        cache.invalidate(1);
        assert_eq!(cache.locations(1), None);
        assert_eq!(cache.locations(2), None, "unrelated volumes are untouched");
    }

    #[test]
    fn invalidate_stale_only_drops_what_the_caller_used() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["stale:8080"]));

        assert!(cache.invalidate_stale(1, &urls(&["stale:8080"])));
        assert_eq!(cache.locations(1), None);

        // A newer lookup has already replaced the entry the failed read used.
        cache.learn(1, urls(&["fresh:8080"]));
        assert!(!cache.invalidate_stale(1, &urls(&["stale:8080"])));
        assert_eq!(
            cache.locations(1),
            Some(urls(&["fresh:8080"])),
            "a read failing against replaced servers must not drop the fresh entry"
        );
    }

    #[test]
    fn invalidate_stale_on_an_unknown_volume_is_a_no_op() {
        let cache = LocationCache::new();
        assert!(!cache.invalidate_stale(1, &urls(&["h1:8080"])));
    }

    #[test]
    fn invalidate_only_touches_its_own_volume() {
        let cache = LocationCache::new();
        cache.learn(1, urls(&["h1:8080"]));
        cache.learn(2, urls(&["h2:8080"]));
        cache.invalidate(1);
        assert_eq!(cache.locations(2), Some(urls(&["h2:8080"])));
        assert_eq!(cache.len(), 1);
    }

    // A volume relearned every generation must always resolve, however many
    // resets run concurrently with the readers.
    #[test]
    fn concurrent_resets_never_strand_a_live_volume() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let cache = Arc::new(LocationCache::with_retain_generations(2));
        cache.learn(1, urls(&["h1:8080"]));
        let stop = Arc::new(AtomicBool::new(false));

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        assert!(
                            cache.locations(1).is_some(),
                            "live volume went missing during a reset"
                        );
                        std::thread::yield_now();
                    }
                })
            })
            .collect();

        for _ in 0..2_000 {
            cache.reset();
            cache.learn(1, urls(&["h1:8080"]));
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().unwrap();
        }
    }
}
