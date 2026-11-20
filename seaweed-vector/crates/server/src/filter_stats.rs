//! How often, and how slowly, each index's queries filter on each metadata
//! key over the last hour. Keys come from the parsed filter; the number of
//! indexes and of keys per index is bounded, so arbitrary keys cannot grow
//! memory without limit. Not exported as metrics: a key is no label.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use weed_vector_core::indexed_filter::operand;

use crate::pb::vector as pb;

const BUCKETS: usize = 12;
const BUCKET: Duration = Duration::from_secs(300);
pub const WINDOW: Duration = Duration::from_secs(BUCKET.as_secs() * BUCKETS as u64);
pub const MAX_INDEXES: usize = 512;
pub const MAX_KEYS_PER_INDEX: usize = 32;

#[derive(Default, Clone, Copy)]
struct Bucket {
    /// The bucket's number since the stats started.
    at: u64,
    queries: u64,
    seconds: f64,
}

#[derive(Default)]
struct KeyStats {
    kinds: u8,
    buckets: [Bucket; BUCKETS],
}

#[derive(Default)]
struct IndexStats {
    keys: HashMap<String, KeyStats>,
    truncated: bool,
    last: u64,
}

pub struct FilterStats {
    start: Instant,
    indexes: Mutex<HashMap<String, IndexStats>>,
}

impl Default for FilterStats {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterStats {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            indexes: Mutex::new(HashMap::new()),
        }
    }

    fn bucket(&self, now: Instant) -> u64 {
        (now.duration_since(self.start).as_secs() / BUCKET.as_secs()) + 1
    }

    /// One query of `index` that took `elapsed` and filtered on `keys`.
    pub fn record(&self, index: &str, keys: &BTreeMap<String, u8>, elapsed: Duration) {
        self.record_at(index, keys, elapsed, Instant::now());
    }

    fn record_at(&self, index: &str, keys: &BTreeMap<String, u8>, elapsed: Duration, now: Instant) {
        if keys.is_empty() {
            return;
        }
        let at = self.bucket(now);
        let mut indexes = self.indexes.lock().unwrap();
        if !indexes.contains_key(index) && indexes.len() >= MAX_INDEXES {
            let Some(oldest) = indexes
                .iter()
                .min_by_key(|(_, stats)| stats.last)
                .map(|(name, _)| name.clone())
            else {
                return;
            };
            indexes.remove(&oldest);
        }
        let stats = indexes.entry(index.to_string()).or_default();
        stats.last = at;
        for (key, kinds) in keys {
            if !stats.keys.contains_key(key) && stats.keys.len() >= MAX_KEYS_PER_INDEX {
                // Make room only by dropping a key no query used in the window.
                let idle = stats
                    .keys
                    .iter()
                    .find(|(_, k)| k.buckets.iter().all(|b| b.at + BUCKETS as u64 <= at))
                    .map(|(name, _)| name.clone());
                match idle {
                    Some(idle) => {
                        stats.keys.remove(&idle);
                    }
                    None => {
                        stats.truncated = true;
                        continue;
                    }
                }
            }
            let entry = stats.keys.entry(key.clone()).or_default();
            entry.kinds |= kinds;
            let bucket = &mut entry.buckets[(at % BUCKETS as u64) as usize];
            if bucket.at != at {
                *bucket = Bucket {
                    at,
                    ..Default::default()
                };
            }
            bucket.queries += 1;
            bucket.seconds += elapsed.as_secs_f64();
        }
    }

    /// The stats of `index` over the window.
    pub fn snapshot(&self, index: &str) -> pb::FilterStatsResponse {
        self.snapshot_at(index, Instant::now())
    }

    fn snapshot_at(&self, index: &str, now: Instant) -> pb::FilterStatsResponse {
        let at = self.bucket(now);
        let window_seconds = now.duration_since(self.start).min(WINDOW).as_secs() as u32;
        let indexes = self.indexes.lock().unwrap();
        let Some(stats) = indexes.get(index) else {
            return pb::FilterStatsResponse {
                window_seconds,
                ..Default::default()
            };
        };
        let mut keys: Vec<pb::FilterKeyStats> = stats
            .keys
            .iter()
            .filter_map(|(key, s)| {
                let live = s.buckets.iter().filter(|b| b.at + (BUCKETS as u64) > at);
                let (queries, seconds) =
                    live.fold((0, 0.0), |(q, t), b| (q + b.queries, t + b.seconds));
                (queries > 0).then(|| pb::FilterKeyStats {
                    key: key.clone(),
                    queries,
                    seconds,
                    string_operand: s.kinds & operand::STRING != 0,
                    number_operand: s.kinds & operand::NUMBER != 0,
                    bool_operand: s.kinds & operand::BOOL != 0,
                })
            })
            .collect();
        keys.sort_by(|a, b| b.queries.cmp(&a.queries).then(a.key.cmp(&b.key)));
        pb::FilterStatsResponse {
            keys,
            window_seconds,
            truncated: stats.truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(pairs: &[(&str, u8)]) -> BTreeMap<String, u8> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn counts_queries_and_latency_per_key_within_the_window() {
        let stats = FilterStats::new();
        let t0 = stats.start;
        let ms = Duration::from_millis;
        stats.record_at("b/i", &keys(&[("author", operand::STRING)]), ms(10), t0);
        stats.record_at(
            "b/i",
            &keys(&[("author", operand::STRING), ("year", operand::NUMBER)]),
            ms(30),
            t0 + Duration::from_secs(600),
        );
        stats.record_at("b/j", &keys(&[("x", operand::BOOL)]), ms(5), t0);
        let got = stats.snapshot_at("b/i", t0 + Duration::from_secs(900));
        assert_eq!(got.window_seconds, 900);
        assert_eq!(got.keys.len(), 2);
        assert_eq!(got.keys[0].key, "author");
        assert_eq!(got.keys[0].queries, 2);
        assert!((got.keys[0].seconds - 0.04).abs() < 1e-9);
        assert!(got.keys[0].string_operand && !got.keys[0].number_operand);
        assert_eq!(got.keys[1].key, "year");
        assert!(got.keys[1].number_operand);
        // The first query leaves the window an hour after its bucket.
        let later = stats.snapshot_at("b/i", t0 + WINDOW + Duration::from_secs(1));
        assert_eq!(later.window_seconds, WINDOW.as_secs() as u32);
        assert_eq!(later.keys[0].queries, 1);
        let gone = stats.snapshot_at("b/i", t0 + WINDOW + Duration::from_secs(900));
        assert!(gone.keys.is_empty());
        assert!(stats.snapshot_at("other", t0).keys.is_empty());
    }

    #[test]
    fn indexes_and_keys_are_bounded() {
        let stats = FilterStats::new();
        let t0 = stats.start;
        for i in 0..MAX_KEYS_PER_INDEX + 5 {
            stats.record_at(
                "b/i",
                &keys(&[(&format!("k{i}"), operand::STRING)]),
                Duration::ZERO,
                t0,
            );
        }
        let got = stats.snapshot_at("b/i", t0);
        assert_eq!(got.keys.len(), MAX_KEYS_PER_INDEX);
        assert!(got.truncated);
        // A key idle for the whole window makes room for a new one.
        let later = t0 + WINDOW + BUCKET;
        stats.record_at(
            "b/i",
            &keys(&[("new", operand::STRING)]),
            Duration::ZERO,
            later,
        );
        let got = stats.snapshot_at("b/i", later);
        assert_eq!(got.keys.len(), 1);
        assert_eq!(got.keys[0].key, "new");

        for i in 0..MAX_INDEXES + 3 {
            stats.record_at(
                &format!("b/{i}"),
                &keys(&[("k", operand::STRING)]),
                Duration::ZERO,
                later,
            );
        }
        assert_eq!(stats.indexes.lock().unwrap().len(), MAX_INDEXES);
    }
}
