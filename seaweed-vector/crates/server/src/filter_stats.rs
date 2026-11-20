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
const MAX_RETIRED_TABLES: usize = 4;

#[derive(Default, Clone, Copy)]
struct Bucket {
    /// The bucket's number since the stats started.
    at: u64,
    queries: u64,
    seconds: f64,
    /// The operand kinds the bucket's queries compared the key with.
    kinds: u8,
}

#[derive(Default)]
struct KeyStats {
    buckets: [Bucket; BUCKETS],
}

#[derive(Default)]
struct IndexStats {
    /// The table the stats were counted on: an index dropped and created
    /// again under its name is another table.
    table: String,
    /// Tables the name held before, whose late queries are not counted.
    retired: Vec<String>,
    keys: HashMap<String, KeyStats>,
    /// The last bucket a key was left uncounted in.
    truncated: Option<u64>,
    last: u64,
}

impl IndexStats {
    /// Starts counting on `table`, retiring the table counted on before.
    fn replace(&mut self, table: &str) {
        let mut retired = std::mem::take(&mut self.retired);
        if !self.table.is_empty() {
            retired.push(std::mem::take(&mut self.table));
            if retired.len() > MAX_RETIRED_TABLES {
                retired.remove(0);
            }
        }
        *self = IndexStats {
            table: table.to_string(),
            retired,
            last: self.last,
            ..Default::default()
        };
    }
}

/// Whether bucket `at` is still in the window of bucket `now`.
fn live(at: u64, now: u64) -> bool {
    at + BUCKETS as u64 > now
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

    /// Drops the stats of `index`, which now holds `table` ("" once
    /// dropped): late queries of the table it held before are not counted.
    pub fn forget(&self, index: &str, table: &str) {
        let mut indexes = self.indexes.lock().unwrap();
        if let Some(stats) = indexes.get_mut(index)
            && (table.is_empty() || stats.table != table)
        {
            stats.replace(table);
        }
    }

    /// One query of `index`, whose table is `table`, that took `elapsed` and
    /// filtered on `keys`. Stats counted on another table under the name are
    /// dropped; a query of a table the name no longer holds is not counted.
    pub fn record(&self, index: &str, table: &str, keys: &BTreeMap<String, u8>, elapsed: Duration) {
        self.record_table_at(index, table, keys, elapsed, Instant::now());
    }

    #[cfg(test)]
    fn record_at(&self, index: &str, keys: &BTreeMap<String, u8>, elapsed: Duration, now: Instant) {
        self.record_table_at(index, "", keys, elapsed, now);
    }

    fn record_table_at(
        &self,
        index: &str,
        table: &str,
        keys: &BTreeMap<String, u8>,
        elapsed: Duration,
        now: Instant,
    ) {
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
        if !table.is_empty() && stats.table != table {
            if stats.retired.iter().any(|t| t == table) {
                return;
            }
            if stats.table.is_empty() {
                stats.table = table.to_string();
            } else {
                stats.replace(table);
            }
        }
        stats.last = stats.last.max(at);
        for (key, kinds) in keys {
            if !stats.keys.contains_key(key) && stats.keys.len() >= MAX_KEYS_PER_INDEX {
                // Make room only by dropping a key no query used in the window.
                let idle = stats
                    .keys
                    .iter()
                    .find(|(_, k)| k.buckets.iter().all(|b| !live(b.at, at)))
                    .map(|(name, _)| name.clone());
                match idle {
                    Some(idle) => {
                        stats.keys.remove(&idle);
                    }
                    None => {
                        stats.truncated = stats.truncated.max(Some(at));
                        continue;
                    }
                }
            }
            let entry = stats.keys.entry(key.clone()).or_default();
            let bucket = &mut entry.buckets[(at % BUCKETS as u64) as usize];
            if bucket.at != at {
                *bucket = Bucket {
                    at,
                    ..Default::default()
                };
            }
            bucket.queries += 1;
            bucket.seconds += elapsed.as_secs_f64();
            bucket.kinds |= kinds;
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
                let (queries, seconds, kinds) = s
                    .buckets
                    .iter()
                    .filter(|b| live(b.at, at))
                    .fold((0, 0.0, 0), |(q, t, k), b| {
                        (q + b.queries, t + b.seconds, k | b.kinds)
                    });
                (queries > 0).then(|| pb::FilterKeyStats {
                    key: key.clone(),
                    queries,
                    seconds,
                    string_operand: kinds & operand::STRING != 0,
                    number_operand: kinds & operand::NUMBER != 0,
                    bool_operand: kinds & operand::BOOL != 0,
                })
            })
            .collect();
        keys.sort_by(|a, b| b.queries.cmp(&a.queries).then(a.key.cmp(&b.key)));
        pb::FilterStatsResponse {
            keys,
            window_seconds,
            truncated: stats.truncated.is_some_and(|t| live(t, at)),
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
        let year = later.keys.iter().find(|k| k.key == "year").unwrap();
        assert!(year.number_operand);
        // A string query that left the window no longer marks the key.
        stats.record_at(
            "b/i",
            &keys(&[("author", operand::NUMBER)]),
            ms(1),
            t0 + WINDOW + Duration::from_secs(2),
        );
        let mixed = stats.snapshot_at("b/i", t0 + WINDOW + Duration::from_secs(3));
        let author = mixed.keys.iter().find(|k| k.key == "author").unwrap();
        assert!(author.string_operand && author.number_operand);
        let numeric = stats.snapshot_at("b/i", t0 + WINDOW + Duration::from_secs(700));
        let author = numeric.keys.iter().find(|k| k.key == "author").unwrap();
        assert!(!author.string_operand && author.number_operand);
        let gone = stats.snapshot_at("b/i", t0 + 2 * WINDOW + Duration::from_secs(900));
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
        assert!(!got.truncated, "the uncounted keys left the window");
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

    #[test]
    fn a_record_from_an_earlier_bucket_keeps_the_later_truncation() {
        let stats = FilterStats::new();
        let t0 = stats.start;
        for i in 0..=MAX_KEYS_PER_INDEX {
            stats.record_at(
                "b/i",
                &keys(&[(&format!("k{i}"), operand::STRING)]),
                Duration::ZERO,
                t0 + BUCKET,
            );
        }
        // Took its time before the lock while the later bucket recorded.
        stats.record_at(
            "b/i",
            &keys(&[("late", operand::STRING)]),
            Duration::ZERO,
            t0,
        );
        let got = stats.snapshot_at("b/i", t0 + WINDOW);
        assert!(
            got.truncated,
            "the later bucket's uncounted key is in the window"
        );
        assert_eq!(stats.indexes.lock().unwrap()["b/i"].last, 2);
    }

    #[test]
    fn stats_follow_the_table_behind_an_index_name() {
        let stats = FilterStats::new();
        let t0 = stats.start;
        let author = keys(&[("author", operand::STRING)]);
        let count = |stats: &FilterStats| {
            stats
                .snapshot_at("b/i", t0)
                .keys
                .first()
                .map_or(0, |k| k.queries)
        };
        stats.record_table_at("b/i", "t1", &author, Duration::ZERO, t0);
        stats.record_table_at("b/i", "t1", &author, Duration::ZERO, t0);
        assert_eq!(count(&stats), 2);
        // The index was created again: its first query starts over.
        stats.record_table_at("b/i", "t2", &author, Duration::ZERO, t0);
        assert_eq!(count(&stats), 1);
        // A late query of the old table is not counted on the new one.
        stats.record_table_at("b/i", "t1", &author, Duration::ZERO, t0);
        assert_eq!(count(&stats), 1);
        stats.forget("b/i", "");
        assert_eq!(count(&stats), 0);
        // A first query records its table: a later table starts over.
        stats.record_table_at("b/i", "t3", &author, Duration::ZERO, t0);
        stats.record_table_at("b/i", "t4", &author, Duration::ZERO, t0);
        assert_eq!(count(&stats), 1);
    }

    #[test]
    fn a_late_query_after_a_drop_is_not_counted_on_the_new_table() {
        let stats = FilterStats::new();
        let t0 = stats.start;
        let author = keys(&[("author", operand::STRING)]);
        stats.record_table_at("b/i", "t1", &author, Duration::ZERO, t0);
        stats.forget("b/i", "");
        stats.record_table_at("b/i", "t2", &author, Duration::ZERO, t0);
        stats.record_table_at("b/i", "t2", &author, Duration::ZERO, t0);
        stats.record_table_at("b/i", "t1", &author, Duration::ZERO, t0);
        assert_eq!(stats.snapshot_at("b/i", t0).keys[0].queries, 2);
    }
}
