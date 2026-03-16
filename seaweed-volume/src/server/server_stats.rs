use chrono::{Datelike, Local, Timelike};
use serde::Serialize;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

static START_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);
static SERVER_STATS: LazyLock<ServerStats> = LazyLock::new(ServerStats::default);

#[derive(Default)]
pub struct ServerStats {
    inner: Mutex<ServerStatsInner>,
}

#[derive(Default)]
struct ServerStatsInner {
    requests: DurationCounter,
    connections: DurationCounter,
    assign_requests: DurationCounter,
    read_requests: DurationCounter,
    write_requests: DurationCounter,
    delete_requests: DurationCounter,
    bytes_in: DurationCounter,
    bytes_out: DurationCounter,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ServerStatsSnapshot {
    pub requests: DurationCounterSnapshot,
    pub connections: DurationCounterSnapshot,
    pub assign_requests: DurationCounterSnapshot,
    pub read_requests: DurationCounterSnapshot,
    pub write_requests: DurationCounterSnapshot,
    pub delete_requests: DurationCounterSnapshot,
    pub bytes_in: DurationCounterSnapshot,
    pub bytes_out: DurationCounterSnapshot,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DurationCounterSnapshot {
    pub minute_counter: RoundRobinCounterSnapshot,
    pub hour_counter: RoundRobinCounterSnapshot,
    pub day_counter: RoundRobinCounterSnapshot,
    pub week_counter: RoundRobinCounterSnapshot,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct RoundRobinCounterSnapshot {
    pub last_index: i32,
    pub values: Vec<i64>,
    pub counts: Vec<i64>,
}

#[derive(Clone)]
struct DurationCounter {
    minute_counter: RoundRobinCounter,
    hour_counter: RoundRobinCounter,
    day_counter: RoundRobinCounter,
    week_counter: RoundRobinCounter,
}

#[derive(Clone)]
struct RoundRobinCounter {
    last_index: i32,
    values: Vec<i64>,
    counts: Vec<i64>,
}

impl Default for DurationCounter {
    fn default() -> Self {
        Self {
            minute_counter: RoundRobinCounter::new(60),
            hour_counter: RoundRobinCounter::new(60),
            day_counter: RoundRobinCounter::new(24),
            week_counter: RoundRobinCounter::new(7),
        }
    }
}

impl RoundRobinCounter {
    fn new(slots: usize) -> Self {
        Self {
            last_index: -1,
            values: vec![0; slots],
            counts: vec![0; slots],
        }
    }

    fn add(&mut self, index: usize, val: i64) {
        if index >= self.values.len() {
            return;
        }
        while self.last_index != index as i32 {
            self.last_index = (self.last_index + 1).rem_euclid(self.values.len() as i32);
            self.values[self.last_index as usize] = 0;
            self.counts[self.last_index as usize] = 0;
        }
        self.values[index] += val;
        self.counts[index] += 1;
    }

    fn snapshot(&self) -> RoundRobinCounterSnapshot {
        RoundRobinCounterSnapshot {
            last_index: self.last_index,
            values: self.values.clone(),
            counts: self.counts.clone(),
        }
    }
}

impl DurationCounter {
    fn add_now(&mut self, val: i64) {
        let now = Local::now();
        self.minute_counter.add(now.second() as usize, val);
        self.hour_counter.add(now.minute() as usize, val);
        self.day_counter.add(now.hour() as usize, val);
        self.week_counter
            .add(now.weekday().num_days_from_sunday() as usize, val);
    }

    fn snapshot(&self) -> DurationCounterSnapshot {
        DurationCounterSnapshot {
            minute_counter: self.minute_counter.snapshot(),
            hour_counter: self.hour_counter.snapshot(),
            day_counter: self.day_counter.snapshot(),
            week_counter: self.week_counter.snapshot(),
        }
    }
}

impl ServerStatsInner {
    fn snapshot(&self) -> ServerStatsSnapshot {
        ServerStatsSnapshot {
            requests: self.requests.snapshot(),
            connections: self.connections.snapshot(),
            assign_requests: self.assign_requests.snapshot(),
            read_requests: self.read_requests.snapshot(),
            write_requests: self.write_requests.snapshot(),
            delete_requests: self.delete_requests.snapshot(),
            bytes_in: self.bytes_in.snapshot(),
            bytes_out: self.bytes_out.snapshot(),
        }
    }
}

impl ServerStats {
    fn update<F>(&self, update: F)
    where
        F: FnOnce(&mut ServerStatsInner),
    {
        let mut inner = self.inner.lock().unwrap();
        update(&mut inner);
    }

    fn snapshot(&self) -> ServerStatsSnapshot {
        self.inner.lock().unwrap().snapshot()
    }
}

impl RoundRobinCounterSnapshot {
    pub fn to_list(&self) -> Vec<i64> {
        if self.values.is_empty() {
            return Vec::new();
        }
        let mut ret = Vec::with_capacity(self.values.len());
        let mut index = self.last_index;
        let mut step = self.values.len();
        while step > 0 {
            step -= 1;
            index += 1;
            if index >= self.values.len() as i32 {
                index = 0;
            }
            ret.push(self.values[index as usize]);
        }
        ret
    }
}

pub fn init_process_start() {
    LazyLock::force(&START_TIME);
    LazyLock::force(&SERVER_STATS);
}

pub fn uptime_string() -> String {
    let secs = START_TIME.elapsed().as_secs();
    let hours = secs / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    let mut out = String::new();
    if hours > 0 {
        out.push_str(&format!("{}h", hours));
    }
    if hours > 0 || minutes > 0 {
        out.push_str(&format!("{}m", minutes));
    }
    out.push_str(&format!("{}s", seconds));
    out
}

pub fn snapshot() -> ServerStatsSnapshot {
    SERVER_STATS.snapshot()
}

pub fn record_request_open() {
    SERVER_STATS.update(|inner| inner.requests.add_now(1));
}

pub fn record_request_close() {
    SERVER_STATS.update(|inner| inner.requests.add_now(-1));
}

pub fn record_read_request() {
    SERVER_STATS.update(|inner| inner.read_requests.add_now(1));
}

pub fn record_write_request() {
    SERVER_STATS.update(|inner| inner.write_requests.add_now(1));
}

pub fn record_delete_request() {
    SERVER_STATS.update(|inner| inner.delete_requests.add_now(1));
}

pub fn record_bytes_in(bytes: i64) {
    SERVER_STATS.update(|inner| inner.bytes_in.add_now(bytes));
}

pub fn record_bytes_out(bytes: i64) {
    SERVER_STATS.update(|inner| inner.bytes_out.add_now(bytes));
}

#[cfg(test)]
pub fn reset_for_tests() {
    LazyLock::force(&START_TIME);
    let mut inner = SERVER_STATS.inner.lock().unwrap();
    *inner = ServerStatsInner::default();
}

