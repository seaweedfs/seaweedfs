//! Mirror of `weed/storage/store_volume_report.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

/// Identifies one reported copy. Keyed by disk as well as id because a volume
/// id can be mounted on two disks, and reporting one of them would leave the
/// other's changes untold.
pub type VolumeReportKey = (u32, u32);

/// What the master was told about one volume copy: the hash that detects
/// change, and the heartbeat pass that last found the copy held.
#[derive(Clone, Copy)]
struct ReportedVolume {
    hash: u64,
    pass: u64,
}

/// Remembers what the master was last told about each volume, so a heartbeat
/// can carry only what moved since.
///
/// Per-connection: a server that reconnects, or reaches a different master,
/// knows nothing about what that master holds and starts again from the full
/// list. The default has told no master anything, so it sends the whole list
/// until one accepts changes.
#[derive(Default)]
pub struct VolumeReportState {
    /// Set once the master says it compares digests. Until then the whole list
    /// goes every time, which is what an older master needs.
    deltas_accepted: AtomicBool,
    full_list_needed: AtomicBool,
    /// Counts requests for the whole list, so one arriving while a heartbeat is
    /// being built is not marked satisfied by it.
    full_list_generation: AtomicU64,
    /// Numbers heartbeats, so one can mark the copies it finds held without
    /// building a second map of them.
    pass: AtomicU64,
    last_reported: Mutex<HashMap<VolumeReportKey, ReportedVolume>>,
}

impl VolumeReportState {
    /// Drops everything known about the master's view.
    pub fn reset(&self) {
        self.deltas_accepted.store(false, Ordering::Relaxed);
        self.full_list_needed.store(true, Ordering::Relaxed);
        self.full_list_generation.fetch_add(1, Ordering::Relaxed);
        self.last_reported.lock().unwrap().clear();
    }

    pub fn accept_deltas(&self) {
        self.deltas_accepted.store(true, Ordering::Relaxed);
    }

    pub fn request_full_list(&self) {
        self.full_list_needed.store(true, Ordering::Relaxed);
        self.full_list_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Opens a heartbeat: whether it must carry the whole list, the request it
    /// answers, and the pass number that marks the copies it finds still held.
    pub fn begin(&self) -> (bool, u64, u64) {
        let full = self.full_list_needed.load(Ordering::Relaxed)
            || !self.deltas_accepted.load(Ordering::Relaxed);
        let pass = self.pass.fetch_add(1, Ordering::Relaxed) + 1;
        (
            full,
            self.full_list_generation.load(Ordering::Relaxed),
            pass,
        )
    }

    /// Reports whether the master needs telling about this volume, given what
    /// it was last told. For a caller that is only taking a snapshot and so
    /// must leave the reporting state alone; a heartbeat calls `record`.
    pub fn changed(&self, key: VolumeReportKey, hash: u64) -> bool {
        self.last_reported
            .lock()
            .unwrap()
            .get(&key)
            .is_none_or(|previous| previous.hash != hash)
    }

    /// Marks one volume copy as held by the heartbeat being built, and reports
    /// whether the master needs telling about it. It updates the entry already
    /// held rather than build a second map beside it, so a server whose volumes
    /// are quiet allocates nothing per volume per heartbeat.
    pub fn record(&self, key: VolumeReportKey, hash: u64, pass: u64) -> bool {
        let mut last_reported = self.last_reported.lock().unwrap();
        match last_reported.get_mut(&key) {
            Some(previous) => {
                let changed = previous.hash != hash;
                previous.hash = hash;
                previous.pass = pass;
                changed
            }
            None => {
                last_reported.insert(key, ReportedVolume { hash, pass });
                true
            }
        }
    }

    /// Closes the heartbeat. Copies this pass did not find are forgotten, so one
    /// that comes back is reported again.
    pub fn commit(&self, pass: u64, generation: u64) {
        self.last_reported
            .lock()
            .unwrap()
            .retain(|_, reported| reported.pass == pass);
        // A request that arrived while this heartbeat was being built asked
        // about a later state than it carries, so it stands.
        if self.full_list_generation.load(Ordering::Relaxed) == generation {
            self.full_list_needed.store(false, Ordering::Relaxed);
        }
    }
}
