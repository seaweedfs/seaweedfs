//! Mirror of `weed/storage/store_volume_report.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Identifies one reported copy. Keyed by disk as well as id because a volume
/// id can be mounted on two disks, and reporting one of them would leave the
/// other's changes untold.
pub type VolumeReportKey = (u32, u32);

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
    last_reported: Mutex<HashMap<VolumeReportKey, u64>>,
}

impl VolumeReportState {
    /// Drops everything known about the master's view.
    pub fn reset(&self) {
        self.deltas_accepted.store(false, Ordering::Relaxed);
        self.full_list_needed.store(true, Ordering::Relaxed);
        self.last_reported.lock().unwrap().clear();
    }

    pub fn accept_deltas(&self) {
        self.deltas_accepted.store(true, Ordering::Relaxed);
    }

    pub fn request_full_list(&self) {
        self.full_list_needed.store(true, Ordering::Relaxed);
    }

    /// Reports whether this heartbeat must carry the whole list.
    pub fn begin(&self) -> bool {
        self.full_list_needed.load(Ordering::Relaxed)
            || !self.deltas_accepted.load(Ordering::Relaxed)
    }

    /// Reports whether the master needs telling about this volume, given what
    /// it was last told.
    pub fn changed(&self, key: VolumeReportKey, hash: u64) -> bool {
        self.last_reported.lock().unwrap().get(&key) != Some(&hash)
    }

    /// Records what this heartbeat told the master. Volumes absent from
    /// `reported` are forgotten, so one that comes back is reported again.
    pub fn commit(&self, reported: HashMap<VolumeReportKey, u64>) {
        *self.last_reported.lock().unwrap() = reported;
        self.full_list_needed.store(false, Ordering::Relaxed);
    }
}
