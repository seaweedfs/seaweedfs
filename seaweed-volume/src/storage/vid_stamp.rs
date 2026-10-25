//! Persisted volume-id high-water mark ("vid stamp"), mirroring Go's
//! `weed/storage/vid_stamp.go`.
//!
//! Each volume dir carries a `.vid_stamp` JSON file with the highest volume
//! id the server has ever observed plus the topology id it last heartbeated
//! to. The bound is fed by heartbeat echoes, AllocateVolume, and local
//! volume/EC creation, is reported back in `Heartbeat.max_volume_id_seen`,
//! and survives master state loss so rebuilt masters cannot reissue ids an
//! offline server still holds.

use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::storage::types::VolumeId;

use super::store::Store;

const VID_STAMP_FILE_NAME: &str = ".vid_stamp";

#[derive(Serialize, Deserialize, Default)]
struct VidStamp {
    #[serde(rename = "maxVolumeId")]
    max_volume_id: u32,
    #[serde(rename = "topologyId", default, skip_serializing_if = "String::is_empty")]
    topology_id: String,
}

impl Store {
    pub fn max_volume_id_seen(&self) -> u32 {
        self.max_volume_id_seen.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn last_seen_topology_id(&self) -> String {
        self.last_seen_topology_id
            .lock()
            .unwrap()
            .clone()
    }

    pub fn update_max_volume_id_seen(&self, vid: u32) {
        use std::sync::atomic::Ordering::Relaxed;
        let prev = self.max_volume_id_seen.fetch_max(vid, Relaxed);
        if vid > prev || self.vid_stamp_dirty.load(Relaxed) {
            self.save_vid_stamps();
        }
    }

    pub fn update_last_seen_topology_id(&self, id: &str) {
        if id.is_empty() {
            return;
        }
        {
            let mut guard = self.last_seen_topology_id.lock().unwrap();
            if *guard == id && !self.vid_stamp_dirty.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }
            *guard = id.to_string();
        }
        self.save_vid_stamps();
    }

    /// Seed the bound from a freshly added location: its persisted stamp,
    /// its volumes, and its EC shards. Called per location from
    /// `add_location` after the EC reconcile passes have run.
    pub(crate) fn fold_location_into_vid_stamp(&self, loc: &super::disk_location::DiskLocation) {
        if let Ok(stamp) = read_vid_stamp(Path::new(&loc.directory)) {
            self.update_max_volume_id_seen(stamp.max_volume_id);
            if !stamp.topology_id.is_empty() {
                self.update_last_seen_topology_id(&stamp.topology_id);
            }
        }
        let mut max_vid: u32 = 0;
        for vid in loc.volume_ids() {
            max_vid = max_vid.max(vid.0);
        }
        for (vid, _) in loc.ec_volumes() {
            max_vid = max_vid.max(vid.0);
        }
        if max_vid > 0 {
            self.update_max_volume_id_seen(max_vid);
        }
    }

    pub(crate) fn save_vid_stamps(&self) {
        let _guard = self.vid_stamp_lock.lock().unwrap();
        let stamp = match serde_json::to_vec(&VidStamp {
            max_volume_id: self.max_volume_id_seen(),
            topology_id: self.last_seen_topology_id(),
        }) {
            Ok(s) => s,
            Err(_) => return,
        };
        let mut failed = false;
        for loc in &self.locations {
            if let Err(e) = write_vid_stamp(Path::new(&loc.directory), &stamp) {
                tracing::warn!("write vid stamp in {}: {}", loc.directory, e);
                failed = true;
            }
        }
        self.vid_stamp_dirty
            .store(failed, std::sync::atomic::Ordering::Relaxed);
    }
}

fn read_vid_stamp(dir: &Path) -> io::Result<VidStamp> {
    let data = fs::read(dir.join(VID_STAMP_FILE_NAME))?;
    serde_json::from_slice(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn write_vid_stamp(dir: &Path, stamp: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{}.tmp", VID_STAMP_FILE_NAME));
    {
        use std::io::Write;
        let mut f = fs::File::create(&tmp)?;
        f.write_all(stamp)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(VID_STAMP_FILE_NAME))?;
    if cfg!(unix) {
        fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Stamps the volume id of a newly created volume or EC shard set. Called
/// from `Store::add_volume` / `Store::mount_ec_shards*` so the bound covers
/// ids before their files exist.
impl Store {
    pub(crate) fn stamp_allocated_vid(&self, vid: VolumeId) {
        self.update_max_volume_id_seen(vid.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MinFreeSpace;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::DiskType;
    use tempfile::TempDir;

    fn make_store(dir: &str) -> Store {
        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(dir, dir, 10, DiskType::HardDrive, MinFreeSpace::Percent(1.0), Vec::new())
            .unwrap();
        store
    }

    #[test]
    fn stamp_written_on_bound_increase() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let store = make_store(dir);

        store.update_max_volume_id_seen(42);

        let stamp = read_vid_stamp(tmp.path()).unwrap();
        assert_eq!(stamp.max_volume_id, 42);
    }

    #[test]
    fn stamp_survives_restart() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let store = make_store(dir);
        store.update_max_volume_id_seen(42);
        store.update_last_seen_topology_id("cluster-a");
        drop(store);

        let restarted = make_store(dir);
        assert_eq!(restarted.max_volume_id_seen(), 42);
        assert_eq!(restarted.last_seen_topology_id(), "cluster-a");
    }

    #[test]
    fn later_disk_gets_combined_stamp() {
        let tmp1 = TempDir::new().unwrap();
        let tmp2 = TempDir::new().unwrap();
        // dir2 carries a stale stamp from an older run
        write_vid_stamp(
            tmp2.path(),
            &serde_json::to_vec(&VidStamp {
                max_volume_id: 10,
                topology_id: String::new(),
            })
            .unwrap(),
        )
        .unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                tmp1.path().to_str().unwrap(),
                tmp1.path().to_str().unwrap(),
                10,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        store.update_max_volume_id_seen(100);
        store
            .add_location(
                tmp2.path().to_str().unwrap(),
                tmp2.path().to_str().unwrap(),
                10,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();

        let stamp2 = read_vid_stamp(tmp2.path()).unwrap();
        assert_eq!(stamp2.max_volume_id, 100);
    }

    #[test]
    fn bound_is_monotonic() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let store = make_store(dir);

        store.update_max_volume_id_seen(10);
        store.update_max_volume_id_seen(5);
        assert_eq!(store.max_volume_id_seen(), 10);
    }
}
