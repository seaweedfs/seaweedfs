//! Merging a peer's `.ecj` deletion ids into a local EC journal. Mirrors Go's
//! `Store.MergeEcJournal` (`weed/storage/store_ec_journal.go`).

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::RwLock;

use crate::storage::erasure_coding::EcVolume;
use crate::storage::erasure_coding::ecj_merge::{append_ecj_ids, read_ecj_ids};
use crate::storage::store::Store;
use crate::storage::types::{NeedleId, VolumeId};

/// How often an unmounted merge re-reads a journal that changed under it. Only
/// a mount-delete-unmount or a concurrent merge between the read and the
/// append changes it, so one retry is nearly always enough.
const ECJ_MERGE_ATTEMPTS: usize = 5;

/// Fold a peer's deletion `ids` into the local journal of EC volume `vid` on
/// the receiving disk, the one whose data directory is `data_dir`; `ecj_path`
/// is that journal's path in the disk's index directory. Appends only the ids
/// the journal lacks and returns how many it added. Blocking: call it from
/// `spawn_blocking`.
///
/// A mounted volume owns its journal: the merge goes through its open handle
/// and in-memory set. That is the receiving disk's own runtime for `vid`,
/// wherever its journal lives (it may sit in the data dir rather than
/// `ecj_path`'s index dir), else a sibling runtime journaling into `ecj_path`
/// itself: disks sharing one index directory, or reconciliation mounting `vid`
/// on a disk that journals into another's (#9212). Otherwise `ecj_path` is
/// appended to under the store write lock, which mounts take, so no mount can
/// open it mid-append. The read that computes the delta runs outside the lock,
/// and a journal that changed in between — including by a concurrent merge —
/// is re-read.
pub fn merge_ec_journal(
    store: &RwLock<Store>,
    vid: VolumeId,
    data_dir: &str,
    ecj_path: &str,
    ids: &HashSet<NeedleId>,
) -> io::Result<usize> {
    merge_ec_journal_with(store, vid, data_dir, ecj_path, ids, read_ecj_ids)
}

/// `merge_ec_journal` with the unlocked journal read injected, so a test can
/// mount the volume between that read and the append.
fn merge_ec_journal_with(
    store: &RwLock<Store>,
    vid: VolumeId,
    data_dir: &str,
    ecj_path: &str,
    ids: &HashSet<NeedleId>,
    mut read: impl FnMut(&str) -> io::Result<(HashSet<NeedleId>, u64)>,
) -> io::Result<usize> {
    for _ in 0..ECJ_MERGE_ATTEMPTS {
        {
            let mut store = store
                .write()
                .map_err(|_| io::Error::other("store lock poisoned"))?;
            if let Some(ecv) = mounted_ec_journal(&mut store, vid, data_dir, ecj_path)? {
                return ecv.merge_journal(ids);
            }
        }
        let (local, size) = read(ecj_path)?;
        let mut store = store
            .write()
            .map_err(|_| io::Error::other("store lock poisoned"))?;
        if let Some(ecv) = mounted_ec_journal(&mut store, vid, data_dir, ecj_path)? {
            // Mounted since the read: its handle owns the journal now.
            return ecv.merge_journal(ids);
        }
        if let Some(added) = append_ecj_ids(ecj_path, &local, ids, size)? {
            return Ok(added);
        }
    }
    Err(io::Error::other(format!(
        "ec volume {}: journal {} kept changing during merge",
        vid.0, ecj_path
    )))
}

/// The mounted runtime a merge into `ecj_path` on the disk at `data_dir` must
/// go through, if any: that disk's own runtime for `vid`, else the first
/// sibling's whose journal is `ecj_path`. Errors when no disk is at
/// `data_dir`.
fn mounted_ec_journal<'a>(
    store: &'a mut Store,
    vid: VolumeId,
    data_dir: &str,
    ecj_path: &str,
) -> io::Result<Option<&'a mut EcVolume>> {
    let owner = store
        .locations
        .iter()
        .position(|loc| Path::new(&loc.directory) == Path::new(data_dir))
        .ok_or_else(|| {
            io::Error::other(format!(
                "ec volume {}: no disk at {} owns journal {}",
                vid.0, data_dir, ecj_path
            ))
        })?;
    let runtime = if store.locations[owner].has_ec_volume(vid) {
        Some(owner)
    } else {
        store.locations.iter().position(|loc| {
            loc.find_ec_volume(vid)
                .is_some_and(|ecv| Path::new(&ecv.ecj_file_name()) == Path::new(ecj_path))
        })
    };
    Ok(runtime.and_then(|i| store.locations[i].find_ec_volume_mut(vid)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MinFreeSpace;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::DiskType;
    use crate::storage::volume::{VifEcShardConfig, VifVolumeInfo};
    use tempfile::TempDir;

    const COLLECTION: &str = "c";
    const VID: VolumeId = VolumeId(9);

    /// A store with one disk per entry of `data`, all sharing `idx` when given,
    /// else each indexing into its own data dir.
    fn make_store(tmp: &TempDir, data: &[&str], idx: Option<&str>) -> RwLock<Store> {
        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in data {
            let dir = tmp.path().join(d).to_string_lossy().into_owned();
            let idx_dir = idx
                .map(|i| tmp.path().join(i).to_string_lossy().into_owned())
                .unwrap_or_else(|| dir.clone());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::create_dir_all(&idx_dir).unwrap();
            store
                .add_location(
                    &dir,
                    &idx_dir,
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }
        RwLock::new(store)
    }

    fn dir(tmp: &TempDir, d: &str) -> String {
        tmp.path().join(d).to_string_lossy().into_owned()
    }

    fn records(ids: &[u64]) -> Vec<u8> {
        let ids: Vec<NeedleId> = ids.iter().copied().map(NeedleId).collect();
        crate::storage::erasure_coding::ecj_merge::encode_ecj_ids(&ids)
    }

    fn id_set(ids: &[u64]) -> HashSet<NeedleId> {
        ids.iter().copied().map(NeedleId).collect()
    }

    /// Shard 0 of `VID` and its `.vif` in `data_dir`.
    fn write_shard0(data_dir: &str) {
        let base = format!("{}/{}_{}", data_dir, COLLECTION, VID.0);
        std::fs::write(format!("{}.ec00", base), b"shard data nonempty").unwrap();
        let vif = VifVolumeInfo {
            version: 3,
            ec_shard_config: Some(VifEcShardConfig {
                data_shards: 10,
                parity_shards: 4,
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string(&vif).unwrap(),
        )
        .unwrap();
    }

    /// `VID`'s `.ecx` and a `.ecj` holding `deleted` in `dir`; returns the
    /// journal path.
    fn write_index(dir: &str, deleted: &[u64]) -> String {
        let base = format!("{}/{}_{}", dir, COLLECTION, VID.0);
        std::fs::write(format!("{}.ecx", base), vec![0u8; 16]).unwrap();
        let ecj = format!("{}.ecj", base);
        std::fs::write(&ecj, records(deleted)).unwrap();
        ecj
    }

    fn deleted_on(store: &RwLock<Store>, disk: usize, id: u64) -> bool {
        store.read().unwrap().locations[disk]
            .find_ec_volume(VID)
            .expect("mounted")
            .is_needle_deleted(NeedleId(id))
    }

    /// Disks sharing one index directory all hold the same journal path.
    /// Copying shards onto a disk that has not mounted `vid` must still reach
    /// the sibling runtime holding that journal open.
    #[test]
    fn shared_index_dir_reaches_sibling_mount() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp, &["d0", "d1"], Some("idx"));
        write_shard0(&dir(&tmp, "d0"));
        let ecj = write_index(&dir(&tmp, "idx"), &[1]);
        store.write().unwrap().locations[0]
            .mount_ec_shards(VID, COLLECTION, &[0], "")
            .unwrap();
        assert_eq!(
            store.read().unwrap().locations[0]
                .find_ec_volume(VID)
                .unwrap()
                .ecj_file_name(),
            ecj
        );

        let added =
            merge_ec_journal(&store, VID, &dir(&tmp, "d1"), &ecj, &id_set(&[1, 2])).unwrap();
        assert_eq!(added, 1);
        assert!(
            deleted_on(&store, 0, 2),
            "the mounted sibling must see id 2"
        );
        assert_eq!(std::fs::read(&ecj).unwrap(), records(&[1, 2]));
    }

    /// A sibling disk can mount `vid` from the receiving disk's index (#9212)
    /// while the merge reads the journal unlocked. The merge must go through
    /// that mount and report what it added.
    #[test]
    fn mount_during_read_is_merged_through_and_counted() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp, &["d0", "d1"], None);
        let owner = dir(&tmp, "d0");
        let ecj = write_index(&owner, &[1]);
        write_shard0(&dir(&tmp, "d1"));

        let mut mounted = false;
        let read = |path: &str| {
            let read = read_ecj_ids(path);
            if !mounted {
                store.write().unwrap().locations[1]
                    .mount_ec_shards_with_idx_dir(VID, COLLECTION, &[0], &owner, "")
                    .unwrap();
                mounted = true;
            }
            read
        };
        let added =
            merge_ec_journal_with(&store, VID, &owner, &ecj, &id_set(&[1, 2, 3]), read).unwrap();
        assert_eq!(added, 2, "the ids merged through the new mount are counted");
        assert!(deleted_on(&store, 1, 2) && deleted_on(&store, 1, 3));
        assert_eq!(std::fs::read(&ecj).unwrap(), records(&[1, 2, 3]));
    }

    /// An unmounted journal is merged on disk and a repeat adds nothing; a
    /// data dir that is no disk is refused.
    #[test]
    fn unmounted_journal_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let store = make_store(&tmp, &["d0"], Some("idx"));
        let ecj = write_index(&dir(&tmp, "idx"), &[1, 2]);
        let d0 = dir(&tmp, "d0");
        for want in [2, 0, 0] {
            let added = merge_ec_journal(&store, VID, &d0, &ecj, &id_set(&[2, 3, 4])).unwrap();
            assert_eq!(added, want);
        }
        assert_eq!(std::fs::read(&ecj).unwrap(), records(&[1, 2, 3, 4]));
        assert!(
            merge_ec_journal(&store, VID, &dir(&tmp, "elsewhere"), &ecj, &id_set(&[1])).is_err()
        );
    }
}
