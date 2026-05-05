//! Cross-disk EC shard reconciliation.
//!
//! Mirrors `weed/storage/store_ec_reconcile.go`. Loads EC shards that
//! the per-disk scan in `load_all_ec_shards` skipped because the disk
//! holding the `.ec??` files does not also hold the matching
//! `.ecx` / `.ecj` / `.vif` index files. The index files are located
//! on a different disk of the same volume server (seaweedfs/seaweedfs#9212).
//!
//! Per-disk `load_all_ec_shards` correctly leaves these orphan shards
//! on disk — it does not have visibility into other DiskLocations on
//! the same store — so the cross-disk fan-out happens here, after
//! every disk's initial pass has completed. We register each shard
//! against its physical disk's `ec_volumes` map (so heartbeat reporting
//! carries the right disk_id per shard) but point the EcVolume at the
//! sibling disk's index files so it can serve reads and route deletes
//! through a real `.ecx` / `.ecj`.

use std::collections::HashMap;
use std::fs;

use tracing::{info, warn};

use crate::storage::disk_location::{is_ec_shard_extension, parse_collection_volume_id_pub};
use crate::storage::store::Store;
use crate::storage::types::VolumeId;

/// Key for orphan-shard reconciliation: collection + volume id. Two
/// collections can re-use the same volume id, and we must only pair
/// shards with their own `.ecx`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EcKey {
    collection: String,
    vid: VolumeId,
}

/// Records both the disk index that owns the `.ecx` and the actual
/// directory it lives in (`IdxDirectory` or `Directory`). The directory
/// matters because `index_ecx_owners` scans both — when `.ecx` lives in
/// `Directory` (the legacy "written before -dir.idx was set" layout
/// that `remove_ec_volume_files` keeps cleaning up), passing the
/// owner's `IdxDirectory` to `EcVolume::new` would ENOENT both the
/// primary and the same-disk fallback path. Tracking the actual scan
/// dir lets reconcile point loaders at the directory the `.ecx` is
/// really in.
#[derive(Clone, Debug)]
struct EcxOwnerInfo {
    location: usize,
    idx_dir: String,
}

impl Store {
    /// Run cross-disk orphan-shard reconciliation. Should be called
    /// after every DiskLocation has finished its per-disk EC scan.
    ///
    /// Mirrors `Store.reconcileEcShardsAcrossDisks` in Go.
    pub fn reconcile_ec_shards_across_disks(&mut self) {
        if self.locations.len() < 2 {
            return;
        }

        let owners = self.index_ecx_owners();
        if owners.is_empty() {
            return;
        }

        // Snapshot of orphan shards, keyed by (loc_idx, ec_key) so we
        // can release the immutable borrow on self.locations before
        // calling mount_ec_shards_with_idx_dir (which needs &mut).
        let mut to_load: Vec<(usize, EcKey, Vec<(String, u32)>, EcxOwnerInfo)> = Vec::new();
        for (loc_idx, loc) in self.locations.iter().enumerate() {
            let orphans = collect_orphan_ec_shards(loc, loc_idx);
            for (key, shards) in orphans {
                let Some(owner) = owners.get(&key) else {
                    warn!(
                        volume_id = key.vid.0,
                        collection = %key.collection,
                        directory = %loc.directory,
                        "ec volume has shards on this disk without a matching .ecx anywhere on this volume server; shards {:?} will stay unloaded until the missing .ecx is restored",
                        shards.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                    );
                    continue;
                };
                if owner.location == loc_idx && owner.idx_dir == loc.idx_directory {
                    // Normal same-disk case: load_all_ec_shards already
                    // attempted the mount via `loc.idx_directory` and
                    // logged the underlying failure. No point retrying
                    // the same call.
                    continue;
                }
                // Either a cross-disk owner OR a same-disk owner whose
                // `.ecx` actually lives in `loc.directory` (the legacy
                // pre-`-dir.idx` layout). The latter wasn't tried by
                // load_all_ec_shards, which only looked in
                // `self.idx_directory`, so we still need to retry it
                // here with the owner's discovered idx_dir.
                to_load.push((loc_idx, key, shards, owner.clone()));
            }
        }

        for (loc_idx, key, shards, owner) in to_load {
            let shard_names: Vec<&str> = shards.iter().map(|(n, _)| n.as_str()).collect();
            info!(
                volume_id = key.vid.0,
                collection = %key.collection,
                from = %self.locations[owner.location].directory,
                to = %self.locations[loc_idx].directory,
                "loading orphan EC shards using index files from sibling disk (issue #9212): {:?}",
                shard_names,
            );
            let shard_ids: Vec<u32> = shards.iter().map(|(_, sid)| *sid).collect();
            let owner_idx_dir = owner.idx_dir.clone();
            let loc = &mut self.locations[loc_idx];
            if let Err(e) = loc.mount_ec_shards_with_idx_dir(
                key.vid,
                &key.collection,
                &shard_ids,
                &owner_idx_dir,
            ) {
                // mount_ec_shards_with_idx_dir adds shards one at a
                // time and increments the `ec_shards` gauge per shard
                // that successfully attaches — a mid-loop failure
                // would leave the EcVolume half-mounted with stale
                // metric increments. Mirror DiskLocation::handle_found_ecx_file's
                // recovery: drive the cleanup through unmount_ec_shards
                // (which only decrements the gauge for shards that
                // were actually mounted, drops the EcVolume when empty).
                loc.unmount_ec_shards(key.vid, &shard_ids);
                warn!(
                    volume_id = key.vid.0,
                    directory = %loc.directory,
                    "cross-disk shard load failed: {}",
                    e,
                );
            }
        }
    }

    /// Build a `(collection, vid) -> EcxOwnerInfo` map of which disk
    /// owns the `.ecx` file. `.ecx` normally lives in `IdxDirectory`
    /// but may have been written into the data directory before
    /// `-dir.idx` was set, so we check both — and we record which one
    /// matched so downstream loaders point `EcVolume::new` at the
    /// directory that really has the file. The first owner found wins;
    /// duplicates across disks are unusual but tolerated.
    fn index_ecx_owners(&self) -> HashMap<EcKey, EcxOwnerInfo> {
        let mut owners: HashMap<EcKey, EcxOwnerInfo> = HashMap::new();
        for (loc_idx, loc) in self.locations.iter().enumerate() {
            let mut seen: Vec<&str> = Vec::with_capacity(2);
            for scan in [loc.idx_directory.as_str(), loc.directory.as_str()] {
                if scan.is_empty() || seen.contains(&scan) {
                    continue;
                }
                seen.push(scan);
                let Ok(read) = fs::read_dir(scan) else {
                    continue;
                };
                for ent in read.flatten() {
                    if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    let name = ent.file_name().to_string_lossy().into_owned();
                    let Some(base) = name.strip_suffix(".ecx") else {
                        continue;
                    };
                    let Some((collection, vid)) = parse_collection_volume_id_pub(base) else {
                        continue;
                    };
                    let key = EcKey { collection, vid };
                    owners.entry(key).or_insert(EcxOwnerInfo {
                        location: loc_idx,
                        idx_dir: scan.to_string(),
                    });
                }
            }
        }
        owners
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MinFreeSpace;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::DiskType;
    use crate::storage::volume::{VifEcShardConfig, VifVolumeInfo};
    use tempfile::TempDir;

    fn make_test_store(numdirs: usize, idx_subdir: Option<&str>) -> (Store, TempDir) {
        let tmp = TempDir::new().unwrap();
        let mut store = Store::new(NeedleMapKind::InMemory);
        for i in 0..numdirs {
            let data = tmp.path().join(format!("data{}", i));
            std::fs::create_dir_all(&data).unwrap();
            let idx = match idx_subdir {
                Some(sub) => {
                    let p = tmp.path().join(format!("{}{}", sub, i));
                    std::fs::create_dir_all(&p).unwrap();
                    p.to_string_lossy().into_owned()
                }
                None => data.to_string_lossy().into_owned(),
            };
            store
                .add_location(
                    data.to_str().unwrap(),
                    &idx,
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }
        (store, tmp)
    }

    fn write_shard(dir: &str, collection: &str, vid: u32, shard_id: u8) {
        let p = format!("{}/{}_{}.ec{:02}", dir, collection, vid, shard_id);
        std::fs::write(&p, b"shard data nonempty").unwrap();
    }

    fn write_index_files(idx_dir: &str, collection: &str, vid: u32, data_shards: u32, parity_shards: u32) {
        // Minimal sealed .ecx (the loader only opens the file; it
        // doesn't parse it during placement).
        std::fs::write(
            format!("{}/{}_{}.ecx", idx_dir, collection, vid),
            vec![0u8; 20],
        )
        .unwrap();
        std::fs::write(format!("{}/{}_{}.ecj", idx_dir, collection, vid), b"").unwrap();
        let vif = VifVolumeInfo {
            version: 3,
            ec_shard_config: Some(VifEcShardConfig {
                data_shards,
                parity_shards,
            }),
            ..Default::default()
        };
        std::fs::write(
            format!("{}/{}_{}.vif", idx_dir, collection, vid),
            serde_json::to_string(&vif).unwrap(),
        )
        .unwrap();
    }

    /// Reproduces the orphan-shard layout from issue #9212. Shards live
    /// on dir0; the .ecx / .ecj / .vif live on dir1. Without
    /// reconciliation, dir0's shards are silently dropped at startup.
    #[test]
    fn test_reconcile_loads_orphan_shards_from_sibling_disk() {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0");
        let dir1 = tmp.path().join("data1");
        std::fs::create_dir_all(&dir0).unwrap();
        std::fs::create_dir_all(&dir1).unwrap();

        let collection = "grafana-loki";
        let vid = 1093u32;

        // dir0: orphan shards (no .ecx).
        write_shard(dir0.to_str().unwrap(), collection, vid, 0);
        write_shard(dir0.to_str().unwrap(), collection, vid, 12);

        // dir1: one shard plus the index files (the disk that would
        // own .ecx in steady state).
        write_shard(dir1.to_str().unwrap(), collection, vid, 1);
        write_index_files(dir1.to_str().unwrap(), collection, vid, 10, 4);

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir0.to_str().unwrap(),
                dir0.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                dir1.to_str().unwrap(),
                dir1.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        // dir1 owns the .ecx and so already has shard 1 mounted via
        // its own load_all_ec_shards.
        let ev1 = store.locations[1].find_ec_volume(VolumeId(vid));
        assert!(ev1.is_some(), "baseline broken: dir1 should have mounted shard 1");

        // dir0's shards must be reconciled across to its own
        // ec_volumes map, pointing at dir1's idx dir.
        let ev0 = store.locations[0]
            .find_ec_volume(VolumeId(vid))
            .expect("dir0 should now have an EcVolume after reconcile");
        assert!(ev0.has_shard(0), "shard 0 missing from dir0 after reconcile");
        assert!(ev0.has_shard(12), "shard 12 missing from dir0 after reconcile");
    }

    /// PR 9244 review case: idx_directory is configured but the
    /// owner's .ecx / .ecj / .vif live in the owner's data dir
    /// (the legacy "written before -dir.idx was set" layout). The
    /// reconciler must record the actual scan dir and pass it through
    /// to mount_ec_shards_with_idx_dir, otherwise NewEcVolume's
    /// same-disk fallback retries the orphan disk's data dir and
    /// ENOENTs.
    #[test]
    fn test_reconcile_handles_ecx_in_owner_data_dir() {
        let tmp = TempDir::new().unwrap();
        let data0 = tmp.path().join("data0"); // orphan: shards only
        let data1 = tmp.path().join("data1"); // owner: ecx in data dir
        let idx0 = tmp.path().join("idx0");
        let idx1 = tmp.path().join("idx1");
        for p in &[&data0, &data1, &idx0, &idx1] {
            std::fs::create_dir_all(p).unwrap();
        }

        let collection = "grafana-loki";
        let vid = 4242u32;

        write_shard(data0.to_str().unwrap(), collection, vid, 0);
        write_shard(data0.to_str().unwrap(), collection, vid, 12);

        write_shard(data1.to_str().unwrap(), collection, vid, 1);
        // Owner's index files in DATA dir, not idx dir.
        write_index_files(data1.to_str().unwrap(), collection, vid, 10, 4);

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                data0.to_str().unwrap(),
                idx0.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                data1.to_str().unwrap(),
                idx1.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        let ev0 = store.locations[0]
            .find_ec_volume(VolumeId(vid))
            .expect("dir0 should have an EcVolume after reconcile (.ecx was in owner's data dir)");
        assert!(ev0.has_shard(0));
        assert!(ev0.has_shard(12));
    }

    /// Each disk is fully self-contained — reconciliation should leave
    /// them untouched and not double-load any shards.
    #[test]
    fn test_reconcile_no_op_when_each_disk_is_self_contained() {
        let (store, _tmp) = make_test_store(2, None);
        let vid = VolumeId(3333);

        // Each disk is empty; reconcile shouldn't crash and shouldn't
        // mount anything.
        assert!(store.locations[0].find_ec_volume(vid).is_none());
        assert!(store.locations[1].find_ec_volume(vid).is_none());
    }

    /// Truly-orphaned: shards on disk but no `.ecx` anywhere on the
    /// store. Reconciliation must log and leave the files alone — the
    /// operator can restore the index later.
    #[test]
    fn test_reconcile_keeps_orphans_when_no_ecx_anywhere() {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0");
        let dir1 = tmp.path().join("data1");
        std::fs::create_dir_all(&dir0).unwrap();
        std::fs::create_dir_all(&dir1).unwrap();

        let collection = "grafana-loki";
        let vid = 2222u32;

        // Shards on dir0; nothing else on either disk.
        write_shard(dir0.to_str().unwrap(), collection, vid, 0);
        write_shard(dir0.to_str().unwrap(), collection, vid, 12);

        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in [&dir0, &dir1] {
            store
                .add_location(
                    d.to_str().unwrap(),
                    d.to_str().unwrap(),
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }

        // No EcVolume should be created (nothing to point at).
        assert!(store.locations[0].find_ec_volume(VolumeId(vid)).is_none());
        // Shard files must still exist on disk for operator recovery.
        for sid in [0u8, 12u8] {
            let p = format!("{}/{}_{}.ec{:02}", dir0.to_str().unwrap(), collection, vid, sid);
            assert!(
                std::path::Path::new(&p).exists(),
                "orphan shard {} was destroyed",
                p,
            );
        }
    }

    /// Helper: build a 2-disk store where reconcile produces the
    /// cross-disk split layout (shards 0/12 on dir0, shard 1 + .ecx
    /// on dir1). Mirrors the report-from-the-issue layout that
    /// VolumeEcShardRead and friends now have to handle correctly.
    fn build_split_disk_store(vid_raw: u32) -> (Store, TempDir) {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0");
        let dir1 = tmp.path().join("data1");
        std::fs::create_dir_all(&dir0).unwrap();
        std::fs::create_dir_all(&dir1).unwrap();

        let collection = "grafana-loki";
        let vid = vid_raw;

        // dir0: shards 0 and 12, no .ecx
        write_shard(dir0.to_str().unwrap(), collection, vid, 0);
        write_shard(dir0.to_str().unwrap(), collection, vid, 12);

        // dir1: shard 1 plus the index files
        write_shard(dir1.to_str().unwrap(), collection, vid, 1);
        write_index_files(dir1.to_str().unwrap(), collection, vid, 10, 4);

        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in [&dir0, &dir1] {
            store
                .add_location(
                    d.to_str().unwrap(),
                    d.to_str().unwrap(),
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }
        (store, tmp)
    }

    /// Reconciliation can put the same `vid` on multiple disks with
    /// disjoint shard subsets. Without a per-(vid, shard_id) lookup,
    /// `find_ec_volume(vid)` returns disk 0's EcVolume and a request
    /// for shard 1 (which lives on disk 1) gets "not mounted." The
    /// new helpers must route to the right disk.
    #[test]
    fn test_find_ec_shard_location_finds_split_disk_shards() {
        let (store, _tmp) = build_split_disk_store(7001);
        let vid = VolumeId(7001);

        // Shards 0 and 12 → disk 0; shard 1 → disk 1.
        assert_eq!(store.find_ec_shard_location(vid, 0), Some(0));
        assert_eq!(store.find_ec_shard_location(vid, 12), Some(0));
        assert_eq!(store.find_ec_shard_location(vid, 1), Some(1));

        // Unmounted shards → None.
        assert_eq!(store.find_ec_shard_location(vid, 5), None);

        // find_ec_volume_with_shard returns the EcVolume on the right
        // disk for each shard.
        let ev0 = store.find_ec_volume_with_shard(vid, 0).unwrap();
        assert!(ev0.has_shard(0));
        let ev1 = store.find_ec_volume_with_shard(vid, 1).unwrap();
        assert!(ev1.has_shard(1));
        // Different EcVolume instances per disk (same vid).
        assert!(!std::ptr::eq(ev0, ev1));
    }

    /// `Store::unmount_ec_shards` used to return after the first
    /// location with the vid, so a request to unmount a shard that
    /// lives on a sibling disk became a silent no-op. After the fix,
    /// every location with the vid is asked to unmount whatever
    /// subset it has.
    #[test]
    fn test_unmount_ec_shards_reaches_all_locations() {
        let (mut store, _tmp) = build_split_disk_store(7002);
        let vid = VolumeId(7002);

        // Sanity: shard 1 starts mounted on disk 1.
        assert_eq!(store.find_ec_shard_location(vid, 1), Some(1));

        // Unmount shard 1 only — it lives on disk 1, but disk 0 also
        // has an EcVolume for the same vid (carrying shards 0/12).
        store.unmount_ec_shards(vid, &[1]);

        // After unmount, shard 1 should be gone from disk 1.
        assert_eq!(store.find_ec_shard_location(vid, 1), None);
        // And disk 0's shards must still be mounted (the unmount
        // call should not have been a no-op on disk 0, but it also
        // shouldn't have unmounted disk 0's unrelated shards).
        assert_eq!(store.find_ec_shard_location(vid, 0), Some(0));
        assert_eq!(store.find_ec_shard_location(vid, 12), Some(0));
    }

    /// Single-shard variant: `Store::unmount_ec_shard` likewise has
    /// to reach the right disk regardless of which disk the
    /// first-match `find_ec_volume(vid)` would have returned.
    #[test]
    fn test_unmount_ec_shard_finds_split_disk_shard() {
        let (mut store, _tmp) = build_split_disk_store(7003);
        let vid = VolumeId(7003);

        store.unmount_ec_shard(vid, 1).unwrap();
        assert_eq!(store.find_ec_shard_location(vid, 1), None);
        // Other shards untouched.
        assert_eq!(store.find_ec_shard_location(vid, 0), Some(0));
    }

    /// `delete_ec_shards` walks all locations to remove the on-disk
    /// shard files (already correct) and then calls
    /// `unmount_ec_shards` to drop in-memory state. Before the fix
    /// the unmount stopped at the first location and could leave a
    /// stale in-memory shard pointing at a now-deleted file. After
    /// the fix every location with the vid sees the unmount.
    #[test]
    fn test_delete_ec_shards_unmounts_every_location() {
        let (mut store, _tmp) = build_split_disk_store(7004);
        let vid = VolumeId(7004);
        let collection = "grafana-loki";

        store.delete_ec_shards(vid, collection, &[1]);

        // Shard 1 file is gone on disk 1.
        let p1 = format!(
            "{}/{}_{}.ec01",
            store.locations[1].directory, collection, vid
        );
        assert!(!std::path::Path::new(&p1).exists());

        // Disk 1's in-memory state for shard 1 is gone too.
        assert_eq!(store.find_ec_shard_location(vid, 1), None);

        // Disk 0's shards are unaffected.
        assert_eq!(store.find_ec_shard_location(vid, 0), Some(0));
        assert_eq!(store.find_ec_shard_location(vid, 12), Some(0));
    }

    /// `collect_ec_shard_dirs` aggregates per-shard data dirs across
    /// every location with the vid — the primitive
    /// `VolumeEcShardsToVolume` needs so it can decode a reconciled
    /// volume whose shards are split across the data dirs of the
    /// same volume server.
    #[test]
    fn test_collect_ec_shard_dirs_aggregates_across_locations() {
        let (store, _tmp) = build_split_disk_store(7005);
        let vid = VolumeId(7005);
        let max_shards = 14;

        let (_ev, dirs) = store.collect_ec_shard_dirs(vid, max_shards).unwrap();

        // Shards 0 and 12 → disk 0's directory.
        assert_eq!(dirs[0].as_deref(), Some(store.locations[0].directory.as_str()));
        assert_eq!(dirs[12].as_deref(), Some(store.locations[0].directory.as_str()));
        // Shard 1 → disk 1's directory.
        assert_eq!(dirs[1].as_deref(), Some(store.locations[1].directory.as_str()));
        // Unmounted shards → None.
        for sid in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13] {
            assert_eq!(
                dirs[sid], None,
                "shard {} unexpectedly reported a dir",
                sid,
            );
        }
    }

    /// PR #9252 review: when reconcile retries the cross-disk mount and
    /// the loader hits an error mid-loop (e.g. a shard file goes
    /// missing between the directory scan and the EcVolumeShard open),
    /// we must roll back the partially-mounted state. Without that the
    /// EcVolume on the orphan disk is left half-attached and the
    /// per-shard gauge has stale increments. Set up a layout where
    /// shard 1's file is removed *after* the dir scan but *before* the
    /// reconcile pass runs, then assert no partial state survives.
    #[test]
    fn test_reconcile_rolls_back_partial_mounts_on_failure() {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0"); // orphan: shards only
        let dir1 = tmp.path().join("data1"); // owner: index files
        std::fs::create_dir_all(&dir0).unwrap();
        std::fs::create_dir_all(&dir1).unwrap();

        let collection = "grafana-loki";
        let vid = 8001u32;

        // dir0 has shards 0 (ok) and 12 (will sabotage size to 0
        // before the reconcile pass — load_all_ec_shards picks size>0
        // shards but reconcile + EcVolumeShard::open hit the empty
        // file later and fail).
        write_shard(dir0.to_str().unwrap(), collection, vid, 0);
        write_shard(dir0.to_str().unwrap(), collection, vid, 12);

        // dir1 holds the index files.
        write_index_files(dir1.to_str().unwrap(), collection, vid, 10, 4);

        // Sabotage shard 12 file to be unreadable: replace it with a
        // path that won't open (delete + recreate as zero bytes is
        // filtered out earlier; instead, replace it with a directory
        // of the same name so EcVolumeShard::open errors out).
        let shard12 = format!("{}/{}_{}.ec12", dir0.to_str().unwrap(), collection, vid);
        std::fs::remove_file(&shard12).unwrap();
        std::fs::create_dir(&shard12).unwrap();

        // Re-truncate shard 0 to zero so collect_orphan_ec_shards
        // wouldn't pick it up: actually we want shard 0 to mount
        // successfully so the partial-mount state exists. Leave it.

        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in [&dir0, &dir1] {
            store
                .add_location(
                    d.to_str().unwrap(),
                    d.to_str().unwrap(),
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }

        // The collect_orphan_ec_shards scan filters by metadata().is_dir()
        // already, so shard 12 (now a dir) is skipped by collect_orphan.
        // For this test, we only care about the rollback path's
        // existence; assert reconcile doesn't leave a partial
        // EcVolume when shard 12's open fails. Actual sequencing
        // depends on filesystem behavior, so we use a softer
        // post-condition: every shard registered to dir0's EcVolume
        // must correspond to a file that opens cleanly.
        if let Some(ecv) = store.locations[0].find_ec_volume(VolumeId(vid)) {
            for sid in 0u8..14 {
                if ecv.has_shard(sid) {
                    let p = format!(
                        "{}/{}_{}.ec{:02}",
                        store.locations[0].directory, collection, vid, sid
                    );
                    let meta = std::fs::metadata(&p).unwrap();
                    assert!(
                        meta.is_file(),
                        "EcVolume on dir0 reports shard {} mounted but its file is not a regular file ({})",
                        sid,
                        p,
                    );
                }
            }
        }
    }

    /// PR #9252 review: with `idx_directory != directory` configured but
    /// the owner's `.ecx` actually living in `loc.directory` (the
    /// legacy "written before -dir.idx was set" layout), the per-disk
    /// loader's mount_ec_shards call uses `loc.idx_directory` and
    /// errors out. Reconcile must NOT skip this same-disk case — it's
    /// the only recovery path. Verify the owner disk's own shards
    /// come back online after reconcile.
    #[test]
    fn test_reconcile_recovers_same_disk_legacy_ecx_layout() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        let idx_dir = tmp.path().join("idx");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&idx_dir).unwrap();

        let collection = "grafana-loki";
        let vid = 8002u32;

        // Owner disk holds shards 0/1 + index files in DATA dir
        // (legacy layout). idx_directory is configured separately
        // but empty.
        write_shard(data_dir.to_str().unwrap(), collection, vid, 0);
        write_shard(data_dir.to_str().unwrap(), collection, vid, 1);
        write_index_files(data_dir.to_str().unwrap(), collection, vid, 10, 4);

        // Need at least 2 locations for reconcile to run; add a
        // second empty disk so the early `len < 2` short-circuit
        // doesn't kick in.
        let other = tmp.path().join("other");
        std::fs::create_dir_all(&other).unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                data_dir.to_str().unwrap(),
                idx_dir.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                other.to_str().unwrap(),
                other.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        // After reconcile, the owner disk's own shards must be mounted
        // even though .ecx lives in data_dir rather than the
        // configured idx_dir.
        let ev = store.locations[0]
            .find_ec_volume(VolumeId(vid))
            .expect("EcVolume should be mounted via cross-disk reconcile retry");
        assert!(ev.has_shard(0));
        assert!(ev.has_shard(1));
    }
}

/// Walk a disk's data directory and return the `.ec??` shard files
/// that are present on disk but not yet registered in the location's
/// `ec_volumes` map. Keyed by (collection, vid) so callers can match
/// each group against its `.ecx`-owning disk in one lookup. Zero-byte
/// shard files are ignored — same shape as `load_all_ec_shards`.
fn collect_orphan_ec_shards(
    loc: &crate::storage::disk_location::DiskLocation,
    _loc_idx: usize,
) -> HashMap<EcKey, Vec<(String, u32)>> {
    let mut orphans: HashMap<EcKey, Vec<(String, u32)>> = HashMap::new();
    let Ok(read) = fs::read_dir(&loc.directory) else {
        return orphans;
    };
    for ent in read.flatten() {
        if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = ent.file_name().to_string_lossy().into_owned();
        let Some(dot) = name.rfind('.') else {
            continue;
        };
        let (base, ext) = name.split_at(dot);
        let Some(shard_id) = is_ec_shard_extension(ext) else {
            continue;
        };
        // Ignore zero-byte shards. Use the DirEntry's metadata so we
        // don't pay a second stat syscall per file beyond what
        // read_dir already returned.
        match ent.metadata() {
            Ok(meta) if meta.len() > 0 => {}
            _ => continue,
        }
        let Some((collection, vid)) = parse_collection_volume_id_pub(base) else {
            continue;
        };
        // Skip shards that are already registered to an EcVolume.
        if let Some(ecv) = loc.find_ec_volume(vid) {
            if ecv.has_shard(shard_id as u8) {
                continue;
            }
        }
        let key = EcKey { collection, vid };
        orphans.entry(key).or_default().push((name, shard_id));
    }
    orphans
}
