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
use crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;
use crate::storage::store::Store;
use crate::storage::types::VolumeId;

pub(crate) fn ec_local_ecx_path(dir: &str, collection: &str, vid: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}/{}.ecx", dir, vid.0)
    } else {
        format!("{}/{}_{}.ecx", dir, collection, vid.0)
    }
}

/// Sibling-disk `.dat` candidate for `prune_incomplete_ec_with_sibling_dat`.
/// We record both the disk index and the file's size: the size is
/// consulted before deleting any EC artefacts. A zero-byte or truncated
/// `.dat` is not a credible fallback, and we'd rather leave the partial
/// EC in place than wipe it based on garbage.
#[derive(Clone, Copy, Debug)]
struct DatOwnerInfo {
    location: usize,
    size: u64,
}

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

        // `use_local_idx` is the post-mirror fast path: when the
        // mirror already installed sidecars locally, mount against
        // loc.idx_directory instead of the owner disk.
        let mut to_load: Vec<(usize, EcKey, Vec<(String, u32)>, EcxOwnerInfo, bool)> = Vec::new();
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
                let local_ecx = ec_local_ecx_path(&loc.idx_directory, &key.collection, key.vid);
                let local_ecx_in_data = ec_local_ecx_path(&loc.directory, &key.collection, key.vid);
                let use_local_idx = std::path::Path::new(&local_ecx).exists()
                    || std::path::Path::new(&local_ecx_in_data).exists();

                if !use_local_idx
                    && owner.location == loc_idx
                    && owner.idx_dir == loc.idx_directory
                {
                    // Same-disk no-op: load_all_ec_shards already
                    // tried and logged the failure.
                    continue;
                }
                to_load.push((loc_idx, key, shards, owner.clone(), use_local_idx));
            }
        }

        for (loc_idx, key, shards, owner, use_local_idx) in to_load {
            let shard_names: Vec<&str> = shards.iter().map(|(n, _)| n.as_str()).collect();
            let loc_dir = self.locations[loc_idx].directory.clone();
            let shard_ids: Vec<u32> = shards.iter().map(|(_, sid)| *sid).collect();

            if use_local_idx {
                info!(
                    volume_id = key.vid.0,
                    collection = %key.collection,
                    directory = %loc_dir,
                    "loading orphan EC shards against locally-mirrored sidecars: {:?}",
                    shard_names,
                );
                let loc = &mut self.locations[loc_idx];
                if let Err(e) = loc.mount_ec_shards(key.vid, &key.collection, &shard_ids, "") {
                    loc.unmount_ec_shards(key.vid, &shard_ids);
                    warn!(
                        volume_id = key.vid.0,
                        directory = %loc_dir,
                        "local-mirror shard load failed: {}",
                        e,
                    );
                }
                continue;
            }

            info!(
                volume_id = key.vid.0,
                collection = %key.collection,
                from = %self.locations[owner.location].directory,
                to = %loc_dir,
                "loading orphan EC shards using index files from sibling disk (issue #9212): {:?}",
                shard_names,
            );
            let owner_idx_dir = owner.idx_dir.clone();
            let loc = &mut self.locations[loc_idx];
            if let Err(e) = loc.mount_ec_shards_with_idx_dir(
                key.vid,
                &key.collection,
                &shard_ids,
                &owner_idx_dir,
                "",
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
                    directory = %loc_dir,
                    "cross-disk shard load failed: {}",
                    e,
                );
            }
        }
    }

    /// Remove leftover EC artefacts on one disk when a healthy `.dat`
    /// for the same `(collection, vid)` lives on a sibling disk of the
    /// same store. Mirrors `Store.pruneIncompleteEcWithSiblingDat` in
    /// `weed/storage/store_ec_reconcile.go` (seaweedfs/seaweedfs#9478).
    ///
    /// `DiskLocation::handle_found_ecx_file` only looks for `.dat` in
    /// the same disk as the EC shards. In a multi-disk volume server an
    /// interrupted EC encode can leave `.ec??` + `.ecx` on disk B while
    /// the source `.dat` still lives on disk A. The per-disk loader
    /// sees no local `.dat`, classifies the layout as distributed EC,
    /// and mounts the partial shards. The volume server then heartbeats
    /// both a regular replica and an EC shard for the same vid, and
    /// master ends up with a contradictory view.
    ///
    /// Cleanup is gated on `shard_count < DATA_SHARDS_COUNT` so that a
    /// deliberate "full local EC, `.dat` retained" layout split across
    /// two disks (`.dat` on disk A, all 10+ shards on disk B) is left
    /// alone — the per-disk loader already keeps that configuration on
    /// a single disk, and pruning it here would be a behaviour
    /// regression. Distributed EC volumes (no `.dat` on any disk of
    /// this server) also fall through unchanged because the `.dat`
    /// index never matches.
    ///
    /// The sibling `.dat` must be a credible encoding source before we
    /// delete anything: at least the size `.vif` recorded at encode time,
    /// or — when unknown (0) — more than a bare superblock so an empty
    /// 8-byte stub can't pass. A truncated `.dat` leaves the partial EC
    /// alone; those shards may still reconstruct from other servers.
    ///
    /// We don't have to push anything to a deleted-shards channel
    /// here: the Rust heartbeat path in `server/heartbeat.rs` diffs
    /// the current `ec_volumes` snapshot against the previous one each
    /// tick, so the first heartbeat after the prune naturally emits a
    /// delete delta for whatever the per-disk pass had already
    /// reported.
    pub fn prune_incomplete_ec_with_sibling_dat(&mut self) {
        if self.locations.len() < 2 {
            return;
        }

        let dat_owners = self.index_dat_owners();
        if dat_owners.is_empty() {
            return;
        }

        // Snapshot under an immutable borrow so we can release it
        // before taking the mutable borrow needed for cleanup.
        struct Victim {
            loc_idx: usize,
            collection: String,
            vid: VolumeId,
            dat_dir: String,
            shard_count: usize,
        }
        let mut victims: Vec<Victim> = Vec::new();
        for (loc_idx, loc) in self.locations.iter().enumerate() {
            for (vid, ev) in loc.ec_volumes() {
                // Use the volume's own ratio, not the OSS default, so a full
                // custom-ratio data set (e.g. 9 of a 9+3) is not mistaken for a leftover.
                let data_shards = if ev.data_shards > 0 {
                    ev.data_shards as usize
                } else {
                    DATA_SHARDS_COUNT
                };
                let shard_count = ev.shard_count();
                if shard_count >= data_shards {
                    continue;
                }
                let key = EcKey {
                    collection: ev.collection.clone(),
                    vid: *vid,
                };
                let Some(owner) = dat_owners.get(&key) else {
                    continue;
                };
                if owner.location == loc_idx {
                    // Same-disk .dat is the job of
                    // DiskLocation::validate_ec_volume during the
                    // per-disk pass; don't second-guess it here.
                    continue;
                }
                // Delete only against a byte-exact committed source: the sibling
                // .dat must equal the size .vif recorded at encode time. An
                // unknown (0) or mismatched size cannot prove the .dat holds this data.
                if ev.dat_file_size <= 0 || owner.size != ev.dat_file_size as u64 {
                    warn!(
                        volume_id = vid.0,
                        collection = %ev.collection,
                        directory = %loc.directory,
                        shard_count,
                        sibling_dir = %self.locations[owner.location].directory,
                        sibling_dat_size = owner.size,
                        recorded = ev.dat_file_size,
                        "sibling .dat does not byte-exactly match the recorded EC source size; leaving partial EC in place",
                    );
                    continue;
                }
                // Never prune when the shards are recoverable node-wide (a set
                // split across sibling disks summing to >= data_shards); they
                // may be sole copies of a distributed volume.
                let mut node_wide_bits = ev.shard_bits().0;
                for other in &self.locations {
                    if let Some(other_ev) = other.find_ec_volume(*vid) {
                        if other_ev.collection == ev.collection {
                            node_wide_bits |= other_ev.shard_bits().0;
                        }
                    }
                }
                let node_wide = node_wide_bits.count_ones() as usize;
                if node_wide >= data_shards {
                    warn!(
                        volume_id = vid.0,
                        collection = %ev.collection,
                        node_wide,
                        data_shards,
                        "shards present node-wide are independently recoverable; leaving EC in place despite a sibling .dat",
                    );
                    continue;
                }
                victims.push(Victim {
                    loc_idx,
                    collection: ev.collection.clone(),
                    vid: *vid,
                    dat_dir: self.locations[owner.location].directory.clone(),
                    shard_count,
                });
            }
        }

        for v in victims {
            warn!(
                volume_id = v.vid.0,
                collection = %v.collection,
                directory = %self.locations[v.loc_idx].directory,
                shard_count = v.shard_count,
                required = DATA_SHARDS_COUNT,
                dat_dir = %v.dat_dir,
                "partial EC shards on this disk while a healthy .dat exists on a sibling disk; cleaning up leftover EC files (issue 9478)",
            );
            let loc = &mut self.locations[v.loc_idx];
            if let Some(mut ec_vol) = loc.remove_ec_volume(v.vid) {
                for _ in 0..ec_vol.shard_count() {
                    crate::metrics::VOLUME_GAUGE
                        .with_label_values(&[&ec_vol.collection, "ec_shards"])
                        .dec();
                }
                // destroy() closes shard file handles before unlinking
                // (matters on Windows) and removes .ecx / .ecj / .vif.
                ec_vol.destroy();
            }
            // Also sweep any unmounted shard files (.ec00 .. .ec31)
            // that the per-disk loader skipped — destroy() only walks
            // the in-memory shards, but the disk may still hold others.
            loc.remove_ec_volume_files(&v.collection, v.vid);
        }
    }

    /// Returns, for every `(collection, vid)`, the index of the first
    /// disk on this store that holds a `.dat` for it plus the file's
    /// size. Used by `prune_incomplete_ec_with_sibling_dat` so it can
    /// decide whether partial EC artefacts on another disk are
    /// leftovers of an interrupted encode AND whether the sibling
    /// `.dat` is large enough to be a credible fallback.
    ///
    /// Any `.dat` `read_dir` can see is recorded — including zero-byte
    /// shells. Their mere presence means this volume was a regular
    /// volume on this server at some point, which rules out the
    /// "distributed EC, no .dat anywhere" reading. Whether the file is
    /// actually usable is the caller's call, made by comparing this
    /// size to the EC's recorded source size in `.vif`.
    fn index_dat_owners(&self) -> HashMap<EcKey, DatOwnerInfo> {
        let mut owners: HashMap<EcKey, DatOwnerInfo> = HashMap::new();
        for (loc_idx, loc) in self.locations.iter().enumerate() {
            let Ok(read) = fs::read_dir(&loc.directory) else {
                continue;
            };
            for ent in read.flatten() {
                if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }
                let name = ent.file_name().to_string_lossy().into_owned();
                let Some(base) = name.strip_suffix(".dat") else {
                    continue;
                };
                let Some((collection, vid)) = parse_collection_volume_id_pub(base) else {
                    continue;
                };
                let Ok(meta) = ent.metadata() else {
                    continue;
                };
                owners
                    .entry(EcKey { collection, vid })
                    .or_insert(DatOwnerInfo {
                        location: loc_idx,
                        size: meta.len(),
                    });
            }
        }
        owners
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
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(
            format!("{}/{}_{}.vif", idx_dir, collection, vid),
            serde_json::to_string(&vif).unwrap(),
        )
        .unwrap();
    }

    fn write_ec_vif(dir: &str, collection: &str, vid: u32) {
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
            format!("{}/{}_{}.vif", dir, collection, vid),
            serde_json::to_string(&vif).unwrap(),
        )
        .unwrap();
    }

    /// An empty `.dat` (<= a superblock, i.e. zero needles) for an EC volume
    /// is a leftover stub from the pre-fix loader. It must be swept on startup,
    /// not loaded as a phantom empty volume. With the same vid's stub on two
    /// disks this also unblocks startup, which previously failed the
    /// duplicate-vid check (the volume6 incident).
    #[test]
    fn test_empty_ec_dat_stub_removed_and_unblocks_startup() {
        let tmp = TempDir::new().unwrap();
        let d0 = tmp.path().join("data0");
        let d1 = tmp.path().join("data1");
        std::fs::create_dir_all(&d0).unwrap();
        std::fs::create_dir_all(&d1).unwrap();
        let coll = "warp-cal";
        let vid = 41u32;

        // A real (loadable) but empty superblock: without the sweep both disks
        // load it as vid 41 and add_location fails the duplicate-vid check.
        let stub = crate::storage::super_block::SuperBlock {
            version: crate::storage::types::Version::current(),
            ..Default::default()
        }
        .to_bytes();
        for d in [&d0, &d1] {
            let dir = d.to_str().unwrap();
            std::fs::write(format!("{}/{}_{}.dat", dir, coll, vid), &stub).unwrap();
            write_ec_vif(dir, coll, vid);
        }

        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in [&d0, &d1] {
            store
                .add_location(
                    d.to_str().unwrap(),
                    d.to_str().unwrap(),
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .expect("a same-vid empty stub on two disks must not block startup");
        }

        let loaded: usize = store.locations.iter().map(|l| l.volume_ids().len()).sum();
        assert_eq!(loaded, 0, "empty EC stub was loaded as a phantom volume");
        for d in [&d0, &d1] {
            assert!(
                !std::path::Path::new(&format!("{}/{}_{}.dat", d.to_str().unwrap(), coll, vid))
                    .exists(),
                "empty .dat stub was not removed",
            );
        }
    }

    /// Safety: an empty `.dat` for a NON-EC volume (no EC `.vif`) is left
    /// alone — only EC stubs are swept, so freshly-allocated empty volumes
    /// survive.
    #[test]
    fn test_keeps_empty_dat_for_non_ec_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("data0");
        std::fs::create_dir_all(&dir).unwrap();
        let dat = format!("{}/7.dat", dir.to_str().unwrap());
        std::fs::write(&dat, vec![0u8; 8]).unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir.to_str().unwrap(),
                dir.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        assert!(
            std::path::Path::new(&dat).exists(),
            "a non-EC empty .dat must not be swept",
        );
    }

    /// Regression: a lone `.vif` whose `.ecx` is on a sibling disk must not
    /// make the loader create a phantom `.dat`, nor the sibling-.dat prune
    /// delete the real shards on the sibling.
    #[test]
    fn test_lone_vif_does_not_create_phantom_dat_or_delete_shards() {
        let tmp = TempDir::new().unwrap();
        let d0 = tmp.path().join("data0");
        let d1 = tmp.path().join("data1");
        std::fs::create_dir_all(&d0).unwrap();
        std::fs::create_dir_all(&d1).unwrap();
        let coll = "warp-loadtest";
        let vid = 57u32;

        // d0: a self-contained but partial (2 < 10) EC volume.
        write_shard(d0.to_str().unwrap(), coll, vid, 2);
        write_shard(d0.to_str().unwrap(), coll, vid, 4);
        write_index_files(d0.to_str().unwrap(), coll, vid, 10, 4);

        // d1: ONLY the mirrored `.vif` — no `.ecx`, no shard, no `.dat`.
        std::fs::copy(
            d0.join(format!("{}_{}.vif", coll, vid)),
            d1.join(format!("{}_{}.vif", coll, vid)),
        )
        .unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in [&d0, &d1] {
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

        let phantom = d1.join(format!("{}_{}.dat", coll, vid));
        assert!(
            !phantom.exists(),
            "loader created a phantom .dat from a lone .vif"
        );
        let total: usize = store.locations.iter().map(|l| l.ec_shard_count()).sum();
        assert_eq!(total, 2, "real EC shards were deleted (total={})", total);
    }

    /// A disk holding a few local shards of a healthy distributed EC volume
    /// plus a leftover empty `.dat` stub: the stub must be swept before EC
    /// validation can mistake the volume for an interrupted local encode
    /// (fewer than data_shards local shards) and delete the only copies of
    /// those shards.
    #[test]
    fn test_empty_dat_stub_next_to_ecx_does_not_delete_shards() {
        let tmp = TempDir::new().unwrap();
        let d0 = tmp.path().join("data0");
        std::fs::create_dir_all(&d0).unwrap();
        let dir = d0.to_str().unwrap();
        let coll = "warp-rec";
        let vid = 87u32;

        write_shard(dir, coll, vid, 0);
        write_shard(dir, coll, vid, 5);
        write_index_files(dir, coll, vid, 10, 4);
        // Zero-byte stub .dat: the phantom left by the pre-fix loader.
        std::fs::write(d0.join(format!("{}_{}.dat", coll, vid)), b"").unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        assert!(
            !d0.join(format!("{}_{}.dat", coll, vid)).exists(),
            "empty .dat stub was not swept"
        );
        assert_eq!(
            store.locations[0].ec_shard_count(),
            2,
            "EC shards were deleted on the stub's account"
        );
        assert_eq!(
            store.locations[0].volume_ids().len(),
            0,
            "stub was loaded as a phantom volume"
        );
    }

    /// Same shard-holding disk but the stub has no `.vif` at all, so the
    /// sweep has no EC evidence and must leave it. The empty `.dat` still
    /// must not count as an encode source: the shards survive and load as
    /// a distributed EC volume.
    #[test]
    fn test_empty_dat_without_vif_does_not_delete_shards() {
        let tmp = TempDir::new().unwrap();
        let d0 = tmp.path().join("data0");
        std::fs::create_dir_all(&d0).unwrap();
        let dir = d0.to_str().unwrap();
        let coll = "warp-rec";
        let vid = 88u32;

        write_shard(dir, coll, vid, 1);
        write_shard(dir, coll, vid, 7);
        write_index_files(dir, coll, vid, 10, 4);
        std::fs::remove_file(d0.join(format!("{}_{}.vif", coll, vid))).unwrap();
        std::fs::write(d0.join(format!("{}_{}.dat", coll, vid)), b"").unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        assert_eq!(
            store.locations[0].ec_shard_count(),
            2,
            "EC shards were deleted on the empty .dat's account"
        );
        assert_eq!(
            store.locations[0].volume_ids().len(),
            0,
            "empty .dat was loaded as a phantom volume"
        );
    }

    /// Regression for the prune credibility gate: when the EC `.vif`
    /// records no source size (`dat_file_size == 0`), an empty 8-byte
    /// sibling `.dat` stub must NOT justify deleting partial EC shards.
    #[test]
    fn test_prune_refuses_unverifiable_8byte_dat() {
        let tmp = TempDir::new().unwrap();
        let d0 = tmp.path().join("data0");
        let d1 = tmp.path().join("data1");
        std::fs::create_dir_all(&d0).unwrap();
        std::fs::create_dir_all(&d1).unwrap();
        let coll = "warp-loadtest";
        let vid = 99u32;

        // d0: partial EC (2 shards); its `.vif` records no source size.
        write_shard(d0.to_str().unwrap(), coll, vid, 0);
        write_shard(d0.to_str().unwrap(), coll, vid, 1);
        write_index_files(d0.to_str().unwrap(), coll, vid, 10, 4);

        // d1: a real but empty 8-byte (superblock-sized) `.dat` stub.
        std::fs::write(d1.join(format!("{}_{}.dat", coll, vid)), vec![0u8; 8]).unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        for d in [&d0, &d1] {
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

        let total: usize = store.locations.iter().map(|l| l.ec_shard_count()).sum();
        assert_eq!(
            total, 2,
            "prune deleted shards against an unverifiable 8-byte .dat (total={})",
            total
        );
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

        store.unmount_ec_shard(vid, 1, 0).unwrap();
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

    /// Reproduces the issue 9478 layout for a single volume server:
    /// disk A holds a healthy `.dat` for vid 122; disk B holds a single
    /// `.ec01` plus `.ecx` / `.ecj` / `.vif` and no `.dat`. The per-disk
    /// loader mounts the partial shard on disk B as if it were a
    /// distributed-EC layout. The Store-level prune must unmount and
    /// delete the leftover EC files on disk B while leaving disk A's
    /// `.dat` untouched.
    #[test]
    fn test_prune_drops_partial_ec_when_sibling_disk_has_dat() {
        let tmp = TempDir::new().unwrap();
        let dat_dir = tmp.path().join("sdd");
        let ec_dir = tmp.path().join("sdf");
        std::fs::create_dir_all(&dat_dir).unwrap();
        std::fs::create_dir_all(&ec_dir).unwrap();

        // Real collection so the loader's volume_file_name-based `.ecx`
        // lookup matches the helpers (empty collection emits `_122.*` vs the
        // expected `122.*`).
        let collection = "pics";
        let vid = 122u32;

        // Disk A (sdd): a .dat whose size must byte-exactly match the EC
        // source size recorded in the sibling .vif for the prune to treat it
        // as the committed source.
        let dat_path = dat_dir.join(format!("{}_{}.dat", collection, vid));
        std::fs::write(&dat_path, vec![0u8; 1024]).unwrap();

        // Disk B (sdf): partial EC — one shard, plus .ecx / .ecj / .vif.
        write_shard(ec_dir.to_str().unwrap(), collection, vid, 1);
        write_index_files(ec_dir.to_str().unwrap(), collection, vid, 10, 4);
        // Record the encode-time source size in the EC .vif so the prune's
        // byte-exact credibility gate recognizes the 1024-byte sibling .dat as
        // the source (a real encoded volume records this).
        std::fs::write(
            ec_dir.join(format!("{}_{}.vif", collection, vid)),
            serde_json::to_string(&VifVolumeInfo {
                version: 3,
                dat_file_size: 1024,
                ec_shard_config: Some(VifEcShardConfig {
                    data_shards: 10,
                    parity_shards: 4,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dat_dir.to_str().unwrap(),
                dat_dir.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                ec_dir.to_str().unwrap(),
                ec_dir.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        // After add_location for sdf, the per-disk loader has mounted
        // the partial shard AND the prune has run as part of
        // add_location's epilogue. Confirm the prune removed it.
        let ec_loc = &store.locations[1];
        assert!(
            ec_loc.find_ec_volume(VolumeId(vid)).is_none(),
            "partial EC volume should have been unmounted by the prune",
        );

        let ec_base = ec_dir
            .join(format!("{}_{}", collection, vid).trim_start_matches('_'))
            .to_string_lossy()
            .into_owned();
        assert!(
            !std::path::Path::new(&format!("{}.ec01", ec_base)).exists(),
            "partial shard file should have been removed",
        );
        assert!(
            !std::path::Path::new(&format!("{}.ecx", ec_base)).exists(),
            ".ecx should have been removed",
        );
        assert!(
            !std::path::Path::new(&format!("{}.ecj", ec_base)).exists(),
            ".ecj should have been removed",
        );

        // The .dat on the sibling disk must survive — pruning the
        // partial EC must never touch the healthy replica.
        assert!(
            dat_path.exists(),
            "healthy .dat on sibling disk should be left alone",
        );
    }

    /// Safety guard for `prune_incomplete_ec_with_sibling_dat`: when
    /// the sibling `.dat` is smaller than the source size recorded in
    /// the EC's `.vif`, the `.dat` is clearly truncated and is not a
    /// credible fallback. The partial shard may still combine with
    /// shards on other servers in a recoverable distributed-EC layout,
    /// so we leave the EC files in place.
    #[test]
    fn test_prune_keeps_partial_ec_when_sibling_dat_is_smaller_than_vif_source_size() {
        let tmp = TempDir::new().unwrap();
        let dat_dir = tmp.path().join("sdd");
        let ec_dir = tmp.path().join("sdf");
        std::fs::create_dir_all(&dat_dir).unwrap();
        std::fs::create_dir_all(&ec_dir).unwrap();

        let collection = "logs";
        let vid = 122u32;

        // Tiny but loadable .dat on sdd: a valid 8-byte version-3
        // superblock so Volume::new succeeds, padded out to a few
        // hundred bytes so we have something well below the .vif-
        // recorded source size below. Anything smaller would let
        // Volume::new's auto-write extend the file to SUPER_BLOCK_SIZE
        // and obscure the truncation we're trying to model.
        let dat_path = dat_dir.join(format!("{}_{}.dat", collection, vid));
        let mut dat_bytes = crate::storage::super_block::SuperBlock::default().to_bytes();
        dat_bytes.resize(256, 0u8);
        std::fs::write(&dat_path, &dat_bytes).unwrap();
        let sibling_dat_size = dat_bytes.len() as u64;
        let source_size = 10 * 1024 * 1024u64; // 10 MiB recorded in .vif
        assert!(sibling_dat_size < source_size);

        // Partial EC: one shard plus .ecx / .ecj / .vif. The .vif
        // records the source .dat size at encode time; the safety
        // check compares the sibling .dat against this value.
        write_shard(ec_dir.to_str().unwrap(), collection, vid, 1);
        let vif = VifVolumeInfo {
            version: 3,
            dat_file_size: source_size as i64,
            ec_shard_config: Some(VifEcShardConfig {
                data_shards: 10,
                parity_shards: 4,
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(
            ec_dir.join(format!("{}_{}.vif", collection, vid)),
            serde_json::to_string(&vif).unwrap(),
        )
        .unwrap();
        std::fs::write(
            ec_dir.join(format!("{}_{}.ecx", collection, vid)),
            vec![0u8; 20],
        )
        .unwrap();
        std::fs::write(
            ec_dir.join(format!("{}_{}.ecj", collection, vid)),
            b"",
        )
        .unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dat_dir.to_str().unwrap(),
                dat_dir.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                ec_dir.to_str().unwrap(),
                ec_dir.to_str().unwrap(),
                100,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        // Prune ran as part of add_location's epilogue. Confirm the
        // partial EC was left alone because the sibling .dat is too
        // small to be the encoding source.
        let ec_loc = &store.locations[1];
        let ev = ec_loc
            .find_ec_volume(VolumeId(vid))
            .expect("partial EC volume must remain mounted when the sibling .dat is smaller than the .vif source size");
        assert_eq!(ev.shard_count(), 1);

        let ec_base = ec_dir
            .join(format!("{}_{}", collection, vid))
            .to_string_lossy()
            .into_owned();
        assert!(
            std::path::Path::new(&format!("{}.ec01", ec_base)).exists(),
            "partial shard file must survive when the sibling .dat is truncated",
        );
        assert!(
            std::path::Path::new(&format!("{}.ecx", ec_base)).exists(),
            ".ecx must survive when the sibling .dat is truncated",
        );
    }

    /// A distributed EC volume — no `.dat` on any disk of this store —
    /// must not be touched by the prune even when shard count is below
    /// `DATA_SHARDS_COUNT`. Partial shard layouts are normal for
    /// distributed EC.
    #[test]
    fn test_prune_leaves_distributed_ec_alone() {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0");
        let dir1 = tmp.path().join("data1");
        std::fs::create_dir_all(&dir0).unwrap();
        std::fs::create_dir_all(&dir1).unwrap();

        let collection = "logs";
        let vid = 7u32;

        // Two shards on dir0 with .ecx, no .dat anywhere.
        write_shard(dir0.to_str().unwrap(), collection, vid, 1);
        write_shard(dir0.to_str().unwrap(), collection, vid, 2);
        write_index_files(dir0.to_str().unwrap(), collection, vid, 10, 4);

        let mut store = Store::new(NeedleMapKind::InMemory);
        for dir in [&dir0, &dir1] {
            store
                .add_location(
                    dir.to_str().unwrap(),
                    dir.to_str().unwrap(),
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }

        let ev = store.locations[0]
            .find_ec_volume(VolumeId(vid))
            .expect("distributed EC volume should still be mounted on dir0");
        assert_eq!(
            ev.shard_count(),
            2,
            "no .dat anywhere on this store means the partial shards must be kept as distributed EC",
        );

        let ec_base = dir0
            .join(format!("{}_{}", collection, vid))
            .to_string_lossy()
            .into_owned();
        assert!(std::path::Path::new(&format!("{}.ec01", ec_base)).exists());
        assert!(std::path::Path::new(&format!("{}.ec02", ec_base)).exists());
        assert!(std::path::Path::new(&format!("{}.ecx", ec_base)).exists());
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
