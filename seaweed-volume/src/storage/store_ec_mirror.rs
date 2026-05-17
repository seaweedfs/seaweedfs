//! Physical EC sidecar mirroring across disks of the same volume server.
//!
//! Mirrors `weed/storage/store_ec_mirror.go`. The EC lifecycle
//! (encode / decode / balance / vacuum / repair) promises a same-disk
//! layout: every shard lives alongside its own `.ecx` / `.ecj` /
//! `.vif` so the `EcVolume` can mount self-contained. Without this
//! pass, a disk that received shards through `ec.balance` or
//! `ec.rebuild` — where only the first shard carries
//! `copy_ecx_file=true` and subsequent shards land via auto-select —
//! can end up with the `.ec??` files but no local sidecars. The
//! orphan reconcile then mounts the shards by pointing the EcVolume
//! at the sibling disk's index files; reads work, but any failure on
//! the index-owning disk (a removed drive, a corrupted fs, an
//! operator who unmounted the wrong sled) takes the shards with it.
//!
//! This module physically replicates the sidecars onto each
//! shard-bearing disk so the shards stay readable as long as their
//! own disk is up. Runs before `reconcile_ec_shards_across_disks` at
//! boot; the cross-disk fallback is preserved for cases where
//! mirroring fails (read-only target, out of space, partial copy) so
//! the volume stays available.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

use tracing::{info, warn};

use crate::storage::disk_location::{parse_collection_volume_id_pub, DiskLocation};
use crate::storage::store::Store;
use crate::storage::types::VolumeId;

/// EC sidecars that must travel with the shards. Order matches
/// `EcVolume::new`'s open sequence so a partial mirror is diagnosable
/// by walking the list and stopping at the first destination that
/// does not yet have its copy.
const EC_MIRRORED_SIDECARS: &[&str] = &[".ecx", ".ecj", ".vif"];

/// Key for matching shard-bearing disks to their sidecar owners.
/// Per-collection because two collections may share a volume id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EcKey {
    collection: String,
    vid: VolumeId,
}

/// Records both the disk index that owns the `.ecx` and the actual
/// directory it lives in. The directory matters because
/// `index_ecx_owners` scans both `idx_directory` and `directory` —
/// when `.ecx` lives in `directory` (legacy pre-`-dir.idx` layout),
/// we want to read the file from where it actually is.
#[derive(Clone, Debug)]
struct EcxOwner {
    location: usize,
    idx_dir: String,
    data_dir: String,
}

impl Store {
    /// Mirror EC sidecars onto every disk of this store that holds
    /// shards but is missing the matching `.ecx` / `.ecj` / `.vif`.
    /// Idempotent: a destination that already has the sidecar is
    /// left untouched, and a missing source sidecar is not an error.
    ///
    /// Runs before `reconcile_ec_shards_across_disks` so the
    /// downstream orphan-shard pass sees the freshly-mirrored layout
    /// and can mount each disk against its own `idx_directory`.
    pub fn mirror_ec_metadata_to_shard_disks(&mut self) {
        if self.locations.len() < 2 {
            return;
        }
        let owners = self.index_ecx_owners_for_mirror();
        if owners.is_empty() {
            return;
        }

        // Two-pass to satisfy the borrow checker: gather work under
        // an immutable borrow, then apply copies disk-by-disk under
        // independent mutable borrows.
        struct Mirror<'a> {
            target_idx: usize,
            owner: &'a EcxOwner,
            collection: String,
            vid: VolumeId,
        }
        let mut mirrors: Vec<Mirror> = Vec::new();
        for (loc_idx, loc) in self.locations.iter().enumerate() {
            let orphans = collect_shard_disk_volumes(loc);
            for (key, _shards) in orphans {
                let Some(owner) = owners.get(&key) else {
                    continue;
                };
                if owner.location == loc_idx {
                    continue;
                }
                if disk_has_all_sidecars(loc, &key.collection, key.vid) {
                    continue;
                }
                mirrors.push(Mirror {
                    target_idx: loc_idx,
                    owner,
                    collection: key.collection,
                    vid: key.vid,
                });
            }
        }

        for m in mirrors {
            let loc_dir = self.locations[m.target_idx].directory.clone();
            let loc_idx_dir = self.locations[m.target_idx].idx_directory.clone();
            match mirror_sidecars_for_volume(
                &m.owner.idx_dir,
                &m.owner.data_dir,
                &loc_dir,
                &loc_idx_dir,
                &m.collection,
                m.vid,
            ) {
                Ok(0) => {}
                Ok(copied) => {
                    info!(
                        volume_id = m.vid.0,
                        collection = %m.collection,
                        from = %m.owner.data_dir,
                        to = %loc_dir,
                        copied,
                        "mirrored EC sidecar(s) for same-disk invariant",
                    );
                }
                Err(e) => {
                    warn!(
                        volume_id = m.vid.0,
                        collection = %m.collection,
                        from = %m.owner.data_dir,
                        to = %loc_dir,
                        error = %e,
                        "mirror EC sidecars failed; cross-disk fallback will handle this volume",
                    );
                }
            }
        }
    }

    /// Build a `(collection, vid) -> EcxOwner` map of which disk owns
    /// the `.ecx` and which directory it actually lives in.
    /// Counterpart to `index_ecx_owners` in `store_ec_reconcile.rs`;
    /// kept private here so the mirror is self-contained and can
    /// record both `idx_dir` and `data_dir` for source-side fallback
    /// when `.vif` lives in the data dir but `.ecx` is in the idx
    /// dir (or vice versa).
    fn index_ecx_owners_for_mirror(&self) -> HashMap<EcKey, EcxOwner> {
        let mut owners: HashMap<EcKey, EcxOwner> = HashMap::new();
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
                    owners
                        .entry(EcKey { collection, vid })
                        .or_insert_with(|| EcxOwner {
                            location: loc_idx,
                            idx_dir: scan.to_string(),
                            data_dir: loc.directory.clone(),
                        });
                }
            }
        }
        owners
    }
}

/// True iff the destination disk already has every EC sidecar that
/// the mirror would otherwise install. Checks the modern routing
/// first (idx_directory for .ecx/.ecj, directory for .vif) and then
/// the opposite directory as a fallback — matching
/// `EcVolume::new`'s own open-with-fallback contract. Without the
/// fallback, a destination that still has a legacy pre-`-dir.idx`
/// .ecx in its data dir would be re-mirrored into idx_directory and
/// end up with two copies on disk.
fn disk_has_all_sidecars(loc: &DiskLocation, collection: &str, vid: VolumeId) -> bool {
    for ext in EC_MIRRORED_SIDECARS {
        let primary = sidecar_dest_path(&loc.directory, &loc.idx_directory, collection, vid, ext);
        if path_is_regular_file(&primary) {
            continue;
        }
        if loc.idx_directory != loc.directory {
            // .ecx/.ecj could be in directory (pre-`-dir.idx`), .vif
            // could be in idx_directory (uncommon but allowed by
            // EcVolume::new's .vif lookup).
            let (fallback_data, fallback_idx) = if *ext == ".vif" {
                (loc.idx_directory.as_str(), loc.directory.as_str())
            } else {
                (loc.directory.as_str(), loc.directory.as_str())
            };
            let fallback = sidecar_dest_path(fallback_data, fallback_idx, collection, vid, ext);
            if path_is_regular_file(&fallback) {
                continue;
            }
        }
        return false;
    }
    true
}

/// Existence + regular-file check rolled into one boolean. Used by
/// the sidecar-presence ladder above so the fallback logic stays
/// readable.
fn path_is_regular_file(path: &str) -> bool {
    fs::metadata(path).map(|m| !m.is_dir()).unwrap_or(false)
}

/// Routing: `.ecx` / `.ecj` go to `idx_directory`, `.vif` goes to
/// `directory`. Matches `EcVolume::new`'s lookup order.
fn sidecar_dest_path(
    data_dir: &str,
    idx_dir: &str,
    collection: &str,
    vid: VolumeId,
    ext: &str,
) -> String {
    let dir = if ext == ".vif" { data_dir } else { idx_dir };
    if collection.is_empty() {
        format!("{}/{}{}", dir, vid.0, ext)
    } else {
        format!("{}/{}_{}{}", dir, collection, vid.0, ext)
    }
}

/// Mirror the three EC sidecars from one disk to another. Returns
/// the count of files actually copied (skipping pre-existing
/// destinations and missing optional sources). Errors abort on the
/// first failure so the caller's warn-and-continue handles partial
/// state.
fn mirror_sidecars_for_volume(
    src_idx_dir: &str,
    src_data_dir: &str,
    dst_data_dir: &str,
    dst_idx_dir: &str,
    collection: &str,
    vid: VolumeId,
) -> io::Result<usize> {
    let mut copied = 0usize;
    for ext in EC_MIRRORED_SIDECARS {
        let dst = sidecar_dest_path(dst_data_dir, dst_idx_dir, collection, vid, ext);
        if fs::metadata(&dst).is_ok() {
            continue;
        }
        let candidates = [
            sidecar_dest_path(src_data_dir, src_idx_dir, collection, vid, ext),
            sidecar_dest_path(src_idx_dir, src_data_dir, collection, vid, ext), // legacy swap
        ];
        let mut src_path: Option<String> = None;
        for c in candidates.iter() {
            if fs::metadata(c).map(|m| !m.is_dir()).unwrap_or(false) {
                src_path = Some(c.clone());
                break;
            }
        }
        let Some(src) = src_path else {
            continue;
        };
        copy_sidecar_atomic(Path::new(&src), Path::new(&dst))?;
        copied += 1;
    }
    Ok(copied)
}

/// Copy a single sidecar atomically: write to `<dst>.mirror.tmp`,
/// fsync, rename. A pre-existing `<dst>.mirror.tmp` from a previous
/// interrupted boot is removed first so `O_EXCL` doesn't refuse the
/// open.
fn copy_sidecar_atomic(src: &Path, dst: &Path) -> io::Result<()> {
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut src_file = fs::File::open(src)?;
    let tmp = {
        let mut s = dst.as_os_str().to_owned();
        s.push(".mirror.tmp");
        std::path::PathBuf::from(s)
    };
    let _ = fs::remove_file(&tmp);
    let mut dst_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)?;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = src_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst_file.write_all(&buf[..n])?;
    }
    dst_file.sync_all()?;
    drop(dst_file);
    if let Err(e) = fs::rename(&tmp, dst) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

/// Walk this disk's data directory and return (collection, vid)
/// groups that have any `.ec??` files on disk. Used by the mirror
/// pass to find candidate destinations.
fn collect_shard_disk_volumes(loc: &DiskLocation) -> HashMap<EcKey, Vec<String>> {
    let mut out: HashMap<EcKey, Vec<String>> = HashMap::new();
    let Ok(read) = fs::read_dir(&loc.directory) else {
        return out;
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
        if crate::storage::disk_location::is_ec_shard_extension(ext).is_none() {
            continue;
        }
        match ent.metadata() {
            Ok(meta) if meta.len() > 0 => {}
            _ => continue,
        }
        let Some((collection, vid)) = parse_collection_volume_id_pub(base) else {
            continue;
        };
        out.entry(EcKey { collection, vid })
            .or_default()
            .push(name);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MinFreeSpace;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::types::DiskType;
    use crate::storage::volume::{VifEcShardConfig, VifVolumeInfo};
    use tempfile::TempDir;

    fn plant_shard(dir: &Path, collection: &str, vid: u32, shard_id: u8) {
        let path = if collection.is_empty() {
            dir.join(format!("{}.ec{:02}", vid, shard_id))
        } else {
            dir.join(format!("{}_{}.ec{:02}", collection, vid, shard_id))
        };
        fs::write(&path, b"shard data nonempty").unwrap();
    }

    fn plant_ecx(dir: &Path, collection: &str, vid: u32, bytes: &[u8]) {
        let path = dir.join(format!("{}_{}.ecx", collection, vid));
        fs::write(&path, bytes).unwrap();
    }

    fn plant_ecj(dir: &Path, collection: &str, vid: u32, bytes: &[u8]) {
        let path = dir.join(format!("{}_{}.ecj", collection, vid));
        fs::write(&path, bytes).unwrap();
    }

    fn plant_vif(dir: &Path, collection: &str, vid: u32, data_shards: u32, parity_shards: u32) {
        let vif = VifVolumeInfo {
            version: 3,
            ec_shard_config: Some(VifEcShardConfig {
                data_shards,
                parity_shards,
            }),
            ..Default::default()
        };
        let path = dir.join(format!("{}_{}.vif", collection, vid));
        fs::write(&path, serde_json::to_string(&vif).unwrap()).unwrap();
    }

    fn add_loc(store: &mut Store, dir: &Path) {
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

    /// dir0: orphan shards. dir1: shard 1 + every sidecar. After
    /// add_location finishes, dir0 must have its own copy of every
    /// sidecar so the EcVolume there mounts self-contained.
    #[test]
    fn mirror_copies_sidecars_to_shard_only_disk() {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0");
        let dir1 = tmp.path().join("data1");
        fs::create_dir_all(&dir0).unwrap();
        fs::create_dir_all(&dir1).unwrap();

        let collection = "video-recordings";
        let vid = 4121u32;

        plant_shard(&dir0, collection, vid, 0);
        plant_shard(&dir0, collection, vid, 12);
        plant_shard(&dir1, collection, vid, 1);

        let ecx = vec![0xA1u8; 20];
        let ecj = vec![0xB2u8; 16];
        plant_ecx(&dir1, collection, vid, &ecx);
        plant_ecj(&dir1, collection, vid, &ecj);
        plant_vif(&dir1, collection, vid, 10, 4);

        let mut store = Store::new(NeedleMapKind::InMemory);
        add_loc(&mut store, &dir0);
        add_loc(&mut store, &dir1);

        for ext in [".ecx", ".ecj", ".vif"] {
            let dst = dir0.join(format!("{}_{}{}", collection, vid, ext));
            assert!(
                dst.exists(),
                "mirror did not install sidecar {} on dir0",
                ext
            );
        }
        let ecx_dst = fs::read(dir0.join(format!("{}_{}.ecx", collection, vid))).unwrap();
        assert_eq!(ecx_dst, ecx, ".ecx mirrored bytes differ from source");
        let ecj_dst = fs::read(dir0.join(format!("{}_{}.ecj", collection, vid))).unwrap();
        assert_eq!(ecj_dst, ecj, ".ecj mirrored bytes differ from source");
    }

    /// dir0 already has its own sidecars (different .ecx bytes than
    /// dir1's). The mirror must NOT overwrite them.
    #[test]
    fn mirror_preserves_existing_destination_sidecars() {
        let tmp = TempDir::new().unwrap();
        let dir0 = tmp.path().join("data0");
        let dir1 = tmp.path().join("data1");
        fs::create_dir_all(&dir0).unwrap();
        fs::create_dir_all(&dir1).unwrap();

        let collection = "video-recordings";
        let vid = 7777u32;

        plant_shard(&dir0, collection, vid, 0);
        plant_shard(&dir0, collection, vid, 12);
        plant_shard(&dir1, collection, vid, 1);

        let ecx_owner = vec![0xC3u8; 20];
        let ecx_local = vec![0x5Au8; 20];
        let ecj_bytes = vec![0xD4u8; 16];
        plant_ecx(&dir1, collection, vid, &ecx_owner);
        plant_ecj(&dir1, collection, vid, &ecj_bytes);
        plant_vif(&dir1, collection, vid, 10, 4);
        plant_ecx(&dir0, collection, vid, &ecx_local);
        plant_ecj(&dir0, collection, vid, &ecj_bytes);
        plant_vif(&dir0, collection, vid, 10, 4);

        let mut store = Store::new(NeedleMapKind::InMemory);
        add_loc(&mut store, &dir0);
        add_loc(&mut store, &dir1);

        let post = fs::read(dir0.join(format!("{}_{}.ecx", collection, vid))).unwrap();
        assert_eq!(post, ecx_local, "mirror overwrote dir0's existing .ecx");
    }
}
