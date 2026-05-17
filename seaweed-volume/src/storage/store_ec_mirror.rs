//! Physical EC sidecar mirroring across disks of the same volume
//! server. Mirrors `weed/storage/store_ec_mirror.go`.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

use tracing::{info, warn};

use crate::storage::disk_location::{parse_collection_volume_id_pub, DiskLocation};
use crate::storage::store::Store;
use crate::storage::types::VolumeId;

// Listed in `EcVolume::new`'s open order.
const EC_MIRRORED_SIDECARS: &[&str] = &[".ecx", ".ecj", ".vif"];

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EcKey {
    collection: String,
    vid: VolumeId,
}

#[derive(Clone, Debug)]
struct EcxOwner {
    location: usize,
    idx_dir: String,
    data_dir: String,
}

impl Store {
    /// Mirror EC sidecars onto every shard-bearing disk that lacks
    /// them, so each disk mounts self-contained. Runs before
    /// `reconcile_ec_shards_across_disks` so the orphan pass can
    /// prefer the local idx_directory.
    pub fn mirror_ec_metadata_to_shard_disks(&mut self) {
        if self.locations.len() < 2 {
            return;
        }
        let owners = self.index_ecx_owners_for_mirror();
        if owners.is_empty() {
            return;
        }

        // Two-pass: gather work under an immutable borrow, then apply
        // copies under independent mutable borrows.
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

    // Records both idx_dir (where .ecx was found) and data_dir, so
    // the mirror can resolve .vif from data_dir even when .ecx lives
    // in idx_directory.
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

// Checks the modern routing and the opposite directory — without
// that fallback, a destination with a legacy pre-`-dir.idx` .ecx in
// its data dir would be re-mirrored into idx_directory.
fn disk_has_all_sidecars(loc: &DiskLocation, collection: &str, vid: VolumeId) -> bool {
    for ext in EC_MIRRORED_SIDECARS {
        let primary = sidecar_dest_path(&loc.directory, &loc.idx_directory, collection, vid, ext);
        if path_is_regular_file(&primary) {
            continue;
        }
        if loc.idx_directory != loc.directory {
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

fn path_is_regular_file(path: &str) -> bool {
    fs::metadata(path).map(|m| !m.is_dir()).unwrap_or(false)
}

// `.ecx`/`.ecj` route to idx_directory, `.vif` to directory.
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
        // An existing local copy is authoritative — it may be newer
        // than the owner's after a delete journal append.
        if fs::metadata(&dst).is_ok() {
            continue;
        }

        let candidates = [
            sidecar_dest_path(src_data_dir, src_idx_dir, collection, vid, ext),
            sidecar_dest_path(src_idx_dir, src_data_dir, collection, vid, ext),
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
