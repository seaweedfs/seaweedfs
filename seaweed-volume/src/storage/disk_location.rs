//! DiskLocation: manages volumes on a single disk/directory.
//!
//! Each DiskLocation represents one storage directory containing .dat + .idx files.
//! A Store contains one or more DiskLocations (one per configured directory).
//! Matches Go's storage/disk_location.go.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::warn;

use crate::config::MinFreeSpace;
use crate::storage::erasure_coding::ec_shard::{
    EcVolumeShard, DATA_SHARDS_COUNT, ERASURE_CODING_LARGE_BLOCK_SIZE,
    ERASURE_CODING_SMALL_BLOCK_SIZE,
};
use crate::storage::erasure_coding::ec_volume::EcVolume;
use crate::storage::needle_map::NeedleMapKind;
use crate::storage::super_block::{ReplicaPlacement, SUPER_BLOCK_SIZE};
use crate::storage::types::*;
use crate::storage::volume::{
    remove_volume_files, volume_file_name, VifVolumeInfo, Volume, VolumeError,
};

/// A single disk location managing volumes in one directory.
pub struct DiskLocation {
    pub directory: String,
    pub idx_directory: String,
    pub directory_uuid: String,
    pub disk_type: DiskType,
    pub tags: Vec<String>,
    pub max_volume_count: AtomicI32,
    pub original_max_volume_count: i32,
    volumes: HashMap<VolumeId, Volume>,
    ec_volumes: HashMap<VolumeId, EcVolume>,
    pub is_disk_space_low: Arc<AtomicBool>,
    pub available_space: AtomicU64,
    pub min_free_space: MinFreeSpace,
}

impl DiskLocation {
    const UUID_FILE_NAME: &'static str = "vol_dir.uuid";

    pub fn new(
        directory: &str,
        idx_directory: &str,
        max_volume_count: i32,
        disk_type: DiskType,
        min_free_space: MinFreeSpace,
        tags: Vec<String>,
    ) -> io::Result<Self> {
        fs::create_dir_all(directory)?;

        let idx_dir = if idx_directory.is_empty() {
            directory.to_string()
        } else {
            fs::create_dir_all(idx_directory)?;
            idx_directory.to_string()
        };
        let directory_uuid = Self::generate_directory_uuid(directory)?;

        Ok(DiskLocation {
            directory: directory.to_string(),
            idx_directory: idx_dir,
            directory_uuid,
            disk_type,
            tags,
            max_volume_count: AtomicI32::new(max_volume_count),
            original_max_volume_count: max_volume_count,
            volumes: HashMap::new(),
            ec_volumes: HashMap::new(),
            is_disk_space_low: Arc::new(AtomicBool::new(false)),
            available_space: AtomicU64::new(0),
            min_free_space,
        })
    }

    fn generate_directory_uuid(directory: &str) -> io::Result<String> {
        let path = std::path::Path::new(directory).join(Self::UUID_FILE_NAME);
        if path.exists() {
            let existing = fs::read_to_string(&path)?;
            if !existing.trim().is_empty() {
                return Ok(existing);
            }
        }

        let dir_uuid = uuid::Uuid::new_v4().to_string();
        fs::write(path, &dir_uuid)?;
        Ok(dir_uuid)
    }

    // ---- Volume management ----

    /// Load existing volumes from the directory.
    ///
    /// Matches Go's `loadExistingVolume`: checks for incomplete volumes (.note file),
    /// validates EC shards before skipping .dat loading, and cleans up stale
    /// compaction temp files (.cpd/.cpx).
    pub fn load_existing_volumes(&mut self, needle_map_kind: NeedleMapKind) -> io::Result<()> {
        // Ensure directory exists
        fs::create_dir_all(&self.directory)?;
        if self.directory != self.idx_directory {
            fs::create_dir_all(&self.idx_directory)?;
        }

        // Recover any interrupted compaction commit before the volume scan. This
        // must run here, not inside the .dat loop: that loop is keyed on .dat
        // files and would miss the marker-only or already-renamed-.idx states a
        // mid-commit crash can leave behind.
        self.reconcile_compact_states();

        // Scan for .dat files
        let entries = fs::read_dir(&self.directory)?;
        let mut dat_files: Vec<(String, VolumeId)> = Vec::new();
        let mut seen = HashSet::new();

        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap_or_default();
            if let Some((collection, vid)) = parse_volume_filename(&name) {
                if seen.insert((collection.clone(), vid)) {
                    dat_files.push((collection, vid));
                }
            }
        }

        for (collection, vid) in dat_files {
            let volume_name = volume_file_name(&self.directory, &collection, vid);
            let idx_name = volume_file_name(&self.idx_directory, &collection, vid);

            // Sweep a leftover empty `.dat` stub (a phantom from the pre-fix
            // loader) before it loads as a phantom volume or blocks startup.
            if remove_empty_ec_dat_stub(&volume_name, &idx_name, vid) {
                continue;
            }

            // If valid EC shards exist (.ecx file present), skip loading .dat
            let ecx_path = format!("{}.ecx", idx_name);
            let ecx_exists = if std::path::Path::new(&ecx_path).exists() {
                true
            } else if self.idx_directory != self.directory {
                // .ecx may have been created before -dir.idx was configured
                let fallback = format!("{}.ecx", volume_name);
                std::path::Path::new(&fallback).exists()
            } else {
                false
            };
            if ecx_exists {
                if self.validate_ec_volume(&collection, vid) {
                    // Valid EC volume — don't load .dat
                    continue;
                } else {
                    warn!(
                        volume_id = vid.0,
                        "EC volume validation failed, removing incomplete EC files"
                    );
                    self.remove_ec_volume_files(&collection, vid);
                    // Fall through to load .dat file
                }
            }

            // Stale .cpd/.cpx temp files were already rolled forward or back by
            // the reconcile_compact_states pre-pass above.

            // Check for an incomplete volume (.note means a VolumeCopy was
            // interrupted). This runs BELOW the empty-stub sweep and EC
            // validation: when an .ecx for this vid coexists on the disk, the
            // regular and EC volumes share <base>.vif, so removing the
            // incomplete regular copy must keep the .vif (keep_vif=true) or it
            // would strip the EC volume's info file.
            let note_path = format!("{}.note", volume_name);
            if std::path::Path::new(&note_path).exists() {
                let note = fs::read_to_string(&note_path).unwrap_or_default();
                warn!(
                    volume_id = vid.0,
                    "volume was not completed: {}, removing files", note
                );
                // Re-check .ecx now (not the pre-validation ecx_exists): the
                // invalid-EC cleanup above may have removed it, in which case
                // the .vif is no longer shared and must not be preserved.
                let keep_vif = std::path::Path::new(&ecx_path).exists()
                    || (self.idx_directory != self.directory
                        && std::path::Path::new(&format!("{}.ecx", volume_name)).exists());
                remove_volume_files(&volume_name, keep_vif);
                remove_volume_files(&idx_name, keep_vif);
                continue;
            }

            // Skip if already loaded (e.g., from a previous call)
            if self.volumes.contains_key(&vid) {
                continue;
            }

            // Load existing data only; never create a phantom `.dat`. A lone
            // `.vif`/`.idx` (e.g. an EC sidecar whose `.ecx` is on a sibling
            // disk) would otherwise have Volume::new write an 8-byte stub that
            // the sibling-.dat prune deletes real shards against. Remote-tiered
            // volumes also have no local `.dat`, but their `.vif` points at
            // remote files and must still load via the remote path.
            // Plain existence check (not check_dat_file_exists): an empty
            // .dat here is a fresh, needle-less volume that must still load.
            // A stat error other than NotFound counts as present so a
            // transient error doesn't skip a real volume.
            let dat_path = format!("{}.dat", volume_name);
            let dat_missing = matches!(fs::metadata(&dat_path),
                Err(ref e) if e.kind() == io::ErrorKind::NotFound);
            if dat_missing
                && !vif_references_remote_file(&format!("{}.vif", volume_name))
                && !vif_references_remote_file(&format!("{}.vif", idx_name))
            {
                continue;
            }

            match Volume::new(
                &self.directory,
                &self.idx_directory,
                &collection,
                vid,
                needle_map_kind,
                None, // replica placement read from superblock
                None, // TTL read from superblock
                0,    // no preallocate on load
                Version::current(),
            ) {
                Ok(mut v) => {
                    v.location_disk_space_low = self.is_disk_space_low.clone();
                    crate::metrics::VOLUME_GAUGE
                        .with_label_values(&[&collection, "volume"])
                        .inc();
                    self.volumes.insert(vid, v);
                }
                Err(e) => {
                    warn!(volume_id = vid.0, error = %e, "failed to load volume");
                }
            }
        }

        // After regular volumes, auto-discover EC shards on disk so a
        // fresh restart picks up shards without an explicit
        // VolumeEcShardsMount RPC. Mirrors Go's loadExistingVolumes
        // calling loadAllEcShards. Failures here only log — they
        // shouldn't fail the whole disk's startup.
        if let Err(e) = self.load_all_ec_shards() {
            warn!(directory = %self.directory, error = %e, "load_all_ec_shards failed");
        }

        Ok(())
    }

    /// Directory pre-pass that recovers interrupted compaction commits. Collects
    /// every volume id that still has a .cpc commit marker or a leftover
    /// .cpd/.cpx temp file across the data and idx directories, then rolls the
    /// swap forward (marker present) or back (marker absent) per volume.
    fn reconcile_compact_states(&self) {
        let mut pending: HashSet<(String, VolumeId)> = HashSet::new();
        let mut collect = |dir: &str| {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name().into_string().unwrap_or_default();
                    let stem = name
                        .strip_suffix(".cpc")
                        .or_else(|| name.strip_suffix(".cpd"))
                        .or_else(|| name.strip_suffix(".cpx"));
                    if let Some(stem) = stem {
                        if let Some(key) = parse_collection_volume_id(stem) {
                            pending.insert(key);
                        }
                    }
                }
            }
        };
        collect(&self.directory);
        if self.idx_directory != self.directory {
            collect(&self.idx_directory);
        }

        for (collection, vid) in pending {
            // On a runtime reload (SIGHUP), an already-loaded volume may be
            // mid-vacuum: its .cpd/.cpx are live, not crash leftovers, and
            // rolling them back would clobber the in-flight compaction. Only
            // reconcile vids not currently loaded; genuine startup recovery
            // runs before any volume is loaded.
            if self.volumes.contains_key(&vid) {
                continue;
            }
            let v = Volume::new_unloaded(&self.directory, &self.idx_directory, &collection, vid);
            if let Err(e) = v.reconcile_compact_state() {
                warn!(volume_id = vid.0, error = %e, "reconcile interrupted compaction failed");
            }
        }
    }

    /// Reports whether the EC files for (collection, vid) on this disk may be
    /// deleted to reclaim the local .dat. Returns false (delete) only when that
    /// provably loses no data; every ambiguity returns true (keep), since the
    /// shards may be the only copy. Mirrors Go's validateEcVolume.
    fn validate_ec_volume(&self, collection: &str, vid: VolumeId) -> bool {
        let base = volume_file_name(&self.directory, collection, vid);
        let dat_path = format!("{}.dat", base);

        // Custom ratio comes from the volume's own .vif; the server holds no
        // cluster EC config in memory.
        let data_shards =
            ec_data_shards_from_vif(&self.directory, &self.idx_directory, collection, vid);

        // On-disk .dat: an empty <= superblock one is a stub; a transient stat
        // error keeps the shards rather than deleting.
        let mut expected_shard_size: Option<i64> = None;
        let dat_exists = match fs::metadata(&dat_path) {
            Ok(meta) if meta.len() > SUPER_BLOCK_SIZE as u64 => {
                expected_shard_size =
                    Some(calculate_expected_shard_size(meta.len() as i64, data_shards));
                true
            }
            Ok(_) => false,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => {
                warn!(volume_id = vid.0, error = %e, "cannot stat .dat; keeping EC shards");
                return true;
            }
        };

        let mut shard_count = 0usize;
        let mut actual_shard_size: Option<i64> = None;
        const MAX_SHARD_COUNT: usize = 32;

        for i in 0..MAX_SHARD_COUNT {
            let shard_path = format!("{}.ec{:02}", base, i);
            match fs::metadata(&shard_path) {
                Ok(meta) if meta.len() > 0 => {
                    let size = meta.len() as i64;
                    if let Some(prev) = actual_shard_size {
                        if size != prev {
                            // Inconsistent sizes signal corruption or mixed
                            // generations; not trusted for deletion -> keep.
                            warn!(volume_id = vid.0, shard = i, size, expected = prev, "EC shard size mismatch; keeping shards");
                            return true;
                        }
                    } else {
                        actual_shard_size = Some(size);
                    }
                    shard_count += 1;
                }
                Err(e) if e.kind() != io::ErrorKind::NotFound => {
                    warn!(volume_id = vid.0, shard = i, error = %e, "cannot stat EC shard; keeping shards");
                    return true;
                }
                _ => {} // not found or zero size — skip
            }
        }

        if !dat_exists {
            return true; // distributed EC; any shard count is valid
        }

        // Reclaim only when it loses no data. Shards smaller than this .dat's
        // full encode are an interrupted encode whose .dat is the complete
        // source -> reclaim. Shards >= expected (valid/distributing EC, or a
        // stale/partial .dat beside larger real shards) may be the only copy -> keep.
        if shard_count == 0 {
            return false;
        }
        if let (Some(actual), Some(expected)) = (actual_shard_size, expected_shard_size) {
            if actual < expected {
                warn!(volume_id = vid.0, actual, expected, "shards smaller than the .dat's full encode; reclaiming the complete .dat");
                return false;
            }
        }
        true
    }

    /// Remove all EC-related files for a volume.
    /// `pub(crate)` so the Store-level cross-disk passes in
    /// `store_ec_reconcile.rs` can call it to scrub partial EC artefacts
    /// when a healthy `.dat` for the same vid lives on a sibling disk
    /// (seaweedfs/seaweedfs#9478).
    pub(crate) fn remove_ec_volume_files(&self, collection: &str, vid: VolumeId) {
        let base = volume_file_name(&self.directory, collection, vid);
        let idx_base = volume_file_name(&self.idx_directory, collection, vid);
        const MAX_SHARD_COUNT: usize = 32;

        // Remove index files from idx directory (.ecx, .ecj)
        let _ = fs::remove_file(format!("{}.ecx", idx_base));
        let _ = fs::remove_file(format!("{}.ecj", idx_base));
        // Also try data directory in case .ecx/.ecj were created before -dir.idx was configured
        if self.idx_directory != self.directory {
            let _ = fs::remove_file(format!("{}.ecx", base));
            let _ = fs::remove_file(format!("{}.ecj", base));
        }

        // Remove all EC shard files (.ec00 ~ .ec31)
        for i in 0..MAX_SHARD_COUNT {
            let _ = fs::remove_file(format!("{}.ec{:02}", base, i));
        }
    }

    /// Full-teardown variant: everything remove_ec_volume_files clears, PLUS the
    /// normal-volume <base>.vif on a SHARD-ONLY node. An interrupted shard copy
    /// (which installs shards + .ecx before .vif) could otherwise mount a fresh
    /// generation under the prior run's identity / ratio / dat_file_size carried by
    /// a stale .vif. Gated on .idx absence so a source-volume holder keeps its live
    /// .vif. Reconcile/load-fallback call remove_ec_volume_files directly and
    /// intentionally preserve it, mirroring Go's removeEcVolumeFiles (reconcile) vs
    /// removeStaleEcArtifacts (teardown) split.
    pub(crate) fn remove_ec_volume_files_full_teardown(&self, collection: &str, vid: VolumeId) {
        self.remove_ec_volume_files(collection, vid);
        let base = volume_file_name(&self.directory, collection, vid);
        let idx_base = volume_file_name(&self.idx_directory, collection, vid);
        // try_exists, not exists(): a stat error on a PRESENT .idx must not read as
        // "shard-only" and delete a live source volume's .vif. Only Ok(false) (genuine
        // NotFound) clears it; on a stat error stay conservative and keep the sidecar.
        if matches!(
            std::path::Path::new(&format!("{}.idx", idx_base)).try_exists(),
            Ok(false)
        ) {
            let _ = fs::remove_file(format!("{}.vif", idx_base));
            if self.idx_directory != self.directory
                && matches!(
                    std::path::Path::new(&format!("{}.idx", base)).try_exists(),
                    Ok(false)
                )
            {
                let _ = fs::remove_file(format!("{}.vif", base));
            }
        }
    }

    /// Find a volume by ID.
    pub fn find_volume(&self, vid: VolumeId) -> Option<&Volume> {
        self.volumes.get(&vid)
    }

    /// Find a volume by ID (mutable).
    pub fn find_volume_mut(&mut self, vid: VolumeId) -> Option<&mut Volume> {
        self.volumes.get_mut(&vid)
    }

    /// Add a volume to this location.
    pub fn set_volume(&mut self, vid: VolumeId, volume: Volume) {
        let collection = volume.collection.clone();
        self.volumes.insert(vid, volume);
        crate::metrics::VOLUME_GAUGE
            .with_label_values(&[&collection, "volume"])
            .inc();
    }

    /// Create a new volume in this location.
    pub fn create_volume(
        &mut self,
        vid: VolumeId,
        collection: &str,
        needle_map_kind: NeedleMapKind,
        replica_placement: Option<ReplicaPlacement>,
        ttl: Option<crate::storage::needle::ttl::TTL>,
        preallocate: u64,
        version: Version,
    ) -> Result<(), VolumeError> {
        let mut v = Volume::new(
            &self.directory,
            &self.idx_directory,
            collection,
            vid,
            needle_map_kind,
            replica_placement,
            ttl,
            preallocate,
            version,
        )?;
        v.location_disk_space_low = self.is_disk_space_low.clone();
        crate::metrics::VOLUME_GAUGE
            .with_label_values(&[collection, "volume"])
            .inc();
        self.volumes.insert(vid, v);
        Ok(())
    }

    /// Remove and close a volume.
    pub fn unload_volume(&mut self, vid: VolumeId) -> Option<Volume> {
        if let Some(mut v) = self.volumes.remove(&vid) {
            crate::metrics::VOLUME_GAUGE
                .with_label_values(&[&v.collection, "volume"])
                .dec();
            v.close();
            Some(v)
        } else {
            None
        }
    }

    /// Remove, close, and delete all files for a volume. When keep_remote_data
    /// is true the cloud-tier object backing the volume is left intact — used
    /// by moves where another server is taking over the same .vif.
    pub fn delete_volume(
        &mut self,
        vid: VolumeId,
        only_empty: bool,
        keep_remote_data: bool,
    ) -> Result<(), VolumeError> {
        if let Some(mut v) = self.volumes.remove(&vid) {
            crate::metrics::VOLUME_GAUGE
                .with_label_values(&[&v.collection, "volume"])
                .dec();
            v.destroy(only_empty, keep_remote_data)?;
            Ok(())
        } else {
            Err(VolumeError::NotFound)
        }
    }

    /// Delete all volumes in a collection.
    pub fn delete_collection(&mut self, collection: &str) -> Result<(), VolumeError> {
        let vids: Vec<VolumeId> = self
            .volumes
            .iter()
            .filter(|(_, v)| v.collection == collection && !v.is_compacting())
            .map(|(vid, _)| *vid)
            .collect();

        for vid in vids {
            if let Some(mut v) = self.volumes.remove(&vid) {
                crate::metrics::VOLUME_GAUGE
                    .with_label_values(&[&v.collection, "volume"])
                    .dec();
                if let Err(e) = v.destroy(false, false) {
                    warn!(volume_id = vid.0, error = %e, "delete collection: failed to destroy volume");
                }
            }
        }

        let ec_vids: Vec<VolumeId> = self
            .ec_volumes
            .iter()
            .filter(|(_, v)| v.collection == collection)
            .map(|(vid, _)| *vid)
            .collect();

        for vid in ec_vids {
            if let Some(mut ec_vol) = self.ec_volumes.remove(&vid) {
                for _ in 0..ec_vol.shard_count() {
                    crate::metrics::VOLUME_GAUGE
                        .with_label_values(&[collection, "ec_shards"])
                        .dec();
                }
                ec_vol.destroy();
            }
        }
        Ok(())
    }

    // ---- Metrics ----

    /// Number of volumes on this disk.
    pub fn volumes_len(&self) -> usize {
        self.volumes.len()
    }

    /// Get all volume IDs, sorted.
    pub fn volume_ids(&self) -> Vec<VolumeId> {
        let mut ids: Vec<VolumeId> = self.volumes.keys().copied().collect();
        ids.sort();
        ids
    }

    /// Iterate over all volumes.
    pub fn iter_volumes(&self) -> impl Iterator<Item = (&VolumeId, &Volume)> {
        self.volumes.iter()
    }

    /// Number of free volume slots.
    /// Matches Go's FindFreeLocation formula:
    ///   free = ((MaxVolumeCount - VolumesLen()) * DataShardsCount - EcShardCount()) / DataShardsCount
    pub fn free_volume_count(&self) -> i32 {
        use crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;
        let max = self.max_volume_count.load(Ordering::Relaxed);
        let free_count = (max as i64 - self.volumes.len() as i64)
            * DATA_SHARDS_COUNT as i64
            - self.ec_shard_count() as i64;
        let effective_free = free_count / DATA_SHARDS_COUNT as i64;
        if effective_free > 0 {
            effective_free as i32
        } else {
            0
        }
    }

    /// Iterate over all volumes.
    pub fn volumes(&self) -> impl Iterator<Item = (&VolumeId, &Volume)> {
        self.volumes.iter()
    }

    /// Iterate over all volumes (mutable).
    pub fn volumes_mut(&mut self) -> impl Iterator<Item = (&VolumeId, &mut Volume)> {
        self.volumes.iter_mut()
    }

    /// Sum of unused space in writable volumes (volumeSizeLimit - actual size per volume).
    /// Used by auto-max-volume-count to estimate how many more volumes can fit.
    pub fn unused_space(&self, volume_size_limit: u64) -> u64 {
        let mut unused: u64 = 0;
        for vol in self.volumes.values() {
            if vol.is_read_only() {
                continue;
            }
            let dat_size = vol.dat_file_size().unwrap_or(0);
            let idx_size = vol.idx_file_size();
            let used = dat_size + idx_size;
            if volume_size_limit > used {
                unused += volume_size_limit - used;
            }
        }
        unused
    }

    /// Check disk space against min_free_space and update is_disk_space_low.
    pub fn check_disk_space(&self) {
        let (total, free) = get_disk_stats(&self.directory);
        if total == 0 {
            return;
        }
        let used = total.saturating_sub(free);
        let is_low = match &self.min_free_space {
            MinFreeSpace::Percent(pct) => {
                let free_pct = (free as f64 / total as f64) * 100.0;
                free_pct < *pct
            }
            MinFreeSpace::Bytes(min_bytes) => free < *min_bytes,
        };
        self.is_disk_space_low.store(is_low, Ordering::Relaxed);
        self.available_space.store(free, Ordering::Relaxed);

        // Update resource gauges
        crate::metrics::RESOURCE_GAUGE
            .with_label_values(&[&self.directory, "all"])
            .set(total as f64);
        crate::metrics::RESOURCE_GAUGE
            .with_label_values(&[&self.directory, "used"])
            .set(used as f64);
        crate::metrics::RESOURCE_GAUGE
            .with_label_values(&[&self.directory, "free"])
            .set(free as f64);
        // "avail" is same as "free" for us (Go subtracts reserved blocks but we use statvfs f_bavail)
        crate::metrics::RESOURCE_GAUGE
            .with_label_values(&[&self.directory, "avail"])
            .set(free as f64);
    }

    // ---- EC volume operations ----

    /// Find an EC volume by ID.
    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<&EcVolume> {
        self.ec_volumes.get(&vid)
    }

    /// Find an EC volume by ID (mutable).
    pub fn find_ec_volume_mut(&mut self, vid: VolumeId) -> Option<&mut EcVolume> {
        self.ec_volumes.get_mut(&vid)
    }

    /// Check if this location has an EC volume.
    pub fn has_ec_volume(&self, vid: VolumeId) -> bool {
        self.ec_volumes.contains_key(&vid)
    }

    /// Reports whether this disk has a sealed `.ecx` index file for the
    /// given (collection, vid). Unlike [`Self::has_ec_volume`] this does
    /// not require the EC volume to be mounted in memory, which makes it
    /// the right primitive for placement decisions during `ec.balance` /
    /// `ec.rebuild` flows where shards may arrive before any
    /// `VolumeEcShardsMount` has happened on the receiving disk. Without
    /// checking the on-disk state, auto-select can split shards from the
    /// `.ecx` that travels with the first shard, which is the source of
    /// the orphan-shard layout reported in seaweedfs/seaweedfs#9212.
    ///
    /// Mirrors `DiskLocation.HasEcxFileOnDisk` in
    /// `weed/storage/disk_location_ec.go`. Skips entries that are
    /// directories so a stray dir named `<collection>_<vid>.ecx` doesn't
    /// register as a present index file.
    pub fn has_ecx_file_on_disk(&self, collection: &str, vid: VolumeId) -> bool {
        let idx_base = volume_file_name(&self.idx_directory, collection, vid);
        let idx_path = format!("{}.ecx", idx_base);
        if let Ok(meta) = fs::metadata(&idx_path) {
            if !meta.is_dir() {
                return true;
            }
        }
        if self.idx_directory != self.directory {
            let data_base = volume_file_name(&self.directory, collection, vid);
            let data_path = format!("{}.ecx", data_base);
            if let Ok(meta) = fs::metadata(&data_path) {
                if !meta.is_dir() {
                    return true;
                }
            }
        }
        false
    }

    /// Remove an EC volume, returning it.
    pub fn remove_ec_volume(&mut self, vid: VolumeId) -> Option<EcVolume> {
        self.ec_volumes.remove(&vid)
    }

    /// Mount EC shards for a volume on this location.
    ///
    /// `source_disk_type` is the source volume's disk type carried on the
    /// `VolumeEcShardsMount` RPC. When non-empty it overrides the in-memory
    /// EC volume's reported disk type so heartbeats keep reporting under
    /// the source's disk type after EC encoding (#9423). Empty means "use
    /// this location's disk type" — used by disk-scan reload paths that
    /// have no orchestrator context.
    pub fn mount_ec_shards(
        &mut self,
        vid: VolumeId,
        collection: &str,
        shard_ids: &[u32],
        source_disk_type: &str,
    ) -> Result<(), VolumeError> {
        let idx_dir = self.idx_directory.clone();
        self.mount_ec_shards_with_idx_dir(vid, collection, shard_ids, &idx_dir, source_disk_type)
    }

    /// Mount EC shards but explicitly specify the idx directory the
    /// EcVolume should pull `.ecx` / `.ecj` / `.vif` from.
    ///
    /// Cross-disk reconcile (mirrors `loadEcShardsWithIdxDir` in
    /// `weed/storage/disk_location_ec.go`): when a volume's shards live
    /// on this disk but the index files were left on a sibling disk
    /// (the seaweedfs/seaweedfs#9212 orphan-shard layout), the
    /// reconciler creates the EcVolume here while pointing it at the
    /// sibling's idx dir. Each shard still ends up registered in this
    /// disk's `ec_volumes` map so heartbeat reporting carries the right
    /// disk_id per shard.
    pub fn mount_ec_shards_with_idx_dir(
        &mut self,
        vid: VolumeId,
        collection: &str,
        shard_ids: &[u32],
        idx_dir: &str,
        source_disk_type: &str,
    ) -> Result<(), VolumeError> {
        let dir = self.directory.clone();
        // Avoid the entry().or_insert_with() pattern here: that closure
        // can't return a Result, so any EcVolume::new failure (e.g.
        // .ecx open error, .ecj create error, malformed .vif) would
        // have to panic via unwrap(). Build the EcVolume up front and
        // propagate the error to the caller.
        if !self.ec_volumes.contains_key(&vid) {
            let ec_vol = EcVolume::new(&dir, idx_dir, collection, vid)
                .map_err(VolumeError::Io)?;
            self.ec_volumes.insert(vid, ec_vol);
        }
        let ec_vol = self
            .ec_volumes
            .get_mut(&vid)
            .expect("just inserted above");
        // When the orchestrator supplied a source disk type on the Mount
        // RPC, override the EC volume's disk type so heartbeats report
        // under the source volume's disk type (#9423). When the caller
        // passed "" (disk-scan reload, orphan-shard reconcile, restart)
        // we only default to this location's disk type for a fresh EC
        // volume; otherwise we leave whatever a prior RPC mount set in
        // place, so empty-source reloads don't reintroduce the drift.
        if !source_disk_type.is_empty() {
            ec_vol.set_disk_type(DiskType::from_string(source_disk_type));
        } else if ec_vol.shard_count() == 0 {
            ec_vol.set_disk_type(self.disk_type.clone());
        }

        for &shard_id in shard_ids {
            let mut shard = EcVolumeShard::new(&dir, collection, vid, shard_id as u8);
            shard.disk_type = ec_vol.disk_type.clone();
            ec_vol.add_shard(shard).map_err(VolumeError::Io)?;
            crate::metrics::VOLUME_GAUGE
                .with_label_values(&[collection, "ec_shards"])
                .inc();
        }
        Ok(())
    }

    /// Unmount EC shards for a volume on this location.
    ///
    /// Only shards that are actually mounted decrement the per-shard
    /// metric — without this guard the gauge would underflow when the
    /// caller passes a shard that lives on a sibling disk
    /// (cross-disk reconcile makes that the common case for the same
    /// `vid` after reconciliation).
    pub fn unmount_ec_shards(&mut self, vid: VolumeId, shard_ids: &[u32]) {
        if let Some(ec_vol) = self.ec_volumes.get_mut(&vid) {
            let collection = ec_vol.collection.clone();
            for &shard_id in shard_ids {
                if !ec_vol.has_shard(shard_id as u8) {
                    continue;
                }
                ec_vol.remove_shard(shard_id as u8);
                crate::metrics::VOLUME_GAUGE
                    .with_label_values(&[&collection, "ec_shards"])
                    .dec();
            }
            if ec_vol.shard_count() == 0 {
                let mut vol = self.ec_volumes.remove(&vid).unwrap();
                vol.close();
            }
        }
    }

    /// Auto-discover .ec?? shard files on disk and mount them.
    ///
    /// Mirrors `loadAllEcShards` in `weed/storage/disk_location_ec.go`.
    /// Reads the data dir (and idx dir, if separate), groups shards by
    /// (collection, vid) using sorted iteration so a volume's shards
    /// land contiguously next to their `.ecx`. When the matching `.ecx`
    /// is encountered, validates and mounts the group.
    ///
    /// Without this, an EC shard on disk is invisible after a Rust
    /// volume-server restart until something explicitly issues
    /// VolumeEcShardsMount — strict superset of seaweedfs/seaweedfs#9212.
    pub fn load_all_ec_shards(&mut self) -> io::Result<()> {
        // Use a HashSet during accumulation so a name appearing in
        // both data dir and idx dir (idempotent legacy layouts can
        // place .ecx in either) is processed exactly once. Without
        // dedup, mount_ec_shards' per-shard metric increment would
        // double-count for those filenames.
        let mut seen: HashSet<String> = HashSet::new();
        let mut entries: Vec<String> = Vec::new();
        for ent in fs::read_dir(&self.directory)? {
            let ent = ent?;
            if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = ent.file_name().to_string_lossy().into_owned();
            if seen.insert(name.clone()) {
                entries.push(name);
            }
        }
        if self.idx_directory != self.directory {
            for ent in fs::read_dir(&self.idx_directory)? {
                let ent = ent?;
                if ent.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    continue;
                }
                let name = ent.file_name().to_string_lossy().into_owned();
                if seen.insert(name.clone()) {
                    entries.push(name);
                }
            }
        }
        entries.sort();

        let mut same_volume_shards: Vec<(String, u32)> = Vec::new(); // (filename, shard_id)
        let mut prev_vid: Option<VolumeId> = None;
        let mut prev_collection: String = String::new();

        for name in entries {
            let Some(dot) = name.rfind('.') else {
                continue;
            };
            let (base, ext) = name.split_at(dot); // ext includes leading '.'
            let Some((collection, vid)) = parse_collection_volume_id(base) else {
                continue;
            };

            if let Some(shard_id) = parse_ec_shard_extension(ext) {
                // Ignore zero-byte shards — same as Go.
                let path = format!("{}/{}", self.directory, name);
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                if size == 0 {
                    continue;
                }

                let same_group = match prev_vid {
                    None => true,
                    Some(p) => p == vid && prev_collection == collection,
                };
                if same_group {
                    same_volume_shards.push((name, shard_id));
                } else {
                    self.check_orphaned_shards(
                        &same_volume_shards,
                        &prev_collection,
                        prev_vid.unwrap_or(VolumeId(0)),
                    );
                    same_volume_shards = vec![(name, shard_id)];
                }
                prev_vid = Some(vid);
                prev_collection = collection;
                continue;
            }

            if ext == ".ecx"
                && prev_vid == Some(vid)
                && prev_collection == collection
                && !same_volume_shards.is_empty()
            {
                let shards = std::mem::take(&mut same_volume_shards);
                self.handle_found_ecx_file(&shards, &collection, vid);
                prev_vid = None;
                prev_collection.clear();
            }
        }

        // Handle any trailing group that never saw a matching .ecx.
        self.check_orphaned_shards(
            &same_volume_shards,
            &prev_collection,
            prev_vid.unwrap_or(VolumeId(0)),
        );

        Ok(())
    }

    /// Validate + mount a (collection, vid) group when its `.ecx` is
    /// found. Mirrors `handleFoundEcxFile` in
    /// `weed/storage/disk_location_ec.go`.
    fn handle_found_ecx_file(&mut self, shards: &[(String, u32)], collection: &str, vid: VolumeId) {
        let base = volume_file_name(&self.directory, collection, vid);
        let dat_path = format!("{}.dat", base);
        let dat_exists = check_dat_file_exists(&dat_path);

        if dat_exists && !self.validate_ec_volume(collection, vid) {
            warn!(
                volume_id = vid.0,
                "Incomplete or invalid EC volume: .dat exists but validation failed, cleaning up EC files",
            );
            self.remove_ec_volume_files(collection, vid);
            return;
        }

        let shard_ids: Vec<u32> = shards.iter().map(|(_, sid)| *sid).collect();
        if let Err(e) = self.mount_ec_shards(vid, collection, &shard_ids, "") {
            // A mount failure (corrupt/locked .ecx, EMFILE, transient I/O) is
            // not proof the shards are disposable -- validate_ec_volume already
            // decided they may be the only copy. Release partially-mounted
            // shards (mirror-decrements the metric) but keep the files; never
            // delete on a load error.
            warn!(
                volume_id = vid.0,
                "Failed to load EC shards: {}; keeping files for retry",
                e,
            );
            self.unmount_ec_shards(vid, &shard_ids);
        }
    }

    /// Mirrors `checkOrphanedShards`: if shards exist on disk without a
    /// matching `.ecx` AND the legacy `.dat` is still present, treat
    /// the encoding as interrupted before `.ecx` was written and clean
    /// up. If `.dat` is gone, leave files alone — they may be
    /// distributed-EC shards waiting for cross-disk reconciliation.
    fn check_orphaned_shards(
        &self,
        shards: &[(String, u32)],
        collection: &str,
        vid: VolumeId,
    ) -> bool {
        if shards.is_empty() || vid.0 == 0 {
            return false;
        }
        let base = volume_file_name(&self.directory, collection, vid);
        let dat_path = format!("{}.dat", base);
        if check_dat_file_exists(&dat_path) {
            warn!(
                volume_id = vid.0,
                "Found {} EC shards without .ecx file (incomplete encoding interrupted before .ecx), cleaning up",
                shards.len(),
            );
            self.remove_ec_volume_files(collection, vid);
            return true;
        }
        false
    }

    /// Total number of EC shards on this location.
    pub fn ec_shard_count(&self) -> usize {
        self.ec_volumes
            .values()
            .map(|ecv| ecv.shards.iter().filter(|s| s.is_some()).count())
            .sum()
    }

    /// Iterate over all EC volumes.
    pub fn ec_volumes(&self) -> impl Iterator<Item = (&VolumeId, &EcVolume)> {
        self.ec_volumes.iter()
    }

    /// Close all volumes.
    pub fn close(&mut self) {
        for (_, v) in self.volumes.iter_mut() {
            v.close();
        }
        self.volumes.clear();
        for (_, mut ec_vol) in self.ec_volumes.drain() {
            ec_vol.close();
        }
    }
}

/// Get total and free disk space for a given path.
/// Returns (total_bytes, free_bytes).
pub fn get_disk_stats(path: &str) -> (u64, u64) {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        let c_path = match CString::new(path) {
            Ok(p) => p,
            Err(_) => return (0, 0),
        };
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
                let all = stat.f_blocks as u64 * stat.f_frsize as u64;
                let free = stat.f_bavail as u64 * stat.f_frsize as u64;
                return (all, free);
            }
        }
        (0, 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        (0, 0)
    }
}

/// Calculate expected EC shard size from .dat file size.
/// Matches Go's `calculateExpectedShardSize`: large blocks (1GB * data_shards) first,
/// then small blocks (1MB * data_shards) for the remainder.
fn calculate_expected_shard_size(dat_file_size: i64, data_shards: usize) -> i64 {
    let large_batch_size = ERASURE_CODING_LARGE_BLOCK_SIZE as i64 * data_shards as i64;
    let num_large_batches = dat_file_size / large_batch_size;
    let mut shard_size = num_large_batches * ERASURE_CODING_LARGE_BLOCK_SIZE as i64;
    let remaining = dat_file_size - (num_large_batches * large_batch_size);

    if remaining > 0 {
        let small_batch_size = ERASURE_CODING_SMALL_BLOCK_SIZE as i64 * data_shards as i64;
        // Ceiling division
        let num_small_batches = (remaining + small_batch_size - 1) / small_batch_size;
        shard_size += num_small_batches * ERASURE_CODING_SMALL_BLOCK_SIZE as i64;
    }

    shard_size
}

/// Resolve the EC data-shard count from the volume's own `.vif` (the volume
/// server never holds the cluster EC config in memory), checking the data dir
/// then the idx dir. Falls back to the default ratio when no EC `.vif` is found.
fn ec_data_shards_from_vif(directory: &str, idx_directory: &str, collection: &str, vid: VolumeId) -> usize {
    for dir in [directory, idx_directory] {
        let vif = format!("{}.vif", volume_file_name(dir, collection, vid));
        if let Some(ds) = fs::read_to_string(&vif)
            .ok()
            .and_then(|s| serde_json::from_str::<VifVolumeInfo>(&s).ok())
            .and_then(|vi| vi.ec_shard_config)
            .map(|c| c.data_shards as usize)
        {
            if ds > 0 {
                return ds;
            }
        }
        if directory == idx_directory {
            break;
        }
    }
    DATA_SHARDS_COUNT
}

/// Parse a volume filename like "collection_42.dat" or "42.dat" into (collection, VolumeId).
/// Parse a `<collection>_<vid>` or `<vid>` base name into its parts.
/// Mirrors `parseCollectionVolumeId` in
/// `weed/storage/disk_location.go`. Used when iterating raw filenames
/// where the extension has already been stripped.
///
/// `pub(crate)` companion `parse_collection_volume_id_pub` is exposed
/// so the cross-disk reconcile in `store_ec_reconcile.rs` can call it
/// without re-implementing the parser.
pub(crate) fn parse_collection_volume_id_pub(base: &str) -> Option<(String, VolumeId)> {
    parse_collection_volume_id(base)
}

fn parse_collection_volume_id(base: &str) -> Option<(String, VolumeId)> {
    if let Some(pos) = base.rfind('_') {
        let collection = &base[..pos];
        let id_str = &base[pos + 1..];
        let id: u32 = id_str.parse().ok()?;
        Some((collection.to_string(), VolumeId(id)))
    } else {
        let id: u32 = base.parse().ok()?;
        Some((String::new(), VolumeId(id)))
    }
}

/// `pub(crate)` re-export of [`parse_ec_shard_extension`] for the
/// cross-disk reconcile in `store_ec_reconcile.rs`.
pub(crate) fn is_ec_shard_extension(ext: &str) -> Option<u32> {
    parse_ec_shard_extension(ext)
}

/// Recognise EC shard extensions `.ec00`–`.ec255` (the ShardId u8
/// range — see `EcVolumeShard`'s typed shard id) and return the
/// shard id. Returns `None` for any other extension.
///
/// Matches the regex `\.ec\d{2,3}` from the Go side, with the same
/// per-shard `id <= 255` clamp Go's loader applies via
/// `strconv.ParseInt(... 10, 64)` followed by the `if shardId < 0 ||
/// shardId > 255` guard. The 3-digit form (`.ec100`–`.ec255`) is
/// retained so the parser can still recognise shards from custom
/// 32+ ratios that fit in a u8 even though OSS only ships 10+4.
fn parse_ec_shard_extension(ext: &str) -> Option<u32> {
    let rest = ext.strip_prefix(".ec")?;
    if rest.len() < 2 || rest.len() > 3 {
        return None;
    }
    let id: u32 = rest.parse().ok()?;
    if id > 255 {
        return None;
    }
    Some(id)
}

/// Robust check that a `.dat` with actual data exists. An empty `.dat`
/// (<= a superblock, zero needles) is a leftover stub, not an encode source,
/// and counts as absent so it never justifies deleting shards. Any unexpected
/// stat error (permission, I/O) is treated as "exists" so we don't misclassify
/// local EC as distributed EC. Mirrors `checkDatFileExists` in Go.
fn check_dat_file_exists(path: &str) -> bool {
    match fs::metadata(path) {
        Ok(meta) => meta.len() > SUPER_BLOCK_SIZE as u64,
        Err(e) if e.kind() == io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

/// True when a `.vif` references remote-tier files: a remote-only volume
/// that has no local `.dat` but must still load via the remote path,
/// rather than be skipped as a lone EC sidecar.
fn vif_references_remote_file(vif_path: &str) -> bool {
    fs::read_to_string(vif_path)
        .ok()
        .and_then(|s| serde_json::from_str::<VifVolumeInfo>(&s).ok())
        .map(|vif| !vif.files.is_empty())
        .unwrap_or(false)
}

/// True when a `.vif` records an EC shard config, i.e. the volume was
/// erasure-coded. Such a volume keeps no local `.dat`, so an empty `.dat`
/// alongside it is a leftover stub safe to remove.
fn vif_is_ec_volume(vif_path: &str) -> bool {
    fs::read_to_string(vif_path)
        .ok()
        .and_then(|s| serde_json::from_str::<VifVolumeInfo>(&s).ok())
        .map(|vif| vif.ec_shard_config.is_some())
        .unwrap_or(false)
}

/// Remove a leftover empty EC `.dat` stub and return whether one was swept.
///
/// A stub is an empty `.dat` (<= a superblock, i.e. zero needles) whose `.vif`
/// records an EC shard config. An EC volume keeps no local `.dat`, so the stub
/// holds no data — its shards live on other servers. Such stubs (phantoms from
/// the pre-fix loader) otherwise load as phantom empty volumes, and a same-vid
/// stub on two disks blocks startup via the duplicate-vid check. The `.dat` and
/// its empty `.idx` are removed; non-EC empty `.dat` files are left alone. The
/// `.vif` is looked up in both the data and idx directories (which differ only
/// when `-dir.idx` is configured), each read at most once.
fn remove_empty_ec_dat_stub(volume_name: &str, idx_name: &str, vid: VolumeId) -> bool {
    let dat_path = format!("{}.dat", volume_name);
    match fs::metadata(&dat_path) {
        Ok(meta) if meta.len() <= SUPER_BLOCK_SIZE as u64 => {}
        _ => return false,
    }

    let data_vif = format!("{}.vif", volume_name);
    let idx_vif = format!("{}.vif", idx_name);
    let is_ec = vif_is_ec_volume(&data_vif) || (idx_vif != data_vif && vif_is_ec_volume(&idx_vif));
    if !is_ec {
        return false;
    }

    warn!(volume_id = vid.0, "removing leftover empty .dat stub for EC volume");
    let _ = fs::remove_file(&dat_path);
    let _ = fs::remove_file(format!("{}.idx", idx_name));
    true
}

fn parse_volume_filename(filename: &str) -> Option<(String, VolumeId)> {
    let stem = filename
        .strip_suffix(".dat")
        .or_else(|| filename.strip_suffix(".vif"))
        .or_else(|| filename.strip_suffix(".idx"))?;
    parse_collection_volume_id(stem)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// When `-dir.idx` is configured the EC `.vif` may live in the idx
    /// directory; the sweep must look there too, not only the data dir.
    #[test]
    fn test_remove_empty_ec_dat_stub_finds_vif_in_idx_dir() {
        let tmp = TempDir::new().unwrap();
        let data = tmp.path().join("data");
        let idx = tmp.path().join("idx");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&idx).unwrap();
        let vbase = format!("{}/warp-cal_42", data.to_str().unwrap());
        let ibase = format!("{}/warp-cal_42", idx.to_str().unwrap());
        std::fs::write(format!("{}.dat", vbase), vec![0u8; 8]).unwrap();

        let vif = VifVolumeInfo {
            version: 3,
            ec_shard_config: Some(crate::storage::volume::VifEcShardConfig {
                data_shards: 10,
                parity_shards: 4,
                ..Default::default()
            }),
            ..Default::default()
        };
        std::fs::write(format!("{}.vif", ibase), serde_json::to_string(&vif).unwrap()).unwrap();

        assert!(
            remove_empty_ec_dat_stub(&vbase, &ibase, VolumeId(42)),
            "EC .vif in the idx dir should be found and the stub removed",
        );
        assert!(!std::path::Path::new(&format!("{}.dat", vbase)).exists());
    }

    // The headline geometry: a stale/partial .dat (e.g. an interrupted decode)
    // smaller than the volume's real source sits next to the full-size shards
    // that are the only copy. validate_ec_volume must keep the shards.
    #[test]
    fn test_validate_ec_volume_partial_dat_next_to_full_shards_keeps() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive, MinFreeSpace::Percent(1.0), Vec::new()).unwrap();
        let base = volume_file_name(dir, "", VolumeId(70));
        let ds = crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;
        let full = calculate_expected_shard_size(30 * 1024 * 1024, ds);
        for i in 0..ds {
            std::fs::File::create(format!("{}.ec{:02}", base, i)).unwrap().set_len(full as u64).unwrap();
        }
        // Partial .dat: bigger than a superblock so it is not swept as a stub,
        // but smaller than what these shards encode.
        std::fs::File::create(format!("{}.dat", base)).unwrap().set_len(5 * 1024 * 1024).unwrap();
        assert!(
            loc.validate_ec_volume("", VolumeId(70)),
            "full-size shards beside a smaller (stale/partial) .dat must be kept",
        );
    }

    // The legitimate cleanup: a full source .dat next to shards SMALLER than it
    // would encode (an interrupted encode). The .dat is the complete source, so
    // reclaiming it loses no data.
    #[test]
    fn test_validate_ec_volume_interrupted_encode_reclaims() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive, MinFreeSpace::Percent(1.0), Vec::new()).unwrap();
        let base = volume_file_name(dir, "", VolumeId(71));
        let ds = crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;
        let dat_size = 30 * 1024 * 1024i64;
        std::fs::File::create(format!("{}.dat", base)).unwrap().set_len(dat_size as u64).unwrap();
        let partial = calculate_expected_shard_size(dat_size, ds) / 3;
        assert!(partial > 0);
        for i in 0..ds {
            std::fs::File::create(format!("{}.ec{:02}", base, i)).unwrap().set_len(partial as u64).unwrap();
        }
        assert!(
            !loc.validate_ec_volume("", VolumeId(71)),
            "shards smaller than the full source .dat should be reclaimable",
        );
    }

    #[test]
    fn test_parse_volume_filename() {
        assert_eq!(
            parse_volume_filename("42.dat"),
            Some(("".to_string(), VolumeId(42)))
        );
        assert_eq!(
            parse_volume_filename("pics_7.dat"),
            Some(("pics".to_string(), VolumeId(7)))
        );
        assert_eq!(
            parse_volume_filename("42.vif"),
            Some(("".to_string(), VolumeId(42)))
        );
        assert_eq!(
            parse_volume_filename("pics_7.idx"),
            Some(("pics".to_string(), VolumeId(7)))
        );
        assert_eq!(parse_volume_filename("notadat.idx"), None);
        assert_eq!(parse_volume_filename("bad.dat"), None);
    }

    #[test]
    fn test_disk_location_create_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();

        loc.create_volume(
            VolumeId(1),
            "",
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();

        assert_eq!(loc.volumes_len(), 1);
        assert!(loc.find_volume(VolumeId(1)).is_some());
        assert!(loc.find_volume(VolumeId(99)).is_none());
        assert_eq!(loc.free_volume_count(), 9);
    }

    #[test]
    fn test_disk_location_load_existing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Create volumes
        {
            let mut loc = DiskLocation::new(
                dir,
                dir,
                10,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
            loc.create_volume(
                VolumeId(1),
                "",
                NeedleMapKind::InMemory,
                None,
                None,
                0,
                Version::current(),
            )
            .unwrap();
            loc.create_volume(
                VolumeId(2),
                "test",
                NeedleMapKind::InMemory,
                None,
                None,
                0,
                Version::current(),
            )
            .unwrap();
            loc.close();
        }

        // Reload
        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();
        loc.load_existing_volumes(NeedleMapKind::InMemory).unwrap();
        assert_eq!(loc.volumes_len(), 2);

        let ids = loc.volume_ids();
        assert!(ids.contains(&VolumeId(1)));
        assert!(ids.contains(&VolumeId(2)));
    }

    #[test]
    fn test_disk_location_delete_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();

        loc.create_volume(
            VolumeId(1),
            "",
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        loc.create_volume(
            VolumeId(2),
            "",
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        assert_eq!(loc.volumes_len(), 2);

        loc.delete_volume(VolumeId(1), false, false).unwrap();
        assert_eq!(loc.volumes_len(), 1);
        assert!(loc.find_volume(VolumeId(1)).is_none());
    }

    #[test]
    fn test_disk_location_delete_collection() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();

        loc.create_volume(
            VolumeId(1),
            "pics",
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        loc.create_volume(
            VolumeId(2),
            "pics",
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        loc.create_volume(
            VolumeId(3),
            "docs",
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        assert_eq!(loc.volumes_len(), 3);

        loc.delete_collection("pics").unwrap();
        assert_eq!(loc.volumes_len(), 1);
        assert!(loc.find_volume(VolumeId(3)).is_some());
    }

    #[test]
    fn test_disk_location_delete_collection_removes_ec_volumes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();

        let shard_path = format!("{}/pics_7.ec00", dir);
        std::fs::write(&shard_path, b"ec-shard").unwrap();

        loc.mount_ec_shards(VolumeId(7), "pics", &[0], "").unwrap();
        assert!(loc.has_ec_volume(VolumeId(7)));
        assert!(std::path::Path::new(&shard_path).exists());
        assert!(std::path::Path::new(&format!("{}/pics_7.ecj", dir)).exists());

        loc.delete_collection("pics").unwrap();

        assert!(!loc.has_ec_volume(VolumeId(7)));
        assert!(!std::path::Path::new(&shard_path).exists());
        assert!(!std::path::Path::new(&format!("{}/pics_7.ecj", dir)).exists());
    }

    /// #9423: after an RPC `VolumeEcShardsMount` records the source
    /// volume's disk type on the EcVolume, a later empty-source mount
    /// call (disk-scan reload, orphan-shard reconcile, restart) must
    /// not clobber that override back to the physical location's disk
    /// type — otherwise heartbeat reporting drifts back to "hdd" on
    /// every reload.
    #[test]
    fn test_mount_ec_shards_empty_source_preserves_existing_disk_type() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();

        // Plant the first shard so the EC volume can mount, then call
        // mount_ec_shards with source_disk_type="ssd" — simulating the
        // VolumeEcShardsMount RPC path.
        std::fs::write(format!("{}/pics_7.ec00", dir), b"ec-shard").unwrap();
        loc.mount_ec_shards(VolumeId(7), "pics", &[0], "ssd").unwrap();
        {
            let ec_vol = loc.find_ec_volume(VolumeId(7)).expect("ec volume mounted");
            assert_eq!(
                ec_vol.disk_type,
                DiskType::Ssd,
                "first RPC mount should have set disk type to source's value",
            );
        }

        // Plant another shard and re-mount with an empty source —
        // matching what a disk-scan reload or orphan-shard reconcile
        // would pass. The previously-recorded "ssd" override must
        // survive.
        std::fs::write(format!("{}/pics_7.ec01", dir), b"ec-shard").unwrap();
        loc.mount_ec_shards(VolumeId(7), "pics", &[1], "").unwrap();
        {
            let ec_vol = loc.find_ec_volume(VolumeId(7)).expect("ec volume still mounted");
            assert_eq!(
                ec_vol.disk_type,
                DiskType::Ssd,
                "empty-source remount must not reset disk type to the physical location's",
            );
        }
    }

    #[test]
    fn test_disk_location_persists_directory_uuid_and_tags() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            vec!["fast".to_string(), "ssd".to_string()],
        )
        .unwrap();
        let directory_uuid = loc.directory_uuid.clone();
        assert_eq!(loc.tags, vec!["fast".to_string(), "ssd".to_string()]);
        drop(loc);

        let reloaded = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(1.0),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(reloaded.directory_uuid, directory_uuid);
    }

    /// Sanity: `parse_ec_shard_extension` accepts `.ec00` through
    /// `.ec999` and rejects anything else.
    #[test]
    fn test_parse_ec_shard_extension() {
        assert_eq!(parse_ec_shard_extension(".ec00"), Some(0));
        assert_eq!(parse_ec_shard_extension(".ec13"), Some(13));
        assert_eq!(parse_ec_shard_extension(".ec999"), None); // > 255
        assert_eq!(parse_ec_shard_extension(".ec255"), Some(255));
        assert_eq!(parse_ec_shard_extension(".ec0"), None); // need 2 digits
        assert_eq!(parse_ec_shard_extension(".ecx"), None);
        assert_eq!(parse_ec_shard_extension(".ecj"), None);
        assert_eq!(parse_ec_shard_extension(".dat"), None);
        assert_eq!(parse_ec_shard_extension("ec00"), None); // missing dot
    }

    /// Auto-load: shards + .ecx on the same disk get mounted into
    /// `ec_volumes` after `load_existing_volumes` finishes. Without
    /// this, EC shards on disk are invisible until something issues
    /// `VolumeEcShardsMount`.
    #[test]
    fn test_load_all_ec_shards_mounts_pairs_with_ecx() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let collection = "grafana-loki";
        let vid = VolumeId(1234);

        // Two shards + .ecx + .ecj + .vif (the post-encode steady state).
        for &sid in &[0u8, 4u8] {
            let path = format!("{}/{}_{}.ec{:02}", dir, collection, vid.0, sid);
            std::fs::write(&path, b"shard data nonempty").unwrap();
        }
        std::fs::write(format!("{}/{}_{}.ecx", dir, collection, vid.0), vec![0u8; 20])
            .unwrap();
        std::fs::write(format!("{}/{}_{}.ecj", dir, collection, vid.0), b"").unwrap();
        std::fs::write(
            format!("{}/{}_{}.vif", dir, collection, vid.0),
            br#"{"version":3}"#,
        )
        .unwrap();

        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(0.0),
            Vec::new(),
        )
        .unwrap();
        loc.load_existing_volumes(NeedleMapKind::InMemory).unwrap();

        let ec_vol = loc.find_ec_volume(vid).expect("EcVolume should be mounted");
        assert_eq!(ec_vol.shard_count(), 2, "both shards should be loaded");
    }

    /// Orphan shards with no `.ecx` and no `.dat` must stay on disk
    /// untouched (distributed-EC scenario, where the .ecx lives on
    /// another server). Mirrors Go's `checkOrphanedShards` no-op
    /// branch.
    #[test]
    fn test_load_all_ec_shards_keeps_orphan_shards_when_no_dat() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let collection = "grafana-loki";
        let vid = VolumeId(2345);

        // Shards on disk, but no .ecx and no .dat — distributed-EC
        // with the .ecx on a sibling/server.
        let shard_paths: Vec<String> = [0u8, 12u8]
            .iter()
            .map(|sid| format!("{}/{}_{}.ec{:02}", dir, collection, vid.0, sid))
            .collect();
        for p in &shard_paths {
            std::fs::write(p, b"shard data nonempty").unwrap();
        }

        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(0.0),
            Vec::new(),
        )
        .unwrap();
        loc.load_existing_volumes(NeedleMapKind::InMemory).unwrap();

        // No EcVolume should be mounted (no .ecx).
        assert!(loc.find_ec_volume(vid).is_none());
        // But the shard files must still be on disk for cross-disk
        // reconciliation / operator recovery.
        for p in &shard_paths {
            assert!(
                std::path::Path::new(p).exists(),
                "orphan shard {} was destroyed",
                p,
            );
        }
    }

    /// Orphan shards WITH a stale `.dat` are treated as an
    /// interrupted-encoding remnant and cleaned up. Mirrors Go's
    /// `checkOrphanedShards` cleanup branch. Guards a subtle case
    /// where a partial encoding wrote shards before .ecx and a crash
    /// left the .dat behind.
    #[test]
    fn test_load_all_ec_shards_cleans_orphan_shards_when_dat_exists() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let collection = "grafana-loki";
        let vid = VolumeId(3456);

        let shard_paths: Vec<String> = [0u8, 1u8]
            .iter()
            .map(|sid| format!("{}/{}_{}.ec{:02}", dir, collection, vid.0, sid))
            .collect();
        for p in &shard_paths {
            std::fs::write(p, b"shard data nonempty").unwrap();
        }
        // Stale .dat from interrupted encoding.
        let dat_path = format!("{}/{}_{}.dat", dir, collection, vid.0);
        std::fs::write(&dat_path, b"unfinished").unwrap();

        let mut loc = DiskLocation::new(
            dir,
            dir,
            10,
            DiskType::HardDrive,
            MinFreeSpace::Percent(0.0),
            Vec::new(),
        )
        .unwrap();
        // load_existing_volumes processes .dat first; ec auto-load
        // runs after and finds the orphans. The .dat itself may or may
        // not be loadable as a regular volume — that's not what this
        // test is about. Just check that the orphan shard files were
        // removed.
        let _ = loc.load_existing_volumes(NeedleMapKind::InMemory);

        for p in &shard_paths {
            assert!(
                !std::path::Path::new(p).exists(),
                "orphan shard {} should have been cleaned up because .dat exists",
                p,
            );
        }
    }
}
