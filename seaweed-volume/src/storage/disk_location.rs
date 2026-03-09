//! DiskLocation: manages volumes on a single disk/directory.
//!
//! Each DiskLocation represents one storage directory containing .dat + .idx files.
//! A Store contains one or more DiskLocations (one per configured directory).
//! Matches Go's storage/disk_location.go.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};

use tracing::{info, warn};

use crate::config::MinFreeSpace;
use crate::storage::erasure_coding::ec_shard::{
    DATA_SHARDS_COUNT, ERASURE_CODING_LARGE_BLOCK_SIZE, ERASURE_CODING_SMALL_BLOCK_SIZE,
};
use crate::storage::needle_map::NeedleMapKind;
use crate::storage::super_block::ReplicaPlacement;
use crate::storage::types::*;
use crate::storage::volume::{remove_volume_files, volume_file_name, Volume, VolumeError};

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
    pub is_disk_space_low: AtomicBool,
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
            is_disk_space_low: AtomicBool::new(false),
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

        // Scan for .dat files
        let entries = fs::read_dir(&self.directory)?;
        let mut dat_files: Vec<(String, VolumeId)> = Vec::new();

        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap_or_default();
            if name.ends_with(".dat") {
                if let Some((collection, vid)) = parse_volume_filename(&name) {
                    dat_files.push((collection, vid));
                }
            }
        }

        for (collection, vid) in dat_files {
            let volume_name = volume_file_name(&self.directory, &collection, vid);
            let idx_name = volume_file_name(&self.idx_directory, &collection, vid);

            // Check for incomplete volume (.note file means a VolumeCopy was interrupted)
            let note_path = format!("{}.note", volume_name);
            if std::path::Path::new(&note_path).exists() {
                let note = fs::read_to_string(&note_path).unwrap_or_default();
                warn!(
                    volume_id = vid.0,
                    "volume was not completed: {}, removing files", note
                );
                remove_volume_files(&volume_name);
                remove_volume_files(&idx_name);
                continue;
            }

            // If valid EC shards exist (.ecx file present), skip loading .dat
            let ecx_path = format!("{}.ecx", idx_name);
            if std::path::Path::new(&ecx_path).exists() {
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

            // Clean up stale compaction temp files
            let cpd_path = format!("{}.cpd", volume_name);
            let cpx_path = format!("{}.cpx", idx_name);
            if std::path::Path::new(&cpd_path).exists() {
                info!(volume_id = vid.0, "removing stale compaction file .cpd");
                let _ = fs::remove_file(&cpd_path);
            }
            if std::path::Path::new(&cpx_path).exists() {
                info!(volume_id = vid.0, "removing stale compaction file .cpx");
                let _ = fs::remove_file(&cpx_path);
            }

            // Skip if already loaded (e.g., from a previous call)
            if self.volumes.contains_key(&vid) {
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
                Ok(v) => {
                    self.volumes.insert(vid, v);
                }
                Err(e) => {
                    warn!(volume_id = vid.0, error = %e, "failed to load volume");
                }
            }
        }

        Ok(())
    }

    /// Validate EC volume shards: all shards must be same size, and if .dat exists,
    /// need at least DATA_SHARDS_COUNT shards with size matching expected.
    fn validate_ec_volume(&self, collection: &str, vid: VolumeId) -> bool {
        let base = volume_file_name(&self.directory, collection, vid);
        let dat_path = format!("{}.dat", base);

        let mut expected_shard_size: Option<i64> = None;
        let dat_exists = std::path::Path::new(&dat_path).exists();

        if dat_exists {
            if let Ok(meta) = fs::metadata(&dat_path) {
                expected_shard_size = Some(calculate_expected_shard_size(meta.len() as i64));
            } else {
                return false;
            }
        }

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
                            warn!(
                                volume_id = vid.0,
                                shard = i,
                                size,
                                expected = prev,
                                "EC shard size mismatch"
                            );
                            return false;
                        }
                    } else {
                        actual_shard_size = Some(size);
                    }
                    shard_count += 1;
                }
                Err(e) if e.kind() != io::ErrorKind::NotFound => {
                    warn!(
                        volume_id = vid.0,
                        shard = i,
                        error = %e,
                        "failed to stat EC shard"
                    );
                    return false;
                }
                _ => {} // not found or zero size — skip
            }
        }

        // If .dat exists, validate shard size matches expected
        if dat_exists {
            if let (Some(actual), Some(expected)) = (actual_shard_size, expected_shard_size) {
                if actual != expected {
                    warn!(
                        volume_id = vid.0,
                        actual_shard_size = actual,
                        expected_shard_size = expected,
                        "EC shard size doesn't match .dat file"
                    );
                    return false;
                }
            }
        }

        // Distributed EC (no .dat): any shard count is valid
        if !dat_exists {
            return true;
        }

        // With .dat: need at least DATA_SHARDS_COUNT shards
        if shard_count < DATA_SHARDS_COUNT {
            warn!(
                volume_id = vid.0,
                shard_count,
                required = DATA_SHARDS_COUNT,
                "EC volume has .dat but too few shards"
            );
            return false;
        }

        true
    }

    /// Remove all EC-related files for a volume.
    fn remove_ec_volume_files(&self, collection: &str, vid: VolumeId) {
        let base = volume_file_name(&self.directory, collection, vid);
        let idx_base = volume_file_name(&self.idx_directory, collection, vid);
        const MAX_SHARD_COUNT: usize = 32;

        // Remove index files first (.ecx, .ecj)
        let _ = fs::remove_file(format!("{}.ecx", idx_base));
        let _ = fs::remove_file(format!("{}.ecj", idx_base));

        // Remove all EC shard files (.ec00 ~ .ec31)
        for i in 0..MAX_SHARD_COUNT {
            let _ = fs::remove_file(format!("{}.ec{:02}", base, i));
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
        self.volumes.insert(vid, volume);
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
        let v = Volume::new(
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
        self.volumes.insert(vid, v);
        Ok(())
    }

    /// Remove and close a volume.
    pub fn unload_volume(&mut self, vid: VolumeId) -> Option<Volume> {
        if let Some(mut v) = self.volumes.remove(&vid) {
            v.close();
            Some(v)
        } else {
            None
        }
    }

    /// Remove, close, and delete all files for a volume.
    pub fn delete_volume(&mut self, vid: VolumeId) -> Result<(), VolumeError> {
        if let Some(mut v) = self.volumes.remove(&vid) {
            v.destroy()?;
            Ok(())
        } else {
            Err(VolumeError::NotFound)
        }
    }

    /// Delete all volumes in a collection.
    pub fn delete_collection(&mut self, collection: &str) {
        let vids: Vec<VolumeId> = self
            .volumes
            .iter()
            .filter(|(_, v)| v.collection == collection)
            .map(|(vid, _)| *vid)
            .collect();

        for vid in vids {
            if let Some(mut v) = self.volumes.remove(&vid) {
                let _ = v.destroy();
            }
        }
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
    pub fn free_volume_count(&self) -> i32 {
        let max = self.max_volume_count.load(Ordering::Relaxed);
        let used = self.volumes.len() as i32;
        if max > used {
            max - used
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

    /// Check disk space against min_free_space and update is_disk_space_low.
    pub fn check_disk_space(&self) {
        let (total, free) = get_disk_stats(&self.directory);
        if total == 0 {
            return;
        }
        let is_low = match &self.min_free_space {
            MinFreeSpace::Percent(pct) => {
                let free_pct = (free as f64 / total as f64) * 100.0;
                free_pct < *pct
            }
            MinFreeSpace::Bytes(min_bytes) => free < *min_bytes,
        };
        self.is_disk_space_low.store(is_low, Ordering::Relaxed);
        self.available_space.store(free, Ordering::Relaxed);
    }

    /// Close all volumes.
    pub fn close(&mut self) {
        for (_, v) in self.volumes.iter_mut() {
            v.close();
        }
        self.volumes.clear();
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
fn calculate_expected_shard_size(dat_file_size: i64) -> i64 {
    let large_batch_size =
        ERASURE_CODING_LARGE_BLOCK_SIZE as i64 * DATA_SHARDS_COUNT as i64;
    let num_large_batches = dat_file_size / large_batch_size;
    let mut shard_size = num_large_batches * ERASURE_CODING_LARGE_BLOCK_SIZE as i64;
    let remaining = dat_file_size - (num_large_batches * large_batch_size);

    if remaining > 0 {
        let small_batch_size =
            ERASURE_CODING_SMALL_BLOCK_SIZE as i64 * DATA_SHARDS_COUNT as i64;
        // Ceiling division
        let num_small_batches = (remaining + small_batch_size - 1) / small_batch_size;
        shard_size += num_small_batches * ERASURE_CODING_SMALL_BLOCK_SIZE as i64;
    }

    shard_size
}

/// Parse a volume filename like "collection_42.dat" or "42.dat" into (collection, VolumeId).
fn parse_volume_filename(filename: &str) -> Option<(String, VolumeId)> {
    let stem = filename.strip_suffix(".dat")?;
    if let Some(pos) = stem.rfind('_') {
        let collection = &stem[..pos];
        let id_str = &stem[pos + 1..];
        let id: u32 = id_str.parse().ok()?;
        Some((collection.to_string(), VolumeId(id)))
    } else {
        let id: u32 = stem.parse().ok()?;
        Some((String::new(), VolumeId(id)))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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

        loc.create_volume(VolumeId(1), "", NeedleMapKind::InMemory, None, None, 0, Version::current())
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
            loc.create_volume(VolumeId(1), "", NeedleMapKind::InMemory, None, None, 0, Version::current())
                .unwrap();
            loc.create_volume(VolumeId(2), "test", NeedleMapKind::InMemory, None, None, 0, Version::current())
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

        loc.create_volume(VolumeId(1), "", NeedleMapKind::InMemory, None, None, 0, Version::current())
            .unwrap();
        loc.create_volume(VolumeId(2), "", NeedleMapKind::InMemory, None, None, 0, Version::current())
            .unwrap();
        assert_eq!(loc.volumes_len(), 2);

        loc.delete_volume(VolumeId(1)).unwrap();
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

        loc.create_volume(VolumeId(1), "pics", NeedleMapKind::InMemory, None, None, 0, Version::current())
            .unwrap();
        loc.create_volume(VolumeId(2), "pics", NeedleMapKind::InMemory, None, None, 0, Version::current())
            .unwrap();
        loc.create_volume(VolumeId(3), "docs", NeedleMapKind::InMemory, None, None, 0, Version::current())
            .unwrap();
        assert_eq!(loc.volumes_len(), 3);

        loc.delete_collection("pics");
        assert_eq!(loc.volumes_len(), 1);
        assert!(loc.find_volume(VolumeId(3)).is_some());
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
}
