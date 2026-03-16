//! Store: the top-level storage manager for a volume server.
//!
//! A Store manages multiple DiskLocations (one per configured directory).
//! It coordinates volume placement, lookup, and lifecycle operations.
//! Matches Go's storage/store.go.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::MinFreeSpace;
use crate::storage::disk_location::DiskLocation;
use crate::storage::erasure_coding::ec_shard::EcVolumeShard;
use crate::storage::erasure_coding::ec_volume::EcVolume;
use crate::storage::needle::needle::Needle;
use crate::storage::needle_map::NeedleMapKind;
use crate::storage::super_block::ReplicaPlacement;
use crate::storage::types::*;
use crate::storage::volume::{VifVolumeInfo, VolumeError};

/// Top-level storage manager containing all disk locations and their volumes.
pub struct Store {
    pub locations: Vec<DiskLocation>,
    pub needle_map_kind: NeedleMapKind,
    pub volume_size_limit: AtomicU64,
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub grpc_port: u16,
    pub public_url: String,
    pub data_center: String,
    pub rack: String,
}

impl Store {
    pub fn new(needle_map_kind: NeedleMapKind) -> Self {
        Store {
            locations: Vec::new(),
            needle_map_kind,
            volume_size_limit: AtomicU64::new(0),
            id: String::new(),
            ip: String::new(),
            port: 0,
            grpc_port: 0,
            public_url: String::new(),
            data_center: String::new(),
            rack: String::new(),
        }
    }

    /// Add a disk location and load existing volumes from it.
    pub fn add_location(
        &mut self,
        directory: &str,
        idx_directory: &str,
        max_volume_count: i32,
        disk_type: DiskType,
        min_free_space: MinFreeSpace,
        tags: Vec<String>,
    ) -> io::Result<()> {
        let mut loc = DiskLocation::new(
            directory,
            idx_directory,
            max_volume_count,
            disk_type,
            min_free_space,
            tags,
        )?;
        loc.load_existing_volumes(self.needle_map_kind)?;

        // Check for duplicate volume IDs across existing locations
        for vid in loc.volume_ids() {
            if self.find_volume(vid).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "volume {} already exists in another location, conflicting dir: {}",
                        vid, directory
                    ),
                ));
            }
        }

        self.locations.push(loc);
        Ok(())
    }

    /// Scan disk locations for new volume files and load them.
    /// Mirrors Go's `Store.LoadNewVolumes()`.
    pub fn load_new_volumes(&mut self) {
        for loc in &mut self.locations {
            if let Err(e) = loc.load_existing_volumes(self.needle_map_kind) {
                tracing::error!("load_new_volumes error in {}: {}", loc.directory, e);
            }
        }
    }

    // ---- Volume lookup ----

    /// Find which location contains a volume.
    pub fn find_volume(&self, vid: VolumeId) -> Option<(usize, &crate::storage::volume::Volume)> {
        for (i, loc) in self.locations.iter().enumerate() {
            if let Some(v) = loc.find_volume(vid) {
                return Some((i, v));
            }
        }
        None
    }

    /// Find which location contains a volume (mutable).
    pub fn find_volume_mut(
        &mut self,
        vid: VolumeId,
    ) -> Option<(usize, &mut crate::storage::volume::Volume)> {
        for (i, loc) in self.locations.iter_mut().enumerate() {
            if let Some(v) = loc.find_volume_mut(vid) {
                return Some((i, v));
            }
        }
        None
    }

    /// Check if a volume exists.
    pub fn has_volume(&self, vid: VolumeId) -> bool {
        self.find_volume(vid).is_some()
    }

    // ---- Volume lifecycle ----

    /// Find the location with fewest volumes (load-balance) of the given disk type.
    fn find_free_location(&self, disk_type: &DiskType) -> Option<usize> {
        let mut best: Option<(usize, usize)> = None; // (index, volume_count)
        for (i, loc) in self.locations.iter().enumerate() {
            if &loc.disk_type != disk_type {
                continue;
            }
            if loc.free_volume_count() <= 0 {
                continue;
            }
            if loc.is_disk_space_low.load(Ordering::Relaxed) {
                continue;
            }
            let count = loc.volumes_len();
            if best.is_none() || count < best.unwrap().1 {
                best = Some((i, count));
            }
        }
        best.map(|(i, _)| i)
    }

    /// Create a new volume, placing it on the location with the most free space.
    pub fn add_volume(
        &mut self,
        vid: VolumeId,
        collection: &str,
        replica_placement: Option<ReplicaPlacement>,
        ttl: Option<crate::storage::needle::ttl::TTL>,
        preallocate: u64,
        disk_type: DiskType,
        version: Version,
    ) -> Result<(), VolumeError> {
        if self.find_volume(vid).is_some() {
            return Err(VolumeError::AlreadyExists);
        }
        let loc_idx = self.find_free_location(&disk_type).ok_or_else(|| {
            VolumeError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("no free location for disk type {:?}", disk_type),
            ))
        })?;

        self.locations[loc_idx].create_volume(
            vid,
            collection,
            self.needle_map_kind,
            replica_placement,
            ttl,
            preallocate,
            version,
        )
    }

    /// Delete a volume from any location.
    pub fn delete_volume(&mut self, vid: VolumeId) -> Result<(), VolumeError> {
        for loc in &mut self.locations {
            if loc.find_volume(vid).is_some() {
                return loc.delete_volume(vid);
            }
        }
        Err(VolumeError::NotFound)
    }

    /// Unload (unmount) a volume without deleting its files.
    pub fn unmount_volume(&mut self, vid: VolumeId) -> bool {
        for loc in &mut self.locations {
            if loc.unload_volume(vid).is_some() {
                return true;
            }
        }
        false
    }

    /// Mount a volume from an existing .dat file.
    pub fn mount_volume(
        &mut self,
        vid: VolumeId,
        collection: &str,
        disk_type: DiskType,
    ) -> Result<(), VolumeError> {
        if self.find_volume(vid).is_some() {
            return Err(VolumeError::AlreadyExists);
        }
        // Find the location where the .dat file exists
        for loc in &mut self.locations {
            if &loc.disk_type != &disk_type {
                continue;
            }
            let base = crate::storage::volume::volume_file_name(&loc.directory, collection, vid);
            let dat_path = format!("{}.dat", base);
            if std::path::Path::new(&dat_path).exists() {
                return loc.create_volume(
                    vid,
                    collection,
                    self.needle_map_kind,
                    None,
                    None,
                    0,
                    Version::current(),
                );
            }
        }
        Err(VolumeError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("volume {} not found on disk", vid),
        )))
    }

    /// Mount a volume by id only (Go's MountVolume behavior).
    /// Scans all locations for a matching .dat file and loads with its collection prefix.
    pub fn mount_volume_by_id(&mut self, vid: VolumeId) -> Result<(), VolumeError> {
        if self.find_volume(vid).is_some() {
            return Err(VolumeError::AlreadyExists);
        }
        if let Some((loc_idx, _base_path, collection)) = self.find_volume_file_base(vid) {
            let loc = &mut self.locations[loc_idx];
            return loc.create_volume(
                vid,
                &collection,
                self.needle_map_kind,
                None,
                None,
                0,
                Version::current(),
            );
        }
        Err(VolumeError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("volume {} not found on disk", vid),
        )))
    }

    fn find_volume_file_base(&self, vid: VolumeId) -> Option<(usize, String, String)> {
        for (loc_idx, loc) in self.locations.iter().enumerate() {
            if let Ok(entries) = std::fs::read_dir(&loc.directory) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if !name.ends_with(".dat") {
                        continue;
                    }
                    if let Some((collection, file_vid)) = parse_volume_filename(&name) {
                        if file_vid == vid {
                            let base = name.trim_end_matches(".dat");
                            let base_path = format!("{}/{}", loc.directory, base);
                            return Some((loc_idx, base_path, collection));
                        }
                    }
                }
            }
        }
        None
    }

    /// Configure a volume's replica placement on disk.
    /// The volume must already be unmounted. This opens the .dat file directly,
    /// modifies the replica_placement byte (offset 1), and writes it back.
    pub fn configure_volume(&self, vid: VolumeId, rp: ReplicaPlacement) -> Result<(), VolumeError> {
        let (_, base_path, _) = self.find_volume_file_base(vid).ok_or_else(|| {
            VolumeError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("volume {} not found on disk", vid),
            ))
        })?;
        let vif_path = format!("{}.vif", base_path);
        let mut vif = load_vif_volume_info(&vif_path)?;
        vif.replication = rp.to_string();
        save_vif_volume_info(&vif_path, &vif)?;
        Ok(())
    }

    // ---- Read / Write / Delete ----

    /// Read a needle from a volume.
    pub fn read_volume_needle(&self, vid: VolumeId, n: &mut Needle) -> Result<i32, VolumeError> {
        let (_, vol) = self.find_volume(vid).ok_or(VolumeError::NotFound)?;
        vol.read_needle(n)
    }

    /// Read a needle from a volume, optionally reading deleted needles.
    pub fn read_volume_needle_opt(
        &self,
        vid: VolumeId,
        n: &mut Needle,
        read_deleted: bool,
    ) -> Result<i32, VolumeError> {
        let (_, vol) = self.find_volume(vid).ok_or(VolumeError::NotFound)?;
        vol.read_needle_opt(n, read_deleted)
    }

    /// Read needle metadata and return streaming info for large file reads.
    pub fn read_volume_needle_stream_info(
        &self,
        vid: VolumeId,
        n: &mut Needle,
        read_deleted: bool,
    ) -> Result<crate::storage::volume::NeedleStreamInfo, VolumeError> {
        let (_, vol) = self.find_volume(vid).ok_or(VolumeError::NotFound)?;
        vol.read_needle_stream_info(n, read_deleted)
    }

    /// Re-lookup a needle's data-file offset after compaction may have moved it.
    /// Returns `(new_data_file_offset, current_compaction_revision)`.
    pub fn re_lookup_needle_data_offset(
        &self,
        vid: VolumeId,
        needle_id: NeedleId,
    ) -> Result<(u64, u16), VolumeError> {
        let (_, vol) = self.find_volume(vid).ok_or(VolumeError::NotFound)?;
        vol.re_lookup_needle_data_offset(needle_id)
    }

    /// Write a needle to a volume.
    pub fn write_volume_needle(
        &mut self,
        vid: VolumeId,
        n: &mut Needle,
    ) -> Result<(u64, Size, bool), VolumeError> {
        // Check disk space on the location containing this volume.
        // We do this before the mutable borrow to avoid borrow conflicts.
        let loc_idx = self
            .find_volume(vid)
            .map(|(i, _)| i)
            .ok_or(VolumeError::NotFound)?;
        if self.locations[loc_idx]
            .is_disk_space_low
            .load(Ordering::Relaxed)
        {
            return Err(VolumeError::ReadOnly);
        }

        let (_, vol) = self.find_volume_mut(vid).ok_or(VolumeError::NotFound)?;
        vol.write_needle(n, true)
    }

    /// Delete a needle from a volume.
    pub fn delete_volume_needle(
        &mut self,
        vid: VolumeId,
        n: &mut Needle,
    ) -> Result<Size, VolumeError> {
        let (_, vol) = self.find_volume_mut(vid).ok_or(VolumeError::NotFound)?;
        vol.delete_needle(n)
    }

    // ---- Collection operations ----

    /// Delete all volumes in a collection.
    pub fn delete_collection(&mut self, collection: &str) {
        for loc in &mut self.locations {
            loc.delete_collection(collection);
        }
    }

    // ---- Metrics ----

    /// Total volume count across all locations.
    pub fn total_volume_count(&self) -> usize {
        self.locations.iter().map(|loc| loc.volumes_len()).sum()
    }

    /// Total max volumes across all locations.
    pub fn max_volume_count(&self) -> i32 {
        self.locations
            .iter()
            .map(|loc| loc.max_volume_count.load(Ordering::Relaxed))
            .sum()
    }

    /// Total EC shard count across all locations.
    pub fn ec_shard_count(&self) -> usize {
        self.locations.iter().map(|loc| loc.ec_shard_count()).sum()
    }

    /// Recalculate max volume counts for locations with original_max_volume_count == 0.
    /// Returns true if any max changed (caller should re-send heartbeat).
    pub fn maybe_adjust_volume_max(&self) -> bool {
        let volume_size_limit = self.volume_size_limit.load(Ordering::Relaxed);
        if volume_size_limit == 0 {
            return false;
        }

        let mut has_changes = false;
        let mut new_max_total: i32 = 0;

        for loc in &self.locations {
            if loc.original_max_volume_count == 0 {
                let current = loc.max_volume_count.load(Ordering::Relaxed);
                let (_, free) = super::disk_location::get_disk_stats(&loc.directory);

                let unused_space = loc.unused_space(volume_size_limit);
                let unclaimed = (free as i64) - (unused_space as i64);

                let vol_count = loc.volumes_len() as i32;
                let loc_ec_shards = loc.ec_shard_count();
                let ec_equivalent = ((loc_ec_shards
                    + crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT)
                    / crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT)
                    as i32;
                let mut max_count = vol_count + ec_equivalent;

                if unclaimed > volume_size_limit as i64 {
                    max_count += (unclaimed as u64 / volume_size_limit) as i32 - 1;
                }

                loc.max_volume_count.store(max_count, Ordering::Relaxed);
                new_max_total += max_count;
                has_changes = has_changes || current != max_count;
            } else {
                new_max_total += loc.original_max_volume_count;
            }
        }

        crate::metrics::MAX_VOLUMES.set(new_max_total as i64);
        has_changes
    }

    /// Free volume slots across all locations.
    pub fn free_volume_count(&self) -> i32 {
        self.locations
            .iter()
            .map(|loc| loc.free_volume_count())
            .sum()
    }

    /// All volume IDs across all locations.
    pub fn all_volume_ids(&self) -> Vec<VolumeId> {
        let mut ids: Vec<VolumeId> = self
            .locations
            .iter()
            .flat_map(|loc| loc.volume_ids())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    // ---- EC volume operations ----

    /// Mount EC shards for a volume.
    pub fn mount_ec_shards(
        &mut self,
        vid: VolumeId,
        collection: &str,
        shard_ids: &[u32],
    ) -> Result<(), VolumeError> {
        // Find the location where the EC files live
        let loc_idx = self.find_ec_location(vid, collection).ok_or_else(|| {
            VolumeError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("ec volume {} shards not found on disk", vid),
            ))
        })?;

        self.locations[loc_idx].mount_ec_shards(vid, collection, shard_ids)
    }

    /// Unmount EC shards for a volume.
    pub fn unmount_ec_shards(&mut self, vid: VolumeId, shard_ids: &[u32]) {
        for loc in &mut self.locations {
            if loc.has_ec_volume(vid) {
                loc.unmount_ec_shards(vid, shard_ids);
                return;
            }
        }
    }

    /// Find an EC volume across all locations.
    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<&EcVolume> {
        for loc in &self.locations {
            if let Some(ecv) = loc.find_ec_volume(vid) {
                return Some(ecv);
            }
        }
        None
    }

    /// Find an EC volume across all locations (mutable).
    pub fn find_ec_volume_mut(&mut self, vid: VolumeId) -> Option<&mut EcVolume> {
        for loc in &mut self.locations {
            if let Some(ecv) = loc.find_ec_volume_mut(vid) {
                return Some(ecv);
            }
        }
        None
    }

    /// Check if any location has an EC volume.
    pub fn has_ec_volume(&self, vid: VolumeId) -> bool {
        self.locations.iter().any(|loc| loc.has_ec_volume(vid))
    }

    /// Remove an EC volume from whichever location has it.
    pub fn remove_ec_volume(&mut self, vid: VolumeId) -> Option<EcVolume> {
        for loc in &mut self.locations {
            if let Some(ecv) = loc.remove_ec_volume(vid) {
                return Some(ecv);
            }
        }
        None
    }

    /// Find the location index containing EC files for a volume.
    pub fn find_ec_location(&self, vid: VolumeId, collection: &str) -> Option<usize> {
        for (i, loc) in self.locations.iter().enumerate() {
            let base = crate::storage::volume::volume_file_name(&loc.directory, collection, vid);
            let ecx_path = format!("{}.ecx", base);
            if std::path::Path::new(&ecx_path).exists() {
                return Some(i);
            }
        }
        None
    }

    /// Delete EC shard files from disk.
    pub fn delete_ec_shards(&mut self, vid: VolumeId, collection: &str, shard_ids: &[u32]) {
        // Delete shard files from disk
        for loc in &self.locations {
            for &shard_id in shard_ids {
                let shard = EcVolumeShard::new(&loc.directory, collection, vid, shard_id as u8);
                let path = shard.file_name();
                let _ = std::fs::remove_file(&path);
            }
        }

        // Also unmount if mounted
        self.unmount_ec_shards(vid, shard_ids);

        // If all shards are gone, remove .ecx and .ecj files from both idx and data dirs
        let all_gone = self.check_all_ec_shards_deleted(vid, collection);
        if all_gone {
            for loc in &self.locations {
                let idx_base =
                    crate::storage::volume::volume_file_name(&loc.idx_directory, collection, vid);
                let _ = std::fs::remove_file(format!("{}.ecx", idx_base));
                let _ = std::fs::remove_file(format!("{}.ecj", idx_base));
                // Also try data directory in case .ecx/.ecj were created before -dir.idx
                if loc.idx_directory != loc.directory {
                    let data_base =
                        crate::storage::volume::volume_file_name(&loc.directory, collection, vid);
                    let _ = std::fs::remove_file(format!("{}.ecx", data_base));
                    let _ = std::fs::remove_file(format!("{}.ecj", data_base));
                }
            }
        }
    }

    /// Check if all EC shard files have been deleted for a volume.
    fn check_all_ec_shards_deleted(&self, vid: VolumeId, collection: &str) -> bool {
        for loc in &self.locations {
            for shard_id in 0..14u8 {
                let shard = EcVolumeShard::new(&loc.directory, collection, vid, shard_id);
                if std::path::Path::new(&shard.file_name()).exists() {
                    return false;
                }
            }
        }
        true
    }

    /// Find the directory containing EC files for a volume.
    pub fn find_ec_dir(&self, vid: VolumeId, collection: &str) -> Option<String> {
        for loc in &self.locations {
            // Check idx directory first
            let idx_base =
                crate::storage::volume::volume_file_name(&loc.idx_directory, collection, vid);
            if std::path::Path::new(&format!("{}.ecx", idx_base)).exists() {
                return Some(loc.directory.clone());
            }
            // Fall back to data directory if .ecx was created before -dir.idx was configured
            if loc.idx_directory != loc.directory {
                let data_base =
                    crate::storage::volume::volume_file_name(&loc.directory, collection, vid);
                if std::path::Path::new(&format!("{}.ecx", data_base)).exists() {
                    return Some(loc.directory.clone());
                }
            }
        }
        None
    }

    /// Find the directory containing a specific EC shard file.
    pub fn find_ec_shard_dir(
        &self,
        vid: VolumeId,
        collection: &str,
        shard_id: u8,
    ) -> Option<String> {
        for loc in &self.locations {
            let shard = EcVolumeShard::new(&loc.directory, collection, vid, shard_id);
            if std::path::Path::new(&shard.file_name()).exists() {
                return Some(loc.directory.clone());
            }
        }
        None
    }

    // ---- Vacuum / Compaction ----

    /// Check the garbage level of a volume.
    pub fn check_compact_volume(&self, vid: VolumeId) -> Result<f64, String> {
        if let Some((_, v)) = self.find_volume(vid) {
            Ok(v.garbage_level())
        } else {
            Err(format!(
                "volume id {} is not found during check compact",
                vid.0
            ))
        }
    }

    /// Compact a volume by rewriting only live needles.
    pub fn compact_volume<F>(
        &mut self,
        vid: VolumeId,
        preallocate: u64,
        max_bytes_per_second: i64,
        progress_fn: F,
    ) -> Result<(), String>
    where
        F: Fn(i64) -> bool,
    {
        let loc_idx = self
            .find_volume(vid)
            .map(|(i, _)| i)
            .ok_or_else(|| format!("volume id {} is not found during compact", vid.0))?;

        let dir = self.locations[loc_idx].directory.clone();
        let (_, free) = crate::storage::disk_location::get_disk_stats(&dir);

        // Compute required space from current volume sizes
        let required_space = {
            let (_, v) = self.find_volume(vid).unwrap();
            v.dat_file_size().unwrap_or(0) + v.idx_file_size()
        };

        if free < required_space {
            return Err(format!(
                "not enough free space to compact volume {}. Required: {}, Free: {}",
                vid.0, required_space, free
            ));
        }

        if let Some((_, v)) = self.find_volume_mut(vid) {
            v.compact_by_index(preallocate, max_bytes_per_second, progress_fn)
                .map_err(|e| format!("compact volume {}: {}", vid.0, e))
        } else {
            Err(format!("volume id {} is not found during compact", vid.0))
        }
    }

    /// Commit a completed compaction: swap files and reload.
    pub fn commit_compact_volume(&mut self, vid: VolumeId) -> Result<(bool, u64), String> {
        if let Some((_, v)) = self.find_volume_mut(vid) {
            let is_read_only = v.is_read_only();
            v.commit_compact()
                .map_err(|e| format!("commit compact volume {}: {}", vid.0, e))?;
            let volume_size = v.dat_file_size().unwrap_or(0);
            Ok((is_read_only, volume_size))
        } else {
            Err(format!(
                "volume id {} is not found during commit compact",
                vid.0
            ))
        }
    }

    /// Clean up leftover compaction files.
    pub fn cleanup_compact_volume(&mut self, vid: VolumeId) -> Result<(), String> {
        if let Some((_, v)) = self.find_volume_mut(vid) {
            v.cleanup_compact()
                .map_err(|e| format!("cleanup volume {}: {}", vid.0, e))
        } else {
            Err(format!(
                "volume id {} is not found during cleaning up",
                vid.0
            ))
        }
    }

    /// Close all locations and their volumes.
    pub fn close(&mut self) {
        for loc in &mut self.locations {
            loc.close();
        }
    }
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

fn load_vif_volume_info(path: &str) -> Result<VifVolumeInfo, VolumeError> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(VifVolumeInfo::default()),
        Err(e) => return Err(VolumeError::Io(e)),
    };
    if content.trim().is_empty() {
        return Ok(VifVolumeInfo::default());
    }
    if let Ok(vif) = serde_json::from_str::<VifVolumeInfo>(&content) {
        return Ok(vif);
    }
    #[derive(serde::Deserialize)]
    struct LegacyVolumeInfo {
        read_only: bool,
    }
    if let Ok(legacy) = serde_json::from_str::<LegacyVolumeInfo>(&content) {
        let mut vif = VifVolumeInfo::default();
        vif.read_only = legacy.read_only;
        return Ok(vif);
    }
    Err(VolumeError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("invalid volume info file {}", path),
    )))
}

fn save_vif_volume_info(path: &str, info: &VifVolumeInfo) -> Result<(), VolumeError> {
    let content = serde_json::to_string_pretty(info)
        .map_err(|e| VolumeError::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?;
    std::fs::write(path, content)?;
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::needle::needle::Needle;
    use tempfile::TempDir;

    fn make_test_store(dirs: &[&str]) -> Store {
        let mut store = Store::new(NeedleMapKind::InMemory);
        for dir in dirs {
            store
                .add_location(
                    dir,
                    dir,
                    10,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(1.0),
                    Vec::new(),
                )
                .unwrap();
        }
        store
    }

    #[test]
    fn test_store_add_location() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                10,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(store.locations.len(), 1);
        assert_eq!(store.max_volume_count(), 10);
    }

    #[test]
    fn test_store_add_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = make_test_store(&[dir]);

        store
            .add_volume(
                VolumeId(1),
                "",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        assert!(store.has_volume(VolumeId(1)));
        assert!(!store.has_volume(VolumeId(2)));
        assert_eq!(store.total_volume_count(), 1);
    }

    #[test]
    fn test_store_read_write_delete() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = make_test_store(&[dir]);
        store
            .add_volume(
                VolumeId(1),
                "",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();

        // Write
        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xaa),
            data: b"hello store".to_vec(),
            data_size: 11,
            ..Needle::default()
        };
        let (offset, _size, unchanged) = store.write_volume_needle(VolumeId(1), &mut n).unwrap();
        assert!(!unchanged);
        assert!(offset > 0);

        // Read
        let mut read_n = Needle {
            id: NeedleId(1),
            ..Needle::default()
        };
        let count = store.read_volume_needle(VolumeId(1), &mut read_n).unwrap();
        assert_eq!(count, 11);
        assert_eq!(read_n.data, b"hello store");

        // Delete
        let mut del_n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xaa),
            ..Needle::default()
        };
        let deleted = store.delete_volume_needle(VolumeId(1), &mut del_n).unwrap();
        assert!(deleted.0 > 0);
    }

    #[test]
    fn test_store_multi_location() {
        let tmp1 = TempDir::new().unwrap();
        let tmp2 = TempDir::new().unwrap();
        let dir1 = tmp1.path().to_str().unwrap();
        let dir2 = tmp2.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir1,
                dir1,
                5,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                dir2,
                dir2,
                5,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(store.max_volume_count(), 10);

        // Add volumes — should go to location with fewest volumes
        store
            .add_volume(
                VolumeId(1),
                "",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(2),
                "",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();

        assert_eq!(store.total_volume_count(), 2);
        // Both locations should have 1 volume each (load-balanced)
        assert_eq!(store.locations[0].volumes_len(), 1);
        assert_eq!(store.locations[1].volumes_len(), 1);
    }

    #[test]
    fn test_store_delete_collection() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = make_test_store(&[dir]);

        store
            .add_volume(
                VolumeId(1),
                "pics",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(2),
                "pics",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(3),
                "docs",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        assert_eq!(store.total_volume_count(), 3);

        store.delete_collection("pics");
        assert_eq!(store.total_volume_count(), 1);
        assert!(store.has_volume(VolumeId(3)));
    }

    #[test]
    fn test_store_volume_not_found() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let store = make_test_store(&[dir]);

        let mut n = Needle {
            id: NeedleId(1),
            ..Needle::default()
        };
        let err = store.read_volume_needle(VolumeId(99), &mut n);
        assert!(matches!(err, Err(VolumeError::NotFound)));
    }
}
