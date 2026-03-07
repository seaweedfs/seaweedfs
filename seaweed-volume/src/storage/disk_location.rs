//! DiskLocation: manages volumes on a single disk/directory.
//!
//! Each DiskLocation represents one storage directory containing .dat + .idx files.
//! A Store contains one or more DiskLocations (one per configured directory).
//! Matches Go's storage/disk_location.go.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

use crate::storage::needle_map::NeedleMapKind;
use crate::storage::super_block::ReplicaPlacement;
use crate::storage::types::*;
use crate::storage::volume::{Volume, VolumeError};

/// A single disk location managing volumes in one directory.
pub struct DiskLocation {
    pub directory: String,
    pub idx_directory: String,
    pub disk_type: DiskType,
    pub max_volume_count: AtomicI32,
    pub original_max_volume_count: i32,
    volumes: HashMap<VolumeId, Volume>,
    pub is_disk_space_low: bool,
    pub available_space: AtomicU64,
}

impl DiskLocation {
    pub fn new(directory: &str, idx_directory: &str, max_volume_count: i32, disk_type: DiskType) -> Self {
        let idx_dir = if idx_directory.is_empty() {
            directory.to_string()
        } else {
            idx_directory.to_string()
        };

        DiskLocation {
            directory: directory.to_string(),
            idx_directory: idx_dir,
            disk_type,
            max_volume_count: AtomicI32::new(max_volume_count),
            original_max_volume_count: max_volume_count,
            volumes: HashMap::new(),
            is_disk_space_low: false,
            available_space: AtomicU64::new(0),
        }
    }

    // ---- Volume management ----

    /// Load existing volumes from the directory.
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
                    eprintln!("Error loading volume {}: {}", vid, e);
                }
            }
        }

        Ok(())
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
            Version::current(),
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

    /// Close all volumes.
    pub fn close(&mut self) {
        for (_, v) in self.volumes.iter_mut() {
            v.close();
        }
        self.volumes.clear();
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
        let mut loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive);

        loc.create_volume(
            VolumeId(1), "", NeedleMapKind::InMemory,
            None, None, 0,
        ).unwrap();

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
            let mut loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive);
            loc.create_volume(VolumeId(1), "", NeedleMapKind::InMemory, None, None, 0).unwrap();
            loc.create_volume(VolumeId(2), "test", NeedleMapKind::InMemory, None, None, 0).unwrap();
            loc.close();
        }

        // Reload
        let mut loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive);
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
        let mut loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive);

        loc.create_volume(VolumeId(1), "", NeedleMapKind::InMemory, None, None, 0).unwrap();
        loc.create_volume(VolumeId(2), "", NeedleMapKind::InMemory, None, None, 0).unwrap();
        assert_eq!(loc.volumes_len(), 2);

        loc.delete_volume(VolumeId(1)).unwrap();
        assert_eq!(loc.volumes_len(), 1);
        assert!(loc.find_volume(VolumeId(1)).is_none());
    }

    #[test]
    fn test_disk_location_delete_collection() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut loc = DiskLocation::new(dir, dir, 10, DiskType::HardDrive);

        loc.create_volume(VolumeId(1), "pics", NeedleMapKind::InMemory, None, None, 0).unwrap();
        loc.create_volume(VolumeId(2), "pics", NeedleMapKind::InMemory, None, None, 0).unwrap();
        loc.create_volume(VolumeId(3), "docs", NeedleMapKind::InMemory, None, None, 0).unwrap();
        assert_eq!(loc.volumes_len(), 3);

        loc.delete_collection("pics");
        assert_eq!(loc.volumes_len(), 1);
        assert!(loc.find_volume(VolumeId(3)).is_some());
    }
}
