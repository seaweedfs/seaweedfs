//! Store: the top-level storage manager for a volume server.
//!
//! A Store manages multiple DiskLocations (one per configured directory).
//! It coordinates volume placement, lookup, and lifecycle operations.
//! Matches Go's storage/store.go.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::disk_location::DiskLocation;
use crate::storage::needle::needle::Needle;
use crate::storage::needle_map::NeedleMapKind;
use crate::storage::super_block::ReplicaPlacement;
use crate::storage::types::*;
use crate::storage::volume::VolumeError;

/// Top-level storage manager containing all disk locations and their volumes.
pub struct Store {
    pub locations: Vec<DiskLocation>,
    pub needle_map_kind: NeedleMapKind,
    pub volume_size_limit: AtomicU64,
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
    ) -> io::Result<()> {
        let mut loc = DiskLocation::new(directory, idx_directory, max_volume_count, disk_type);
        loc.load_existing_volumes(self.needle_map_kind)?;
        self.locations.push(loc);
        Ok(())
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
    fn find_volume_mut(&mut self, vid: VolumeId) -> Option<(usize, &mut crate::storage::volume::Volume)> {
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
            if loc.is_disk_space_low {
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
    ) -> Result<(), VolumeError> {
        let loc_idx = self.find_free_location(&disk_type).ok_or_else(|| {
            VolumeError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("no free location for disk type {:?}", disk_type),
            ))
        })?;

        self.locations[loc_idx].create_volume(
            vid, collection, self.needle_map_kind,
            replica_placement, ttl, preallocate,
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
        // Find the location where the .dat file exists
        for loc in &mut self.locations {
            if &loc.disk_type != &disk_type {
                continue;
            }
            let base = crate::storage::volume::volume_file_name(
                &loc.directory, collection, vid,
            );
            let dat_path = format!("{}.dat", base);
            if std::path::Path::new(&dat_path).exists() {
                return loc.create_volume(
                    vid, collection, self.needle_map_kind,
                    None, None, 0,
                );
            }
        }
        Err(VolumeError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("volume {} not found on any disk", vid),
        )))
    }

    // ---- Read / Write / Delete ----

    /// Read a needle from a volume.
    pub fn read_volume_needle(&self, vid: VolumeId, n: &mut Needle) -> Result<i32, VolumeError> {
        let (_, vol) = self.find_volume(vid).ok_or(VolumeError::NotFound)?;
        vol.read_needle(n)
    }

    /// Write a needle to a volume.
    pub fn write_volume_needle(
        &mut self, vid: VolumeId, n: &mut Needle,
    ) -> Result<(u64, Size, bool), VolumeError> {
        let (_, vol) = self.find_volume_mut(vid).ok_or(VolumeError::NotFound)?;
        vol.write_needle(n, true)
    }

    /// Delete a needle from a volume.
    pub fn delete_volume_needle(
        &mut self, vid: VolumeId, n: &mut Needle,
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
        self.locations.iter()
            .map(|loc| loc.max_volume_count.load(Ordering::Relaxed))
            .sum()
    }

    /// Free volume slots across all locations.
    pub fn free_volume_count(&self) -> i32 {
        self.locations.iter()
            .map(|loc| loc.free_volume_count())
            .sum()
    }

    /// All volume IDs across all locations.
    pub fn all_volume_ids(&self) -> Vec<VolumeId> {
        let mut ids: Vec<VolumeId> = self.locations.iter()
            .flat_map(|loc| loc.volume_ids())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Close all locations and their volumes.
    pub fn close(&mut self) {
        for loc in &mut self.locations {
            loc.close();
        }
    }
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
            store.add_location(dir, dir, 10, DiskType::HardDrive).unwrap();
        }
        store
    }

    #[test]
    fn test_store_add_location() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store.add_location(dir, dir, 10, DiskType::HardDrive).unwrap();
        assert_eq!(store.locations.len(), 1);
        assert_eq!(store.max_volume_count(), 10);
    }

    #[test]
    fn test_store_add_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = make_test_store(&[dir]);

        store.add_volume(VolumeId(1), "", None, None, 0, DiskType::HardDrive).unwrap();
        assert!(store.has_volume(VolumeId(1)));
        assert!(!store.has_volume(VolumeId(2)));
        assert_eq!(store.total_volume_count(), 1);
    }

    #[test]
    fn test_store_read_write_delete() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = make_test_store(&[dir]);
        store.add_volume(VolumeId(1), "", None, None, 0, DiskType::HardDrive).unwrap();

        // Write
        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xaa),
            data: b"hello store".to_vec(),
            data_size: 11,
            ..Needle::default()
        };
        let (offset, size, unchanged) = store.write_volume_needle(VolumeId(1), &mut n).unwrap();
        assert!(!unchanged);
        assert!(offset > 0);

        // Read
        let mut read_n = Needle { id: NeedleId(1), ..Needle::default() };
        let count = store.read_volume_needle(VolumeId(1), &mut read_n).unwrap();
        assert_eq!(count, 11);
        assert_eq!(read_n.data, b"hello store");

        // Delete
        let mut del_n = Needle { id: NeedleId(1), cookie: Cookie(0xaa), ..Needle::default() };
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
        store.add_location(dir1, dir1, 5, DiskType::HardDrive).unwrap();
        store.add_location(dir2, dir2, 5, DiskType::HardDrive).unwrap();
        assert_eq!(store.max_volume_count(), 10);

        // Add volumes — should go to location with fewest volumes
        store.add_volume(VolumeId(1), "", None, None, 0, DiskType::HardDrive).unwrap();
        store.add_volume(VolumeId(2), "", None, None, 0, DiskType::HardDrive).unwrap();

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

        store.add_volume(VolumeId(1), "pics", None, None, 0, DiskType::HardDrive).unwrap();
        store.add_volume(VolumeId(2), "pics", None, None, 0, DiskType::HardDrive).unwrap();
        store.add_volume(VolumeId(3), "docs", None, None, 0, DiskType::HardDrive).unwrap();
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

        let mut n = Needle { id: NeedleId(1), ..Needle::default() };
        let err = store.read_volume_needle(VolumeId(99), &mut n);
        assert!(matches!(err, Err(VolumeError::NotFound)));
    }
}
