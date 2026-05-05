//! Store: the top-level storage manager for a volume server.
//!
//! A Store manages multiple DiskLocations (one per configured directory).
//! It coordinates volume placement, lookup, and lifecycle operations.
//! Matches Go's storage/store.go.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::config::MinFreeSpace;
use crate::pb::master_pb;
use crate::storage::disk_location::DiskLocation;
use crate::storage::erasure_coding::ec_shard::{EcVolumeShard, MAX_SHARD_COUNT};
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
    preallocate: AtomicBool,
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
            preallocate: AtomicBool::new(false),
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

        // After every disk has finished its per-disk EC scan, sweep
        // the store for shards that live on a disk without local index
        // files and load them by reaching across to a sibling disk's
        // .ecx / .ecj / .vif (seaweedfs/seaweedfs#9212 / #9244).
        // ec.balance / ec.rebuild can move shards onto a destination
        // node's second disk while leaving the index on the disk that
        // already held the volume; without this pass those orphan
        // shards stay invisible to the master.
        self.reconcile_ec_shards_across_disks();

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
        self.reconcile_ec_shards_across_disks();
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
    /// Matches Go's FindFreeLocation: accounts for EC shards when computing free slots.
    fn find_free_location(&self, disk_type: &DiskType) -> Option<usize> {
        use crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;

        let mut best: Option<(usize, i64)> = None; // (index, effective_free)
        for (i, loc) in self.locations.iter().enumerate() {
            if &loc.disk_type != disk_type {
                continue;
            }
            // Go treats MaxVolumeCount == 0 as unlimited (hasFreeDiskLocation)
            let max = loc.max_volume_count.load(Ordering::Relaxed) as i64;
            let effective_free = if max == 0 {
                i64::MAX // unlimited
            } else {
                // Go formula: currentFreeCount = (MaxVolumeCount - VolumesLen()) * DataShardsCount - EcShardCount()
                //             currentFreeCount /= DataShardsCount
                let free_count = (max - loc.volumes_len() as i64) * DATA_SHARDS_COUNT as i64
                    - loc.ec_shard_count() as i64;
                free_count / DATA_SHARDS_COUNT as i64
            };
            if effective_free <= 0 {
                continue;
            }
            if loc.is_disk_space_low.load(Ordering::Relaxed) {
                continue;
            }
            if best.is_none() || effective_free > best.unwrap().1 {
                best = Some((i, effective_free));
            }
        }
        best.map(|(i, _)| i)
    }

    /// Find a free location matching a predicate.
    /// Matches Go's Store.FindFreeLocation: picks the matching location with the
    /// most remaining volume capacity, while skipping low-disk locations.
    pub fn find_free_location_predicate<F>(&self, pred: F) -> Option<usize>
    where
        F: Fn(&DiskLocation) -> bool,
    {
        use crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT;

        let mut best: Option<(usize, i64)> = None;
        for (i, loc) in self.locations.iter().enumerate() {
            if !pred(loc) || loc.is_disk_space_low.load(Ordering::Relaxed) {
                continue;
            }

            let max = loc.max_volume_count.load(Ordering::Relaxed) as i64;
            let effective_free = if max == 0 {
                i64::MAX
            } else {
                let free_count = (max - loc.volumes_len() as i64) * DATA_SHARDS_COUNT as i64
                    - loc.ec_shard_count() as i64;
                free_count / DATA_SHARDS_COUNT as i64
            };
            if effective_free <= 0 {
                continue;
            }

            if best.is_none() || effective_free > best.unwrap().1 {
                best = Some((i, effective_free));
            }
        }
        best.map(|(i, _)| i)
    }

    /// Returns the index of the disk that should receive a new EC
    /// shard / index file for `(collection, vid)`. Selection order:
    ///
    /// 1. a disk that already has the EC volume mounted (in-memory state),
    /// 2. a disk that owns the `.ecx` file on disk (volume not yet mounted),
    /// 3. any HDD with free space,
    /// 4. any disk with free space.
    ///
    /// Step 2 is the missing primitive that pinned subsequent shards to
    /// the first-shard disk during `ec.rebuild`: rebuild only sets
    /// `CopyEcxFile=true` on the first shard, then relies on auto-select
    /// to land later shards on the same disk. Without an on-disk check,
    /// `has_ec_volume` returns false (no mount yet) and the fallback
    /// picks "any HDD with free space" — which can split shards from
    /// their index files across disks of the same node and lose them at
    /// startup. See seaweedfs/seaweedfs#9212.
    ///
    /// `data_shard_count` is taken as a parameter rather than read from
    /// `DATA_SHARDS_COUNT` so custom-ratio builds can swap the default
    /// without touching this helper.
    ///
    /// Single pass over `self.locations` with tier scoring; the
    /// highest-tier disk wins, ties broken by free shard-slot count.
    /// Mirrors `Store.FindEcShardTargetLocation` in
    /// `weed/storage/store_ec.go`.
    pub fn find_ec_shard_target_location(
        &self,
        collection: &str,
        vid: VolumeId,
        data_shard_count: u32,
    ) -> Option<usize> {
        const TIER_ANY_DISK: u8 = 1;
        const TIER_HDD: u8 = 2;
        const TIER_ECX_ON_DISK: u8 = 3;
        const TIER_MOUNTED: u8 = 4;

        let mut best: Option<(usize, u8, i64)> = None;
        for (i, loc) in self.locations.iter().enumerate() {
            if loc.is_disk_space_low.load(Ordering::Relaxed) {
                continue;
            }
            let free = ec_free_shard_count(loc, data_shard_count);
            if free <= 0 {
                continue;
            }
            let mut tier = TIER_ANY_DISK;
            if loc.disk_type == DiskType::HardDrive {
                tier = TIER_HDD;
            }
            if loc.has_ecx_file_on_disk(collection, vid) {
                tier = TIER_ECX_ON_DISK;
            }
            if loc.has_ec_volume(vid) {
                tier = TIER_MOUNTED;
            }
            let better = match best {
                None => true,
                Some((_, b_tier, b_free)) => tier > b_tier || (tier == b_tier && free > b_free),
            };
            if better {
                best = Some((i, tier, free));
            }
        }
        best.map(|(i, _, _)| i)
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
    pub fn delete_volume(&mut self, vid: VolumeId, only_empty: bool) -> Result<(), VolumeError> {
        for loc in &mut self.locations {
            if loc.find_volume(vid).is_some() {
                return loc.delete_volume(vid, only_empty);
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
            let vif_path = format!("{}.vif", base);
            if std::path::Path::new(&dat_path).exists() || std::path::Path::new(&vif_path).exists()
            {
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
                    if let Some((collection, file_vid)) = parse_volume_filename(&name) {
                        if file_vid == vid {
                            let base = strip_volume_suffix(&name)?;
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
        // Match Go's DeleteVolumeNeedle: check noWriteOrDelete before proceeding.
        let (_, vol) = self.find_volume(vid).ok_or(VolumeError::NotFound)?;
        if vol.is_no_write_or_delete() {
            return Err(VolumeError::ReadOnly);
        }

        let (_, vol) = self.find_volume_mut(vid).ok_or(VolumeError::NotFound)?;
        vol.delete_needle(n)
    }

    // ---- Collection operations ----

    /// Delete all volumes in a collection.
    pub fn delete_collection(&mut self, collection: &str) -> Result<(), String> {
        for loc in &mut self.locations {
            loc.delete_collection(collection)
                .map_err(|e| format!("delete collection {}: {}", collection, e))?;
        }
        crate::metrics::delete_collection_metrics(collection);
        Ok(())
    }

    // ---- Metrics ----

    /// Total volume count across all locations.
    pub fn total_volume_count(&self) -> usize {
        self.locations.iter().map(|loc| loc.volumes_len()).sum()
    }

    pub fn set_preallocate(&self, preallocate: bool) {
        self.preallocate.store(preallocate, Ordering::Relaxed);
    }

    pub fn get_preallocate(&self) -> bool {
        self.preallocate.load(Ordering::Relaxed)
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

                let unused_space = if self.get_preallocate() {
                    0
                } else {
                    loc.unused_space(volume_size_limit)
                };
                let unclaimed = (free as i64) - (unused_space as i64);

                let vol_count = loc.volumes_len() as i32;
                let loc_ec_shards = loc.ec_shard_count();
                let ec_equivalent = ((loc_ec_shards
                    + crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT
                    - 1)
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

    /// Mount EC shards for a volume (batch).
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

    /// Mount a single EC shard, searching all locations for the shard file.
    /// Matches Go's Store.MountEcShards which mounts one shard at a time.
    pub fn mount_ec_shard(
        &mut self,
        vid: VolumeId,
        collection: &str,
        shard_id: u32,
    ) -> Result<(), VolumeError> {
        for loc in &mut self.locations {
            // Check if the shard file exists on this location
            let shard = EcVolumeShard::new(&loc.directory, collection, vid, shard_id as u8);
            if std::path::Path::new(&shard.file_name()).exists() {
                loc.mount_ec_shards(vid, collection, &[shard_id])?;
                return Ok(());
            }
        }
        Err(VolumeError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("MountEcShards {}.{} not found on disk", vid, shard_id),
        )))
    }

    /// Unmount EC shards for a volume (batch).
    ///
    /// Iterates every location that has an EcVolume for `vid` and
    /// asks it to unmount whatever subset of `shard_ids` it actually
    /// has — required for cross-disk reconciled volumes where the
    /// requested shards may legitimately live on different disks of
    /// the same store (#9252). DiskLocation::unmount_ec_shards
    /// already skips shards that aren't mounted, so this is safe to
    /// fan out blindly.
    pub fn unmount_ec_shards(&mut self, vid: VolumeId, shard_ids: &[u32]) {
        for loc in &mut self.locations {
            if loc.has_ec_volume(vid) {
                loc.unmount_ec_shards(vid, shard_ids);
            }
        }
    }

    /// Unmount a single EC shard, searching all locations.
    /// Matches Go's Store.UnmountEcShards which unmounts one shard at a time.
    pub fn unmount_ec_shard(&mut self, vid: VolumeId, shard_id: u32) -> Result<(), VolumeError> {
        // Walk all locations rather than stopping at the first with the
        // vid — split-disk reconciled volumes can have the same vid on
        // multiple disks, with the target shard on any of them.
        for loc in &mut self.locations {
            if loc.has_ec_volume(vid) {
                loc.unmount_ec_shards(vid, &[shard_id]);
            }
        }
        // Go returns nil if shard not found (no error)
        Ok(())
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

    /// Returns the index of the disk location that has `(vid, shard_id)`
    /// mounted, if any. Mirrors Go's `Store.findEcShard` and is the
    /// right primitive for read/unmount/delete operations on a single
    /// shard, since reconciliation can mount the same `vid` on multiple
    /// disks (each holding a disjoint subset of the shards). Without
    /// this, callers using `find_ec_volume(vid)` would only see the
    /// first disk and miss shards that live on a sibling.
    pub fn find_ec_shard_location(&self, vid: VolumeId, shard_id: u32) -> Option<usize> {
        for (i, loc) in self.locations.iter().enumerate() {
            if let Some(ecv) = loc.find_ec_volume(vid) {
                if ecv.has_shard(shard_id as u8) {
                    return Some(i);
                }
            }
        }
        None
    }

    /// Like [`Self::find_ec_shard_location`] but returns the EcVolume
    /// reference directly. Borrows the store immutably for the
    /// EcVolume's lifetime.
    pub fn find_ec_volume_with_shard(
        &self,
        vid: VolumeId,
        shard_id: u32,
    ) -> Option<&EcVolume> {
        for loc in &self.locations {
            if let Some(ecv) = loc.find_ec_volume(vid) {
                if ecv.has_shard(shard_id as u8) {
                    return Some(ecv);
                }
            }
        }
        None
    }

    /// Aggregate the data dir of each mounted shard for `vid` across
    /// all locations. Returns the EcVolume to use for metadata
    /// (`.ecx`, `dir_idx`, etc — any per-disk EcVolume for this vid
    /// will do, since they all open the same `.ecx`) and a vector of
    /// per-shard data dirs (`None` when the shard isn't mounted on
    /// any disk).
    ///
    /// Mirrors Go's `Store.CollectEcShards` in
    /// `weed/storage/store_ec.go`. The decoder needs per-shard paths
    /// to support cross-disk reconciled volumes whose shards are
    /// split across data dirs.
    pub fn collect_ec_shard_dirs(
        &self,
        vid: VolumeId,
        max_shard_count: usize,
    ) -> Option<(&EcVolume, Vec<Option<String>>)> {
        let mut found_vol: Option<&EcVolume> = None;
        let mut dirs: Vec<Option<String>> = vec![None; max_shard_count];
        for loc in &self.locations {
            if let Some(ecv) = loc.find_ec_volume(vid) {
                if found_vol.is_none() {
                    found_vol = Some(ecv);
                }
                for shard_id in 0..max_shard_count {
                    if dirs[shard_id].is_none() && ecv.has_shard(shard_id as u8) {
                        dirs[shard_id] = Some(loc.directory.clone());
                    }
                }
            }
        }
        found_vol.map(|v| (v, dirs))
    }

    pub fn delete_expired_ec_volumes(
        &mut self,
    ) -> (
        Vec<master_pb::VolumeEcShardInformationMessage>,
        Vec<master_pb::VolumeEcShardInformationMessage>,
    ) {
        let mut ec_shards = Vec::new();
        let mut deleted = Vec::new();

        for (disk_id, loc) in self.locations.iter_mut().enumerate() {
            let mut expired_vids = Vec::new();
            for (vid, ec_vol) in loc.ec_volumes() {
                if ec_vol.is_time_to_destroy() {
                    expired_vids.push(*vid);
                } else {
                    ec_shards
                        .extend(ec_vol.to_volume_ec_shard_information_messages(disk_id as u32));
                }
            }

            for vid in expired_vids {
                let messages = loc
                    .find_ec_volume(vid)
                    .map(|ec_vol| ec_vol.to_volume_ec_shard_information_messages(disk_id as u32))
                    .unwrap_or_default();
                if let Some(mut ec_vol) = loc.remove_ec_volume(vid) {
                    for _ in 0..ec_vol.shard_count() {
                        crate::metrics::VOLUME_GAUGE
                            .with_label_values(&[&ec_vol.collection, "ec_shards"])
                            .dec();
                    }
                    ec_vol.destroy();
                    deleted.extend(messages);
                } else {
                    ec_shards.extend(messages);
                }
            }
        }

        (ec_shards, deleted)
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
    /// Uses MAX_SHARD_COUNT to support non-standard EC configurations.
    fn check_all_ec_shards_deleted(&self, vid: VolumeId, collection: &str) -> bool {
        for loc in &self.locations {
            for shard_id in 0..MAX_SHARD_COUNT as u8 {
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

        // Compute required space: use the larger of preallocate or estimated volume size
        // matching Go's CompactVolume space check
        let space_needed = {
            let (_, v) = self.find_volume(vid).unwrap();
            let estimated = v.dat_file_size().unwrap_or(0) + v.idx_file_size();
            std::cmp::max(preallocate, estimated)
        };

        if free < space_needed {
            return Err(format!(
                "not enough free space to compact volume {}. Required: {}, Free: {}",
                vid.0, space_needed, free
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
    let stem = strip_volume_suffix(filename)?;
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

fn strip_volume_suffix(filename: &str) -> Option<&str> {
    filename
        .strip_suffix(".dat")
        .or_else(|| filename.strip_suffix(".vif"))
        .or_else(|| filename.strip_suffix(".idx"))
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

/// Free EC shard capacity of `loc`, expressed in shard slots (not
/// volume-equivalent slots). `find_free_location_predicate` does similar
/// math but divides by `data_shard_count` at the end. That truncation can
/// exclude a disk that still has room for several individual shards
/// (e.g. `MaxVolumeCount=1`, `EcShardCount=1`, `data_shard_count=10` →
/// reports 0 despite 9 free shard slots), which would re-route subsequent
/// shards off the `.ecx`-owning disk and re-introduce the orphan-shard
/// layout this helper is meant to prevent (seaweedfs/seaweedfs#9212).
///
/// `MaxVolumeCount == 0` is the "unlimited" sentinel honoured elsewhere
/// in the store; report a synthetic large free count decremented by
/// current usage so unlimited disks are eligible and tie-breaks still
/// prefer the less-loaded one.
///
/// Mirrors `ecFreeShardCount` in `weed/storage/store_ec.go`.
fn ec_free_shard_count(loc: &DiskLocation, data_shard_count: u32) -> i64 {
    if data_shard_count == 0 {
        return 0;
    }
    let dsc = data_shard_count as i64;
    let max = loc.max_volume_count.load(Ordering::Relaxed) as i64;
    if max <= 0 {
        // Synthetic "unlimited" capacity. Use a large but
        // well-below-overflow base (1 << 60 ≈ 1.15e18) and saturating
        // arithmetic so even pathological usage never wraps and
        // tie-breaks among unlimited disks still meaningfully prefer
        // the less-loaded one. Clamp to ≥ 1 so the disk stays eligible
        // for placement no matter how loaded it is.
        const UNLIMITED_FREE: i64 = 1 << 60;
        let used = (loc.volumes_len() as i64)
            .saturating_mul(dsc)
            .saturating_add(loc.ec_shard_count() as i64);
        return UNLIMITED_FREE.saturating_sub(used).max(1);
    }
    let mut free = (max - loc.volumes_len() as i64) * dsc;
    free -= loc.ec_shard_count() as i64;
    if free < 0 {
        return 0;
    }
    free
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::needle::needle::Needle;
    use crate::storage::volume::volume_file_name;
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

        store.delete_collection("pics").unwrap();
        assert_eq!(store.total_volume_count(), 1);
        assert!(store.has_volume(VolumeId(3)));
    }

    #[test]
    fn test_maybe_adjust_volume_max_honors_preallocate_flag() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir,
                dir,
                2,
                DiskType::HardDrive,
                MinFreeSpace::Percent(1.0),
                Vec::new(),
            )
            .unwrap();
        // Use a large volume_size_limit so the unused-space difference between
        // preallocate=true (0) and preallocate=false (~2 × limit) is big enough
        // that integer-division rounding and disk-free fluctuations between the
        // two get_disk_stats calls cannot make the quotients equal.
        store
            .volume_size_limit
            .store(100 * 1024 * 1024, Ordering::Relaxed);
        store
            .add_volume(
                VolumeId(61),
                "preallocate_case",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        store
            .add_volume(
                VolumeId(62),
                "preallocate_case",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();
        for vid in [VolumeId(61), VolumeId(62)] {
            let dat_path = store.find_volume(vid).unwrap().1.dat_path();
            std::fs::OpenOptions::new()
                .write(true)
                .open(dat_path)
                .unwrap()
                .set_len((crate::storage::super_block::SUPER_BLOCK_SIZE + 1) as u64)
                .unwrap();
        }
        store.locations[0].original_max_volume_count = 0;
        store.locations[0]
            .max_volume_count
            .store(0, Ordering::Relaxed);

        store.set_preallocate(false);
        assert!(store.maybe_adjust_volume_max());
        let without_preallocate = store.locations[0].max_volume_count.load(Ordering::Relaxed);

        store.set_preallocate(true);
        assert!(store.maybe_adjust_volume_max());
        let with_preallocate = store.locations[0].max_volume_count.load(Ordering::Relaxed);

        assert!(with_preallocate > without_preallocate);
    }

    #[test]
    fn test_find_free_location_predicate_prefers_more_capacity_and_skips_low_disk() {
        let tmp1 = TempDir::new().unwrap();
        let dir1 = tmp1.path().to_str().unwrap();
        let tmp2 = TempDir::new().unwrap();
        let dir2 = tmp2.path().to_str().unwrap();

        let mut store = Store::new(NeedleMapKind::InMemory);
        store
            .add_location(
                dir1,
                dir1,
                3,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();
        store
            .add_location(
                dir2,
                dir2,
                5,
                DiskType::HardDrive,
                MinFreeSpace::Percent(0.0),
                Vec::new(),
            )
            .unwrap();

        store
            .add_volume(
                VolumeId(71),
                "find_free_location_case",
                None,
                None,
                0,
                DiskType::HardDrive,
                Version::current(),
            )
            .unwrap();

        let selected =
            store.find_free_location_predicate(|loc| loc.disk_type == DiskType::HardDrive);
        assert_eq!(selected, Some(1));

        store.locations[1]
            .is_disk_space_low
            .store(true, Ordering::Relaxed);

        let selected =
            store.find_free_location_predicate(|loc| loc.disk_type == DiskType::HardDrive);
        assert_eq!(selected, Some(0));
    }

    #[test]
    fn test_delete_expired_ec_volumes_removes_expired_entries() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut store = make_test_store(&[dir]);

        std::fs::write(format!("{}/expired_ec_case_9.ec00", dir), b"expired").unwrap();
        store.locations[0]
            .mount_ec_shards(VolumeId(9), "expired_ec_case", &[0])
            .unwrap();
        store.find_ec_volume_mut(VolumeId(9)).unwrap().expire_at_sec = 1;

        let (ec_shards, deleted) = store.delete_expired_ec_volumes();

        assert!(ec_shards.is_empty());
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].id, 9);
        assert!(!store.has_ec_volume(VolumeId(9)));
        assert!(!std::path::Path::new(&format!("{}/expired_ec_case_9.ec00", dir)).exists());
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

    /// Build a Store with N HDD disk locations under a single TempDir.
    /// Returns the store and the TempDir guard so callers keep the dirs
    /// alive for the test's lifetime.
    fn make_ec_target_test_store(numdirs: usize) -> (Store, TempDir) {
        let tmp = TempDir::new().unwrap();
        let mut store = Store::new(NeedleMapKind::InMemory);
        for i in 0..numdirs {
            let path = tmp.path().join(format!("data{}", i));
            std::fs::create_dir_all(&path).unwrap();
            store
                .add_location(
                    path.to_str().unwrap(),
                    path.to_str().unwrap(),
                    100,
                    DiskType::HardDrive,
                    MinFreeSpace::Percent(0.0),
                    Vec::new(),
                )
                .unwrap();
        }
        (store, tmp)
    }

    /// Reproduces the placement half of seaweedfs/seaweedfs#9212. After
    /// `ec.rebuild`'s first VolumeEcShardsCopy lands `.ecx` on disk N,
    /// subsequent shards arrive with `CopyEcxFile=false` and rely on
    /// auto-select. Without an on-disk check, `has_ec_volume` returns
    /// false (no mount yet) and the fallback picks "any HDD with free
    /// space" — splitting shards from their index files across disks.
    /// `find_ec_shard_target_location` must pin to the `.ecx`-owning
    /// disk via the on-disk check.
    #[test]
    fn test_find_ec_shard_target_location_pins_to_ecx_on_disk() {
        let (store, _tmp) = make_ec_target_test_store(3);
        let collection = "grafana-loki";
        let vid = VolumeId(1093);

        // Drop a sealed .ecx onto disk 2. Nothing is mounted yet — this
        // is the state right after the first VolumeEcShardsCopy with
        // CopyEcxFile=true and before any VolumeEcShardsMount has run.
        let base = volume_file_name(&store.locations[2].idx_directory, collection, vid);
        std::fs::write(format!("{}.ecx", base), vec![0u8; 20]).unwrap();

        let got = store.find_ec_shard_target_location(collection, vid, 10);
        assert_eq!(
            got,
            Some(2),
            "placement leaked off the .ecx-owning disk; got {:?}",
            got,
        );
    }

    /// An already-mounted EC volume on disk 1 must win over a stray
    /// `.ecx` on disk 2. Protects the post-startup steady state from
    /// being perturbed by leftover index files from a prior failed move.
    #[test]
    fn test_find_ec_shard_target_location_prefers_mounted_over_ecx() {
        let (mut store, _tmp) = make_ec_target_test_store(3);
        let collection = "grafana-loki";
        let vid = VolumeId(2222);

        // Mount an EC shard on disk 1 so has_ec_volume returns true.
        std::fs::write(
            format!("{}/{}_{}.ec00", store.locations[1].directory, collection, vid.0),
            b"shard data",
        )
        .unwrap();
        store.locations[1]
            .mount_ec_shards(vid, collection, &[0])
            .unwrap();

        // Stray .ecx on disk 2 must not win.
        let base = volume_file_name(&store.locations[2].idx_directory, collection, vid);
        std::fs::write(format!("{}.ecx", base), vec![0u8; 20]).unwrap();

        let got = store.find_ec_shard_target_location(collection, vid, 10);
        assert_eq!(got, Some(1), "expected the mounted disk to win; got {:?}", got);
    }

    /// Cold-volume case: no mount, no `.ecx` anywhere on this server.
    /// Selection should still fall through to an HDD fallback.
    #[test]
    fn test_find_ec_shard_target_location_falls_through_to_hdd_when_nothing_matches() {
        let (store, _tmp) = make_ec_target_test_store(2);
        let got = store.find_ec_shard_target_location("grafana-loki", VolumeId(3333), 10);
        assert!(got.is_some(), "expected an HDD fallback");
        assert_eq!(store.locations[got.unwrap()].disk_type, DiskType::HardDrive);
    }

    /// `MaxVolumeCount=0` is the "unlimited disk" sentinel. The previous
    /// formula returned a negative free count for unlimited disks,
    /// making placement skip them entirely. The unlimited branch in
    /// `ec_free_shard_count` must report a synthetic large free count.
    #[test]
    fn test_find_ec_shard_target_location_honours_unlimited_disk() {
        let (store, _tmp) = make_ec_target_test_store(1);
        store.locations[0]
            .max_volume_count
            .store(0, Ordering::Relaxed);

        let got = store.find_ec_shard_target_location("grafana-loki", VolumeId(4444), 10);
        assert_eq!(
            got,
            Some(0),
            "expected the only (unlimited) disk to be picked",
        );
    }

    /// Truncation hazard: with `MaxVolumeCount=1, EcShardCount=1,
    /// data_shard_count=10`, the old formula `(1*10 - 1) / 10 = 0`
    /// would have rounded the disk to "full" and routed subsequent
    /// shards elsewhere — the orphan-shard layout this PR exists to
    /// prevent. Accounting in shard slots fixes it.
    #[test]
    fn test_find_ec_shard_target_location_tight_provisioning_keeps_ecx_disk() {
        let (mut store, _tmp) = make_ec_target_test_store(2);
        store.locations[0]
            .max_volume_count
            .store(1, Ordering::Relaxed);
        store.locations[1]
            .max_volume_count
            .store(1, Ordering::Relaxed);

        let collection = "grafana-loki";
        let vid = VolumeId(5555);

        // Seed disk 1 with one EC shard so it owns .ecx and has 9
        // free shard slots remaining; the old formula would have
        // rounded that to 0.
        std::fs::write(
            format!("{}/{}_{}.ec00", store.locations[1].directory, collection, vid.0),
            b"shard data",
        )
        .unwrap();
        store.locations[1]
            .mount_ec_shards(vid, collection, &[0])
            .unwrap();

        let got = store.find_ec_shard_target_location(collection, vid, 10);
        assert_eq!(
            got,
            Some(1),
            "expected the .ecx-owning disk (1 shard placed, 9 free shard slots) to be picked; got {:?}",
            got,
        );
    }
}
