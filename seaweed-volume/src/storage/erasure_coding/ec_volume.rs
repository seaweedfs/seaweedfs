//! EcVolume: an erasure-coded volume with up to 14 shards.
//!
//! Each EcVolume has a sorted index (.ecx) and a deletion journal (.ecj).
//! Shards (.ec00-.ec13) may be distributed across multiple servers.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};

use crate::storage::erasure_coding::ec_locate;
use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::types::*;

/// An erasure-coded volume managing its local shards and index.
pub struct EcVolume {
    pub volume_id: VolumeId,
    pub collection: String,
    pub dir: String,
    pub dir_idx: String,
    pub version: Version,
    pub shards: Vec<Option<EcVolumeShard>>, // indexed by ShardId (0..14)
    pub dat_file_size: i64,
    pub data_shards: u32,
    pub parity_shards: u32,
    ecx_file: Option<File>,
    ecx_file_size: i64,
    ecj_file: Option<File>,
    pub disk_type: DiskType,
    /// Directory where .ecx/.ecj were actually found (may differ from dir_idx after fallback).
    ecx_actual_dir: String,
    /// Maps shard ID -> list of server addresses where that shard exists.
    /// Used for distributed EC reads across the cluster.
    pub shard_locations: HashMap<ShardId, Vec<String>>,
    /// EC volume expiration time (unix epoch seconds), set during EC encode from TTL.
    pub expire_at_sec: u64,
}

pub fn read_ec_shard_config(dir: &str, volume_id: VolumeId) -> (u32, u32) {
    let mut data_shards = crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT as u32;
    let mut parity_shards = crate::storage::erasure_coding::ec_shard::PARITY_SHARDS_COUNT as u32;
    let vif_path = format!("{}/{}.vif", dir, volume_id.0);
    if let Ok(vif_content) = std::fs::read_to_string(&vif_path) {
        if let Ok(vif_info) =
            serde_json::from_str::<crate::storage::volume::VifVolumeInfo>(&vif_content)
        {
            if let Some(ec) = vif_info.ec_shard_config {
                if ec.data_shards > 0 && ec.parity_shards > 0 {
                    data_shards = ec.data_shards;
                    parity_shards = ec.parity_shards;
                }
            }
        }
    }
    (data_shards, parity_shards)
}

impl EcVolume {
    /// Create a new EcVolume. Loads .ecx index and .ecj journal if present.
    pub fn new(
        dir: &str,
        dir_idx: &str,
        collection: &str,
        volume_id: VolumeId,
    ) -> io::Result<Self> {
        let (data_shards, parity_shards) = read_ec_shard_config(dir, volume_id);

        let total_shards = (data_shards + parity_shards) as usize;
        let mut shards = Vec::with_capacity(total_shards);
        for _ in 0..total_shards {
            shards.push(None);
        }

        // Read expire_at_sec from .vif if present
        let expire_at_sec = {
            let base =
                crate::storage::volume::volume_file_name(dir, collection, volume_id);
            let vif_path = format!("{}.vif", base);
            if let Ok(vif_content) = std::fs::read_to_string(&vif_path) {
                if let Ok(vif_info) =
                    serde_json::from_str::<crate::storage::volume::VifVolumeInfo>(&vif_content)
                {
                    vif_info.expire_at_sec
                } else {
                    0
                }
            } else {
                0
            }
        };

        let mut vol = EcVolume {
            volume_id,
            collection: collection.to_string(),
            dir: dir.to_string(),
            dir_idx: dir_idx.to_string(),
            version: Version::current(),
            shards,
            dat_file_size: 0,
            data_shards,
            parity_shards,
            ecx_file: None,
            ecx_file_size: 0,
            ecj_file: None,
            disk_type: DiskType::default(),
            ecx_actual_dir: dir_idx.to_string(),
            shard_locations: HashMap::new(),
            expire_at_sec,
        };

        // Open .ecx file (sorted index), with fallback to data dir
        let ecx_path = vol.ecx_file_name();
        if std::path::Path::new(&ecx_path).exists() {
            let file = File::open(&ecx_path)?;
            vol.ecx_file_size = file.metadata()?.len() as i64;
            vol.ecx_file = Some(file);
        } else if dir_idx != dir {
            // Fall back to data directory if .ecx was created before -dir.idx was configured
            let data_base = crate::storage::volume::volume_file_name(dir, collection, volume_id);
            let fallback_ecx = format!("{}.ecx", data_base);
            if std::path::Path::new(&fallback_ecx).exists() {
                tracing::info!(
                    volume_id = volume_id.0,
                    "ecx file not found in idx dir, falling back to data dir"
                );
                let file = File::open(&fallback_ecx)?;
                vol.ecx_file_size = file.metadata()?.len() as i64;
                vol.ecx_file = Some(file);
                vol.ecx_actual_dir = dir.to_string();
            }
        }

        // Open .ecj file (deletion journal) — use ecx_actual_dir for consistency
        let ecj_base = crate::storage::volume::volume_file_name(
            &vol.ecx_actual_dir, collection, volume_id,
        );
        let ecj_path = format!("{}.ecj", ecj_base);
        let ecj_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&ecj_path)?;
        vol.ecj_file = Some(ecj_file);

        Ok(vol)
    }

    // ---- File names ----

    #[allow(dead_code)]
    fn base_name(&self) -> String {
        crate::storage::volume::volume_file_name(&self.dir, &self.collection, self.volume_id)
    }

    fn idx_base_name(&self) -> String {
        crate::storage::volume::volume_file_name(&self.dir_idx, &self.collection, self.volume_id)
    }

    pub fn ecx_file_name(&self) -> String {
        format!("{}.ecx", self.idx_base_name())
    }

    pub fn ecj_file_name(&self) -> String {
        format!("{}.ecj", self.idx_base_name())
    }

    // ---- Shard management ----

    /// Add a shard to this volume.
    pub fn add_shard(&mut self, mut shard: EcVolumeShard) -> io::Result<()> {
        let id = shard.shard_id as usize;
        let total_shards = (self.data_shards + self.parity_shards) as usize;
        if id >= total_shards {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid shard id: {} (max {})", id, total_shards - 1),
            ));
        }
        shard.open()?;
        self.shards[id] = Some(shard);
        Ok(())
    }

    /// Remove and close a shard.
    pub fn remove_shard(&mut self, shard_id: ShardId) {
        if let Some(ref mut shard) = self.shards[shard_id as usize] {
            shard.close();
        }
        self.shards[shard_id as usize] = None;
    }

    /// Get a ShardBits bitmap of locally available shards.
    pub fn shard_bits(&self) -> ShardBits {
        let mut bits = ShardBits::default();
        for (i, shard) in self.shards.iter().enumerate() {
            if shard.is_some() {
                bits.add_shard_id(i as ShardId);
            }
        }
        bits
    }

    /// Count of locally available shards.
    pub fn shard_count(&self) -> usize {
        self.shards.iter().filter(|s| s.is_some()).count()
    }

    // ---- Shard locations (distributed tracking) ----

    /// Set the list of server addresses for a given shard ID.
    pub fn set_shard_locations(&mut self, shard_id: ShardId, locations: Vec<String>) {
        self.shard_locations.insert(shard_id, locations);
    }

    /// Get the list of server addresses for a given shard ID.
    pub fn get_shard_locations(&self, shard_id: ShardId) -> &[String] {
        self.shard_locations
            .get(&shard_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    // ---- Index operations ----

    /// Find a needle's offset and size in the sorted .ecx index via binary search.
    pub fn find_needle_from_ecx(&self, needle_id: NeedleId) -> io::Result<Option<(Offset, Size)>> {
        let ecx_file = self
            .ecx_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "ecx file not open"))?;

        let entry_count = self.ecx_file_size as usize / NEEDLE_MAP_ENTRY_SIZE;
        if entry_count == 0 {
            return Ok(None);
        }

        // Binary search
        let mut lo: usize = 0;
        let mut hi: usize = entry_count;
        let mut entry_buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let file_offset = (mid * NEEDLE_MAP_ENTRY_SIZE) as u64;

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                ecx_file.read_exact_at(&mut entry_buf, file_offset)?;
            }

            let (key, offset, size) = idx_entry_from_bytes(&entry_buf);
            if key == needle_id {
                return Ok(Some((offset, size)));
            } else if key < needle_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        Ok(None)
    }

    /// Locate the EC shard intervals needed to read a needle.
    pub fn locate_needle(
        &self,
        needle_id: NeedleId,
    ) -> io::Result<Option<(Offset, Size, Vec<ec_locate::Interval>)>> {
        let (offset, size) = match self.find_needle_from_ecx(needle_id)? {
            Some((o, s)) => (o, s),
            None => return Ok(None),
        };

        if size.is_deleted() || offset.is_zero() {
            return Ok(None);
        }

        let shard_size = self.shard_file_size();
        let intervals = ec_locate::locate_data(
            offset.to_actual_offset(),
            size,
            shard_size,
            self.data_shards,
        );

        Ok(Some((offset, size, intervals)))
    }

    /// Get the size of a single shard (all shards are the same size).
    fn shard_file_size(&self) -> i64 {
        for shard in &self.shards {
            if let Some(s) = shard {
                return s.file_size();
            }
        }
        0
    }

    // ---- Deletion journal ----

    /// Append a deleted needle ID to the .ecj journal.
    pub fn journal_delete(&mut self, needle_id: NeedleId) -> io::Result<()> {
        let ecj_file = self
            .ecj_file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "ecj file not open"))?;

        let mut buf = [0u8; NEEDLE_ID_SIZE];
        needle_id.to_bytes(&mut buf);
        ecj_file.write_all(&buf)?;
        ecj_file.sync_all()?;
        Ok(())
    }

    /// Append a deleted needle ID to the .ecj journal, validating the cookie first.
    /// Matches Go's DeleteEcShardNeedle which validates cookie before journaling.
    /// A cookie of 0 means skip cookie check (e.g., orphan cleanup).
    pub fn journal_delete_with_cookie(
        &mut self,
        needle_id: NeedleId,
        cookie: crate::storage::types::Cookie,
    ) -> io::Result<()> {
        // cookie == 0 indicates SkipCookieCheck was requested
        if cookie.0 != 0 {
            // Try to read the needle's cookie from the EC shards to validate
            // Look up the needle in ecx index to find its offset, then read header from shard
            if let Ok(Some((offset, size))) = self.find_needle_from_ecx(needle_id) {
                if !size.is_deleted() && !offset.is_zero() {
                    let actual_offset = offset.to_actual_offset() as u64;
                    // Determine which shard contains this offset and read the cookie
                    let shard_size = self
                        .shards
                        .iter()
                        .filter_map(|s| s.as_ref())
                        .map(|s| s.file_size())
                        .next()
                        .unwrap_or(0) as u64;
                    if shard_size > 0 {
                        let shard_id = (actual_offset / shard_size) as usize;
                        let shard_offset = actual_offset % shard_size;
                        if let Some(Some(shard)) = self.shards.get(shard_id) {
                            let mut header_buf = [0u8; 4]; // cookie is first 4 bytes of needle
                            if shard.read_at(&mut header_buf, shard_offset).is_ok() {
                                let needle_cookie =
                                    crate::storage::types::Cookie(u32::from_be_bytes(header_buf));
                                if needle_cookie != cookie {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("unexpected cookie {:x}", cookie.0),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
        self.journal_delete(needle_id)
    }

    /// Read all deleted needle IDs from the .ecj journal.
    pub fn read_deleted_needles(&self) -> io::Result<Vec<NeedleId>> {
        let ecj_path = self.ecj_file_name();
        if !std::path::Path::new(&ecj_path).exists() {
            return Ok(Vec::new());
        }

        let data = fs::read(&ecj_path)?;
        let count = data.len() / NEEDLE_ID_SIZE;
        let mut needles = Vec::with_capacity(count);
        for i in 0..count {
            let start = i * NEEDLE_ID_SIZE;
            let id = NeedleId::from_bytes(&data[start..start + NEEDLE_ID_SIZE]);
            needles.push(id);
        }
        Ok(needles)
    }

    // ---- Lifecycle ----

    pub fn close(&mut self) {
        for shard in &mut self.shards {
            if let Some(s) = shard {
                s.close();
            }
            *shard = None;
        }
        self.ecx_file = None;
        self.ecj_file = None;
    }

    pub fn destroy(&mut self) {
        for shard in &mut self.shards {
            if let Some(s) = shard {
                s.destroy();
            }
            *shard = None;
        }
        // Remove .ecx/.ecj from ecx_actual_dir (where they were found)
        let actual_base = crate::storage::volume::volume_file_name(
            &self.ecx_actual_dir, &self.collection, self.volume_id,
        );
        let _ = fs::remove_file(format!("{}.ecx", actual_base));
        let _ = fs::remove_file(format!("{}.ecj", actual_base));
        // Also try the configured idx dir and data dir in case files exist in either
        if self.ecx_actual_dir != self.dir_idx {
            let _ = fs::remove_file(self.ecx_file_name());
            let _ = fs::remove_file(self.ecj_file_name());
        }
        if self.ecx_actual_dir != self.dir && self.dir_idx != self.dir {
            let data_base = crate::storage::volume::volume_file_name(
                &self.dir, &self.collection, self.volume_id,
            );
            let _ = fs::remove_file(format!("{}.ecx", data_base));
            let _ = fs::remove_file(format!("{}.ecj", data_base));
        }
        self.ecx_file = None;
        self.ecj_file = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::idx;
    use tempfile::TempDir;

    fn write_ecx_file(
        dir: &str,
        collection: &str,
        vid: VolumeId,
        entries: &[(NeedleId, Offset, Size)],
    ) {
        let base = crate::storage::volume::volume_file_name(dir, collection, vid);
        let ecx_path = format!("{}.ecx", base);
        let mut file = File::create(&ecx_path).unwrap();

        // Write sorted entries
        for &(key, offset, size) in entries {
            let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];
            idx_entry_to_bytes(&mut buf, key, offset, size);
            file.write_all(&buf).unwrap();
        }
    }

    #[test]
    fn test_ec_volume_find_needle() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Write sorted ecx entries
        let entries = vec![
            (NeedleId(1), Offset::from_actual_offset(8), Size(100)),
            (NeedleId(5), Offset::from_actual_offset(200), Size(200)),
            (NeedleId(10), Offset::from_actual_offset(500), Size(300)),
        ];
        write_ecx_file(dir, "", VolumeId(1), &entries);

        let vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();

        // Found
        let result = vol.find_needle_from_ecx(NeedleId(5)).unwrap();
        assert!(result.is_some());
        let (offset, size) = result.unwrap();
        assert_eq!(offset.to_actual_offset(), 200);
        assert_eq!(size, Size(200));

        // Not found
        let result = vol.find_needle_from_ecx(NeedleId(7)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_ec_volume_journal() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Need ecx file for EcVolume::new to succeed
        write_ecx_file(dir, "", VolumeId(1), &[]);

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();

        vol.journal_delete(NeedleId(10)).unwrap();
        vol.journal_delete(NeedleId(20)).unwrap();

        let deleted = vol.read_deleted_needles().unwrap();
        assert_eq!(deleted, vec![NeedleId(10), NeedleId(20)]);
    }

    #[test]
    fn test_ec_volume_shard_bits() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(dir, "", VolumeId(1), &[]);

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        assert_eq!(vol.shard_count(), 0);

        // Create a shard file so we can add it
        let mut shard = EcVolumeShard::new(dir, "", VolumeId(1), 3);
        shard.create().unwrap();
        shard.write_all(&[0u8; 100]).unwrap();
        shard.close();

        vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 3))
            .unwrap();
        assert_eq!(vol.shard_count(), 1);
        assert!(vol.shard_bits().has_shard_id(3));
    }
}
