//! EcVolumeShard: a single shard file (.ec00-.ec13) of an erasure-coded volume.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};

use crate::storage::types::*;

pub const DATA_SHARDS_COUNT: usize = 10;
pub const PARITY_SHARDS_COUNT: usize = 4;
pub const TOTAL_SHARDS_COUNT: usize = DATA_SHARDS_COUNT + PARITY_SHARDS_COUNT;
pub const MAX_SHARD_COUNT: usize = 32;
pub const MIN_TOTAL_DISKS: usize = TOTAL_SHARDS_COUNT / PARITY_SHARDS_COUNT + 1;
pub const ERASURE_CODING_LARGE_BLOCK_SIZE: usize = 1024 * 1024 * 1024; // 1GB
pub const ERASURE_CODING_SMALL_BLOCK_SIZE: usize = 1024 * 1024; // 1MB

pub type ShardId = u8;

/// A single erasure-coded shard file.
pub struct EcVolumeShard {
    pub volume_id: VolumeId,
    pub shard_id: ShardId,
    pub collection: String,
    pub dir: String,
    pub disk_type: DiskType,
    ecd_file: Option<File>,
    ecd_file_size: i64,
}

impl EcVolumeShard {
    /// Create a new shard reference (does not open the file).
    pub fn new(dir: &str, collection: &str, volume_id: VolumeId, shard_id: ShardId) -> Self {
        EcVolumeShard {
            volume_id,
            shard_id,
            collection: collection.to_string(),
            dir: dir.to_string(),
            disk_type: DiskType::default(),
            ecd_file: None,
            ecd_file_size: 0,
        }
    }

    /// Shard file name, e.g. "dir/collection_42.ec03"
    pub fn file_name(&self) -> String {
        let base =
            crate::storage::volume::volume_file_name(&self.dir, &self.collection, self.volume_id);
        format!("{}.ec{:02}", base, self.shard_id)
    }

    /// Open the shard file for reading.
    pub fn open(&mut self) -> io::Result<()> {
        let path = self.file_name();
        let file = File::open(&path)?;
        self.ecd_file_size = file.metadata()?.len() as i64;
        self.ecd_file = Some(file);
        Ok(())
    }

    /// Create the shard file for writing.
    pub fn create(&mut self) -> io::Result<()> {
        let path = self.file_name();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        self.ecd_file = Some(file);
        self.ecd_file_size = 0;
        Ok(())
    }

    /// Read data at a specific offset.
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let file = self
            .ecd_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "shard file not open"))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_at(buf, offset)
        }

        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            // File::read_at is unix-only; fall back to seek + read.
            // We need a mutable reference for seek/read, so clone the handle.
            let mut f = file.try_clone()?;
            f.seek(SeekFrom::Start(offset))?;
            f.read(buf)
        }
    }

    /// Write data to the shard file (appends).
    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let file = self
            .ecd_file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "shard file not open"))?;
        file.write_all(data)?;
        self.ecd_file_size += data.len() as i64;
        Ok(())
    }

    pub fn file_size(&self) -> i64 {
        self.ecd_file_size
    }

    /// Close the shard file.
    pub fn close(&mut self) {
        if let Some(ref file) = self.ecd_file {
            let _ = file.sync_all();
        }
        self.ecd_file = None;
    }

    /// Delete the shard file from disk.
    pub fn destroy(&mut self) {
        self.close();
        let _ = fs::remove_file(self.file_name());
    }
}

/// ShardBits: bitmap tracking which shards are present.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardBits(pub u32);

impl ShardBits {
    pub fn add_shard_id(&mut self, id: ShardId) {
        assert!((id as usize) < 32, "shard id {} out of bounds (max 31)", id,);
        self.0 |= 1 << id;
    }

    pub fn remove_shard_id(&mut self, id: ShardId) {
        assert!((id as usize) < 32, "shard id {} out of bounds (max 31)", id,);
        self.0 &= !(1 << id);
    }

    pub fn has_shard_id(&self, id: ShardId) -> bool {
        if (id as usize) >= 32 {
            return false;
        }
        self.0 & (1 << id) != 0
    }

    pub fn shard_id_count(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Iterator over present shard IDs.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        let mut ids = Vec::with_capacity(self.shard_id_count());
        for i in 0..32 {
            if self.has_shard_id(i) {
                ids.push(i);
            }
        }
        ids
    }

    pub fn minus(&self, other: ShardBits) -> ShardBits {
        ShardBits(self.0 & !other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_bits() {
        let mut bits = ShardBits::default();
        assert_eq!(bits.shard_id_count(), 0);

        bits.add_shard_id(0);
        bits.add_shard_id(3);
        bits.add_shard_id(13);
        assert_eq!(bits.shard_id_count(), 3);
        assert!(bits.has_shard_id(0));
        assert!(bits.has_shard_id(3));
        assert!(!bits.has_shard_id(1));

        bits.remove_shard_id(3);
        assert!(!bits.has_shard_id(3));
        assert_eq!(bits.shard_id_count(), 2);
    }

    #[test]
    fn test_shard_bits_ids() {
        let mut bits = ShardBits::default();
        bits.add_shard_id(1);
        bits.add_shard_id(5);
        bits.add_shard_id(9);
        assert_eq!(bits.shard_ids(), vec![1, 5, 9]);
    }

    #[test]
    fn test_shard_bits_minus() {
        let mut a = ShardBits::default();
        a.add_shard_id(0);
        a.add_shard_id(1);
        a.add_shard_id(2);

        let mut b = ShardBits::default();
        b.add_shard_id(1);

        let c = a.minus(b);
        assert_eq!(c.shard_ids(), vec![0, 2]);
    }

    #[test]
    fn test_shard_file_name() {
        let shard = EcVolumeShard::new("/data", "pics", VolumeId(42), 3);
        assert_eq!(shard.file_name(), "/data/pics_42.ec03");
    }

    #[test]
    fn test_shard_file_name_no_collection() {
        let shard = EcVolumeShard::new("/data", "", VolumeId(7), 13);
        assert_eq!(shard.file_name(), "/data/7.ec13");
    }
}
