//! Volume: the core storage unit — a .dat file + .idx index.
//!
//! Each volume contains many needles (files). It manages:
//!   - Reading/writing/deleting needles from the .dat file
//!   - Maintaining the in-memory NeedleMap (NeedleId → Offset+Size)
//!   - SuperBlock at offset 0 of the .dat file
//!   - Metrics (file count, content size, deleted count)
//!
//! Matches Go's storage/volume.go, volume_loading.go, volume_read.go,
//! volume_write.go, volume_super_block.go.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::warn;

use crate::storage::needle::needle::{self, Needle, NeedleError, get_actual_size};
use crate::storage::needle_map::{CompactNeedleMap, NeedleMapKind};
use crate::storage::super_block::{SuperBlock, ReplicaPlacement, SUPER_BLOCK_SIZE};
use crate::storage::types::*;

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum VolumeError {
    #[error("not found")]
    NotFound,

    #[error("already deleted")]
    Deleted,

    #[error("needle size mismatch")]
    SizeMismatch,

    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("cookie mismatch: {0:#x}")]
    CookieMismatch(u32),

    #[error("volume not empty")]
    NotEmpty,

    #[error("volume already exists")]
    AlreadyExists,

    #[error("volume is read-only")]
    ReadOnly,

    #[error("volume size limit exceeded: current {current}, limit {limit}")]
    SizeLimitExceeded { current: u64, limit: u64 },

    #[error("volume not initialized")]
    NotInitialized,

    #[error("needle error: {0}")]
    Needle(#[from] NeedleError),

    #[error("super block error: {0}")]
    SuperBlock(#[from] crate::storage::super_block::SuperBlockError),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

// ============================================================================
// Volume
// ============================================================================

pub struct Volume {
    pub id: VolumeId,
    dir: String,
    dir_idx: String,
    pub collection: String,

    dat_file: Option<File>,
    nm: Option<CompactNeedleMap>,
    needle_map_kind: NeedleMapKind,

    pub super_block: SuperBlock,

    no_write_or_delete: bool,
    no_write_can_delete: bool,

    last_modified_ts_seconds: u64,
    last_append_at_ns: u64,

    last_compact_index_offset: u64,
    last_compact_revision: u16,

    is_compacting: bool,

    _last_io_error: Option<io::Error>,
}

/// Windows helper: loop seek_read until buffer is fully filled.
#[cfg(windows)]
fn read_exact_at(file: &File, buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.seek_read(&mut buf[filled..], offset)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF in seek_read"));
        }
        filled += n;
        offset += n as u64;
    }
    Ok(())
}

impl Volume {
    /// Create and load a volume from disk.
    pub fn new(
        dirname: &str,
        dir_idx: &str,
        collection: &str,
        id: VolumeId,
        needle_map_kind: NeedleMapKind,
        replica_placement: Option<ReplicaPlacement>,
        ttl: Option<crate::storage::needle::ttl::TTL>,
        preallocate: u64,
        version: Version,
    ) -> Result<Self, VolumeError> {
        let mut v = Volume {
            id,
            dir: dirname.to_string(),
            dir_idx: dir_idx.to_string(),
            collection: collection.to_string(),
            dat_file: None,
            nm: None,
            needle_map_kind,
            super_block: SuperBlock {
                replica_placement: replica_placement.unwrap_or_default(),
                ttl: ttl.unwrap_or(crate::storage::needle::ttl::TTL::EMPTY),
                ..SuperBlock::default()
            },
            no_write_or_delete: false,
            no_write_can_delete: false,
            last_modified_ts_seconds: 0,
            last_append_at_ns: 0,
            last_compact_index_offset: 0,
            last_compact_revision: 0,
            is_compacting: false,
            _last_io_error: None,
        };

        v.load(true, true, preallocate, version)?;
        Ok(v)
    }

    // ---- File naming (matching Go) ----

    /// Base filename: dir/collection_id or dir/id
    pub fn data_file_name(&self) -> String {
        volume_file_name(&self.dir, &self.collection, self.id)
    }

    pub fn index_file_name(&self) -> String {
        volume_file_name(&self.dir_idx, &self.collection, self.id)
    }

    pub fn file_name(&self, ext: &str) -> String {
        match ext {
            ".idx" | ".cpx" | ".ldb" | ".cpldb" => {
                format!("{}{}", self.index_file_name(), ext)
            }
            _ => {
                format!("{}{}", self.data_file_name(), ext)
            }
        }
    }

    pub fn version(&self) -> Version {
        self.super_block.version
    }

    // ---- Loading ----

    fn load(
        &mut self,
        also_load_index: bool,
        create_dat_if_missing: bool,
        preallocate: u64,
        version: Version,
    ) -> Result<(), VolumeError> {
        let dat_path = self.file_name(".dat");
        let mut already_has_super_block = false;

        if Path::new(&dat_path).exists() {
            let metadata = fs::metadata(&dat_path)?;

            // Try to open read-write; fall back to read-only
            match OpenOptions::new().read(true).write(true).open(&dat_path) {
                Ok(file) => {
                    self.dat_file = Some(file);
                }
                Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                    self.dat_file = Some(File::open(&dat_path)?);
                    self.no_write_or_delete = true;
                }
                Err(e) => return Err(e.into()),
            }

            self.last_modified_ts_seconds = metadata
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH)
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if metadata.len() >= SUPER_BLOCK_SIZE as u64 {
                already_has_super_block = true;
            }
        } else if create_dat_if_missing {
            // Create directory if needed
            if let Some(parent) = Path::new(&dat_path).parent() {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&dat_path)?;
            if preallocate > 0 {
                file.set_len(preallocate)?;
                file.set_len(0)?; // truncate back — the preallocate is just a hint
            }
            self.dat_file = Some(file);
        } else {
            return Err(VolumeError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("volume data file {} does not exist", dat_path),
            )));
        }

        if already_has_super_block {
            self.read_super_block()?;
            if !self.super_block.version.is_supported() {
                return Err(VolumeError::UnsupportedVersion(self.super_block.version.0));
            }
        } else {
            self.maybe_write_super_block(version)?;
        }

        if also_load_index {
            self.load_index()?;
        }

        Ok(())
    }

    fn load_index(&mut self) -> Result<(), VolumeError> {
        if self.needle_map_kind != NeedleMapKind::InMemory {
            warn!(
                volume_id = self.id.0,
                kind = ?self.needle_map_kind,
                "only InMemory needle map is currently supported, falling back to InMemory"
            );
        }

        let idx_path = self.file_name(".idx");

        // Ensure idx directory exists
        if let Some(parent) = Path::new(&idx_path).parent() {
            fs::create_dir_all(parent)?;
        }

        if self.no_write_or_delete {
            // Open read-only
            if Path::new(&idx_path).exists() {
                let mut idx_file = File::open(&idx_path)?;
                let nm = CompactNeedleMap::load_from_idx(&mut idx_file)?;
                self.nm = Some(nm);
            } else {
                // Missing .idx with existing .dat could orphan needles
                let dat_path = self.file_name(".dat");
                if Path::new(&dat_path).exists() {
                    let dat_size = fs::metadata(&dat_path).map(|m| m.len()).unwrap_or(0);
                    if dat_size > SUPER_BLOCK_SIZE as u64 {
                        warn!(
                            volume_id = self.id.0,
                            ".idx file missing but .dat exists with data; needles may be orphaned"
                        );
                    }
                }
                self.nm = Some(CompactNeedleMap::new());
            }
        } else {
            // Open read-write (create if missing)
            let idx_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&idx_path)?;

            let idx_size = idx_file.metadata()?.len();
            let mut idx_reader = io::BufReader::new(&idx_file);
            let mut nm = CompactNeedleMap::load_from_idx(&mut idx_reader)?;

            // Re-open for append-only writes
            let write_file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&idx_path)?;
            nm.set_idx_file(Box::new(write_file), idx_size);
            self.nm = Some(nm);
        }

        Ok(())
    }

    // ---- SuperBlock I/O ----

    fn read_super_block(&mut self) -> Result<(), VolumeError> {
        let dat_file = self.dat_file.as_mut().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;

        dat_file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; SUPER_BLOCK_SIZE];
        dat_file.read_exact(&mut header)?;

        let extra_size = u16::from_be_bytes([header[6], header[7]]);
        let total_size = SUPER_BLOCK_SIZE + extra_size as usize;

        let mut full_buf = vec![0u8; total_size];
        full_buf[..SUPER_BLOCK_SIZE].copy_from_slice(&header);
        if extra_size > 0 {
            dat_file.read_exact(&mut full_buf[SUPER_BLOCK_SIZE..])?;
        }

        self.super_block = SuperBlock::from_bytes(&full_buf)?;
        Ok(())
    }

    fn maybe_write_super_block(&mut self, version: Version) -> Result<(), VolumeError> {
        let dat_file = self.dat_file.as_mut().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;

        let dat_size = dat_file.metadata()?.len();
        if dat_size == 0 {
            if !version.is_supported() {
                return Err(VolumeError::UnsupportedVersion(version.0));
            }
            self.super_block.version = version;
            let bytes = self.super_block.to_bytes();
            dat_file.seek(SeekFrom::Start(0))?;
            dat_file.write_all(&bytes)?;
            dat_file.sync_all()?;
        }
        Ok(())
    }

    // ---- Read ----

    /// Read a needle by its ID from the volume.
    pub fn read_needle(&self, n: &mut Needle) -> Result<i32, VolumeError> {
        self.read_needle_opt(n, false)
    }

    pub fn read_needle_opt(&self, n: &mut Needle, read_deleted: bool) -> Result<i32, VolumeError> {
        let nm = self.nm.as_ref().ok_or(VolumeError::NotFound)?;
        let nv = nm.get(n.id).ok_or(VolumeError::NotFound)?;

        if nv.offset.is_zero() {
            return Err(VolumeError::NotFound);
        }

        let mut read_size = nv.size;
        if read_size.is_deleted() {
            if read_deleted && !read_size.is_tombstone() {
                // Negate to get original size
                read_size = Size(-read_size.0);
            } else {
                return Err(VolumeError::Deleted);
            }
        }
        if read_size.0 == 0 {
            return Ok(0);
        }

        self.read_needle_data_at(n, nv.offset.to_actual_offset(), read_size)?;

        // TTL expiry check
        if n.has_ttl() {
            if let Some(ref ttl) = n.ttl {
                let ttl_minutes = ttl.minutes();
                if ttl_minutes > 0 && n.has_last_modified_date() && n.append_at_ns > 0 {
                    let expire_at_ns = n.append_at_ns + (ttl_minutes as u64) * 60 * 1_000_000_000;
                    let now_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as u64;
                    if now_ns >= expire_at_ns {
                        return Err(VolumeError::NotFound);
                    }
                }
            }
        }

        Ok(n.data_size as i32)
    }

    /// Read needle data from .dat file at given offset.
    pub fn read_needle_data_at(&self, n: &mut Needle, offset: i64, size: Size) -> Result<(), VolumeError> {
        let dat_file = self.dat_file.as_ref().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;

        let version = self.version();
        let actual_size = get_actual_size(size, version);

        // Use pread (read_at) to avoid seeking with shared reference
        let mut buf = vec![0u8; actual_size as usize];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            dat_file.read_exact_at(&mut buf, offset as u64)?;
        }
        #[cfg(windows)]
        {
            read_exact_at(dat_file, &mut buf, offset as u64)?;
        }
        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("Platform not supported: only unix and windows are supported");
        }

        n.read_bytes(&mut buf, offset, size, version)?;
        Ok(())
    }

    /// Read raw needle blob at a specific offset.
    pub fn read_needle_blob(&self, offset: i64, size: Size) -> Result<Vec<u8>, VolumeError> {
        let dat_file = self.dat_file.as_ref().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;

        let version = self.version();
        let actual_size = get_actual_size(size, version);
        let mut buf = vec![0u8; actual_size as usize];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            dat_file.read_exact_at(&mut buf, offset as u64)?;
        }
        #[cfg(windows)]
        {
            read_exact_at(dat_file, &mut buf, offset as u64)?;
        }

        Ok(buf)
    }

    // ---- Write ----

    /// Write a needle to the volume (synchronous path).
    pub fn write_needle(&mut self, n: &mut Needle, check_cookie: bool) -> Result<(u64, Size, bool), VolumeError> {
        if self.no_write_or_delete {
            return Err(VolumeError::ReadOnly);
        }

        self.do_write_request(n, check_cookie)
    }

    fn do_write_request(&mut self, n: &mut Needle, check_cookie: bool) -> Result<(u64, Size, bool), VolumeError> {
        // Ensure checksum is computed before dedup check
        if n.checksum == crate::storage::needle::crc::CRC(0) && !n.data.is_empty() {
            n.checksum = crate::storage::needle::crc::CRC::new(&n.data);
        }

        // Dedup check
        if self.is_file_unchanged(n) {
            return Ok((0, Size(n.data_size as i32), true));
        }

        // Cookie validation for existing needle
        if let Some(nm) = &self.nm {
            if let Some(nv) = nm.get(n.id) {
                if !nv.offset.is_zero() && nv.size.is_valid() {
                    let mut existing = Needle::default();
                    // Read only the header to check cookie
                    self.read_needle_header(&mut existing, nv.offset.to_actual_offset())?;

                    if n.cookie.0 == 0 && !check_cookie {
                        n.cookie = existing.cookie;
                    }
                    if existing.cookie != n.cookie {
                        return Err(VolumeError::CookieMismatch(n.cookie.0));
                    }
                }
            }
        }

        // Update append timestamp
        n.append_at_ns = get_append_at_ns(self.last_append_at_ns);

        // Append to .dat file
        let (offset, size, _actual_size) = self.append_needle(n)?;
        self.last_append_at_ns = n.append_at_ns;

        // Update needle map
        let should_update = if let Some(nm) = &self.nm {
            match nm.get(n.id) {
                Some(nv) => (nv.offset.to_actual_offset() as u64) < offset,
                None => true,
            }
        } else {
            true
        };

        if should_update {
            if let Some(nm) = &mut self.nm {
                nm.put(n.id, Offset::from_actual_offset(offset as i64), n.size)?;
            }
        }

        if self.last_modified_ts_seconds < n.last_modified {
            self.last_modified_ts_seconds = n.last_modified;
        }

        Ok((offset, size, false))
    }

    fn read_needle_header(&self, n: &mut Needle, offset: i64) -> Result<(), VolumeError> {
        let dat_file = self.dat_file.as_ref().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;

        let mut header = [0u8; NEEDLE_HEADER_SIZE];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            dat_file.read_exact_at(&mut header, offset as u64)?;
        }
        #[cfg(windows)]
        {
            read_exact_at(dat_file, &mut header, offset as u64)?;
        }

        n.read_header(&header);
        Ok(())
    }

    fn is_file_unchanged(&self, n: &Needle) -> bool {
        // Don't dedup for volumes with TTL
        if self.super_block.ttl != crate::storage::needle::ttl::TTL::EMPTY {
            return false;
        }

        if let Some(nm) = &self.nm {
            if let Some(nv) = nm.get(n.id) {
                if !nv.offset.is_zero() && nv.size.is_valid() {
                    let mut old = Needle::default();
                    if self.read_needle_data_at(&mut old, nv.offset.to_actual_offset(), nv.size).is_ok() {
                        if old.cookie == n.cookie
                            && old.checksum == n.checksum
                            && old.data == n.data
                        {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }

    /// Append a needle to the .dat file. Returns (offset, size, actual_size).
    fn append_needle(&mut self, n: &mut Needle) -> Result<(u64, Size, i64), VolumeError> {
        let version = self.version();
        let bytes = n.write_bytes(version);
        let actual_size = bytes.len() as i64;

        let dat_file = self.dat_file.as_mut().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;

        let offset = dat_file.seek(SeekFrom::End(0))?;
        dat_file.write_all(&bytes)?;

        Ok((offset, n.size, actual_size))
    }

    // ---- Delete ----

    /// Delete a needle from the volume.
    pub fn delete_needle(&mut self, n: &mut Needle) -> Result<Size, VolumeError> {
        if self.no_write_or_delete {
            return Err(VolumeError::ReadOnly);
        }
        self.do_delete_request(n)
    }

    fn do_delete_request(&mut self, n: &mut Needle) -> Result<Size, VolumeError> {
        let (found, size, _stored_offset) = if let Some(nm) = &self.nm {
            if let Some(nv) = nm.get(n.id) {
                if !nv.size.is_deleted() {
                    (true, nv.size, nv.offset)
                } else {
                    (false, Size(0), Offset::default())
                }
            } else {
                (false, Size(0), Offset::default())
            }
        } else {
            return Ok(Size(0));
        };

        if !found {
            return Ok(Size(0));
        }

        // Write tombstone: append needle with empty data
        n.data = vec![];
        n.append_at_ns = get_append_at_ns(self.last_append_at_ns);
        let (offset, _, _) = self.append_needle(n)?;
        self.last_append_at_ns = n.append_at_ns;

        // Update index
        if let Some(nm) = &mut self.nm {
            nm.delete(n.id, Offset::from_actual_offset(offset as i64))?;
        }

        Ok(size)
    }

    // ---- Metrics ----

    pub fn content_size(&self) -> u64 {
        self.nm.as_ref().map_or(0, |nm| nm.content_size())
    }

    pub fn deleted_size(&self) -> u64 {
        self.nm.as_ref().map_or(0, |nm| nm.deleted_size())
    }

    pub fn file_count(&self) -> i64 {
        self.nm.as_ref().map_or(0, |nm| nm.file_count())
    }

    pub fn deleted_count(&self) -> i64 {
        self.nm.as_ref().map_or(0, |nm| nm.deleted_count())
    }

    pub fn max_file_key(&self) -> NeedleId {
        self.nm.as_ref().map_or(NeedleId(0), |nm| nm.max_file_key())
    }

    pub fn is_read_only(&self) -> bool {
        self.no_write_or_delete || self.no_write_can_delete
    }

    pub fn last_compact_revision(&self) -> u16 {
        self.last_compact_revision
    }

    pub fn last_modified_ts(&self) -> u64 {
        self.last_modified_ts_seconds
    }

    /// Read all live needles from the volume (for ReadAllNeedles streaming RPC).
    pub fn read_all_needles(&self) -> Result<Vec<Needle>, VolumeError> {
        let nm = self.nm.as_ref().ok_or(VolumeError::NotFound)?;
        let mut needles = Vec::new();
        for (&key, nv) in nm.iter() {
            if !nv.size.is_valid() {
                continue; // skip deleted
            }
            let mut n = Needle {
                id: key,
                ..Needle::default()
            };
            if let Ok(()) = self.read_needle_data_at(&mut n, nv.offset.to_actual_offset(), nv.size) {
                needles.push(n);
            }
        }
        Ok(needles)
    }

    /// Scan raw needle entries from the .dat file starting at `from_offset`.
    /// Returns (needle_header_bytes, needle_body_bytes, append_at_ns) for each needle.
    /// Used by VolumeTailSender to stream raw bytes.
    pub fn scan_raw_needles_from(&self, from_offset: u64) -> Result<Vec<(Vec<u8>, Vec<u8>, u64)>, VolumeError> {
        let dat_file = self.dat_file.as_ref().ok_or(VolumeError::NotFound)?;
        let version = self.super_block.version;
        let dat_size = dat_file.metadata()?.len();
        let mut entries = Vec::new();
        let mut offset = from_offset;

        let mut dat = dat_file.try_clone()?;
        while offset < dat_size {
            // Read needle header (16 bytes)
            let mut header = [0u8; NEEDLE_HEADER_SIZE];
            dat.seek(SeekFrom::Start(offset))?;
            match dat.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let (_cookie, _id, size) = Needle::parse_header(&header);
            if size.0 == 0 && _id.is_empty() {
                break;
            }

            let body_length = needle::needle_body_length(size, version);
            let total_size = NEEDLE_HEADER_SIZE as u64 + body_length as u64;

            if size.is_deleted() || size.0 <= 0 {
                offset += total_size;
                continue;
            }

            // Read body bytes
            let mut body = vec![0u8; body_length as usize];
            dat.seek(SeekFrom::Start(offset + NEEDLE_HEADER_SIZE as u64))?;
            match dat.read_exact(&mut body) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Parse the needle to get append_at_ns
            let mut full = vec![0u8; total_size as usize];
            full[..NEEDLE_HEADER_SIZE].copy_from_slice(&header);
            full[NEEDLE_HEADER_SIZE..].copy_from_slice(&body);
            let mut n = Needle::default();
            let _ = n.read_bytes(&full, offset as i64, size, version);

            entries.push((header.to_vec(), body, n.append_at_ns));
            offset += total_size;
        }

        Ok(entries)
    }

    /// Insert or update a needle index entry (for low-level blob writes).
    pub fn put_needle_index(&mut self, key: NeedleId, offset: Offset, size: Size) -> Result<(), VolumeError> {
        if let Some(ref mut nm) = self.nm {
            nm.put(key, offset, size)
                .map_err(VolumeError::Io)?;
        }
        Ok(())
    }

    /// Mark this volume as read-only (no writes or deletes).
    pub fn set_read_only(&mut self) {
        self.no_write_or_delete = true;
    }

    /// Mark this volume as writable (allow writes and deletes).
    pub fn set_writable(&mut self) {
        self.no_write_or_delete = false;
        self.no_write_can_delete = false;
    }

    /// Change the replication placement and rewrite the super block.
    pub fn set_replica_placement(&mut self, rp: ReplicaPlacement) -> Result<(), VolumeError> {
        self.super_block.replica_placement = rp;
        let bytes = self.super_block.to_bytes();
        let dat_file = self.dat_file.as_mut().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;
        dat_file.seek(SeekFrom::Start(0))?;
        dat_file.write_all(&bytes)?;
        dat_file.sync_all()?;
        Ok(())
    }

    /// Write a raw needle blob at a specific offset in the .dat file.
    pub fn write_needle_blob(&mut self, offset: i64, needle_blob: &[u8]) -> Result<(), VolumeError> {
        if self.no_write_or_delete {
            return Err(VolumeError::ReadOnly);
        }
        let dat_file = self.dat_file.as_mut().ok_or_else(|| {
            VolumeError::Io(io::Error::new(io::ErrorKind::Other, "dat file not open"))
        })?;
        dat_file.seek(SeekFrom::Start(offset as u64))?;
        dat_file.write_all(needle_blob)?;
        Ok(())
    }

    pub fn needs_replication(&self) -> bool {
        self.super_block.replica_placement.get_copy_count() > 1
    }

    /// Garbage ratio: deleted_size / (content_size + deleted_size)
    pub fn garbage_level(&self) -> f64 {
        let content = self.content_size();
        let deleted = self.deleted_size();
        let total = content + deleted;
        if total == 0 {
            return 0.0;
        }
        deleted as f64 / total as f64
    }

    pub fn dat_file_size(&self) -> io::Result<u64> {
        if let Some(ref f) = self.dat_file {
            Ok(f.metadata()?.len())
        } else {
            Ok(0)
        }
    }

    /// Get the modification time of the .dat file as Unix seconds.
    pub fn dat_file_mod_time(&self) -> u64 {
        self.dat_file.as_ref()
            .and_then(|f| f.metadata().ok())
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    pub fn idx_file_size(&self) -> u64 {
        self.nm.as_ref().map_or(0, |nm| nm.index_file_size())
    }

    // ---- Compaction / Vacuum ----

    /// Compact the volume by copying only live needles to new .cpd/.cpx files.
    /// This reads from the current .dat/.idx and writes to .cpd/.cpx.
    /// Call `commit_compact()` after to swap the files.
    pub fn compact_by_index<F>(
        &mut self,
        _preallocate: u64,
        _max_bytes_per_second: i64,
        progress_fn: F,
    ) -> Result<(), VolumeError>
    where
        F: Fn(i64) -> bool,
    {
        if self.is_compacting {
            return Ok(()); // already compacting
        }
        self.is_compacting = true;

        let result = self.do_compact_by_index(progress_fn);

        self.is_compacting = false;
        result
    }

    fn do_compact_by_index<F>(&mut self, progress_fn: F) -> Result<(), VolumeError>
    where
        F: Fn(i64) -> bool,
    {
        // Record state before compaction for makeupDiff
        self.last_compact_index_offset = self.nm.as_ref().map_or(0, |nm| nm.index_file_size());
        self.last_compact_revision = self.super_block.compaction_revision;

        // Sync current data
        self.sync_to_disk()?;

        let cpd_path = self.file_name(".cpd");
        let cpx_path = self.file_name(".cpx");
        let version = self.version();

        // Write new super block with incremented compaction revision
        let mut new_sb = self.super_block.clone();
        new_sb.compaction_revision += 1;
        let sb_bytes = new_sb.to_bytes();

        let mut dst = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&cpd_path)?;
        dst.write_all(&sb_bytes)?;
        let mut new_offset = sb_bytes.len() as i64;

        // Build new index in memory
        let mut new_nm = CompactNeedleMap::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Collect live entries from needle map (sorted ascending)
        let nm = self.nm.as_ref().ok_or(VolumeError::NotInitialized)?;
        let mut entries: Vec<(NeedleId, Offset, Size)> = Vec::new();
        for (&id, nv) in nm.iter() {
            if nv.offset.is_zero() || nv.size.is_deleted() {
                continue;
            }
            entries.push((id, nv.offset, nv.size));
        }
        entries.sort_by_key(|(id, _, _)| *id);

        for (id, offset, size) in entries {
            // Progress callback
            if !progress_fn(offset.to_actual_offset()) {
                // Interrupted
                let _ = fs::remove_file(&cpd_path);
                return Err(VolumeError::Io(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "compaction interrupted",
                )));
            }

            // Read needle from source
            let mut n = Needle {
                id,
                ..Needle::default()
            };
            self.read_needle_data_at(&mut n, offset.to_actual_offset(), size)?;

            // Skip TTL-expired needles
            if n.has_ttl() {
                if let Some(ref ttl) = n.ttl {
                    let ttl_minutes = ttl.minutes();
                    if ttl_minutes > 0 && n.last_modified > 0 {
                        let expire_at = n.last_modified + (ttl_minutes as u64) * 60;
                        if now >= expire_at {
                            continue;
                        }
                    }
                }
            }

            // Write needle to destination
            let bytes = n.write_bytes(version);
            dst.write_all(&bytes)?;

            // Update new index
            new_nm.put(id, Offset::from_actual_offset(new_offset), n.size)?;
            new_offset += bytes.len() as i64;
        }

        dst.sync_all()?;

        // Save new index
        new_nm.save_to_idx(&cpx_path)?;

        Ok(())
    }

    /// Commit a previously completed compaction: swap .cpd/.cpx to .dat/.idx and reload.
    pub fn commit_compact(&mut self) -> Result<(), VolumeError> {
        // Close current files
        if let Some(ref mut nm) = self.nm {
            nm.close();
        }
        self.nm = None;
        if let Some(ref dat_file) = self.dat_file {
            let _ = dat_file.sync_all();
        }
        self.dat_file = None;

        let cpd_path = self.file_name(".cpd");
        let cpx_path = self.file_name(".cpx");
        let dat_path = self.file_name(".dat");
        let idx_path = self.file_name(".idx");

        // Check that compact files exist
        if !Path::new(&cpd_path).exists() || !Path::new(&cpx_path).exists() {
            return Err(VolumeError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "compact files (.cpd/.cpx) not found",
            )));
        }

        // Swap files: .cpd → .dat, .cpx → .idx
        fs::rename(&cpd_path, &dat_path)?;
        fs::rename(&cpx_path, &idx_path)?;

        // Remove any leveldb files
        let ldb_path = self.file_name(".ldb");
        let _ = fs::remove_dir_all(&ldb_path);

        // Reload
        self.load(true, false, 0, self.version())?;

        Ok(())
    }

    /// Clean up leftover compaction files (.cpd, .cpx).
    pub fn cleanup_compact(&self) -> Result<(), VolumeError> {
        let cpd_path = self.file_name(".cpd");
        let cpx_path = self.file_name(".cpx");
        let cpldb_path = self.file_name(".cpldb");

        let e1 = fs::remove_file(&cpd_path);
        let e2 = fs::remove_file(&cpx_path);
        let e3 = fs::remove_dir_all(&cpldb_path);

        // Ignore NotFound errors
        if let Err(e) = e1 {
            if e.kind() != io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        if let Err(e) = e2 {
            if e.kind() != io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        if let Err(e) = e3 {
            if e.kind() != io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }

        Ok(())
    }

    // ---- Sync / Close ----

    pub fn sync_to_disk(&mut self) -> io::Result<()> {
        if let Some(ref dat_file) = self.dat_file {
            dat_file.sync_all()?;
        }
        if let Some(ref nm) = self.nm {
            nm.sync()?;
        }
        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(ref dat_file) = self.dat_file {
            let _ = dat_file.sync_all();
        }
        self.dat_file = None;
        if let Some(ref nm) = self.nm {
            let _ = nm.sync();
        }
        self.nm = None;
    }

    /// Remove all volume files from disk.
    pub fn destroy(&mut self) -> Result<(), VolumeError> {
        if self.is_compacting {
            return Err(VolumeError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("volume {} is compacting", self.id),
            )));
        }
        self.close();
        remove_volume_files(&self.data_file_name());
        remove_volume_files(&self.index_file_name());
        Ok(())
    }

    #[allow(dead_code)]
    fn check_read_write_error(&mut self, err: &io::Error) {
        if err.raw_os_error() == Some(5) {
            // EIO
            self._last_io_error = Some(io::Error::new(err.kind(), err.to_string()));
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Generate volume file base name: dir/collection_id or dir/id
pub fn volume_file_name(dir: &str, collection: &str, id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}/{}", dir, id.0)
    } else {
        format!("{}/{}_{}", dir, collection, id.0)
    }
}

/// Generate a monotonically increasing append timestamp.
fn get_append_at_ns(last: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    if now <= last {
        last + 1
    } else {
        now
    }
}

/// Remove all files associated with a volume.
fn remove_volume_files(base: &str) {
    for ext in &[".dat", ".idx", ".vif", ".sdx", ".cpd", ".cpx", ".note"] {
        let _ = fs::remove_file(format!("{}{}", base, ext));
    }
    let _ = fs::remove_dir_all(format!("{}.ldb", base));
}

// ============================================================================
// ScanVolumeFile — iterate all needles in a .dat file
// ============================================================================

/// Callback for scanning needles in a volume file.
pub trait VolumeFileVisitor {
    fn visit_super_block(&mut self, sb: &SuperBlock) -> Result<(), VolumeError>;
    fn read_needle_body(&self) -> bool;
    fn visit_needle(&mut self, n: &Needle, offset: i64) -> Result<(), VolumeError>;
}

/// Scan all needles in a volume's .dat file.
pub fn scan_volume_file(
    dat_path: &str,
    visitor: &mut dyn VolumeFileVisitor,
) -> Result<(), VolumeError> {
    let mut file = File::open(dat_path)?;

    // Read super block
    let mut sb_buf = [0u8; SUPER_BLOCK_SIZE];
    file.read_exact(&mut sb_buf)?;
    let sb = SuperBlock::from_bytes(&sb_buf)?;
    visitor.visit_super_block(&sb)?;

    let version = sb.version;
    let mut offset = sb.block_size() as i64;

    loop {
        // Read needle header
        let mut header = [0u8; NEEDLE_HEADER_SIZE];
        file.seek(SeekFrom::Start(offset as u64))?;
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let (_cookie, _id, size) = Needle::parse_header(&header);

        if size.0 == 0 && _id.is_empty() {
            break; // end of valid data
        }

        let body_length = needle::needle_body_length(size, version);
        let total_size = NEEDLE_HEADER_SIZE as i64 + body_length;

        // Skip full body parsing for deleted needles (tombstone or negative size)
        if size.is_deleted() || size.0 <= 0 {
            let mut n = Needle::default();
            n.read_header(&header);
            visitor.visit_needle(&n, offset)?;
        } else if visitor.read_needle_body() {
            let mut buf = vec![0u8; total_size as usize];
            file.seek(SeekFrom::Start(offset as u64))?;
            file.read_exact(&mut buf)?;

            let mut n = Needle::default();
            n.read_bytes(&buf, offset, size, version)?;
            visitor.visit_needle(&n, offset)?;
        } else {
            let mut n = Needle::default();
            n.read_header(&header);
            visitor.visit_needle(&n, offset)?;
        }

        offset += total_size;
    }

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::needle::crc::CRC;
    use tempfile::TempDir;

    fn make_test_volume(dir: &str) -> Volume {
        Volume::new(
            dir, dir, "", VolumeId(1),
            NeedleMapKind::InMemory,
            None, None, 0,
            Version::current(),
        ).unwrap()
    }

    #[test]
    fn test_volume_file_name() {
        assert_eq!(volume_file_name("/data", "", VolumeId(1)), "/data/1");
        assert_eq!(volume_file_name("/data", "pics", VolumeId(42)), "/data/pics_42");
    }

    #[test]
    fn test_volume_create_and_load() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        let v = make_test_volume(dir);
        assert_eq!(v.version(), VERSION_3);
        assert_eq!(v.file_count(), 0);
        assert_eq!(v.content_size(), 0);

        // .dat and .idx files should exist
        assert!(Path::new(&v.file_name(".dat")).exists());
        assert!(Path::new(&v.file_name(".idx")).exists());
    }

    #[test]
    fn test_volume_write_read() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = make_test_volume(dir);

        // Write a needle
        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0x12345678),
            data: b"hello world".to_vec(),
            data_size: 11,
            flags: 0,
            ..Needle::default()
        };
        let (offset, size, unchanged) = v.write_needle(&mut n, true).unwrap();
        assert!(!unchanged);
        assert!(offset > 0); // after superblock
        assert!(size.0 > 0);
        assert_eq!(v.file_count(), 1);

        // Read it back
        let mut read_n = Needle { id: NeedleId(1), ..Needle::default() };
        let count = v.read_needle(&mut read_n).unwrap();
        assert_eq!(count, 11);
        assert_eq!(read_n.data, b"hello world");
        assert_eq!(read_n.cookie, Cookie(0x12345678));
    }

    #[test]
    fn test_volume_write_dedup() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = make_test_volume(dir);

        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xaa),
            data: b"same data".to_vec(),
            data_size: 9,
            ..Needle::default()
        };
        v.write_needle(&mut n, true).unwrap();

        // Write same needle again — should be unchanged
        let mut n2 = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xaa),
            data: b"same data".to_vec(),
            data_size: 9,
            ..Needle::default()
        };
        n2.checksum = CRC::new(&n2.data);
        let (_, _, unchanged) = v.write_needle(&mut n2, true).unwrap();
        assert!(unchanged);
    }

    #[test]
    fn test_volume_delete() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = make_test_volume(dir);

        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xbb),
            data: b"delete me".to_vec(),
            data_size: 9,
            ..Needle::default()
        };
        v.write_needle(&mut n, true).unwrap();
        assert_eq!(v.file_count(), 1);

        let deleted_size = v.delete_needle(&mut Needle {
            id: NeedleId(1),
            cookie: Cookie(0xbb),
            ..Needle::default()
        }).unwrap();
        assert!(deleted_size.0 > 0);
        assert_eq!(v.file_count(), 0);
        assert_eq!(v.deleted_count(), 1);

        // Read should fail with Deleted
        let mut read_n = Needle { id: NeedleId(1), ..Needle::default() };
        let err = v.read_needle(&mut read_n).unwrap_err();
        assert!(matches!(err, VolumeError::Deleted));
    }

    #[test]
    fn test_volume_multiple_needles() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = make_test_volume(dir);

        for i in 1..=10 {
            let data = format!("needle data {}", i);
            let mut n = Needle {
                id: NeedleId(i),
                cookie: Cookie(i as u32),
                data: data.as_bytes().to_vec(),
                data_size: data.len() as u32,
                ..Needle::default()
            };
            v.write_needle(&mut n, true).unwrap();
        }

        assert_eq!(v.file_count(), 10);
        assert_eq!(v.max_file_key(), NeedleId(10));

        // Read back needle 5
        let mut n = Needle { id: NeedleId(5), ..Needle::default() };
        v.read_needle(&mut n).unwrap();
        assert_eq!(n.data, b"needle data 5");
    }

    #[test]
    fn test_volume_reload_from_disk() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Write some needles
        {
            let mut v = make_test_volume(dir);
            for i in 1..=3 {
                let data = format!("data {}", i);
                let mut n = Needle {
                    id: NeedleId(i),
                    cookie: Cookie(i as u32),
                    data: data.as_bytes().to_vec(),
                    data_size: data.len() as u32,
                    ..Needle::default()
                };
                v.write_needle(&mut n, true).unwrap();
            }
            v.sync_to_disk().unwrap();
        }

        // Reload and verify
        let v = Volume::new(
            dir, dir, "", VolumeId(1),
            NeedleMapKind::InMemory,
            None, None, 0,
            Version::current(),
        ).unwrap();
        assert_eq!(v.file_count(), 3);

        let mut n = Needle { id: NeedleId(2), ..Needle::default() };
        v.read_needle(&mut n).unwrap();
        assert_eq!(std::str::from_utf8(&n.data).unwrap(), "data 2");
    }

    #[test]
    fn test_volume_cookie_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = make_test_volume(dir);

        let mut n = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xaa),
            data: b"original".to_vec(),
            data_size: 8,
            ..Needle::default()
        };
        v.write_needle(&mut n, true).unwrap();

        // Write with wrong cookie
        let mut n2 = Needle {
            id: NeedleId(1),
            cookie: Cookie(0xbb),
            data: b"overwrite".to_vec(),
            data_size: 9,
            ..Needle::default()
        };
        let err = v.write_needle(&mut n2, true).unwrap_err();
        assert!(matches!(err, VolumeError::CookieMismatch(_)));
    }

    #[test]
    fn test_volume_destroy() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let dat_path;
        let idx_path;

        {
            let mut v = make_test_volume(dir);
            dat_path = v.file_name(".dat");
            idx_path = v.file_name(".idx");
            assert!(Path::new(&dat_path).exists());
            v.destroy().unwrap();
        }

        assert!(!Path::new(&dat_path).exists());
        assert!(!Path::new(&idx_path).exists());
    }

    #[test]
    fn test_get_append_at_ns() {
        let t1 = get_append_at_ns(0);
        assert!(t1 > 0);
        let t2 = get_append_at_ns(t1);
        assert!(t2 > t1);
        // If we pass a future timestamp, should return last+1
        let future = u64::MAX - 1;
        let t3 = get_append_at_ns(future);
        assert_eq!(t3, future + 1);
    }

    #[test]
    fn test_volume_compact() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = make_test_volume(dir);

        // Write 3 needles
        for i in 1..=3u64 {
            let mut n = Needle {
                id: NeedleId(i),
                cookie: Cookie(i as u32),
                data: format!("data-{}", i).into_bytes(),
                data_size: format!("data-{}", i).len() as u32,
                ..Needle::default()
            };
            v.write_needle(&mut n, true).unwrap();
        }
        assert_eq!(v.file_count(), 3);

        // Delete needle 2
        let mut del = Needle {
            id: NeedleId(2),
            cookie: Cookie(2),
            ..Needle::default()
        };
        v.delete_needle(&mut del).unwrap();
        assert_eq!(v.file_count(), 2);
        assert_eq!(v.deleted_count(), 1);

        let dat_size_before = v.dat_file_size().unwrap();

        // Compact
        v.compact_by_index(0, 0, |_| true).unwrap();

        // Verify compact files exist
        assert!(Path::new(&v.file_name(".cpd")).exists());
        assert!(Path::new(&v.file_name(".cpx")).exists());

        // Commit: swap files and reload
        v.commit_compact().unwrap();

        // After compaction: 2 live needles, 0 deleted
        assert_eq!(v.file_count(), 2);
        assert_eq!(v.deleted_count(), 0);

        // Dat should be smaller (deleted needle removed)
        let dat_size_after = v.dat_file_size().unwrap();
        assert!(dat_size_after < dat_size_before, "dat should shrink after compact");

        // Read back live needles
        let mut n1 = Needle { id: NeedleId(1), ..Needle::default() };
        v.read_needle(&mut n1).unwrap();
        assert_eq!(n1.data, b"data-1");

        let mut n3 = Needle { id: NeedleId(3), ..Needle::default() };
        v.read_needle(&mut n3).unwrap();
        assert_eq!(n3.data, b"data-3");

        // Needle 2 should not exist
        let mut n2 = Needle { id: NeedleId(2), ..Needle::default() };
        assert!(v.read_needle(&mut n2).is_err());

        // Compact files should not exist after commit
        assert!(!Path::new(&v.file_name(".cpd")).exists());
        assert!(!Path::new(&v.file_name(".cpx")).exists());

        // Cleanup should be a no-op
        v.cleanup_compact().unwrap();
    }
}
