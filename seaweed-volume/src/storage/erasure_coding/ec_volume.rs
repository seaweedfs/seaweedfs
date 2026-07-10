//! EcVolume: an erasure-coded volume with up to MAX_SHARD_COUNT shards.
//!
//! Each EcVolume has a sorted index (.ecx) and a deletion journal (.ecj).
//! Shards (.ec00-.ec13) may be distributed across multiple servers.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::pb::master_pb;
use crate::storage::erasure_coding::ec_locate;
use crate::storage::erasure_coding::ec_shard::*;
use crate::storage::needle::needle::{get_actual_size, Needle, NeedleError};
use crate::storage::types::*;

/// An erasure-coded volume managing its local shards and index.
pub struct EcVolume {
    pub volume_id: VolumeId,
    pub collection: String,
    pub dir: String,
    pub dir_idx: String,
    pub version: Version,
    pub shards: Vec<Option<EcVolumeShard>>, // indexed by ShardId (0..MAX_SHARD_COUNT)
    pub dat_file_size: i64,
    pub data_shards: u32,
    pub parity_shards: u32,
    ecx_file: Option<File>,
    ecx_file_size: i64,
    ecj_file: Option<File>,
    /// On-disk size of the .ecj deletion journal. Used only by IO helpers
    /// (seek / set_len on partial writes) — the authoritative runtime
    /// delete count comes from `deleted_needles.len()`.
    ecj_file_size: i64,
    /// In-memory set of needle ids that have been deleted since the volume
    /// was encoded. .ecx is immutable at runtime — it only stores the
    /// sorted (id, offset, size) index written at encode time — and runtime
    /// deletes are journaled to .ecj + tracked here. Reads consult this
    /// set to mask out deleted needles on top of the sealed .ecx lookup.
    /// Seeded from .ecj in `new()` and updated by `journal_delete`.
    deleted_needles: RwLock<HashSet<NeedleId>>,
    pub disk_type: DiskType,
    /// Directory where .ecx/.ecj were actually found (may differ from dir_idx after fallback).
    ecx_actual_dir: String,
    /// Maps shard ID -> list of server addresses where that shard exists.
    /// Used for distributed EC reads across the cluster. Wrapped in
    /// `RwLock` so the read path can refresh the map (under master
    /// lookup) without holding the Store write lock — mirrors Go's
    /// `ShardLocationsLock sync.RWMutex` in `weed/storage/erasure_coding/ec_volume.go`.
    pub shard_locations: std::sync::RwLock<HashMap<ShardId, Vec<String>>>,
    /// Wall-clock timestamp of the most recent successful
    /// `LookupEcVolume` refresh of `shard_locations`. `None` until the
    /// first refresh. Drives the staleness heuristic in
    /// `cached_lookup_ec_shard_locations` (mirrors Go's
    /// `ShardLocationsRefreshTime`).
    pub shard_locations_refresh_time: std::sync::Mutex<Option<std::time::Instant>>,
    /// EC volume expiration time (unix epoch seconds), set during EC encode from TTL.
    pub expire_at_sec: u64,
    /// Encode-run identity (unix nanos) loaded from the .vif EcShardConfig. A read
    /// served from a shard of a different encode run is rejected (server- and
    /// client-side); 0 for a pre-feature volume, which is treated leniently.
    pub encode_ts_ns: i64,
    /// Active-generation EC bitrot checksum sidecar (`<base>.ecsum`), loaded and
    /// validated at mount. `None` unless `bitrot_status == On`.
    pub(crate) bitrot: Option<crate::pb::volume_server_pb::EcBitrotProtection>,
    /// Resolved protection state of the loaded sidecar. Cached alongside `bitrot`
    /// so `bitrot_protection()` can return the `Off`/`Invalid` distinction without
    /// re-reading, mirroring Go's `EcVolume.bitrotStatus`.
    pub(crate) bitrot_status: crate::storage::erasure_coding::ec_bitrot::BitrotStatus,
}

/// Locate the `.vif` for a (collection, vid) by preferring the data dir
/// and falling back to the idx dir when it lives there instead. The
/// fallback covers the cross-disk reconcile path: when a volume's
/// shards live on one disk but its `.ecx` / `.ecj` / `.vif` live on a
/// sibling disk (seaweedfs/seaweedfs#9212 / #9244), we want to read the
/// real `.vif` from the sibling rather than write a stub on the shard
/// disk and lose the EC config + dat file size.
fn locate_vif_path(dir: &str, dir_idx: &str, collection: &str, volume_id: VolumeId) -> String {
    let data_vif = format!(
        "{}.vif",
        crate::storage::volume::volume_file_name(dir, collection, volume_id),
    );
    if dir_idx != dir && !std::path::Path::new(&data_vif).exists() {
        let idx_vif = format!(
            "{}.vif",
            crate::storage::volume::volume_file_name(dir_idx, collection, volume_id),
        );
        if std::path::Path::new(&idx_vif).exists() {
            return idx_vif;
        }
    }
    data_vif
}

/// Read EC data/parity shard counts from `.vif`, defaulting to the
/// build's standard ratio when no `.vif` is present or is malformed.
/// Looks at the data dir first, then the idx dir — see [`locate_vif_path`].
pub fn read_ec_shard_config(
    dir: &str,
    dir_idx: &str,
    collection: &str,
    volume_id: VolumeId,
) -> (u32, u32) {
    let mut data_shards = crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT as u32;
    let mut parity_shards = crate::storage::erasure_coding::ec_shard::PARITY_SHARDS_COUNT as u32;
    let vif_path = locate_vif_path(dir, dir_idx, collection, volume_id);
    if let Ok(vif_content) = std::fs::read_to_string(&vif_path) {
        if let Ok(vif_info) =
            serde_json::from_str::<crate::storage::volume::VifVolumeInfo>(&vif_content)
        {
            if let Some(ec) = vif_info.ec_shard_config {
                if ec.data_shards > 0
                    && ec.parity_shards > 0
                    && (ec.data_shards + ec.parity_shards) <= MAX_SHARD_COUNT as u32
                {
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
        let (data_shards, parity_shards) = read_ec_shard_config(dir, dir_idx, collection, volume_id);

        let total_shards = (data_shards + parity_shards) as usize;
        let mut shards = Vec::with_capacity(total_shards);
        for _ in 0..total_shards {
            shards.push(None);
        }

        // Read expire_at_sec, version, and dat_file_size from .vif if
        // present (matches Go's MaybeLoadVolumeInfo). Prefer the data
        // dir; fall back to the idx dir for the cross-disk reconcile
        // case (#9212 / #9244). `dat_file_size` is the source .dat
        // size at encode time, used both by `locate_ec_shard_needle`
        // for shard-size math and by the Store-level prune in
        // `store_ec_reconcile.rs` to verify a sibling-disk .dat is
        // plausibly the encoding source (#9478).
        let (expire_at_sec, vif_version, vif_dat_file_size, encode_ts_ns) = {
            let vif_path = locate_vif_path(dir, dir_idx, collection, volume_id);
            if let Ok(vif_content) = std::fs::read_to_string(&vif_path) {
                if let Ok(vif_info) =
                    serde_json::from_str::<crate::storage::volume::VifVolumeInfo>(&vif_content)
                {
                    let ver = if vif_info.version > 0 {
                        Version(vif_info.version as u8)
                    } else {
                        Version::current()
                    };
                    let cfg_encode_ts_ns = vif_info
                        .ec_shard_config
                        .as_ref()
                        .map_or(0, |c| c.encode_ts_ns);
                    (
                        vif_info.expire_at_sec,
                        ver,
                        vif_info.dat_file_size,
                        cfg_encode_ts_ns,
                    )
                } else {
                    (0, Version::current(), 0, 0)
                }
            } else {
                (0, Version::current(), 0, 0)
            }
        };

        let mut vol = EcVolume {
            volume_id,
            collection: collection.to_string(),
            dir: dir.to_string(),
            dir_idx: dir_idx.to_string(),
            version: vif_version,
            shards,
            dat_file_size: vif_dat_file_size,
            data_shards,
            parity_shards,
            ecx_file: None,
            ecx_file_size: 0,
            ecj_file: None,
            ecj_file_size: 0,
            deleted_needles: RwLock::new(HashSet::new()),
            disk_type: DiskType::default(),
            ecx_actual_dir: dir_idx.to_string(),
            shard_locations: std::sync::RwLock::new(HashMap::new()),
            shard_locations_refresh_time: std::sync::Mutex::new(None),
            expire_at_sec,
            encode_ts_ns,
            bitrot: None,
            bitrot_status: crate::storage::erasure_coding::ec_bitrot::BitrotStatus::Off,
        };

        // Open .ecx file (sorted index) in read/write mode for in-place deletion marking.
        // Matches Go which opens ecx for writing via MarkNeedleDeleted.
        let ecx_path = vol.ecx_file_name();
        if std::path::Path::new(&ecx_path).exists() {
            let file = OpenOptions::new().read(true).write(true).open(&ecx_path)?;
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
                let file = OpenOptions::new().read(true).write(true).open(&fallback_ecx)?;
                vol.ecx_file_size = file.metadata()?.len() as i64;
                vol.ecx_file = Some(file);
                vol.ecx_actual_dir = dir.to_string();
            }
        }

        // Open .ecj file (deletion journal) — use ecx_actual_dir for consistency.
        // Note: Go does NOT replay .ecj into .ecx at volume load (RebuildEcxFile
        // is only invoked from specific decode/rebuild gRPC handlers), so we
        // don't either. Tombstones from prior sessions were already written
        // in-place in .ecx, and the journal grows monotonically until a
        // decode/rebuild operation folds it in.
        let ecj_base =
            crate::storage::volume::volume_file_name(&vol.ecx_actual_dir, collection, volume_id);
        let ecj_path = format!("{}.ecj", ecj_base);
        let ecj_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&ecj_path)?;
        vol.ecj_file_size = ecj_file.metadata()?.len() as i64;
        vol.ecj_file = Some(ecj_file);

        // Seed the in-memory deleted set from the journal.
        vol.load_deleted_needles_from_ecj()?;

        // Load the generation-0 EC bitrot checksum sidecar (optional; best-effort).
        vol.load_active_bitrot_sidecar();

        Ok(vol)
    }

    /// Load the generation-0 checksum sidecar into `self.bitrot`/`self.bitrot_status`.
    /// OSS only produces generation-0 (fresh-encode) sidecars, mirroring Go's
    /// `loadActiveBitrotSidecar`.
    fn load_active_bitrot_sidecar(&mut self) {
        self.load_bitrot_for_generation(0);
    }

    /// Load and validate the sidecar describing `generation`, setting
    /// `self.bitrot`/`self.bitrot_status`. Absent or generation/config-mismatched
    /// => `Off` (protection off, not corruption); self-integrity or manifest
    /// failure => `Invalid` with a warning (protection off pending repair); usable
    /// => `On`. Mirrors Go's `loadBitrotForGeneration`.
    fn load_bitrot_for_generation(&mut self, generation: u32) {
        use crate::storage::erasure_coding::ec_bitrot;
        let base = self.base_name();
        let path = ec_bitrot::bitrot_sidecar_path(&base, generation);
        let loaded = ec_bitrot::load_bitrot_sidecar(&path);
        let status = ec_bitrot::resolve_status(
            &loaded,
            generation,
            self.data_shards as usize,
            self.parity_shards as usize,
        );
        self.bitrot = None;
        self.bitrot_status = status;
        match status {
            ec_bitrot::BitrotStatus::On => self.bitrot = loaded.ok(),
            ec_bitrot::BitrotStatus::Off => {}
            ec_bitrot::BitrotStatus::Invalid => {
                tracing::warn!(
                    volume_id = self.volume_id.0,
                    path = %path,
                    generation,
                    "ec volume: bitrot sidecar present but invalid; protection off pending repair",
                );
            }
        }
    }

    /// The active-generation bitrot protection AND its status (cached at mount),
    /// mirroring Go's `EcVolume.BitrotProtection()`. Preserves the distinction
    /// `checksum_scrub` needs: an absent/generation-mismatched sidecar is `Off`
    /// (a clean no-op), a present-but-malformed one is `Invalid` (a real integrity
    /// error). Returns `Some(prot)` only for `On`.
    pub(crate) fn bitrot_protection(
        &self,
    ) -> (
        Option<crate::pb::volume_server_pb::EcBitrotProtection>,
        crate::storage::erasure_coding::ec_bitrot::BitrotStatus,
    ) {
        (self.bitrot.clone(), self.bitrot_status)
    }

    /// Read-only EC bitrot checksum scrub of the LOCAL shards of this volume's
    /// active generation.
    ///
    /// Loads the active-generation `.ecsum` sidecar and, for each locally-held
    /// shard, reads the on-disk shard file in `block_size` chunks and compares
    /// each block's CRC32C against the sidecar. Returns
    /// `(blocks_scanned, mismatched_shards, errors)`.
    ///
    /// If more than `parity_shards` shards mismatch wholesale (i.e. every block
    /// of those shards is wrong — the signature of a stale/wrong sidecar rather
    /// than localized disk rot), the result is classified as a suspect sidecar:
    /// `mismatched_shards` is cleared and an integrity note is added to `errors`
    /// instead. A genuine multi-shard disk failure of that magnitude is
    /// already unrecoverable, so treating it as a sidecar-integrity issue avoids
    /// raising false shard-corruption alarms.
    ///
    /// This method NEVER deletes or mutates anything — it is purely diagnostic.
    pub fn checksum_scrub(&self) -> (u64, Vec<u32>, Vec<String>) {
        use crate::storage::erasure_coding::ec_bitrot;
        use crate::storage::erasure_coding::ec_bitrot::BitrotStatus;

        let mut errors: Vec<String> = Vec::new();

        // Resolve the active-generation protection AND its status, mirroring
        // Go's `ChecksumScrub` (`prot, status := ecv.BitrotProtection()`):
        //   - BitrotOff   => sidecars are OPTIONAL; an absent (or generation/
        //     config-mismatched) sidecar simply means protection is not enabled
        //     for this generation. Return a CLEAN, EMPTY result — NOT an error —
        //     so legacy/intentionally-unprotected volumes are never reported
        //     broken. (Go: `case BitrotOff: return 0, nil, nil`.)
        //   - BitrotInvalid => the sidecar is PRESENT but malformed/unverifiable
        //     (self-integrity or manifest failure). That is the only status that
        //     yields an integrity error here.
        //   - BitrotOn    => scan local shards against it.
        let prot = match self.bitrot_protection() {
            (_, BitrotStatus::Off) => {
                // Unprotected generation: nothing to verify. Not an error.
                return (0, Vec::new(), Vec::new());
            }
            (_, BitrotStatus::Invalid) => {
                return (
                    0,
                    Vec::new(),
                    vec![format!(
                        "EC volume {} bitrot sidecar is malformed/unverifiable (sidecar integrity)",
                        self.volume_id.0
                    )],
                );
            }
            (Some(p), BitrotStatus::On) => p,
            (None, BitrotStatus::On) => {
                // Unreachable: BitrotOn always carries a loaded sidecar. Treat a
                // missing payload defensively as protection off (clean no-op).
                return (0, Vec::new(), Vec::new());
            }
        };

        let block_size = prot.block_size as i64;
        let generation = prot.generation;
        let base = self.base_name();

        let mut blocks_scanned: u64 = 0;
        let mut mismatched_shards: Vec<u32> = Vec::new();
        // Track shards whose blocks ALL mismatch (wholesale) to detect a
        // stale/wrong sidecar.
        let mut wholesale_mismatch = 0usize;

        for (i, slot) in self.shards.iter().enumerate() {
            if slot.is_none() {
                continue; // not local
            }
            let shard_id = i as u32;
            let Some(entry) = ec_bitrot::shard_checksums(&prot, shard_id) else {
                errors.push(format!(
                    "EC volume {} shard {} present but missing from sidecar manifest",
                    self.volume_id.0, shard_id
                ));
                continue;
            };

            // Resolve the on-disk shard file path for the active generation,
            // mirroring EcVolumeShard::reopen_against_generation's convention.
            let path = if generation == 0 {
                format!("{}.ec{:02}", base, shard_id)
            } else {
                format!("{}.ec{:02}.v{}", base, shard_id, generation)
            };

            let expected_blocks = entry.block_crc32c.len() / 4;
            match ec_bitrot::verify_shard_file_blocks(&path, entry, block_size) {
                Ok(mismatched) => {
                    blocks_scanned += expected_blocks as u64;
                    if !mismatched.is_empty() {
                        mismatched_shards.push(shard_id);
                        if expected_blocks > 0 && mismatched.len() == expected_blocks {
                            wholesale_mismatch += 1;
                        }
                    }
                }
                Err(e) => {
                    errors.push(format!(
                        "EC volume {} shard {} scrub read error: {}",
                        self.volume_id.0, shard_id, e
                    ));
                }
            }
        }

        // If more shards mismatch wholesale than parity can mask, the sidecar
        // itself is the likely culprit (stale generation / wrong volume), so
        // suppress the shard-corruption verdict and flag a sidecar-integrity
        // issue instead.
        if wholesale_mismatch > self.parity_shards as usize {
            errors.push(format!(
                "EC volume {}: {} shards mismatch wholesale (> {} parity); suspect stale/wrong sidecar, not shard corruption",
                self.volume_id.0, wholesale_mismatch, self.parity_shards
            ));
            mismatched_shards.clear();
        }

        mismatched_shards.sort_unstable();
        (blocks_scanned, mismatched_shards, errors)
    }

    /// Walk the .ecj journal and populate `deleted_needles`. Called once
    /// from `new()` under exclusive ownership of the just-constructed
    /// EcVolume, so locking is not strictly required — but we take the
    /// write lock anyway for symmetry with later mutations.
    fn load_deleted_needles_from_ecj(&mut self) -> io::Result<()> {
        let ecj_file = match self.ecj_file.as_ref() {
            Some(f) => f,
            None => return Ok(()),
        };
        if self.ecj_file_size < NEEDLE_ID_SIZE as i64 {
            return Ok(());
        }
        let mut buf = [0u8; NEEDLE_ID_SIZE];
        let mut set = self
            .deleted_needles
            .write()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "deleted_needles lock poisoned"))?;
        let mut off: i64 = 0;
        while off + NEEDLE_ID_SIZE as i64 <= self.ecj_file_size {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                ecj_file.read_exact_at(&mut buf, off as u64)?;
            }
            set.insert(NeedleId::from_bytes(&buf));
            off += NEEDLE_ID_SIZE as i64;
        }
        Ok(())
    }

    /// Returns (file_count, delete_count) for this EC volume. Mirrors Go's
    /// `EcVolume.FileAndDeleteCount`:
    ///
    ///   file_count   = ecx_file_size / NEEDLE_MAP_ENTRY_SIZE  — total
    ///                  entries in the sealed sorted .ecx index.
    ///   delete_count = deleted_needles.len()                  — unique
    ///                  runtime deletes tracked in memory (seeded from
    ///                  .ecj on load and updated by `journal_delete`).
    ///
    /// Because each needle delete is applied on exactly one shard holder,
    /// the admin aggregation sums delete_count across nodes while taking
    /// file_count from a single holder (they are identical per volume).
    pub fn file_and_delete_count(&self) -> (u64, u64) {
        let file_count = (self.ecx_file_size as u64) / (NEEDLE_MAP_ENTRY_SIZE as u64);
        let delete_count = self
            .deleted_needles
            .read()
            .map(|s| s.len() as u64)
            .unwrap_or(0);
        (file_count, delete_count)
    }

    /// Reports whether the given needle id is in the in-memory deleted set.
    pub fn is_needle_deleted(&self, needle_id: NeedleId) -> bool {
        self.deleted_needles
            .read()
            .map(|s| s.contains(&needle_id))
            .unwrap_or(false)
    }

    // ---- File names ----

    #[allow(dead_code)]
    fn base_name(&self) -> String {
        crate::storage::volume::volume_file_name(&self.dir, &self.collection, self.volume_id)
    }

    /// Base path for the .ecx / .ecj index pair. Resolved from
    /// `ecx_actual_dir` (initialized to `dir_idx` and only updated after a
    /// successful idx-dir → data-dir fallback in `new()`), so every call site
    /// agrees on the same file regardless of whether the fallback fired.
    fn idx_base_name(&self) -> String {
        crate::storage::volume::volume_file_name(
            &self.ecx_actual_dir,
            &self.collection,
            self.volume_id,
        )
    }

    pub fn ecx_file_name(&self) -> String {
        format!("{}.ecx", self.idx_base_name())
    }

    pub fn ecj_file_name(&self) -> String {
        format!("{}.ecj", self.idx_base_name())
    }

    /// Sync the EC volume's journal and index files to disk (matching Go's ecv.Sync()).
    /// Go flushes both .ecj and .ecx to ensure in-place deletion marks are persisted.
    pub fn sync_to_disk(&self) -> io::Result<()> {
        if let Some(ref ecj_file) = self.ecj_file {
            ecj_file.sync_all()?;
        }
        if let Some(ref ecx_file) = self.ecx_file {
            ecx_file.sync_all()?;
        }
        Ok(())
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

    /// Override the disk type the EC volume (and its already-mounted
    /// shards) reports under. Used by the `VolumeEcShardsMount` handler
    /// so the source volume's disk type is preserved across encoding
    /// (#9423). Not persisted across restarts — disk-scan reload paths
    /// default to the physical location's disk type.
    pub fn set_disk_type(&mut self, d: DiskType) {
        self.disk_type = d.clone();
        for slot in self.shards.iter_mut() {
            if let Some(shard) = slot {
                shard.disk_type = d.clone();
            }
        }
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

    /// Reports whether `shard_id` is currently registered to this
    /// EcVolume (used by the cross-disk reconcile to skip already-
    /// loaded shards).
    pub fn has_shard(&self, shard_id: u8) -> bool {
        self.shards
            .get(shard_id as usize)
            .map(|s| s.is_some())
            .unwrap_or(false)
    }

    /// Directory where this EcVolume's `.ecx` was actually opened
    /// (may differ from `dir_idx` when the legacy "written before
    /// -dir.idx was set" fallback or the cross-disk reconcile path
    /// pointed it elsewhere).
    pub fn ecx_actual_dir(&self) -> &str {
        &self.ecx_actual_dir
    }

    pub fn is_time_to_destroy(&self) -> bool {
        self.expire_at_sec > 0
            && SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                > self.expire_at_sec
    }

    pub fn to_volume_ec_shard_information_messages(
        &self,
        disk_id: u32,
    ) -> Vec<master_pb::VolumeEcShardInformationMessage> {
        let mut ec_index_bits: u32 = 0;
        let mut shard_sizes = Vec::new();
        for shard in self.shards.iter().flatten() {
            ec_index_bits |= 1u32 << shard.shard_id;
            shard_sizes.push(shard.file_size());
        }

        if ec_index_bits == 0 {
            return Vec::new();
        }

        let (file_count, delete_count) = self.file_and_delete_count();

        vec![master_pb::VolumeEcShardInformationMessage {
            id: self.volume_id.0,
            collection: self.collection.clone(),
            ec_index_bits,
            shard_sizes,
            disk_type: self.disk_type.to_string(),
            expire_at_sec: self.expire_at_sec,
            disk_id,
            file_count,
            delete_count,
            encode_ts_ns: self.encode_ts_ns,
            ..Default::default()
        }]
    }

    // ---- Shard locations (distributed tracking) ----

    /// Set the list of server addresses for a single shard ID. Does
    /// NOT touch `shard_locations_refresh_time` — a per-shard write
    /// from inside a multi-shard population (e.g. iterating the
    /// `LookupEcVolume` response shard-by-shard) would otherwise
    /// flip the staleness flag while the map is still incomplete,
    /// letting a concurrent reader observe `needs_refresh == false`
    /// against a half-populated cache and return NotFound for the
    /// not-yet-inserted shards.
    ///
    /// Callers writing back a whole `LookupEcVolume` reply should use
    /// [`Self::merge_shard_locations`] instead — it upserts the reply's
    /// shards under the write lock and advances the refresh timestamp in
    /// one step, retaining cached shards the reply omits.
    pub fn set_shard_locations(&self, shard_id: ShardId, locations: Vec<String>) {
        self.shard_locations
            .write()
            .unwrap()
            .insert(shard_id, locations);
    }

    /// Atomically replace the entire shard-locations map and stamp
    /// the refresh time. Used by the distributed-read path's
    /// post-`LookupEcVolume` write-back so the cache transitions
    /// from old → fresh in a single observable step — concurrent
    /// readers either see the full prior map or the full new map,
    /// never an intermediate state with the freshness flag flipped.
    pub fn replace_shard_locations(&self, locations: HashMap<ShardId, Vec<String>>) {
        *self.shard_locations.write().unwrap() = locations;
        *self.shard_locations_refresh_time.lock().unwrap() = Some(std::time::Instant::now());
    }

    /// Merge a fresh `LookupEcVolume` reply into the shard-locations cache and
    /// stamp the refresh time, returning a clone of the resulting map.
    ///
    /// Per-shard upsert (mirrors Go's `cachedLookupEcShardLocations`): each shard
    /// id present in the reply overwrites its cached entry, while shard ids absent
    /// from the reply keep their previously-cached locations — so a reply that
    /// passes the data-shard completeness guard but omits a shard already in cache
    /// does not drop that shard's known location (unlike a full replace).
    pub fn merge_shard_locations(
        &self,
        locations: HashMap<ShardId, Vec<String>>,
    ) -> HashMap<ShardId, Vec<String>> {
        let merged = {
            let mut guard = self.shard_locations.write().unwrap();
            for (shard_id, addrs) in locations {
                guard.insert(shard_id, addrs);
            }
            guard.clone()
        };
        *self.shard_locations_refresh_time.lock().unwrap() = Some(std::time::Instant::now());
        merged
    }

    /// Get a cloned list of server addresses for a given shard ID.
    pub fn get_shard_locations(&self, shard_id: ShardId) -> Vec<String> {
        self.shard_locations
            .read()
            .unwrap()
            .get(&shard_id)
            .cloned()
            .unwrap_or_default()
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
                // Apply runtime deletion state on top of the sealed .ecx
                // lookup: a needle in the in-memory deleted set is
                // reported with TOMBSTONE_FILE_SIZE even though the .ecx
                // record itself is untouched.
                if self.is_needle_deleted(needle_id) {
                    return Ok(Some((offset, TOMBSTONE_FILE_SIZE)));
                }
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
    /// Locate the EC shard intervals covering a needle at `actual_offset` whose
    /// index size is `size`. Mirrors Go's EcVolume.LocateEcShardNeedleInterval.
    pub fn locate_ec_shard_needle_interval(
        &self,
        actual_offset: i64,
        size: Size,
    ) -> Vec<ec_locate::Interval> {
        // shardSize = datFileSize / DataShards when known, else ecdFileSize - 1
        // (shards are padded to the small block size; the -1 avoids an
        // off-by-one in the large-block row count).
        let shard_size = if self.dat_file_size > 0 {
            self.dat_file_size / self.data_shards as i64
        } else {
            self.shard_file_size() - 1
        };
        // locate_data wants the on-disk size (header+body+checksum+timestamp+padding).
        let actual = get_actual_size(size, self.version);
        ec_locate::locate_data(actual_offset, Size(actual as i32), shard_size, self.data_shards)
    }

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

        let intervals = self.locate_ec_shard_needle_interval(offset.to_actual_offset(), size);
        Ok(Some((offset, size, intervals)))
    }

    /// Read a full needle from locally available EC shards.
    ///
    /// Locates the needle in the .ecx index, determines which shard intervals
    /// contain its data, reads from local shards, and parses the result into
    /// a fully populated Needle (including last_modified, checksum, ttl).
    ///
    /// Returns `Ok(None)` if the needle is not found or is deleted.
    /// Returns an error if a required shard is not available locally.
    pub fn read_ec_shard_needle(&self, needle_id: NeedleId) -> io::Result<Option<Needle>> {
        let (offset, size, intervals) = match self.locate_needle(needle_id)? {
            Some(v) => v,
            None => return Ok(None),
        };

        if intervals.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no intervals for needle",
            ));
        }

        // Compute the total bytes we need to read (full needle on disk)
        let actual_size = get_actual_size(size, self.version) as usize;
        let mut bytes = Vec::with_capacity(actual_size);

        for interval in &intervals {
            let (shard_id, shard_offset) = interval.to_shard_id_and_offset(self.data_shards);
            let shard = self
                .shards
                .get(shard_id as usize)
                .and_then(|s| s.as_ref())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("ec shard {} not available locally", shard_id),
                    )
                })?;

            let mut buf = vec![0u8; interval.size as usize];
            let n = shard.read_at(&mut buf, shard_offset as u64)?;
            if n != buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "short read on ec shard {}: read {} of {} bytes for needle {}",
                        shard_id,
                        n,
                        buf.len(),
                        needle_id
                    ),
                ));
            }
            bytes.extend_from_slice(&buf);
        }

        // Truncate to exact actual_size (intervals may span more than needed)
        bytes.truncate(actual_size);

        if bytes.len() < actual_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "read {} bytes but need {} for needle {}",
                    bytes.len(),
                    actual_size,
                    needle_id
                ),
            ));
        }

        let mut n = Needle::default();
        n.read_bytes(&bytes, offset.to_actual_offset(), size, self.version)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}", e)))?;

        Ok(Some(n))
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

    /// Walk the .ecx index and return (file_count, file_deleted_count, total_size).
    /// total_size sums size.Raw() for all entries (including deleted), matching Go's WalkIndex.
    pub fn walk_ecx_stats(&self) -> io::Result<(u64, u64, u64)> {
        let ecx_file = match self.ecx_file.as_ref() {
            Some(f) => f,
            None => return Ok((0, 0, 0)),
        };

        let entry_count = self.ecx_file_size as usize / NEEDLE_MAP_ENTRY_SIZE;
        let mut files: u64 = 0;
        let mut files_deleted: u64 = 0;
        let mut total_size: u64 = 0;
        let mut entry_buf = [0u8; NEEDLE_MAP_ENTRY_SIZE];

        for i in 0..entry_count {
            let file_offset = (i * NEEDLE_MAP_ENTRY_SIZE) as u64;
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                ecx_file.read_exact_at(&mut entry_buf, file_offset)?;
            }
            let (_key, _offset, size) = idx_entry_from_bytes(&entry_buf);
            // Match Go's Size.Raw(): tombstone (-1) returns 0, other negatives return abs
            if !size.is_tombstone() {
                total_size += size.0.unsigned_abs() as u64;
            }
            if size.is_deleted() {
                files_deleted += 1;
            } else {
                files += 1;
            }
        }

        Ok((files, files_deleted, total_size))
    }

    /// ScrubIndex verifies index integrity of an EC volume.
    /// Matches Go's `(ev *EcVolume) ScrubIndex()` → `idx.CheckIndexFile()`.
    /// Returns (entry_count, errors).
    pub fn scrub_index(&self) -> (u64, Vec<String>) {
        if self.ecx_file.is_none() {
            return (
                0,
                vec![format!(
                    "no ECX file associated with EC volume {}",
                    self.volume_id.0
                )],
            );
        }
        if self.ecx_file_size == 0 {
            return (
                0,
                vec![format!("zero-size ECX file for EC volume {}", self.volume_id.0)],
            );
        }

        // Walk a private fd so the structural scan never moves the shared
        // ecx_file cursor (the cached handle is read positionally elsewhere).
        let ecx_path = self.ecx_file_name();
        let mut ecx_file = match File::open(&ecx_path) {
            Ok(f) => f,
            Err(e) => return (0, vec![format!("open ECX file {}: {}", ecx_path, e)]),
        };
        crate::storage::idx::check_index_file(&mut ecx_file, self.ecx_file_size, self.version)
    }

    /// ScrubLocal verifies each needle against the LOCAL shards only; it cannot
    /// CRC-check a needle whose intervals span shards held on other servers.
    /// Mirrors Go's EcVolume.ScrubLocal. Returns (rows walked, broken shards, errors).
    pub fn scrub_local(
        &self,
    ) -> (u64, Vec<crate::pb::volume_server_pb::EcShardInfo>, Vec<String>) {
        // Local scan also verifies the index.
        let (_, mut errs) = self.scrub_index();

        let mut broken_shards: HashSet<ShardId> = HashSet::new();
        let mut count: u64 = 0;

        let ecx_path = self.ecx_file_name();
        let mut ecx_file = match File::open(&ecx_path) {
            Ok(f) => f,
            Err(e) => {
                errs.push(format!("open ECX file {}: {}", ecx_path, e));
                return (count, Vec::new(), errs);
            }
        };

        // Reused across every needle/chunk to avoid a per-chunk allocation.
        let mut chunk_buf: Vec<u8> = Vec::new();
        let walk = crate::storage::idx::walk_index_file(&mut ecx_file, 0, |id, offset, size| {
            count += 1;
            if size.is_tombstone() {
                return Ok(());
            }

            let locations = self.locate_ec_shard_needle_interval(offset.to_actual_offset(), size);
            // A needle is verifiable locally only if every shard it spans is local;
            // when any is remote, skip the reassembly buffer entirely.
            let has_remote_chunks = locations.iter().any(|iv| {
                let (sid, _) = iv.to_shard_id_and_offset(self.data_shards);
                self.shards.get(sid as usize).and_then(|s| s.as_ref()).is_none()
            });
            let mut read: i64 = 0;
            let mut data: Vec<u8> = if has_remote_chunks {
                Vec::new()
            } else {
                Vec::with_capacity(get_actual_size(size, self.version) as usize)
            };
            let mut local_shard_ids: Vec<ShardId> = Vec::new();

            for (i, iv) in locations.iter().enumerate() {
                let (sid, soffset) = iv.to_shard_id_and_offset(self.data_shards);
                let ssize = iv.size;
                let shard = match self.shards.get(sid as usize).and_then(|s| s.as_ref()) {
                    Some(s) => s,
                    None => {
                        // Shard is not local; we can't verify it without decoding.
                        read += ssize;
                        continue;
                    }
                };
                local_shard_ids.push(sid);

                if soffset + ssize > shard.file_size() {
                    broken_shards.insert(sid);
                    errs.push(format!(
                        "local shard {} for needle {} is too short ({}), cannot read chunk {}/{}",
                        sid,
                        id.0,
                        shard.file_size(),
                        i + 1,
                        locations.len()
                    ));
                    continue;
                }

                chunk_buf.resize(ssize as usize, 0);
                match shard.read_at(&mut chunk_buf, soffset as u64) {
                    Err(e) => {
                        broken_shards.insert(sid);
                        errs.push(format!(
                            "failed to read chunk {}/{} for needle {} from local shard {} at offset {}: {}",
                            i + 1,
                            locations.len(),
                            id.0,
                            sid,
                            soffset,
                            e
                        ));
                        continue;
                    }
                    Ok(got) if got as i64 != ssize => {
                        broken_shards.insert(sid);
                        errs.push(format!(
                            "expected {} bytes for chunk {}/{} for needle {} from local shard {}, got {}",
                            ssize,
                            i + 1,
                            locations.len(),
                            id.0,
                            sid,
                            got
                        ));
                        continue;
                    }
                    Ok(_) => {}
                }

                if !has_remote_chunks {
                    data.extend_from_slice(&chunk_buf);
                }
                read += ssize;
            }

            local_shard_ids.sort_unstable();

            let want = get_actual_size(size, self.version);
            if read != want {
                // Like Go, returning from the walk callback aborts the scan.
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "expected {} bytes for needle {} on volume {}, got {}",
                        want, id.0, self.volume_id.0, read
                    ),
                ));
            }

            // Only a fully-local needle can be reassembled and CRC-checked.
            if !has_remote_chunks {
                let mut n = Needle::default();
                if let Err(e) = n.read_bytes(&data, 0, size, self.version) {
                    // A delete-state disagreement between the .ecx index and the reassembled
                    // on-disk header (live index vs zero header size) is not corruption.
                    let delete_state_disagrees = matches!(
                        &e,
                        NeedleError::SizeMismatch { found, .. } if size.is_deleted() != (found.0 == 0)
                    );
                    if !delete_state_disagrees {
                        errs.push(format!(
                            "needle {} on volume {}, shards {:?}: {}",
                            id.0, self.volume_id.0, local_shard_ids, e
                        ));
                    }
                }
            }
            Ok(())
        });
        if let Err(e) = walk {
            // Go appends the walk/callback error verbatim.
            errs.push(e.to_string());
        }

        let mut broken: Vec<crate::pb::volume_server_pb::EcShardInfo> = broken_shards
            .iter()
            .filter_map(|sid| self.shards.get(*sid as usize).and_then(|s| s.as_ref()))
            .map(|s| s.to_ec_shard_info())
            .collect();
        broken.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));

        (count, broken, errs)
    }

    // ---- Deletion ----

    /// Write `TOMBSTONE_FILE_SIZE` over the Size field of an existing .ecx
    /// entry, matching Go's `MarkNeedleDeleted`. Only used by the offline
    /// `rebuild_ecx_from_journal` path — the runtime delete path does not
    /// touch .ecx because the index is treated as an immutable sorted
    /// (id, offset, size) table. Returns `false` if the needle is not in
    /// the index (ignored by callers) and an error on IO failure.
    fn tombstone_ecx_entry(&self, needle_id: NeedleId) -> io::Result<bool> {
        let ecx_file = self.ecx_file.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "ec volume {} has no open .ecx file (closed or corrupt)",
                    self.volume_id.0
                ),
            )
        })?;

        let entry_count = self.ecx_file_size as usize / NEEDLE_MAP_ENTRY_SIZE;
        if entry_count == 0 {
            return Ok(false);
        }

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
            let (key, _offset, _old_size) = idx_entry_from_bytes(&entry_buf);
            if key == needle_id {
                let size_offset = file_offset + NEEDLE_ID_SIZE as u64 + OFFSET_SIZE as u64;
                let mut size_buf = [0u8; SIZE_SIZE];
                TOMBSTONE_FILE_SIZE.to_bytes(&mut size_buf);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    ecx_file.write_all_at(&size_buf, size_offset)?;
                }
                return Ok(true);
            } else if key < needle_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(false)
    }

    /// Replay .ecj journal entries into .ecx: for each needle id in .ecj,
    /// overwrite its .ecx size field with a tombstone, then remove the
    /// journal file. Mirrors Go's `RebuildEcxFile`, which is invoked from
    /// specific decode / rebuild gRPC handlers — it is intentionally
    /// **not** called on volume load (runtime reads consult
    /// `deleted_needles` instead). The rebuild is atomic with respect to
    /// the journal: if any individual write fails the .ecj file is left
    /// in place and the error is propagated so tombstones are not lost.
    #[allow(dead_code)]
    fn rebuild_ecx_from_journal(&mut self) -> io::Result<()> {
        let ecj_path = self.ecj_file_name();
        if !std::path::Path::new(&ecj_path).exists() {
            return Ok(());
        }

        let data = fs::read(&ecj_path)?;
        if data.is_empty() {
            return Ok(());
        }

        let count = data.len() / NEEDLE_ID_SIZE;
        for i in 0..count {
            let start = i * NEEDLE_ID_SIZE;
            if start + NEEDLE_ID_SIZE > data.len() {
                break;
            }
            let needle_id = NeedleId::from_bytes(&data[start..start + NEEDLE_ID_SIZE]);
            // A needle that never made it into .ecx is fine (e.g. the
            // delete raced against encode). Any other IO error aborts the
            // rebuild so the journal survives to be retried later.
            self.tombstone_ecx_entry(needle_id)?;
        }

        // Durably flush the newly-written .ecx tombstones before dropping
        // the journal: the writes went through write_all_at and may still
        // be in page cache.
        if let Some(ref ecx_file) = self.ecx_file {
            ecx_file.sync_all()?;
        }

        // Fold successful — drop and recreate the journal, clear the
        // in-memory deleted set (all of its contents are now materialized
        // in .ecx), and reset the cached size.
        fs::remove_file(&ecj_path)?;
        let ecj_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&ecj_path)?;
        self.ecj_file = Some(ecj_file);
        self.ecj_file_size = 0;
        if let Ok(mut set) = self.deleted_needles.write() {
            set.clear();
        }

        Ok(())
    }

    // ---- Deletion journal ----

    /// Record a needle delete: append the id to the .ecj deletion journal
    /// and insert it into the in-memory deleted set. `.ecx` is not touched
    /// at runtime — it is a sealed sorted (id, offset, size) index and
    /// runtime deletion state lives exclusively in .ecj + `deleted_needles`.
    /// A lookup via `find_needle_from_ecx` masks the id out by returning
    /// `TOMBSTONE_FILE_SIZE` on a subsequent read.
    ///
    /// The .ecj append is the durable commit point. On any failure the
    /// file is truncated back to the pre-append length so the on-disk
    /// journal and in-memory state cannot drift. Only after the sync
    /// succeeds is the id published into the set, so a failure leaves
    /// the delete invisible to readers.
    pub fn journal_delete(&mut self, needle_id: NeedleId) -> io::Result<()> {
        // Look the needle up read-only. Missing is a silent no-op; a
        // pre-existing .ecx tombstone (from a prior decode/rebuild) is
        // mirrored into the in-memory set so delete_count stays accurate
        // without needing to walk .ecx on every heartbeat.
        match self.find_needle_from_ecx_raw(needle_id)? {
            None => return Ok(()),
            Some((_, size)) if size.is_deleted() => {
                if let Ok(mut set) = self.deleted_needles.write() {
                    set.insert(needle_id);
                }
                return Ok(());
            }
            Some(_) => {}
        }

        // Idempotent fast path for repeat deletes — avoids the journal
        // append entirely so the derived delete_count stays stable.
        if self.is_needle_deleted(needle_id) {
            return Ok(());
        }

        let prev_ecj_size = self.ecj_file_size;
        let append_result: io::Result<()> = {
            let ecj_file = self
                .ecj_file
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "ecj file not open"))?;
            let mut buf = [0u8; NEEDLE_ID_SIZE];
            needle_id.to_bytes(&mut buf);
            ecj_file
                .write_all(&buf)
                .and_then(|_| ecj_file.sync_all())
        };

        match append_result {
            Ok(()) => {
                self.ecj_file_size += NEEDLE_ID_SIZE as i64;
                if let Ok(mut set) = self.deleted_needles.write() {
                    set.insert(needle_id);
                }
                Ok(())
            }
            Err(e) => {
                // write_all may have extended the file on disk before
                // sync_all failed; truncate back to the known-good size so
                // the on-disk journal never drifts past `deleted_needles`.
                if let Some(ecj) = self.ecj_file.as_mut() {
                    if let Err(trunc_err) = ecj.set_len(prev_ecj_size as u64) {
                        tracing::error!(
                            volume_id = self.volume_id.0,
                            needle_id = needle_id.0,
                            truncate_error = %trunc_err,
                            "failed to truncate ecj after append failure"
                        );
                    }
                }
                Err(e)
            }
        }
    }

    /// Internal: binary search .ecx without masking by `deleted_needles`.
    /// Used by `journal_delete` so a repeat delete can still see the raw
    /// pre-existing .ecx tombstone from a prior rebuild.
    fn find_needle_from_ecx_raw(
        &self,
        needle_id: NeedleId,
    ) -> io::Result<Option<(Offset, Size)>> {
        let ecx_file = self
            .ecx_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "ecx file not open"))?;
        let entry_count = self.ecx_file_size as usize / NEEDLE_MAP_ENTRY_SIZE;
        if entry_count == 0 {
            return Ok(None);
        }
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
        // Sync .ecx before closing to flush in-place deletion marks (matches Go's ev.ecxFile.Sync())
        if let Some(ref ecx_file) = self.ecx_file {
            let _ = ecx_file.sync_all();
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
        // Remove .ecx/.ecj/.vif from ecx_actual_dir (where they were found)
        // Go's Destroy() removes .ecx, .ecj, and .vif files.
        let actual_base = crate::storage::volume::volume_file_name(
            &self.ecx_actual_dir,
            &self.collection,
            self.volume_id,
        );
        let _ = fs::remove_file(format!("{}.ecx", actual_base));
        let _ = fs::remove_file(format!("{}.ecj", actual_base));
        let _ = fs::remove_file(format!("{}.vif", actual_base));
        // Also sweep the originally-configured idx dir in case stale files
        // exist there (ecx_file_name() / ecj_file_name() now resolve from
        // ecx_actual_dir, so we have to build the idx-dir paths explicitly).
        if self.ecx_actual_dir != self.dir_idx {
            let idx_base = crate::storage::volume::volume_file_name(
                &self.dir_idx,
                &self.collection,
                self.volume_id,
            );
            let _ = fs::remove_file(format!("{}.ecx", idx_base));
            let _ = fs::remove_file(format!("{}.ecj", idx_base));
            let _ = fs::remove_file(format!("{}.vif", idx_base));
        }
        if self.ecx_actual_dir != self.dir && self.dir_idx != self.dir {
            let data_base = crate::storage::volume::volume_file_name(
                &self.dir,
                &self.collection,
                self.volume_id,
            );
            let _ = fs::remove_file(format!("{}.ecx", data_base));
            let _ = fs::remove_file(format!("{}.ecj", data_base));
            let _ = fs::remove_file(format!("{}.vif", data_base));
        }
        // Go's Destroy() also removes bitrot checksum sidecars so a later
        // volume-id reuse cannot load stale protection, and so
        // collection.delete does not leave orphaned <base>.ecsum files.
        // Sweep every base dir the index/data files may live under.
        let mut bases = vec![crate::storage::volume::volume_file_name(
            &self.dir,
            &self.collection,
            self.volume_id,
        )];
        if self.dir_idx != self.dir {
            bases.push(crate::storage::volume::volume_file_name(
                &self.dir_idx,
                &self.collection,
                self.volume_id,
            ));
        }
        if self.ecx_actual_dir != self.dir && self.ecx_actual_dir != self.dir_idx {
            bases.push(crate::storage::volume::volume_file_name(
                &self.ecx_actual_dir,
                &self.collection,
                self.volume_id,
            ));
        }
        for base in bases {
            let _ = crate::storage::erasure_coding::ec_bitrot::remove_bitrot_sidecars(&base);
        }
        self.ecx_file = None;
        self.ecj_file = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// `destroy()` must remove co-located `.ecsum` sidecars (Go Destroy parity).
    /// Without this, `collection.delete` leaves orphaned bitrot files that
    /// inflate EC-health scanners after the shards are gone.
    #[test]
    fn test_destroy_removes_bitrot_sidecar() {
        use crate::storage::needle_map::NeedleMapKind;
        use crate::storage::volume::Volume;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = Volume::new(
            dir,
            dir,
            "ec1c",
            VolumeId(2074),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        for i in 1..=3 {
            let data = format!("needle {}", i);
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
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            dir,
            dir,
            "ec1c",
            VolumeId(2074),
            10,
            4,
        )
        .unwrap();

        let base = crate::storage::volume::volume_file_name(dir, "ec1c", VolumeId(2074));
        let ecsum = format!("{}.ecsum", base);
        assert!(
            std::path::Path::new(&ecsum).exists(),
            "precondition: encode must write generation-0 .ecsum"
        );
        assert!(
            std::path::Path::new(&format!("{}.ec00", base)).exists(),
            "precondition: encode must write shards"
        );

        let mut vol = EcVolume::new(dir, dir, "ec1c", VolumeId(2074)).unwrap();
        // Mount at least one local shard so destroy's shard loop has work.
        vol.add_shard(EcVolumeShard::new(dir, "ec1c", VolumeId(2074), 0))
            .unwrap();
        vol.destroy();

        assert!(
            !std::path::Path::new(&format!("{}.ec00", base)).exists(),
            "destroy should remove shards"
        );
        assert!(
            !std::path::Path::new(&ecsum).exists(),
            "destroy should remove co-located .ecsum (Go Destroy parity)"
        );
        assert!(
            !std::path::Path::new(&format!("{}.ecx", base)).exists(),
            "destroy should remove .ecx"
        );
    }

    /// Mounting an EC volume loads and validates its generation-0 `.ecsum`
    /// sidecar, so `bitrot_protection()` reports `On` with the parsed manifest.
    #[test]
    fn test_mount_loads_bitrot_sidecar() {
        use crate::storage::erasure_coding::ec_bitrot::BitrotStatus;
        use crate::storage::needle_map::NeedleMapKind;
        use crate::storage::volume::Volume;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = Volume::new(
            dir,
            dir,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        for i in 1..=5 {
            let data = format!("test data for needle {}", i);
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
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(dir, dir, "", VolumeId(1), 10, 4)
            .unwrap();

        let vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        assert!(
            vol.bitrot.is_some(),
            "mount should load the generation-0 .ecsum sidecar"
        );
        let (prot, status) = vol.bitrot_protection();
        assert_eq!(status, BitrotStatus::On);
        assert_eq!(prot.unwrap().shards.len(), 14);
    }

    /// CHECKSUM scrub verifies clean shards against the sidecar and flags a shard
    /// whose bytes are corrupted after encode.
    #[test]
    fn test_checksum_scrub_clean_and_detects_corruption() {
        use crate::storage::needle_map::NeedleMapKind;
        use crate::storage::volume::Volume;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut v = Volume::new(
            dir,
            dir,
            "",
            VolumeId(1),
            NeedleMapKind::InMemory,
            None,
            None,
            0,
            Version::current(),
        )
        .unwrap();
        for i in 1..=8 {
            let data = format!("test data for needle {} with a bit more length", i);
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
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(dir, dir, "", VolumeId(1), 10, 4)
            .unwrap();

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        for id in 0..14u8 {
            vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), id))
                .unwrap();
        }

        // Clean scrub: no mismatches, no errors, blocks scanned > 0.
        let (scanned, broken, errs) = vol.checksum_scrub();
        assert!(errs.is_empty(), "unexpected scrub errors: {:?}", errs);
        assert!(broken.is_empty(), "unexpected mismatches: {:?}", broken);
        assert!(scanned > 0, "scrub should scan at least one block");

        // Corrupt one byte of shard 3 on disk; re-scrub must report shard 3.
        let shard3 = format!("{}/1.ec03", dir);
        let mut bytes = std::fs::read(&shard3).unwrap();
        assert!(!bytes.is_empty());
        bytes[0] ^= 0xFF;
        std::fs::write(&shard3, &bytes).unwrap();

        let (_, broken2, _) = vol.checksum_scrub();
        assert!(
            broken2.contains(&3),
            "corrupted shard 3 should be flagged, got {:?}",
            broken2
        );
    }

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

        // .ecj append is gated on a live->tombstone transition in .ecx, so
        // the fixture must contain the needles we are about to delete.
        let entries = vec![
            (NeedleId(10), Offset::from_actual_offset(8), Size(100)),
            (NeedleId(20), Offset::from_actual_offset(200), Size(200)),
        ];
        write_ecx_file(dir, "", VolumeId(1), &entries);

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        let (fc0, dc0) = vol.file_and_delete_count();
        assert_eq!((fc0, dc0), (2, 0));

        vol.journal_delete(NeedleId(10)).unwrap();
        vol.journal_delete(NeedleId(20)).unwrap();

        let deleted = vol.read_deleted_needles().unwrap();
        assert_eq!(deleted, vec![NeedleId(10), NeedleId(20)]);

        let (fc, dc) = vol.file_and_delete_count();
        assert_eq!((fc, dc), (2, 2));

        // Idempotent re-delete must not bump delete_count.
        vol.journal_delete(NeedleId(10)).unwrap();
        // Deleting a missing needle must not bump delete_count either.
        vol.journal_delete(NeedleId(999)).unwrap();
        let (fc, dc) = vol.file_and_delete_count();
        assert_eq!((fc, dc), (2, 2));
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

    #[test]
    fn test_ec_volume_uses_collection_prefixed_vif_config() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(dir, "pics", VolumeId(1), &[]);

        let vif = crate::storage::volume::VifVolumeInfo {
            ec_shard_config: Some(crate::storage::volume::VifEcShardConfig {
                data_shards: 6,
                parity_shards: 3,
                ..Default::default()
            }),
            ..Default::default()
        };
        let base = crate::storage::volume::volume_file_name(dir, "pics", VolumeId(1));
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string_pretty(&vif).unwrap(),
        )
        .unwrap();

        let vol = EcVolume::new(dir, dir, "pics", VolumeId(1)).unwrap();
        assert_eq!(vol.data_shards, 6);
        assert_eq!(vol.parity_shards, 3);
    }

    #[test]
    fn test_ec_volume_invalid_vif_config_falls_back_to_defaults() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(dir, "pics", VolumeId(1), &[]);

        // data + parity exceeds MAX_SHARD_COUNT, so the config is rejected.
        let vif = crate::storage::volume::VifVolumeInfo {
            ec_shard_config: Some(crate::storage::volume::VifEcShardConfig {
                data_shards: 20,
                parity_shards: 20,
                ..Default::default()
            }),
            ..Default::default()
        };
        let base = crate::storage::volume::volume_file_name(dir, "pics", VolumeId(1));
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string_pretty(&vif).unwrap(),
        )
        .unwrap();

        let vol = EcVolume::new(dir, dir, "pics", VolumeId(1)).unwrap();
        assert_eq!(vol.data_shards, DATA_SHARDS_COUNT as u32);
        assert_eq!(vol.parity_shards, PARITY_SHARDS_COUNT as u32);
    }

    #[test]
    fn test_ec_volume_wide_ratio_vif_config() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(dir, "pics", VolumeId(1), &[]);

        // A wider-than-default ratio within MAX_SHARD_COUNT must load as-is.
        let vif = crate::storage::volume::VifVolumeInfo {
            ec_shard_config: Some(crate::storage::volume::VifEcShardConfig {
                data_shards: 16,
                parity_shards: 4,
                ..Default::default()
            }),
            ..Default::default()
        };
        let base = crate::storage::volume::volume_file_name(dir, "pics", VolumeId(1));
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string_pretty(&vif).unwrap(),
        )
        .unwrap();

        let vol = EcVolume::new(dir, dir, "pics", VolumeId(1)).unwrap();
        assert_eq!(vol.data_shards, 16);
        assert_eq!(vol.parity_shards, 4);
    }

    #[test]
    fn test_scrub_local_skips_tombstones() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let entries = vec![(NeedleId(1), Offset::from_actual_offset(0), Size(-1))];
        write_ecx_file(dir, "", VolumeId(1), &entries);

        let vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        let (count, broken, errs) = vol.scrub_local();
        assert_eq!(count, 1);
        assert!(broken.is_empty(), "{:?}", broken);
        assert!(errs.is_empty(), "{:?}", errs);
    }

    #[test]
    fn test_scrub_local_clean_when_no_local_shards() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let entries = vec![(NeedleId(1), Offset::from_actual_offset(0), Size(100))];
        write_ecx_file(dir, "", VolumeId(1), &entries);

        // dat_file_size makes the shard-size math well-defined.
        let vif = crate::storage::volume::VifVolumeInfo {
            dat_file_size: 14000,
            ..Default::default()
        };
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(1));
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string_pretty(&vif).unwrap(),
        )
        .unwrap();

        // No shard files present: nothing to verify locally, so no errors.
        let vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        let (count, broken, errs) = vol.scrub_local();
        assert_eq!(count, 1);
        assert!(broken.is_empty(), "{:?}", broken);
        assert!(errs.is_empty(), "{:?}", errs);
    }

    #[test]
    fn test_scrub_local_suppresses_delete_state_disagreement() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        // Live index entry (size > 0) whose reassembled on-disk header reports size 0
        // (deleted-on-shards but live-in-index) — a delete-state disagreement, not corruption.
        let entries = vec![(NeedleId(1), Offset::from_actual_offset(0), Size(100))];
        write_ecx_file(dir, "", VolumeId(1), &entries);

        let vif = crate::storage::volume::VifVolumeInfo {
            dat_file_size: 14000,
            ..Default::default()
        };
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(1));
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string_pretty(&vif).unwrap(),
        )
        .unwrap();

        // Shard 0 holds the needle's bytes; all-zero so the parsed header size is 0.
        let mut shard0 = EcVolumeShard::new(dir, "", VolumeId(1), 0);
        shard0.create().unwrap();
        shard0.write_all(&[0u8; 256]).unwrap();
        shard0.close();

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 0))
            .unwrap();

        let (count, broken, errs) = vol.scrub_local();
        assert_eq!(count, 1);
        assert!(broken.is_empty(), "{:?}", broken);
        assert!(
            errs.is_empty(),
            "delete-state disagreement must be suppressed, got {:?}",
            errs
        );
    }

    #[test]
    fn test_scrub_local_reports_genuine_size_corruption() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let entries = vec![(NeedleId(1), Offset::from_actual_offset(0), Size(100))];
        write_ecx_file(dir, "", VolumeId(1), &entries);

        let vif = crate::storage::volume::VifVolumeInfo {
            dat_file_size: 14000,
            ..Default::default()
        };
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(1));
        std::fs::write(
            format!("{}.vif", base),
            serde_json::to_string_pretty(&vif).unwrap(),
        )
        .unwrap();

        // On-disk header reports a non-zero size (50) that disagrees with the live
        // index size (100): genuine corruption, not a delete-state race — must report.
        let mut bytes = vec![0u8; 256];
        bytes[15] = 50; // header size field (big-endian u32) = 50
        let mut shard0 = EcVolumeShard::new(dir, "", VolumeId(1), 0);
        shard0.create().unwrap();
        shard0.write_all(&bytes).unwrap();
        shard0.close();

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 0))
            .unwrap();

        let (_count, _broken, errs) = vol.scrub_local();
        assert!(
            !errs.is_empty(),
            "a non-zero size mismatch is genuine corruption and must be reported"
        );
    }
}
