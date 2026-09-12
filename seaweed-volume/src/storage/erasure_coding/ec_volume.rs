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
use crate::storage::volume_open::open_volume_file;

/// An erasure-coded volume managing its local shards and index.
pub const IO_ERROR_TOLERANCE: i32 = 3;

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
    /// Uniform block layout: each shard is one contiguous block of this many
    /// bytes. 0 = legacy 1GiB/1MiB two-tier layout. Loaded from .vif.
    pub block_size: i64,
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
    /// Marks the map for a prompt re-check: a read that failed against a cached
    /// location has disproved what the map claims, and the normal freshness
    /// window is far too long to serve from a map known to be wrong. Mirrors the
    /// invalidation Go's `forgetShardId` performs. A mutex rather than an atomic
    /// so the refresh can judge the map and consume the mark in one critical
    /// section, and a mark raised meanwhile survives for the next refresh.
    pub shard_locations_stale: std::sync::Mutex<bool>,
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
    /// Directory the active sidecar was actually resolved from. The search in
    /// `load_bitrot_for_generation` spans sibling disks, and a `.ecsum` records
    /// no encode identity, so provenance is the only signal that a loaded
    /// manifest may describe a different encode run. Same purpose as
    /// `ecx_actual_dir`. Empty when no sidecar was found. Always committed in
    /// the same step as `bitrot`/`bitrot_status`, so a reload whose `Err` gets
    /// swallowed by `reload_bitrot_sidecar` cannot leave this describing a
    /// sidecar other than the one those two fields actually hold.
    pub(crate) bitrot_source_dir: String,

    io_error_count: std::sync::atomic::AtomicI32,
    io_error_quarantined: std::sync::atomic::AtomicBool,
    last_io_error: std::sync::Mutex<Option<String>>,
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

/// Load the volume's `.vif` (data dir first, then idx dir, then the active
/// generation's versioned sidecar — see [`locate_vif_path`]). Absent
/// everywhere is legal — legacy volumes predate the sidecar — and returns
/// `None`; so does a zero-byte stub (an ec.decode copy from a source
/// without one), mirroring Go's MaybeLoadVolumeInfo. A
/// present-but-unreadable or malformed `.vif` is an ERROR: silently
/// defaulting would mount a uniform-layout volume with legacy offset math
/// and serve wrong bytes with a straight face.
pub fn load_vif_info(
    dir: &str,
    dir_idx: &str,
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<Option<crate::storage::volume::VifVolumeInfo>> {
    let vif_path = locate_vif_path(dir, dir_idx, collection, volume_id);
    match std::fs::read_to_string(&vif_path) {
        Ok(content) if content.trim().is_empty() => Ok(None),
        Ok(content) => serde_json::from_str(&content).map(Some).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parse {}: {}", vif_path, e),
            )
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("read {}: {}", vif_path, e),
        )),
    }
}

/// Read the EC data/parity shard counts and shard block size from `.vif`,
/// defaulting to the build's standard ratio and the legacy block layout when
/// no `.vif` is present. A present-but-unreadable or malformed `.vif`
/// fails instead of defaulting (see [`load_vif_info`]).
/// Looks at the data dir first, then the idx dir — see [`locate_vif_path`].
pub fn read_ec_shard_config(
    dir: &str,
    dir_idx: &str,
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<(u32, u32, i64)> {
    let vif = load_vif_info(dir, dir_idx, collection, volume_id)?;
    ec_shard_config_from(vif.as_ref(), &[dir, dir_idx], collection, volume_id)
}

/// Load the volume's `.vif` from the selected location or, failing that, from
/// any sibling disk — returning the directory it came from so the caller can
/// resolve the rest of the volume's metadata against the same place. A rebuild
/// picks one disk to write into, but a multi-disk server may hold that
/// volume's metadata on another.
pub fn load_vif_info_across_dirs(
    dir: &str,
    dir_idx: &str,
    other_dirs: &[String],
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<Option<(crate::storage::volume::VifVolumeInfo, String)>> {
    if let Some(vif) = load_vif_info(dir, dir_idx, collection, volume_id)? {
        // Report where it actually came from. load_vif_info probes the data
        // directory then the index directory, so naming `dir` unconditionally
        // pointed callers at the wrong disk whenever the vif lived with the
        // index — and a caller resolving the rest of the volume's metadata
        // against that answer would look in a directory holding none of it.
        let data_vif = format!(
            "{}.vif",
            crate::storage::volume::volume_file_name(dir, collection, volume_id)
        );
        let found_in = if std::path::Path::new(&data_vif).exists() {
            dir.to_string()
        } else {
            dir_idx.to_string()
        };
        return Ok(Some((vif, found_in)));
    }
    for other in other_dirs {
        if let Some(vif) = load_vif_info(other, other, collection, volume_id)? {
            return Ok(Some((vif, other.clone())));
        }
    }
    Ok(None)
}

/// [`read_ec_shard_config`] for a caller that may find the volume's metadata on
/// any of the server's disks. Without this a rebuild that lands on a disk
/// holding only shards reads the default 10+4 and the legacy block layout,
/// then reconstructs a custom-ratio or uniform volume through the wrong matrix
/// and de-striping geometry.
pub fn read_ec_shard_config_across_dirs(
    dir: &str,
    dir_idx: &str,
    other_dirs: &[String],
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<(u32, u32, i64)> {
    // Every directory this volume's metadata could be in. A split -dir/-dir.idx
    // location keeps .vif and .ecsum with the INDEX, and callers exclude their
    // own index directory from other_dirs on the understanding that it is
    // passed here separately — so it is chained explicitly, exactly as the .vif
    // lookup and Go's findBitrotSidecar both do.
    let mut candidates: Vec<&str> = Vec::with_capacity(2 + other_dirs.len());
    candidates.push(dir);
    if !dir_idx.is_empty() && dir_idx != dir {
        candidates.push(dir_idx);
    }
    candidates.extend(other_dirs.iter().map(|s| s.as_str()));

    // A vif that carries no ecShardConfig answers nothing about the layout, so
    // it must NOT short-circuit the sidecar search: a legacy config-free vif
    // and the generation-0 sidecar can sit in different directories, and
    // stopping at the vif resolved a 12+4 uniform volume as 10+4 legacy. Pass
    // the whole candidate list so the fallback covers the same ground the vif
    // lookup did.
    let vif = load_vif_info_across_dirs(dir, dir_idx, other_dirs, collection, volume_id)?;
    ec_shard_config_from(
        vif.as_ref().map(|(v, _)| v),
        &candidates,
        collection,
        volume_id,
    )
}

/// Resolve (data_shards, parity_shards, block_size) from an already-loaded
/// `.vif`. With no vif — or one carrying no EC config — the bitrot sidecar
/// records the same config at encode time and answers the layout question the
/// vif cannot: defaulting a uniform-layout volume to the legacy block sizes
/// maps every read to the wrong shard offset. `weed fix -ecx` reads the
/// sidecar for the same reason. Absent both, the build's standard ratio and
/// the legacy layout.
pub fn ec_shard_config_from(
    vif: Option<&crate::storage::volume::VifVolumeInfo>,
    dirs: &[&str],
    collection: &str,
    volume_id: VolumeId,
) -> io::Result<(u32, u32, i64)> {
    let default_ds = crate::storage::erasure_coding::ec_shard::DATA_SHARDS_COUNT as u32;
    let default_ps = crate::storage::erasure_coding::ec_shard::PARITY_SHARDS_COUNT as u32;
    // Sum as u64: counts near the u32 ceiling would wrap and pass the bound.
    let usable =
        |ds: u32, ps: u32| ds > 0 && ps > 0 && (ds as u64 + ps as u64) <= MAX_SHARD_COUNT as u64;

    if let Some(ec) = vif.and_then(|v| v.ec_shard_config.as_ref()) {
        // A config that is PRESENT but records an impossible ratio is not a
        // volume to fall back on: dropping through to the sidecar or the
        // defaults would read uniform shards with the legacy offset math and
        // answer with the wrong bytes. Only an ENTIRELY absent config means
        // "this predates the record".
        if !usable(ec.data_shards, ec.parity_shards) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "vif for volume {} records invalid shard counts {}+{}",
                    volume_id.0, ec.data_shards, ec.parity_shards
                ),
            ));
        }
        // A recorded block size that no encoder could have produced maps
        // every read to the wrong shard offset. Refuse the mount rather
        // than serve those bytes or silently pick a layout.
        validate_block_size(ec.block_size)?;
        return Ok((ec.data_shards, ec.parity_shards, ec.block_size));
    }
    // With no usable vif config the sidecar is the ONLY record of this volume's
    // layout, so the three answers stay distinct: absent EVERYWHERE means the
    // volume may genuinely predate the sidecar and legacy is the right guess;
    // present-but-unusable means the record is corrupt, and answering reads
    // from a guessed layout returns wrong bytes rather than none.
    //
    // Every candidate directory is searched, not just the first: a split
    // -dir/-dir.idx location keeps the sidecar with the INDEX, and a multi-disk
    // server may keep it on a sibling. Stopping at the data directory resolved
    // a 12+4 uniform volume as 10+4 legacy.
    let mut path = String::new();
    for candidate in dirs.iter().filter(|d| !d.is_empty()) {
        let base = crate::storage::volume::volume_file_name(candidate, collection, volume_id);
        let probe = crate::storage::erasure_coding::ec_bitrot::bitrot_sidecar_path(&base, 0);
        match std::fs::metadata(&probe) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(io::Error::new(e.kind(), format!("stat {}: {}", probe, e)));
            }
            Ok(_) => {
                path = probe;
                break;
            }
        }
    }
    if path.is_empty() {
        return Ok((default_ds, default_ps, 0));
    }
    let prot = crate::storage::erasure_coding::ec_bitrot::load_bitrot_sidecar(&path)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("read {}: {}", path, e)))?;
    // The un-suffixed sidecar describes generation 0 and nothing else.
    if prot.generation != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} records generation {}, not generation 0",
                path, prot.generation
            ),
        ));
    }
    let ec = prot.ec_shard_config.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} records no EC config", path),
        )
    })?;
    if !usable(ec.data_shards, ec.parity_shards) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} records invalid shard counts {}+{}",
                path, ec.data_shards, ec.parity_shards
            ),
        ));
    }
    validate_block_size(ec.block_size)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", path, e)))?;
    Ok((ec.data_shards, ec.parity_shards, ec.block_size))
}

/// Reports whether a `.vif`-recorded shard block size is one an encoder could
/// have produced. 0 means the legacy two-tier layout, which is always valid;
/// anything positive must be a whole number of small blocks, because that is
/// what `uniform_block_size` rounds to. Mirrors Go's `ValidateBlockSize`.
pub fn validate_block_size(block_size: i64) -> io::Result<()> {
    if block_size == 0 {
        return Ok(());
    }
    if block_size < 0
        || block_size
            % crate::storage::erasure_coding::ec_shard::ERASURE_CODING_SMALL_BLOCK_SIZE as i64
            != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "invalid shard block size {}: expected 0 (legacy) or a multiple of {}",
                block_size,
                crate::storage::erasure_coding::ec_shard::ERASURE_CODING_SMALL_BLOCK_SIZE
            ),
        ));
    }
    Ok(())
}

impl EcVolume {
    /// Create a new EcVolume. Loads .ecx index and .ecj journal if present.
    pub fn new(
        dir: &str,
        dir_idx: &str,
        collection: &str,
        volume_id: VolumeId,
    ) -> io::Result<Self> {
        // One load of the volume's `.vif`, used for both the shard config and
        // the version / dat-size fields below.
        let vif = load_vif_info(dir, dir_idx, collection, volume_id)?;
        // Both directories: a split -dir/-dir.idx layout keeps the .ecsum with
        // the INDEX, and it is the only layout record a volume whose vif is
        // absent or config-free still has.
        let (data_shards, parity_shards, block_size) =
            ec_shard_config_from(vif.as_ref(), &[dir, dir_idx], collection, volume_id)?;

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
        let (expire_at_sec, vif_version, vif_dat_file_size, encode_ts_ns) =
            // Absent everywhere = legacy defaults; a present-but-unreadable or
            // malformed vif already failed the mount in load_vif_info above.
            match vif.as_ref() {
                Some(vif_info) => {
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
                }
                None => (0, Version::current(), 0, 0),
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
            block_size,
            ecx_file: None,
            ecx_file_size: 0,
            ecj_file: None,
            ecj_file_size: 0,
            deleted_needles: RwLock::new(HashSet::new()),
            disk_type: DiskType::default(),
            ecx_actual_dir: dir_idx.to_string(),
            shard_locations: std::sync::RwLock::new(HashMap::new()),
            shard_locations_refresh_time: std::sync::Mutex::new(None),
            shard_locations_stale: std::sync::Mutex::new(false),
            expire_at_sec,
            encode_ts_ns,
            bitrot: None,
            bitrot_status: crate::storage::erasure_coding::ec_bitrot::BitrotStatus::Off,
            bitrot_source_dir: String::new(),
            io_error_count: std::sync::atomic::AtomicI32::new(0),
            io_error_quarantined: std::sync::atomic::AtomicBool::new(false),
            last_io_error: std::sync::Mutex::new(None),
        };

        // Open .ecx file (sorted index) in read/write mode for in-place deletion marking.
        // Matches Go which opens ecx for writing via MarkNeedleDeleted.
        let ecx_path = vol.ecx_file_name();
        if std::path::Path::new(&ecx_path).exists() {
            let file = open_volume_file(OpenOptions::new().read(true).write(true), &ecx_path)?;
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
                let file =
                    open_volume_file(OpenOptions::new().read(true).write(true), &fallback_ecx)?;
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
        let ecj_file = open_volume_file(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .append(true),
            &ecj_path,
        )?;
        vol.ecj_file_size = ecj_file.metadata()?.len() as i64;
        vol.ecj_file = Some(ecj_file);

        // Seed the in-memory deleted set from the journal.
        vol.load_deleted_needles_from_ecj()?;

        // Load the generation-0 EC bitrot checksum sidecar. Optional, except
        // when it contradicts the volume's own geometry — see
        // load_bitrot_for_generation.
        vol.load_active_bitrot_sidecar(&[])?;

        Ok(vol)
    }

    /// Re-resolve the checksum sidecar for a volume that is already mounted. A
    /// shard delivery can bring the manifest with it, and the receive path only
    /// writes the file, so without this the in-memory volume keeps the
    /// protection state it resolved at mount (off) until a remount.
    pub fn reload_bitrot_sidecar(&mut self, additional_dirs: &[String]) {
        if let Err(e) = self.load_active_bitrot_sidecar(additional_dirs) {
            tracing::warn!(
                volume_id = self.volume_id.0,
                error = %e,
                "reload bitrot sidecar",
            );
        }
    }

    /// Load the generation-0 checksum sidecar into `self.bitrot`/`self.bitrot_status`.
    /// OSS only produces generation-0 (fresh-encode) sidecars, mirroring Go's
    /// `loadActiveBitrotSidecar`.
    fn load_active_bitrot_sidecar(&mut self, additional_dirs: &[String]) -> io::Result<()> {
        self.load_bitrot_for_generation(0, additional_dirs)
    }

    /// Load and validate the sidecar describing `generation`, setting
    /// `self.bitrot`/`self.bitrot_status`. Absent or generation/config-mismatched
    /// => `Off` (protection off, not corruption); self-integrity or manifest
    /// failure => `Invalid` with a warning (protection off pending repair); usable
    /// => `On`. Mirrors Go's `loadBitrotForGeneration`.
    fn load_bitrot_for_generation(
        &mut self,
        generation: u32,
        additional_dirs: &[String],
    ) -> io::Result<()> {
        use crate::storage::erasure_coding::ec_bitrot;
        // Data base then index base, matching Go's findBitrotSidecar. A split
        // -dir/-dir.idx location keeps the sidecar with the INDEX, and on a
        // multi-disk server sharing one index directory that is the only copy
        // the per-disk runtimes other than the first can see — searching the
        // data base alone left them reporting no protection at all.
        let data_path = ec_bitrot::bitrot_sidecar_path(&self.base_name(), generation);
        let idx_path = ec_bitrot::bitrot_sidecar_path(&self.idx_base_name(), generation);
        // Sibling disks last: startup mirroring gives each shard-bearing disk
        // its own .ecx/.ecj/.vif but not the sidecar, and a delivery lands
        // exactly one copy, so a runtime restricted to its own two directories
        // would report no protection however often it reloaded.
        let path = std::iter::once(data_path.clone())
            .chain(std::iter::once(idx_path))
            .chain(additional_dirs.iter().filter(|d| !d.is_empty()).map(|d| {
                ec_bitrot::bitrot_sidecar_path(
                    &crate::storage::volume::volume_file_name(d, &self.collection, self.volume_id),
                    generation,
                )
            }))
            .find(|p| std::path::Path::new(p).exists())
            .unwrap_or(data_path);
        let loaded = ec_bitrot::load_bitrot_sidecar(&path);
        // A sidecar written for THIS generation that contradicts the volume's
        // geometry is not "no protection" — it says the layout the volume is
        // about to serve reads with is wrong. Fail the mount.
        if let Ok(prot) = &loaded {
            if prot.generation == generation
                && !ec_bitrot::geometry_matches(
                    prot,
                    self.data_shards as usize,
                    self.parity_shards as usize,
                    self.block_size,
                )
            {
                let cfg = prot.ec_shard_config.as_ref();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "ec volume {} generation {}: {} records layout {}+{} block {} but the volume is mounted as {}+{} block {}; refusing to serve one of the two layouts",
                        self.volume_id.0,
                        generation,
                        path,
                        cfg.map(|c| c.data_shards).unwrap_or(0),
                        cfg.map(|c| c.parity_shards).unwrap_or(0),
                        cfg.map(|c| c.block_size).unwrap_or(0),
                        self.data_shards,
                        self.parity_shards,
                        self.block_size,
                    ),
                ));
            }
        }
        let status = ec_bitrot::resolve_status(
            &loaded,
            generation,
            self.data_shards as usize,
            self.parity_shards as usize,
        );
        // Provenance is committed together with the sidecar it describes, so a
        // swallowed reload (`reload_bitrot_sidecar` logs and discards the `Err`
        // above) cannot leave them disagreeing: the geometry-mismatch return
        // happens before this point, so `bitrot`/`bitrot_status` below and
        // `bitrot_source_dir` here always describe the same load attempt.
        // Only a path that actually exists is a real source: `path` falls back
        // to the data path when nothing was found, and recording that would
        // claim a provenance the volume does not have.
        self.bitrot_source_dir = if std::path::Path::new(&path).exists() {
            std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default()
        } else {
            String::new()
        };
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
        Ok(())
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
    /// A third outcome sits ahead of both of the above: if the identity fence
    /// excluded a runtime AND the sidecar that supplied the protection was
    /// resolved from that excluded runtime's directory, the checksums cannot be
    /// trusted against
    /// the merged shards at all. That case returns `(0, [], [errors])` with an
    /// "unverifiable" note and never touches a shard handle — no scan, no
    /// wholesale-mismatch classification, no blamed shard.
    ///
    /// This method NEVER deletes or mutates anything — it is purely diagnostic.
    /// Duplicates the mounted shard handles so `run()` can scan with the store
    /// guard released; see `EcChecksumScrubPlan` for why that matters.
    pub fn checksum_scrub_plan(&self) -> EcChecksumScrubPlan {
        EcChecksumScrubPlan::for_volumes(&[self]).expect("a single runtime is never an empty slice")
    }

    /// Convenience wrapper preserving the original call shape. Callers that
    /// hold the store lock MUST use `checksum_scrub_plan()` + `run()` instead.
    pub fn checksum_scrub(&self) -> (u64, Vec<u32>, Vec<String>) {
        self.checksum_scrub_plan().run()
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
        // A 0-byte shard file beside an index with entries is residue of a
        // failed copy or a truncation, not a mountable shard: registering
        // it would advertise a size-0 claim that serves nothing and, since
        // placement pins re-copies to the owning disk, would keep
        // attracting repairs to a file that was never valid. A 0-byte shard
        // beside a 0-byte index is different — that is the legitimate
        // layout of a volume encoded with no live needles, and it must keep
        // mounting. The startup scan already skips 0-byte shard files; this
        // covers the mount path. Mirrors AddEcVolumeShard in
        // weed/storage/erasure_coding/ec_volume.go.
        if shard.file_size() == 0 && self.ecx_file_size > 0 {
            let path = shard.file_name();
            shard.close();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ec volume shard {} is empty (0 bytes) but the index has entries: residue of a failed copy, not a mountable shard",
                    path
                ),
            ));
        }
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

    // ---- I/O error tracking (mirrors Go's EcVolume IoErrorTracker) ----

    pub fn check_read_write_error(&self, err: Option<&io::Error>) {
        use std::sync::atomic::Ordering;
        if let Some(e) = err {
            if crate::storage::volume::is_storage_io_error(e) {
                self.io_error_count.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut guard) = self.last_io_error.lock() {
                    *guard = Some(e.to_string());
                }
                crate::metrics::STORAGE_IO_ERROR_COUNTER.inc();
                return;
            }
        }
        self.io_error_count.store(0, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_io_error.lock() {
            if guard.is_some() {
                *guard = None;
            }
        }
    }

    pub fn get_io_error_state(&self) -> (Option<String>, i32, bool) {
        use std::sync::atomic::Ordering;
        let err = self.last_io_error.lock().ok().and_then(|g| g.clone());
        let count = self.io_error_count.load(Ordering::Relaxed);
        let quarantined = self.io_error_quarantined.load(Ordering::Relaxed);
        (err, count, quarantined)
    }

    pub fn mark_io_quarantined(&self) {
        self.io_error_quarantined
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn reset_io_error_state(&self) {
        use std::sync::atomic::Ordering;
        self.io_error_count.store(0, Ordering::Relaxed);
        self.io_error_quarantined.store(false, Ordering::Relaxed);
        if let Ok(mut guard) = self.last_io_error.lock() {
            *guard = None;
        }
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
                if let Err(e) = ecx_file.read_exact_at(&mut entry_buf, file_offset) {
                    self.check_read_write_error(Some(&e));
                    return Err(e);
                }
            }
            #[cfg(not(unix))]
            {
                use std::io::{Read, Seek, SeekFrom};
                if let Err(e) = ecx_file.seek(SeekFrom::Start(file_offset)) {
                    self.check_read_write_error(Some(&e));
                    return Err(e);
                }
                if let Err(e) = ecx_file.read_exact(&mut entry_buf) {
                    self.check_read_write_error(Some(&e));
                    return Err(e);
                }
            }

            let (key, offset, size) = idx_entry_from_bytes(&entry_buf);
            if key == needle_id {
                // Apply runtime deletion state on top of the sealed .ecx
                // lookup: a needle in the in-memory deleted set is
                // reported with TOMBSTONE_FILE_SIZE even though the .ecx
                // record itself is untouched.
                if self.is_needle_deleted(needle_id) {
                    self.check_read_write_error(None);
                    return Ok(Some((offset, TOMBSTONE_FILE_SIZE)));
                }
                self.check_read_write_error(None);
                return Ok(Some((offset, size)));
            } else if key < needle_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        self.check_read_write_error(None);
        Ok(None)
    }

    /// Large-block length of this volume's shard layout (the uniform block
    /// size when set, the legacy 1GiB otherwise). Mirrors Go's
    /// ECContext.LargeBlockSize.
    pub fn large_block_size(&self) -> i64 {
        if self.block_size > 0 {
            self.block_size
        } else {
            ERASURE_CODING_LARGE_BLOCK_SIZE as i64
        }
    }

    /// Small-block length of this volume's shard layout.
    pub fn small_block_size(&self) -> i64 {
        if self.block_size > 0 {
            self.block_size
        } else {
            ERASURE_CODING_SMALL_BLOCK_SIZE as i64
        }
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
        ec_locate::locate_data(
            actual_offset,
            Size(actual as i32),
            shard_size,
            self.data_shards,
            self.large_block_size(),
            self.small_block_size(),
        )
    }

    /// Resolve an interval against this volume's shard block layout. Mirrors
    /// Go's EcVolume.IntervalToShardIdAndOffset.
    pub fn interval_to_shard_id_and_offset(
        &self,
        interval: &ec_locate::Interval,
    ) -> (ShardId, i64) {
        interval.to_shard_id_and_offset(
            self.data_shards,
            self.large_block_size(),
            self.small_block_size(),
        )
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
            let (shard_id, shard_offset) = self.interval_to_shard_id_and_offset(interval);
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
    /// Snapshot for `scrub_index`, so the index walk can run with the store
    /// lock released. Same rationale as `checksum_scrub_plan`.
    pub fn scrub_index_plan(&self) -> EcIndexScrubPlan {
        let ecx_path = self.ecx_file_name();
        // Opened under the guard, for the same reason the checksum plan
        // duplicates its shard handles. A fresh open rather than a clone of the
        // cached handle: `check_index_file` seeks, and `dup` would share the
        // cursor the shared handle is read from elsewhere.
        let ecx_handle = File::open(&ecx_path);
        EcIndexScrubPlan {
            volume_id: self.volume_id,
            has_ecx_file: self.ecx_file.is_some(),
            ecx_path,
            ecx_file_size: self.ecx_file_size,
            version: self.version,
            ecx_handle,
        }
    }

    /// Convenience wrapper preserving the original call shape. Callers holding
    /// the store lock MUST use `scrub_index_plan()` + `run()` instead.
    pub fn scrub_index(&self) -> (u64, Vec<String>) {
        self.scrub_index_plan().run()
    }

    /// Snapshot for `scrub_local`, so the needle walk can run with the store
    /// lock released. Same rationale as `checksum_scrub_plan`: this reads every
    /// local needle's bytes, which is GB-scale on a real volume.
    ///
    /// The shard vector keeps its SLOT structure — index is the shard id, gaps
    /// are the shards this node does not hold. `scrub_local` reads that
    /// distinction to decide whether a needle can be reassembled locally at
    /// all, so compacting it would silently change which needles get verified.
    pub fn scrub_local_plan(&self) -> EcLocalScrubPlan {
        EcLocalScrubPlan::for_volumes(&[self]).expect("a single runtime is never an empty slice")
    }

    /// ScrubLocal verifies each needle against the LOCAL shards only; it cannot
    /// CRC-check a needle whose intervals span shards held on other servers.
    /// Mirrors Go's EcVolume.ScrubLocal. Returns (rows walked, broken shards, errors).
    ///
    /// Convenience wrapper preserving the original call shape. Callers that
    /// hold the store lock MUST use `scrub_local_plan()` + `run()` instead.
    pub fn scrub_local(
        &self,
    ) -> (
        u64,
        Vec<crate::pb::volume_server_pb::EcShardInfo>,
        Vec<String>,
    ) {
        self.scrub_local_plan().run()
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
        let ecj_file = open_volume_file(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .append(true),
            &ecj_path,
        )?;
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
            ecj_file.write_all(&buf).and_then(|_| ecj_file.sync_all())
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
    fn find_needle_from_ecx_raw(&self, needle_id: NeedleId) -> io::Result<Option<(Offset, Size)>> {
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
        // ecx_actual_dir is always one of these two dirs.
        let _ =
            crate::storage::erasure_coding::ec_bitrot::remove_bitrot_sidecars(&self.base_name());
        if self.dir_idx != self.dir {
            let idx_base = crate::storage::volume::volume_file_name(
                &self.dir_idx,
                &self.collection,
                self.volume_id,
            );
            let _ = crate::storage::erasure_coding::ec_bitrot::remove_bitrot_sidecars(&idx_base);
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
            v.write_needle(&mut n, true, false).unwrap();
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            dir,
            dir,
            "",
            VolumeId(1),
            10,
            4,
        )
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

    /// REGRESSION: the scrub plans must be SELF-CONTAINED, so the handler can
    /// drop the store lock before the scan runs — see `EcChecksumScrubPlan`.
    ///
    /// The volume is DROPPED before the plans run, and the plans are moved to
    /// another thread. A plan that borrowed from `EcVolume` could do neither, so
    /// this stops COMPILING if the snapshot ever regresses to a borrow.
    #[test]
    fn test_scrub_plans_are_self_contained_and_match_direct_call() {
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            dir,
            dir,
            "",
            VolumeId(1),
            10,
            4,
        )
        .unwrap();

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        for id in 0..14u8 {
            vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), id))
                .unwrap();
        }

        // Baseline via the original call shape, with the volume still alive.
        let direct_checksum = vol.checksum_scrub();
        let direct_index = vol.scrub_index();
        let direct_local = vol.scrub_local();
        assert!(
            direct_checksum.0 > 0 && direct_local.0 > 0,
            "fixture scanned nothing, so the equalities below would be vacuous"
        );

        let checksum_plan = vol.checksum_scrub_plan();
        let index_plan = vol.scrub_index_plan();
        let local_plan = vol.scrub_local_plan();

        // The lock (and here the whole volume) is gone before the scan runs.
        drop(vol);

        let (from_plan_checksum, from_plan_index, from_plan_local) =
            std::thread::spawn(move || (checksum_plan.run(), index_plan.run(), local_plan.run()))
                .join()
                .expect("scrub plans must be runnable off the owning thread");

        assert_eq!(
            from_plan_checksum, direct_checksum,
            "checksum scrub result changed when run from a released-lock plan"
        );
        assert_eq!(
            from_plan_index, direct_index,
            "index scrub result changed when run from a released-lock plan"
        );
        assert_eq!(
            from_plan_local, direct_local,
            "local scrub result changed when run from a released-lock plan"
        );
    }

    /// REGRESSION: a malformed `.ecx` row must not abort the scrub TASK.
    ///
    /// `EcLocalScrubPlan::run()` skips only the -1 tombstone, matching Go's
    /// `ScrubLocal`, so any OTHER negative size reaches the reassembly buffer
    /// with a negative `get_actual_size()`. Go pays nothing for that (it
    /// appends to a nil slice); Rust sizes a per-needle `Vec` from it, and
    /// `Vec::with_capacity(negative as usize)` aborts the process. Now that the
    /// plan runs under `spawn_blocking`, that abort comes back as a JoinError
    /// and would take the whole node-wide scrub RPC down with every result
    /// already collected. The row must be REPORTED, as Go reports it.
    #[test]
    fn test_local_scrub_plan_reports_negative_size_ecx_row() {
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            dir,
            dir,
            "",
            VolumeId(1),
            10,
            4,
        )
        .unwrap();

        // Rewrite the first .ecx row's size as -1000: a negative that is NOT
        // the -1 tombstone the walk skips. A scrub is what you point at an
        // index you already suspect, so an arbitrary i32 in the size field is
        // in-scope input, whatever wrote it.
        let ecx = format!(
            "{}.ecx",
            crate::storage::volume::volume_file_name(dir, "", VolumeId(1))
        );
        let mut raw = std::fs::read(&ecx).unwrap();
        assert!(
            raw.len() >= NEEDLE_MAP_ENTRY_SIZE,
            "fixture must write at least one .ecx row"
        );
        let (key, offset, _) = idx_entry_from_bytes(&raw[..NEEDLE_MAP_ENTRY_SIZE]);
        idx_entry_to_bytes(&mut raw[..NEEDLE_MAP_ENTRY_SIZE], key, offset, Size(-1000));
        std::fs::write(&ecx, &raw).unwrap();
        assert!(
            get_actual_size(Size(-1000), Version::current()) < 0,
            "precondition: the row must drive get_actual_size negative"
        );

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        for id in 0..14u8 {
            vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), id))
                .unwrap();
        }
        let plan = vol.scrub_local_plan();
        drop(vol);

        // The join IS the assertion: a panic here is the JoinError that used to
        // fail the whole ScrubEcVolume RPC.
        let (_count, _broken, errs) = std::thread::spawn(move || plan.run())
            .join()
            .expect("a malformed .ecx row must not abort the scrub task");

        assert!(
            errs.iter()
                .any(|e| e.contains(&format!("bytes for needle {}", key.0))),
            "the malformed row must be reported, got {:?}",
            errs
        );
    }

    /// REGRESSION: a scrub running with the store lock RELEASED must not turn a
    /// concurrent, intentional removal into a corruption report — see
    /// `EcChecksumScrubPlan`.
    ///
    /// Deleting every file after the plans are built is the whole test: reads
    /// that resolve a path diverge from the direct call, reads through the
    /// captured descriptors are identical.
    #[test]
    fn test_scrub_plans_survive_files_removed_after_snapshot() {
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            dir,
            dir,
            "",
            VolumeId(1),
            10,
            4,
        )
        .unwrap();

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        for id in 0..14u8 {
            vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), id))
                .unwrap();
        }

        // Baseline with every file present and the volume still mounted.
        let direct_checksum = vol.checksum_scrub();
        let direct_index = vol.scrub_index();
        let direct_local = vol.scrub_local();
        assert!(
            direct_checksum.0 > 0 && direct_local.0 > 0,
            "fixture scanned nothing, so the equalities below would be vacuous"
        );
        assert!(
            direct_checksum.2.is_empty() && direct_index.1.is_empty() && direct_local.2.is_empty(),
            "fixture is not clean, so a false error could not be told apart: {:?} {:?} {:?}",
            direct_checksum.2,
            direct_index.1,
            direct_local.2
        );

        let checksum_plan = vol.checksum_scrub_plan();
        let index_plan = vol.scrub_index_plan();
        let local_plan = vol.scrub_local_plan();

        // The teardown a store writer would perform, after the plans exist.
        drop(vol);
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(1));
        for id in 0..14u8 {
            std::fs::remove_file(format!("{}.ec{:02}", base, id)).unwrap();
        }
        std::fs::remove_file(format!("{}.ecx", base)).unwrap();
        assert!(
            !std::path::Path::new(&format!("{}.ec00", base)).exists(),
            "the removal under test did not happen"
        );

        assert_eq!(
            checksum_plan.run(),
            direct_checksum,
            "shard files unlinked after the snapshot were reported as corruption"
        );
        assert_eq!(
            index_plan.run(),
            direct_index,
            "the .ecx unlinked after the snapshot was reported as an index error"
        );
        assert_eq!(
            local_plan.run(),
            direct_local,
            "files unlinked after the snapshot were reported as local-scrub errors"
        );
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(
            dir,
            dir,
            "",
            VolumeId(1),
            10,
            4,
        )
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

    /// A 0-byte shard file beside an index WITH entries is residue of a
    /// failed copy: `add_shard` must refuse it and leave the slot
    /// unregistered, so no size-0 claim is advertised (and no re-copy is
    /// attracted to a file that was never valid). Beside a 0-byte index it
    /// is the legitimate empty-volume layout and must keep mounting. The
    /// startup scan already skips such files; this covers the mount path.
    /// Mirrors TestLoadEcShardRefusesEmptyShardFile in Go.
    #[test]
    fn test_add_shard_refuses_empty_shard_file() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(
            dir,
            "",
            VolumeId(1),
            &[(NeedleId(1), Offset::from_actual_offset(8), Size(100))],
        );

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();

        let mut shard = EcVolumeShard::new(dir, "", VolumeId(1), 4);
        shard.create().unwrap();
        shard.close();

        let err = vol
            .add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 4))
            .expect_err("adding a 0-byte shard file must fail when the index has entries");
        assert!(
            err.to_string().contains("empty (0 bytes)"),
            "a 0-byte shard should be refused as empty, got: {}",
            err
        );
        assert_eq!(vol.shard_count(), 0, "a refused shard must not register");
        assert!(!vol.shard_bits().has_shard_id(4));
    }

    /// The legitimate empty-volume layout: a volume encoded with no live
    /// needles has a 0-byte .ecx AND 0-byte shards, and mounting it must
    /// keep working.
    #[test]
    fn test_add_shard_accepts_empty_shard_of_empty_volume() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(dir, "", VolumeId(1), &[]);

        let mut vol = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();

        let mut shard = EcVolumeShard::new(dir, "", VolumeId(1), 4);
        shard.create().unwrap();
        shard.close();

        vol.add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 4))
            .expect("a 0-byte shard of an empty volume (0-byte index) must mount");
        assert!(vol.shard_bits().has_shard_id(4));
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

    /// A vif that RECORDS a ratio no volume could have must fail the mount.
    /// Substituting the default 10+4 (and with it the legacy block layout)
    /// would read a uniform volume's shards at the wrong offsets and answer
    /// with the wrong bytes; only an entirely absent config means "this
    /// predates the record", which
    /// `test_ec_volume_absent_vif_config_uses_defaults` covers.
    #[test]
    fn test_ec_volume_invalid_vif_config_fails_the_mount() {
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

        // EcVolume has no Debug impl, so match rather than expect_err.
        match EcVolume::new(dir, dir, "pics", VolumeId(1)) {
            Ok(_) => panic!("an impossible ratio must fail the mount"),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData, "got {e}"),
        }
    }

    /// The compatibility case the check above must not swallow: a vif with no
    /// EC config at all predates the record, and mounts on the defaults.
    #[test]
    fn test_ec_volume_absent_vif_config_uses_defaults() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        write_ecx_file(dir, "pics", VolumeId(1), &[]);
        let base = crate::storage::volume::volume_file_name(dir, "pics", VolumeId(1));
        std::fs::write(format!("{}.vif", base), r#"{"version":3}"#).unwrap();

        let vol = EcVolume::new(dir, dir, "pics", VolumeId(1)).unwrap();
        assert_eq!(vol.data_shards, DATA_SHARDS_COUNT as u32);
        assert_eq!(vol.parity_shards, PARITY_SHARDS_COUNT as u32);
        assert_eq!(vol.block_size, 0, "legacy layout for a pre-record volume");
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

    /// Build one real encoded EC volume in `dir`, then hand back N runtimes over
    /// it, each holding the shard ids it was given. Models the reconciled
    /// split-disk mount without needing N directories.
    fn split_runtimes(dir: &str, vid: VolumeId, subsets: &[&[u8]]) -> Vec<EcVolume> {
        use crate::storage::needle_map::NeedleMapKind;
        use crate::storage::volume::Volume;

        let mut v = Volume::new(
            dir,
            dir,
            "",
            vid,
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
            v.write_needle(&mut n, true, false).unwrap();
        }
        v.sync_to_disk().unwrap();
        v.close();
        crate::storage::erasure_coding::ec_encoder::write_ec_files(dir, dir, "", vid, 10, 4)
            .unwrap();

        subsets
            .iter()
            .map(|ids| {
                let mut ev = EcVolume::new(dir, dir, "", vid).unwrap();
                for &id in ids.iter() {
                    ev.add_shard(EcVolumeShard::new(dir, "", vid, id)).unwrap();
                }
                ev
            })
            .collect()
    }

    /// The union must reach every disk's shards, and first-disk-wins must
    /// resolve a shard mounted on two disks — the same rule
    /// `collect_ec_shard_dirs` and Go's `CollectEcShards` already use.
    #[test]
    fn test_merge_ec_runtimes_unions_slots_first_disk_wins() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        // Disk 0: 0..=6 plus 9 (9 is also on disk 1 — a duplicate mount).
        // Disk 1: 7..=13.
        let runtimes = split_runtimes(
            dir,
            VolumeId(1),
            &[&[0, 1, 2, 3, 4, 5, 6, 9], &[7, 8, 9, 10, 11, 12, 13]],
        );
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let merged = merge_ec_runtimes(&refs).expect("non-empty input merges");
        assert_eq!(merged.merged.len(), 2, "both runtimes share generation 0");
        assert!(
            merged.skipped.is_empty(),
            "nothing to skip: {:?}",
            merged.skipped
        );

        // Every shard 0..=13 is reachable from the merged slots.
        for id in 0..14usize {
            assert!(
                merged.slots[id].is_some(),
                "shard {} missing from the union",
                id
            );
        }
        // Shard 9 is held by both; the first disk wins.
        let (owner, _) = merged.slots[9].unwrap();
        assert!(
            std::ptr::eq(owner, refs[0]),
            "duplicate shard must resolve to the first disk"
        );
        // Slots are shard-id indexed across the volume's whole shard space,
        // never compacted: index N is shard N or nothing.
        assert_eq!(
            merged.slots.len(),
            14,
            "slots must span the full 10+4 shard space"
        );
    }

    /// Leniency is keyed on the ANCHOR, never the holder: a known identity must
    /// not accept an unstamped holder (store_ec.rs:667-673,
    /// grpc_server.rs:3743-3748). Merging leftover legacy shards beside a
    /// current encode is what produces false corruption reports.
    #[test]
    fn test_merge_ec_runtimes_excludes_incompatible_identities() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut runtimes = split_runtimes(
            dir,
            VolumeId(1),
            &[&[0, 1, 2, 3, 4, 5, 6], &[7, 8, 9], &[10, 11, 12, 13]],
        );
        runtimes[0].encode_ts_ns = 500; // stale, but stamped
        runtimes[1].encode_ts_ns = 0; // legacy, unstamped
        runtimes[2].encode_ts_ns = 900; // the live encode run
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let merged = merge_ec_runtimes(&refs).unwrap();

        // Anchor is the newest known identity; only exact matches merge.
        assert_eq!(merged.anchor.encode_ts_ns, 900);
        assert_eq!(merged.merged.len(), 1);
        assert!(
            merged.slots[10].is_some(),
            "the anchor's own shards must merge"
        );
        assert!(
            merged.slots[0].is_none(),
            "an older known generation must not merge"
        );
        assert!(
            merged.slots[7].is_none(),
            "an unstamped holder must not merge"
        );

        // Exclusions are REPORTED, never silent — that is what keeps this from
        // regressing to the silent-clean bug this whole change fixes.
        assert_eq!(merged.skipped.len(), 2, "got {:?}", merged.skipped);
        assert!(merged.skipped.iter().all(|s| s.contains("not verified")));
    }

    /// When no runtime carries an identity there is nothing to fence on, so
    /// behavior stays exactly as it is today: everything merges.
    #[test]
    fn test_merge_ec_runtimes_is_lenient_when_no_identity_is_known() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let runtimes = split_runtimes(dir, VolumeId(1), &[&[0, 1, 2], &[3, 4, 5]]);
        assert!(runtimes.iter().all(|r| r.encode_ts_ns == 0));
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let merged = merge_ec_runtimes(&refs).unwrap();
        assert_eq!(merged.merged.len(), 2);
        assert!(merged.skipped.is_empty());
        assert!(merged.slots[3].is_some());
    }

    /// The anchor supplies the volume-level metadata, so prefer a shard-bearing
    /// runtime at the anchor generation over an empty one.
    #[test]
    fn test_merge_ec_runtimes_anchor_prefers_a_shard_bearing_runtime() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let runtimes = split_runtimes(dir, VolumeId(1), &[&[], &[0, 1, 2]]);
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let merged = merge_ec_runtimes(&refs).unwrap();
        assert!(
            std::ptr::eq(merged.anchor, refs[1]),
            "anchor must hold shards"
        );
        assert!(
            merged.skipped.is_empty(),
            "same generation: nothing is excluded"
        );
    }

    /// Empty input is the vanished-volume case and the ONLY None.
    #[test]
    fn test_merge_ec_runtimes_none_only_when_empty() {
        assert!(merge_ec_runtimes(&[]).is_none());
    }

    /// The vanished-volume case: callers' `let ... else` guards depend on this
    /// being the ONLY None.
    #[test]
    fn test_for_volumes_is_none_only_for_an_empty_slice() {
        assert!(EcChecksumScrubPlan::for_volumes(&[]).is_none());
        assert!(EcLocalScrubPlan::for_volumes(&[]).is_none());
    }

    /// The whole point: corruption on a shard that only a sibling runtime holds
    /// must be reported. Before this change the scrub saw disk 0 alone and
    /// returned clean.
    #[test]
    fn test_checksum_scrub_for_volumes_catches_sibling_disk_corruption() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        // Shard 11 is held ONLY by the second runtime.
        let runtimes = split_runtimes(
            dir,
            VolumeId(1),
            &[&[0, 1, 2, 3, 4, 5, 6], &[7, 8, 9, 10, 11, 12, 13]],
        );
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        // Clean to start.
        let (scanned, broken, errs) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
        assert!(broken.is_empty(), "unexpected mismatches: {:?}", broken);
        assert!(scanned > 0, "merged plan scanned nothing");

        // Corrupt shard 11 — reachable only through the second runtime.
        let shard11 = format!("{}/1.ec11", dir);
        let mut bytes = std::fs::read(&shard11).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&shard11, &bytes).unwrap();

        let (_, broken2, _) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(
            broken2.contains(&11),
            "sibling-disk corruption must be flagged, got {:?}",
            broken2
        );

        // The single-runtime path is exactly the old blind spot: disk 0 alone
        // still reports clean, which is why aggregation was needed.
        let (_, broken_disk0, _) = refs[0].checksum_scrub();
        assert!(!broken_disk0.contains(&11));
    }

    /// An excluded runtime is reported through the plan's errors, but ONLY on
    /// the On path — the Off arm's clean-empty contract is Go parity
    /// (`case BitrotOff: return 0, nil, nil`) and must not grow errors.
    #[test]
    fn test_checksum_scrub_skips_are_reported_but_never_break_the_off_arm() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut runtimes = split_runtimes(dir, VolumeId(1), &[&[0, 1, 2], &[3, 4, 5]]);
        runtimes[0].encode_ts_ns = 100; // excluded: older known identity
        runtimes[1].encode_ts_ns = 200; // anchor
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        // With protection ON, the skip surfaces as an error line, and the
        // surviving (anchor) runtime is still actually scanned.
        let (scanned, _, errs) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(
            scanned > 0,
            "excluding one runtime must not stop the rest from being scanned"
        );
        assert!(
            errs.iter().any(|e| e.contains("not verified")),
            "excluded runtime must be reported, got {:?}",
            errs
        );

        // With protection OFF, the arm stays byte-for-byte clean.
        let mut off = runtimes;
        for r in off.iter_mut() {
            r.bitrot_status = crate::storage::erasure_coding::ec_bitrot::BitrotStatus::Off;
            r.bitrot = None;
        }
        let off_refs: Vec<&EcVolume> = off.iter().collect();
        assert_eq!(
            EcChecksumScrubPlan::for_volumes(&off_refs).unwrap().run(),
            (0, Vec::new(), Vec::new()),
            "BitrotOff must stay clean-empty for Go parity"
        );
    }

    /// The Invalid arm returns a non-empty error vector of its own, so the
    /// Go-parity contract that silences the Off arm does not reach it. A volume
    /// with BOTH a malformed sidecar and a fenced-out disk must report BOTH:
    /// reporting only the sidecar hides an unscanned disk behind an unrelated
    /// integrity error, which is the failure mode this whole change exists to
    /// close.
    #[test]
    fn test_checksum_scrub_reports_skips_on_a_malformed_sidecar() {
        use crate::storage::erasure_coding::ec_bitrot::BitrotStatus;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut runtimes = split_runtimes(dir, VolumeId(1), &[&[0, 1, 2], &[3, 4, 5]]);
        runtimes[0].encode_ts_ns = 100; // excluded: older known identity
        runtimes[1].encode_ts_ns = 200; // anchor
        for r in runtimes.iter_mut() {
            r.bitrot_status = BitrotStatus::Invalid;
        }
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let (scanned, broken, errs) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert_eq!(
            scanned, 0,
            "a malformed sidecar must not be scanned against"
        );
        assert!(broken.is_empty(), "must never blame shards: {:?}", broken);
        assert_eq!(
            errs.len(),
            2,
            "expected the sidecar error AND the skip: {:?}",
            errs
        );
        assert!(
            errs[0].contains("malformed/unverifiable"),
            "the sidecar error stays first: {:?}",
            errs
        );
        assert!(
            errs[1].contains("belong to encode run 100") && errs[1].contains("not verified"),
            "the excluded disk must still be named: {:?}",
            errs
        );
    }

    /// When the anchor's sidecar came from a directory belonging to a runtime
    /// the fence excluded, the checksums cannot be trusted against the anchor's
    /// shards. Report that, never "shard N is corrupt".
    #[test]
    fn test_checksum_scrub_reports_unverifiable_sidecar_from_excluded_runtime() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut runtimes = split_runtimes(dir, VolumeId(1), &[&[0, 1, 2], &[3, 4, 5]]);

        // Mount actually resolved the real sidecar from `dir` (both subsets
        // share one physical directory in this fixture), confirming
        // `load_bitrot_for_generation` populates the field before the test
        // overwrites it below to simulate a cross-disk borrow.
        assert_eq!(
            runtimes[1].bitrot_source_dir, dir,
            "mount must record the dir the sidecar was actually resolved from"
        );

        // Runtime 0 is an older encode run, and the anchor resolved its sidecar
        // from runtime 0's directory.
        runtimes[0].encode_ts_ns = 100;
        runtimes[0].dir = "/disk-old".to_string();
        runtimes[0].dir_idx = "/disk-old".to_string();
        runtimes[1].encode_ts_ns = 200;
        runtimes[1].dir = "/disk-new".to_string();
        runtimes[1].dir_idx = "/disk-new".to_string();
        runtimes[1].bitrot_source_dir = "/disk-old".to_string();
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let (scanned, broken, errs) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert_eq!(
            scanned, 0,
            "an unverifiable sidecar must not be scanned against"
        );
        assert!(broken.is_empty(), "must never blame shards: {:?}", broken);
        assert!(
            errs.iter().any(|e| e.contains("unverifiable")),
            "expected an unverifiable-protection note, got {:?}",
            errs
        );
        assert!(
            errs.iter().any(|e| e.contains("were not verified")),
            "the fenced-out runtime must still be reported alongside the unverifiable note: {:?}",
            errs
        );
    }

    /// The provenance rule fires only when a runtime was actually excluded. A
    /// healthy single-encode volume whose sidecar lives on a sibling disk is the
    /// normal mirrored case and must scrub normally.
    #[test]
    fn test_checksum_scrub_provenance_rule_does_not_fire_without_exclusions() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut runtimes = split_runtimes(
            dir,
            VolumeId(1),
            &[&[0, 1, 2, 3, 4, 5, 6], &[7, 8, 9, 10, 11, 12, 13]],
        );
        // Same generation: nothing is excluded, so provenance is irrelevant even
        // though the anchor's sidecar appears to have come from somewhere
        // neither runtime owns. (The anchor is runtimes[0]: `merge_ec_runtimes`
        // picks the first shard-bearing runtime at the anchor generation.)
        runtimes[0].bitrot_source_dir = "/somewhere-else".to_string();
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let (scanned, broken, errs) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(scanned > 0, "a normal volume must still be scanned");
        assert!(broken.is_empty(), "{:?}", broken);
        assert!(
            !errs.iter().any(|e| e.contains("unverifiable")),
            "{:?}",
            errs
        );
    }

    /// The identity fence keys on `encode_ts_ns` ALONE, never on geometry, so
    /// two same-generation runtimes whose `.vif`s disagree about the layout do
    /// merge -- and `slots` is then sized to the WIDER of them while the
    /// volume's actual shard-id range is the ANCHOR's `data + parity`.
    ///
    /// The two consumers disagreed about exactly that range: the mode 2|5 arm
    /// builds `dirs` over `0..anchor.data_shards + anchor.parity_shards` and
    /// silently drops the surplus slots, while `EcChecksumScrubPlan` iterated
    /// the full width and reported each one "present but missing from sidecar
    /// manifest". Nothing in this volume describes those ids -- the sidecar
    /// manifest and the Reed-Solomon matrix are both the anchor's -- so that
    /// message is the width disagreement talking, not a finding.
    #[test]
    fn test_checksum_scrub_truncates_slots_to_the_anchors_geometry() {
        use crate::pb::volume_server_pb::{
            ChecksumAlgorithm, EcBitrotProtection, EcShardChecksums,
        };
        use crate::storage::erasure_coding::ec_bitrot;
        use crate::storage::volume::{VifEcShardConfig, VifVolumeInfo};

        let tmp = TempDir::new().unwrap();
        let vid = VolumeId(1);

        // One dir per geometry: `EcVolume::new` reads `data_shards`/
        // `parity_shards` from the `.vif` beside the volume, so two runtimes can
        // only disagree if they mount from different directories.
        let seed = |name: &str, ds: u32, ps: u32, shard_id: u8| -> String {
            let dir = tmp.path().join(name);
            std::fs::create_dir_all(&dir).unwrap();
            let d = dir.to_str().unwrap().to_string();
            let base = crate::storage::volume::volume_file_name(&d, "", vid);
            std::fs::write(format!("{}.ecx", base), b"").unwrap();
            std::fs::write(format!("{}.ecj", base), b"").unwrap();
            std::fs::write(
                format!("{}.vif", base),
                serde_json::to_string(&VifVolumeInfo {
                    version: 3,
                    ec_shard_config: Some(VifEcShardConfig {
                        data_shards: ds,
                        parity_shards: ps,
                        encode_ts_ns: 500,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .unwrap(),
            )
            .unwrap();
            std::fs::write(
                format!("{}.ec{:02}", base, shard_id),
                b"shard data nonempty",
            )
            .unwrap();
            d
        };

        // 10+4, and the only disk with a sidecar -- so it supplies `prot`, and
        // its manifest covers shard ids 0..=13 and nothing else.
        let narrow = seed("narrow", 10, 4, 0);
        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: ec_bitrot::DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_bitrot::ec_shard_config(10, 4, 0)),
            shards: (0..14u32)
                .map(|shard_id| EcShardChecksums {
                    shard_id,
                    covered_size: 4,
                    block_crc32c: vec![0u8; 4],
                })
                .collect(),
            encode_uuid: vec![0u8; 16],
        };
        ec_bitrot::save_bitrot_sidecar(
            &ec_bitrot::bitrot_sidecar_path(
                &crate::storage::volume::volume_file_name(&narrow, "", vid),
                0,
            ),
            &prot,
        )
        .unwrap();

        // 12+4: 16 slots, and it holds shard 14 -- an id the anchor's layout
        // does not contain at all. Same encode_ts_ns, but the geometry fence
        // now excludes it rather than merging incompatible shards.
        let wide = seed("wide", 12, 4, 14);

        let mut narrow_v = EcVolume::new(&narrow, &narrow, "", vid).unwrap();
        narrow_v
            .add_shard(EcVolumeShard::new(&narrow, "", vid, 0))
            .unwrap();
        let mut wide_v = EcVolume::new(&wide, &wide, "", vid).unwrap();
        wide_v
            .add_shard(EcVolumeShard::new(&wide, "", vid, 14))
            .unwrap();

        assert_eq!(narrow_v.shards.len(), 14);
        assert_eq!(wide_v.shards.len(), 16);
        assert_eq!(
            narrow_v.encode_ts_ns, wide_v.encode_ts_ns,
            "the two runtimes must share a generation"
        );
        assert_eq!(
            narrow_v.bitrot_status,
            crate::storage::erasure_coding::ec_bitrot::BitrotStatus::On,
            "the narrow disk must carry usable protection"
        );

        let refs: Vec<&EcVolume> = vec![&narrow_v, &wide_v];
        let merged = merge_ec_runtimes(&refs).unwrap();
        assert!(
            std::ptr::eq(merged.anchor, refs[0]),
            "the NARROW runtime must anchor"
        );
        assert!(
            !merged.skipped.is_empty(),
            "the wide runtime must be excluded by the geometry fence: {:?}",
            merged.skipped
        );
        assert_eq!(
            merged.slots.len(),
            14,
            "slots are sized to the anchor's geometry, not the excluded wide runtime"
        );
        assert!(
            merged.slots.get(14).is_none() || merged.slots.get(14).unwrap().is_none(),
            "shard 14 from the excluded runtime must not appear in slots"
        );

        let (scanned, broken, errs) = EcChecksumScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(
            !errs.iter().any(|e| e.contains("shard 14")),
            "shard 14 belongs to an excluded runtime and must not be scanned: {:?}",
            errs
        );
        assert!(
            scanned > 0,
            "the anchor's shard 0 must still be scanned: {:?}",
            errs
        );
        assert_eq!(
            broken,
            vec![0],
            "shard 0 is inside the anchor's geometry and its bytes do not match \
             the manifest, so it must still be reported: errs={:?}",
            errs
        );
    }

    /// `for_volumes` must reach every runtime's shards, not just the first's.
    /// A needle walk alone can't prove that with this fixture: ~500 bytes of
    /// data under a legacy 1GiB large block puts every needle interval on
    /// shard 0, so the slot vector is asserted directly. The walk-level checks
    /// below additionally show the merged plan verifies cleanly and then
    /// detects corruption reachable through the first runtime, while a
    /// single-runtime plan stays blind to it.
    #[test]
    fn test_scrub_local_for_volumes_merges_slots_across_runtimes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let runtimes = split_runtimes(dir, VolumeId(1), &[&[0, 1, 2], &[7, 8, 9]]);
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let (count, broken, errs) = EcLocalScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(count > 0, "the merged plan walked no needles");
        assert!(
            broken.is_empty(),
            "clean volume reported broken shards: {:?}",
            broken
        );
        assert!(errs.is_empty(), "clean volume reported errors: {:?}", errs);

        // The property this task exists to deliver, asserted directly on the
        // plan rather than through a needle walk. With ~500 bytes of fixture
        // data and a legacy 1GiB large block every needle interval lands in
        // shard 0, so no needle walk can ever reach a sibling runtime's
        // shards -- but the slot vector can. Neither runtime holds every
        // shard here, so the gap at shard 3 makes compaction load-bearing:
        // a compacting implementation could still satisfy a "populated slots
        // are reachable" check while silently reindexing the vector.
        let merged_plan = EcLocalScrubPlan::for_volumes(&refs).unwrap();
        assert_eq!(
            merged_plan.shards.len(),
            14,
            "slots must span the full 10+4 shard space"
        );
        assert!(
            merged_plan.shards[0].is_some(),
            "disk 0's shard 0 must be reachable"
        );
        assert!(
            merged_plan.shards[3].is_none(),
            "a shard no runtime holds must stay an empty slot -- compaction would fill it"
        );
        assert!(
            merged_plan.shards[7].is_some(),
            "disk 1's shard 7 must be reachable -- the bug being fixed"
        );

        // A single-runtime plan still sees only its own disk, which is exactly
        // what made aggregation necessary.
        let solo = EcLocalScrubPlan::for_volumes(&[refs[0]]).unwrap();
        assert!(solo.shards[0].is_some());
        assert!(
            solo.shards[7].is_none(),
            "refs[0] alone must not see the sibling's shard 7"
        );

        // This fixture's few hundred bytes of needle data all land inside
        // shard 0's legacy 1GiB large block (the tiny volume never fills even
        // one), so shard 0 alone — held only by the first runtime — carries
        // every needle. Corrupting a needle body byte there is the LOCAL
        // analogue of Task 3's sibling-disk corruption: the whole point is
        // that a partial-shard `scrub_local` cannot see it at all (it just
        // silently skips a needle it can't reassemble locally — that is not
        // an error), while the merged plan reaches it through refs[0].
        let shard0 = format!("{}/1.ec00", dir);
        let mut bytes = std::fs::read(&shard0).unwrap();
        assert!(
            bytes.len() > 268,
            "fixture shrank below the corruption offset; split_runtimes's \
             needle payloads changed, so this offset needs picking again"
        );
        bytes[268] ^= 0xFF; // inside needle 4's data payload
        std::fs::write(&shard0, &bytes).unwrap();

        let (_, _, merged_errs) = EcLocalScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(
            !merged_errs.is_empty(),
            "merged plan must detect corruption on shard 0, reachable through refs[0]"
        );

        // refs[1] never mounted shard 0, so it cannot see this corruption at
        // all — that silent blind spot is exactly the bug aggregation fixes.
        let (_, _, refs1_errs) = refs[1].scrub_local();
        assert!(
            refs1_errs.is_empty(),
            "runtime without shard 0 has no way to detect this corruption: {:?}",
            refs1_errs
        );
    }

    /// LOCAL's legacy `shard_size` fallback (`dat_file_size == 0`) is now a
    /// NODE-WIDE input: it feeds `locate_data`'s offset math for every merged
    /// sibling's shards, not just the anchor's. `anchor.shard_file_size()`
    /// returns the anchor's FIRST held shard rather than a maximum, so one
    /// truncated shard on the anchor disk would mis-offset every needle read
    /// across every disk and manufacture corruption reports wholesale. Take the
    /// max over the merged slots, the way `verify_ec_shards` already answers the
    /// same question (`if size > shard_size { shard_size = size }`).
    #[test]
    fn test_scrub_local_shard_size_fallback_takes_the_max_across_disks() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();

        // Mount two shardless runtimes first so the shard files can be doctored
        // before `add_shard` caches their sizes.
        let mut runtimes = split_runtimes(dir, VolumeId(1), &[&[], &[]]);

        // Disk 0's shard 0 is truncated -- a partially-copied shard, which is
        // exactly the containment this fallback used to have and now must not
        // lose.
        let shard0_path = format!("{}/1.ec00", dir);
        let full = std::fs::metadata(&shard0_path).unwrap().len();
        assert!(full > 1, "fixture shard is too small to truncate");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&shard0_path)
            .unwrap()
            .set_len(full / 2)
            .unwrap();

        runtimes[0]
            .add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 0))
            .unwrap();
        runtimes[1]
            .add_shard(EcVolumeShard::new(dir, "", VolumeId(1), 1))
            .unwrap();
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let intact = std::fs::metadata(format!("{}/1.ec01", dir)).unwrap().len() as i64;
        let truncated = (full / 2) as i64;
        assert!(
            truncated < intact,
            "fixture must leave disk 0's shard SHORTER than disk 1's"
        );

        let merged = merge_ec_runtimes(&refs).unwrap();
        assert!(
            std::ptr::eq(merged.anchor, refs[0]),
            "the truncated disk must anchor, or the old sizing was already right"
        );
        assert_eq!(
            refs[0].dat_file_size, 0,
            "this fixture writes no .vif, so the legacy fallback is the path \
             under test; with a dat_file_size the branch is never reached"
        );
        assert_eq!(
            refs[0].shard_file_size(),
            truncated,
            "the old source -- the anchor's first held shard -- is the \
             truncated one, which is what makes this observable"
        );

        let plan = EcLocalScrubPlan::for_volumes(&refs).unwrap();
        assert_eq!(
            plan.shard_size,
            intact - 1,
            "one disk's truncated shard must not size every merged sibling's \
             shards; locate_data would then mis-offset every needle on every \
             disk and report corruption that is not there"
        );
    }

    /// LOCAL has no bitrot status gate, so an excluded runtime is always
    /// reported.
    #[test]
    fn test_scrub_local_reports_skipped_runtimes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let mut runtimes = split_runtimes(dir, VolumeId(1), &[&[0, 1, 2], &[3, 4, 5]]);
        runtimes[0].encode_ts_ns = 100;
        runtimes[1].encode_ts_ns = 200;
        let refs: Vec<&EcVolume> = runtimes.iter().collect();

        let (_, _, errs) = EcLocalScrubPlan::for_volumes(&refs).unwrap().run();
        assert!(
            errs.iter().any(|e| e.contains("not verified")),
            "excluded runtime must be reported, got {:?}",
            errs
        );
    }
}

#[cfg(test)]
mod uniform_layout_tests {
    use super::*;
    use crate::storage::needle_map::NeedleMapKind;
    use crate::storage::volume::{VifEcShardConfig, VifVolumeInfo, Volume};
    use tempfile::TempDir;

    // Write ~26MB of needles so the uniform block size (3MB) diverges from the
    // legacy layout, encode, and verify EcVolume reads every needle back
    // through the .vif-recorded geometry. A legacy-encoded fixture (no .vif
    // block size) must keep reading through the legacy interpretation.
    #[test]
    fn test_read_needles_uniform_and_legacy_layouts() {
        for legacy in [false, true] {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path().to_str().unwrap();
            let vid = VolumeId(8);

            let mut v = Volume::new(
                dir,
                dir,
                "",
                vid,
                NeedleMapKind::InMemory,
                None,
                None,
                0,
                Version::current(),
            )
            .unwrap();
            let mut expected: Vec<(NeedleId, Vec<u8>)> = Vec::new();
            for i in 1u64..=11 {
                let size = if i <= 5 { 4 << 20 } else { 1 << 20 };
                let data: Vec<u8> = (0..size)
                    .map(|b| ((b as u64).wrapping_mul(2654435761).wrapping_add(i) >> 8) as u8)
                    .collect();
                let mut n = Needle {
                    id: NeedleId(i),
                    cookie: Cookie(i as u32),
                    data: data.clone(),
                    data_size: data.len() as u32,
                    ..Needle::default()
                };
                v.write_needle(&mut n, true, false).unwrap();
                expected.push((NeedleId(i), data));
            }
            v.sync_to_disk().unwrap();
            let dat_size = v.dat_file_size().unwrap() as i64;
            v.close();

            let block_size = if legacy {
                // Legacy fixture: two-tier encode plus a .vif without a block
                // size, the state every pre-upgrade EC volume is in.
                use crate::storage::erasure_coding::ec_bitrot::{
                    ShardChecksumBuilder, DEFAULT_BITROT_BLOCK_SIZE,
                };
                use reed_solomon_erasure::galois_8::ReedSolomon;
                let base = crate::storage::volume::volume_file_name(dir, "", vid);
                crate::storage::erasure_coding::ec_encoder::write_sorted_ecx_from_idx(
                    &format!("{}.idx", base),
                    &format!("{}.ecx", base),
                )
                .unwrap();
                let dat_file = std::fs::File::open(format!("{}.dat", base)).unwrap();
                let rs = ReedSolomon::new(10, 4).unwrap();
                let mut shards: Vec<EcVolumeShard> = (0..14u8)
                    .map(|i| EcVolumeShard::new(dir, "", vid, i))
                    .collect();
                for shard in &mut shards {
                    shard.create().unwrap();
                }
                let mut builders: Vec<ShardChecksumBuilder> = (0..14)
                    .map(|_| ShardChecksumBuilder::new(DEFAULT_BITROT_BLOCK_SIZE as i64))
                    .collect();
                crate::storage::erasure_coding::ec_encoder::encode_dat_file(
                    &dat_file,
                    dat_size,
                    &rs,
                    &mut shards,
                    &mut builders,
                    10,
                    4,
                    256 * 1024,
                    ERASURE_CODING_LARGE_BLOCK_SIZE,
                    ERASURE_CODING_SMALL_BLOCK_SIZE,
                )
                .unwrap();
                for shard in &mut shards {
                    shard.close();
                }
                0
            } else {
                let bs = crate::storage::erasure_coding::ec_encoder::write_ec_files(
                    dir, dir, "", vid, 10, 4,
                )
                .unwrap();
                assert!(
                    bs > ERASURE_CODING_SMALL_BLOCK_SIZE as i64,
                    "block size {} does not diverge from the legacy layout",
                    bs
                );
                bs
            };

            let base = crate::storage::volume::volume_file_name(dir, "", vid);
            let vif = VifVolumeInfo {
                version: Version::current().0 as u32,
                dat_file_size: dat_size,
                ec_shard_config: Some(VifEcShardConfig {
                    data_shards: 10,
                    parity_shards: 4,
                    block_size,
                    ..Default::default()
                }),
                ..Default::default()
            };
            std::fs::write(
                format!("{}.vif", base),
                serde_json::to_string_pretty(&vif).unwrap(),
            )
            .unwrap();
            std::fs::remove_file(format!("{}.dat", base)).unwrap();
            std::fs::remove_file(format!("{}.idx", base)).unwrap();

            let mut vol = EcVolume::new(dir, dir, "", vid).unwrap();
            assert_eq!(
                vol.block_size, block_size,
                "block size not loaded from .vif"
            );
            for i in 0..10u8 {
                vol.add_shard(EcVolumeShard::new(dir, "", vid, i)).unwrap();
            }

            for (id, data) in &expected {
                let n = vol
                    .read_ec_shard_needle(*id)
                    .unwrap()
                    .unwrap_or_else(|| panic!("needle {} not found (legacy={})", id.0, legacy));
                assert_eq!(
                    n.data, *data,
                    "needle {} data mismatch (legacy={})",
                    id.0, legacy
                );
            }
        }
    }

    // A present-but-malformed .vif must FAIL the mount: every new encode
    // records a positive uniform block size there, and silently defaulting
    // to the legacy layout would serve those shards with the wrong offset
    // math. Absence stays legal — legacy volumes predate the sidecar.
    #[test]
    fn new_fails_on_malformed_vif() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(1));
        std::fs::write(format!("{}.ecx", base), b"").unwrap();
        std::fs::write(format!("{}.vif", base), b"not json").unwrap();
        let res = EcVolume::new(dir, dir, "", VolumeId(1));
        assert!(res.is_err(), "mount over a malformed .vif must fail");
    }

    #[test]
    fn new_absent_vif_mounts_with_defaults() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_str().unwrap();
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(1));
        std::fs::write(format!("{}.ecx", base), b"").unwrap();
        let ev = EcVolume::new(dir, dir, "", VolumeId(1)).unwrap();
        assert_eq!(ev.block_size, 0, "legacy mount must use the legacy layout");
    }

    /// A rebuild lands on one disk, but the volume's metadata may live on
    /// another. Reading only the selected directory returns the default 10+4
    /// and the legacy block layout, which reconstructs a custom-ratio or
    /// uniform volume through the wrong matrix.
    fn seed_uniform_sidecar(base: &str, ds: u32, ps: u32, block: i64) {
        use crate::pb::volume_server_pb::{
            ChecksumAlgorithm, EcBitrotProtection, EcShardChecksums,
        };
        use crate::storage::erasure_coding::ec_bitrot;
        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: ec_bitrot::DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_bitrot::ec_shard_config(ds, ps, block)),
            shards: (0..(ds + ps))
                .map(|shard_id| EcShardChecksums {
                    shard_id,
                    covered_size: 4,
                    block_crc32c: vec![0u8; 4],
                })
                .collect(),
            encode_uuid: vec![0u8; 16],
        };
        ec_bitrot::save_bitrot_sidecar(&ec_bitrot::bitrot_sidecar_path(base, 0), &prot).unwrap();
    }

    fn seed_config_free_vif(base: &str) {
        std::fs::write(format!("{}.vif", base), r#"{"version":3,"datFileSize":0}"#).unwrap();
    }

    // A .vif that omits ecShardConfig answers nothing about the layout, so it
    // must not short-circuit the sidecar search. Returning as soon as any
    // parseable vif turned up resolved a 12+4 uniform volume as 10+4 legacy.
    #[test]
    fn read_ec_shard_config_falls_back_when_the_vif_has_no_config() {
        let d = tempfile::TempDir::new().unwrap();
        let dir = d.path().to_str().unwrap();
        let base = crate::storage::volume::volume_file_name(dir, "", VolumeId(11));
        seed_config_free_vif(&base);
        seed_uniform_sidecar(&base, 12, 4, 3 * 1024 * 1024);

        let got = read_ec_shard_config(dir, dir, "", VolumeId(11)).unwrap();
        assert_eq!(got, (12, 4, 3 * 1024 * 1024));
    }

    // Split -dir/-dir.idx: the vif and the sidecar both live with the INDEX,
    // and the resolver was told to report the DATA directory, so the fallback
    // looked in a directory holding neither.
    #[test]
    fn read_ec_shard_config_finds_a_config_free_vifs_sidecar_in_the_index_dir() {
        let data = tempfile::TempDir::new().unwrap();
        let idx = tempfile::TempDir::new().unwrap();
        let (dir, dir_idx) = (data.path().to_str().unwrap(), idx.path().to_str().unwrap());
        let idx_base = crate::storage::volume::volume_file_name(dir_idx, "", VolumeId(12));
        seed_config_free_vif(&idx_base);
        seed_uniform_sidecar(&idx_base, 12, 4, 3 * 1024 * 1024);

        let got = read_ec_shard_config(dir, dir_idx, "", VolumeId(12)).unwrap();
        assert_eq!(got, (12, 4, 3 * 1024 * 1024));
    }

    // Same gap in the cross-disk resolver the rebuild uses.
    #[test]
    fn read_ec_shard_config_across_dirs_falls_back_when_the_vif_has_no_config() {
        let data = tempfile::TempDir::new().unwrap();
        let sib = tempfile::TempDir::new().unwrap();
        let (dir, sibling) = (data.path().to_str().unwrap(), sib.path().to_str().unwrap());
        seed_config_free_vif(&crate::storage::volume::volume_file_name(
            dir,
            "",
            VolumeId(13),
        ));
        seed_uniform_sidecar(
            &crate::storage::volume::volume_file_name(sibling, "", VolumeId(13)),
            12,
            4,
            3 * 1024 * 1024,
        );

        let got =
            read_ec_shard_config_across_dirs(dir, dir, &[sibling.to_string()], "", VolumeId(13))
                .unwrap();
        assert_eq!(got, (12, 4, 3 * 1024 * 1024));
    }

    // Absence stays legal: a volume predating both records is genuinely legacy.
    #[test]
    fn read_ec_shard_config_config_free_vif_without_a_sidecar_is_legacy() {
        let d = tempfile::TempDir::new().unwrap();
        let dir = d.path().to_str().unwrap();
        seed_config_free_vif(&crate::storage::volume::volume_file_name(
            dir,
            "",
            VolumeId(14),
        ));
        let got = read_ec_shard_config(dir, dir, "", VolumeId(14)).unwrap();
        assert_eq!(got, (10, 4, 0));
    }

    #[test]
    fn read_ec_shard_config_finds_a_sibling_disks_vif() {
        let a = tempfile::TempDir::new().unwrap();
        let b = tempfile::TempDir::new().unwrap();
        let (rebuild, sibling) = (a.path().to_str().unwrap(), b.path().to_str().unwrap());
        let base = crate::storage::volume::volume_file_name(sibling, "", VolumeId(3));
        std::fs::write(
            format!("{}.vif", base),
            r#"{"version":3,"ecShardConfig":{"dataShards":12,"parityShards":4,"blockSize":3145728}}"#,
        )
        .unwrap();

        let (ds, ps, bs) = read_ec_shard_config_across_dirs(
            rebuild,
            rebuild,
            &[sibling.to_string()],
            "",
            VolumeId(3),
        )
        .unwrap();
        assert_eq!((ds, ps, bs), (12, 4, 3 * 1024 * 1024));
    }

    /// Same, for the sidecar: with no .vif anywhere it is the surviving record
    /// of the geometry, wherever it sits.
    // A split -dir/-dir.idx location keeps its metadata with the INDEX, and
    // callers leave their own index directory out of other_dirs because it is
    // passed separately. With no .vif anywhere the generation-0 .ecsum is the
    // only record of the layout, so missing that directory resolves a 12+4
    // uniform volume to 10+4 legacy and reconstructs through the wrong matrix.
    #[test]
    fn read_ec_shard_config_finds_the_sidecar_in_its_own_index_dir() {
        use crate::pb::volume_server_pb::{
            ChecksumAlgorithm, EcBitrotProtection, EcShardChecksums,
        };
        use crate::storage::erasure_coding::ec_bitrot;

        let data = tempfile::TempDir::new().unwrap();
        let idx = tempfile::TempDir::new().unwrap();
        let (dir, dir_idx) = (data.path().to_str().unwrap(), idx.path().to_str().unwrap());
        let base = crate::storage::volume::volume_file_name(dir_idx, "", VolumeId(7));
        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: ec_bitrot::DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_bitrot::ec_shard_config(12, 4, 3 * 1024 * 1024)),
            shards: (0..16u32)
                .map(|shard_id| EcShardChecksums {
                    shard_id,
                    covered_size: 4,
                    block_crc32c: vec![0u8; 4],
                })
                .collect(),
            encode_uuid: vec![0u8; 16],
        };
        ec_bitrot::save_bitrot_sidecar(&ec_bitrot::bitrot_sidecar_path(&base, 0), &prot).unwrap();

        let (ds, ps, bs) =
            read_ec_shard_config_across_dirs(dir, dir_idx, &[], "", VolumeId(7)).unwrap();
        assert_eq!((ds, ps, bs), (12, 4, 3 * 1024 * 1024));
    }

    #[test]
    fn read_ec_shard_config_finds_a_sibling_disks_sidecar() {
        use crate::pb::volume_server_pb::{
            ChecksumAlgorithm, EcBitrotProtection, EcShardChecksums,
        };
        use crate::storage::erasure_coding::ec_bitrot;

        let a = tempfile::TempDir::new().unwrap();
        let b = tempfile::TempDir::new().unwrap();
        let (rebuild, sibling) = (a.path().to_str().unwrap(), b.path().to_str().unwrap());
        let base = crate::storage::volume::volume_file_name(sibling, "", VolumeId(4));
        let prot = EcBitrotProtection {
            algorithm: ChecksumAlgorithm::ChecksumCrc32c as i32,
            block_size: ec_bitrot::DEFAULT_BITROT_BLOCK_SIZE as u32,
            generation: 0,
            ec_shard_config: Some(ec_bitrot::ec_shard_config(12, 4, 3 * 1024 * 1024)),
            shards: (0..16u32)
                .map(|shard_id| EcShardChecksums {
                    shard_id,
                    covered_size: 4,
                    block_crc32c: vec![0u8; 4],
                })
                .collect(),
            encode_uuid: vec![0u8; 16],
        };
        ec_bitrot::save_bitrot_sidecar(&ec_bitrot::bitrot_sidecar_path(&base, 0), &prot).unwrap();

        let (ds, ps, bs) = read_ec_shard_config_across_dirs(
            rebuild,
            rebuild,
            &[sibling.to_string()],
            "",
            VolumeId(4),
        )
        .unwrap();
        assert_eq!((ds, ps, bs), (12, 4, 3 * 1024 * 1024));
    }
}

/// One volume id's per-disk runtimes resolved into a single scrubbable view.
/// Every scrub mode builds from this, so there is exactly one answer to
/// "which disks count" per volume.
pub(crate) struct MergedEcRuntimes<'a> {
    /// Volume-level metadata source: geometry, `.ecx` handles, version. NOT
    /// bitrot protection — the `.ecsum` sidecar is per-DISK state, so
    /// `EcChecksumScrubPlan::for_volumes` sources it from the first `merged`
    /// runtime that has any.
    pub anchor: &'a EcVolume,
    /// Runtimes whose shards are safe to verify together.
    pub merged: Vec<&'a EcVolume>,
    /// Indexed BY SHARD ID. `None` is a shard no merged runtime holds. The
    /// volume's shard-id range is the anchor's geometry
    /// (`0..data_shards + parity_shards`); consumers truncate to it.
    pub slots: Vec<Option<(&'a EcVolume, &'a EcVolumeShard)>>,
    /// One line per runtime excluded by the identity or geometry fence.
    /// Reported, never dropped.
    pub skipped: Vec<String>,
}

/// Resolve `runtimes` (one vid's mounts, in location order) into a merged view.
/// `None` for an empty slice (the vanished-volume case).
///
/// Two fences admit a runtime into `merged`: the anchor's `encode_ts_ns` (the
/// maximum, so `0` implies every runtime is `0` and nothing is excluded —
/// legacy leniency), and the anchor's geometry (`data_shards`, `parity_shards`,
/// `block_size`). Equal timestamps do not guarantee equal layouts, so a
/// same-generation runtime whose `.vif` disagrees is excluded and reported
/// rather than merged into a plan that would apply the anchor's offsets and
/// checksums to incompatible shards.
pub(crate) fn merge_ec_runtimes<'a>(runtimes: &[&'a EcVolume]) -> Option<MergedEcRuntimes<'a>> {
    let anchor_gen = runtimes.iter().map(|v| v.encode_ts_ns).max()?;

    let gen_matches: Vec<&'a EcVolume> = runtimes
        .iter()
        .copied()
        .filter(|v| v.encode_ts_ns == anchor_gen)
        .collect();

    let anchor = gen_matches
        .iter()
        .copied()
        .find(|v| v.shards.iter().any(|s| s.is_some()))
        .unwrap_or(gen_matches[0]);

    let merged: Vec<&'a EcVolume> = gen_matches
        .iter()
        .copied()
        .filter(|v| {
            v.data_shards == anchor.data_shards
                && v.parity_shards == anchor.parity_shards
                && v.block_size == anchor.block_size
        })
        .collect();

    let width = merged.iter().map(|v| v.shards.len()).max().unwrap_or(0);
    let mut slots: Vec<Option<(&'a EcVolume, &'a EcVolumeShard)>> = vec![None; width];
    for v in &merged {
        for (id, slot) in v.shards.iter().enumerate() {
            if let Some(shard) = slot.as_ref() {
                if slots[id].is_none() {
                    slots[id] = Some((*v, shard));
                }
            }
        }
    }

    let skipped: Vec<String> = runtimes
        .iter()
        .enumerate()
        .filter(|(_, v)| {
            v.encode_ts_ns != anchor_gen
                || v.data_shards != anchor.data_shards
                || v.parity_shards != anchor.parity_shards
                || v.block_size != anchor.block_size
        })
        .map(|(pos, v)| {
            if v.encode_ts_ns != anchor_gen {
                format!(
                    "EC volume {} shards at {} (position {}) belong to encode run {} but the scrub anchors on {}; they were not verified",
                    v.volume_id.0, v.dir, pos, v.encode_ts_ns, anchor_gen
                )
            } else {
                format!(
                    "EC volume {} shards at {} (position {}) share encode run {} but disagree on geometry ({}+{} bs {} vs {}+{} bs {}); they were not verified",
                    v.volume_id.0, v.dir, pos, v.encode_ts_ns,
                    v.data_shards, v.parity_shards, v.block_size,
                    anchor.data_shards, anchor.parity_shards, anchor.block_size
                )
            }
        })
        .collect();

    Some(MergedEcRuntimes {
        anchor,
        merged,
        slots,
        skipped,
    })
}

/// Self-contained input for an EC checksum scrub: the sidecar plus a duplicate
/// of every mounted local shard handle, taken under the store read guard.
///
/// Two things follow from holding descriptors rather than paths. `run()` needs
/// no store access, so the guard is released before a scan that reads every
/// byte of every local shard — under the guard that scan parks the periodic
/// heartbeat's `store.write()`, and `std::sync::RwLock` is write-preferring, so
/// every later reader queues behind it and the node stops serving. And a
/// teardown that legitimately unlinks the shards mid-scan (the heartbeat's
/// `delete_expired_ec_volumes`, `volume_ec_shards_delete`) cannot masquerade as
/// bitrot, because the descriptor outlives the name. Go reads the same way:
/// `ChecksumScrub` goes through `shard.ReadAt`.
///
/// Not `Clone`: it owns those descriptors.
#[derive(Debug)]
pub struct EcChecksumScrubPlan {
    pub volume_id: VolumeId,
    pub prot: Option<crate::pb::volume_server_pb::EcBitrotProtection>,
    pub status: crate::storage::erasure_coding::ec_bitrot::BitrotStatus,
    pub parity_shards: u32,
    /// One entry per LOCAL shard: its id and a duplicate of the mounted
    /// handle (or the error to report). Private so a plan can only be built
    /// (via `checksum_scrub_plan()` or `for_volumes()`) from `&EcVolume`
    /// references, which cannot be obtained without the store guard — which
    /// is what makes "captured under the guard" an invariant rather than a
    /// convention.
    ///
    /// `dup` shares the kernel file offset, so every read here is positional.
    shards: Vec<(u32, std::io::Result<File>)>,
    /// Runtimes the identity/geometry fence excluded, one line each. Surfaced
    /// through `run()`'s errors on the `On`/`Invalid` paths.
    skipped: Vec<String>,
    /// `Some(source_dir)` when the sidecar that supplied `prot`/`status` was
    /// resolved from a directory belonging to a runtime the fence excluded.
    /// `run()` reports unverifiable protection instead of blaming shards.
    unverifiable_sidecar: Option<String>,
    /// One line per merged runtime whose sidecar resolved `Invalid`. Reported
    /// even when protection is taken from a sibling that is `On`, so a
    /// malformed sidecar is never silently discarded.
    invalid_sidecar_errors: Vec<String>,
}

impl EcChecksumScrubPlan {
    /// Build one plan over every per-disk runtime of a volume id. `None` only
    /// when `runtimes` is empty (the vanished-volume case).
    pub fn for_volumes(runtimes: &[&EcVolume]) -> Option<Self> {
        use crate::storage::erasure_coding::ec_bitrot::BitrotStatus;

        let merged = merge_ec_runtimes(runtimes)?;
        let anchor = merged.anchor;

        // Protection is per-DISK state: the `.ecsum` sidecar is not mirrored,
        // and at mount each runtime resolves it from its own directories only,
        // so after a restart the runtime without the sidecar mounts `Off`.
        // Take the first merged runtime that has protection: `On` if any, else
        // `Invalid`, else the anchor's `Off`. This only moves toward more
        // verification (`Off->On`, `Off->Invalid`, `Invalid->On`), never toward
        // silence — the anchor is itself in `merged`, so the fallback is `Off`
        // only when every merged runtime is `Off`.
        let protection_source = merged
            .merged
            .iter()
            .copied()
            .find(|v| matches!(v.bitrot_protection().1, BitrotStatus::On))
            .or_else(|| {
                merged
                    .merged
                    .iter()
                    .copied()
                    .find(|v| matches!(v.bitrot_protection().1, BitrotStatus::Invalid))
            })
            .unwrap_or(anchor);
        let (prot, status) = protection_source.bitrot_protection();

        // Collect every merged runtime whose sidecar resolved Invalid, excluding
        // the protection_source (whose error the Invalid arm of run() already
        // reports). These surface when protection is taken from a sibling that
        // is On, so a malformed sidecar is never silently discarded.
        let invalid_sidecar_errors: Vec<String> = merged
            .merged
            .iter()
            .filter(|v| {
                let v_ptr: *const EcVolume = **v;
                let src_ptr: *const EcVolume = protection_source;
                !std::ptr::eq(v_ptr, src_ptr)
                    && matches!(v.bitrot_protection().1, BitrotStatus::Invalid)
            })
            .map(|v| {
                format!(
                    "EC volume {} bitrot sidecar at {} is malformed/unverifiable (sidecar integrity)",
                    v.volume_id.0, v.dir
                )
            })
            .collect();

        let total = (anchor.data_shards + anchor.parity_shards) as usize;

        let shards = match status {
            crate::storage::erasure_coding::ec_bitrot::BitrotStatus::On => merged
                .slots
                .iter()
                .take(total)
                .enumerate()
                .filter_map(|(id, slot)| slot.map(|(_, shard)| (id as u32, shard.try_clone_file())))
                .collect(),
            _ => Vec::new(),
        };

        // If the fence excluded a runtime AND the sidecar that supplied
        // protection was resolved from that excluded runtime's directory, the
        // checksums cannot be trusted against the merged shards.
        let unverifiable_sidecar = if merged.skipped.is_empty() {
            None
        } else {
            let own_dirs: Vec<String> = merged
                .merged
                .iter()
                .flat_map(|v| [v.dir.as_str(), v.dir_idx.as_str()])
                .map(|d| d.trim_end_matches('/').to_string())
                .collect();
            let src = protection_source.bitrot_source_dir.trim_end_matches('/');
            if src.is_empty() || own_dirs.iter().any(|d| d == src) {
                None
            } else {
                Some(protection_source.bitrot_source_dir.clone())
            }
        };

        Some(EcChecksumScrubPlan {
            volume_id: anchor.volume_id,
            prot,
            status,
            parity_shards: anchor.parity_shards,
            shards,
            skipped: merged.skipped,
            unverifiable_sidecar,
            invalid_sidecar_errors,
        })
    }

    /// The byte-verification pass. Touches only the filesystem — no store, no
    /// lock — so it is safe to hand to `spawn_blocking`.
    pub fn run(self) -> (u64, Vec<u32>, Vec<String>) {
        use crate::storage::erasure_coding::ec_bitrot;
        use crate::storage::erasure_coding::ec_bitrot::BitrotStatus;

        let mut errors: Vec<String> = Vec::new();

        // Mirrors Go's `ChecksumScrub` (`prot, status := ecv.BitrotProtection()`):
        //   - Off: sidecars are optional; return clean (Go: `case BitrotOff: return 0, nil, nil`).
        //   - Invalid: sidecar is present but malformed; report an integrity error.
        //   - On: scan local shards against it.
        let prot = match (self.prot, self.status) {
            (_, BitrotStatus::Off) => {
                return (0, Vec::new(), Vec::new());
            }
            (_, BitrotStatus::Invalid) => {
                let mut errs = vec![format!(
                    "EC volume {} bitrot sidecar is malformed/unverifiable (sidecar integrity)",
                    self.volume_id.0
                )];
                errs.extend(self.skipped);
                errs.extend(self.invalid_sidecar_errors);
                return (0, Vec::new(), errs);
            }
            (Some(p), BitrotStatus::On) => p,
            (None, BitrotStatus::On) => {
                return (0, Vec::new(), Vec::new());
            }
        };

        errors.extend(self.skipped);
        errors.extend(self.invalid_sidecar_errors);

        if let Some(src) = self.unverifiable_sidecar {
            errors.push(format!(
                "EC volume {} bitrot sidecar was resolved from {}, which belongs to an excluded encode run; protection unverifiable",
                self.volume_id.0, src
            ));
            return (0, Vec::new(), errors);
        }

        let block_size = prot.block_size as i64;

        let mut blocks_scanned: u64 = 0;
        let mut mismatched_shards: Vec<u32> = Vec::new();
        // Track shards whose blocks ALL mismatch (wholesale) to detect a
        // stale/wrong sidecar.
        let mut wholesale_mismatch = 0usize;

        for (shard_id, handle) in self.shards {
            let Some(entry) = ec_bitrot::shard_checksums(&prot, shard_id) else {
                errors.push(format!(
                    "EC volume {} shard {} present but missing from sidecar manifest",
                    self.volume_id.0, shard_id
                ));
                continue;
            };

            // The handle was opened under the store lock; reading through it
            // means a concurrent unmount/unlink cannot masquerade as bitrot.
            let file = match &handle {
                Ok(f) => f,
                Err(e) => {
                    errors.push(format!(
                        "EC volume {} shard {} scrub read error: {}",
                        self.volume_id.0, shard_id, e
                    ));
                    continue;
                }
            };

            let expected_blocks = entry.block_crc32c.len() / 4;
            match ec_bitrot::verify_shard_blocks(file, entry, block_size) {
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
}

/// Self-contained input for an EC index scrub.
///
/// Not `Clone`: it owns the .ecx handle opened under the store lock.
#[derive(Debug)]
pub struct EcIndexScrubPlan {
    pub volume_id: VolumeId,
    pub has_ecx_file: bool,
    pub ecx_path: String,
    pub ecx_file_size: i64,
    pub version: Version,
    /// The .ecx handle opened while the store lock was held. Private, so the
    /// plan can only come from `EcVolume::scrub_index_plan`.
    ecx_handle: std::io::Result<File>,
}

impl EcIndexScrubPlan {
    /// Structural walk of the .ecx index. Filesystem only — no store, no lock.
    pub fn run(self) -> (u64, Vec<String>) {
        if !self.has_ecx_file {
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
                vec![format!(
                    "zero-size ECX file for EC volume {}",
                    self.volume_id.0
                )],
            );
        }

        // A private fd, so the structural scan never moves the shared ecx_file
        // cursor (the cached handle is read positionally elsewhere). Checked
        // after the two guards above so the error ordering is unchanged.
        let mut ecx_file = match self.ecx_handle {
            Ok(f) => f,
            Err(e) => return (0, vec![format!("open ECX file {}: {}", self.ecx_path, e)]),
        };
        crate::storage::idx::check_index_file(&mut ecx_file, self.ecx_file_size, self.version)
    }
}

/// One local shard as `EcLocalScrubPlan` sees it: the mounted descriptor, the
/// size the scan compares against, and the identity a broken-shard report needs.
#[derive(Debug)]
pub struct EcLocalShard {
    file: std::io::Result<File>,
    /// The shard's cached size, as `scrub_local` has always compared against —
    /// deliberately not a live `metadata()` call in `run()`.
    file_size: i64,
    info: crate::pb::volume_server_pb::EcShardInfo,
}

impl EcLocalShard {
    /// Positional read through the duplicated handle. `dup` shares the kernel
    /// offset with the mounted shard, so this must never seek.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let file = self
            .file
            .as_ref()
            .map_err(|e| io::Error::new(e.kind(), e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_at(buf, offset)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = file.try_clone()?;
            f.seek(SeekFrom::Start(offset))?;
            f.read(buf)
        }
    }
}

/// Self-contained input for an EC LOCAL scrub: the index snapshot, a private
/// .ecx descriptor for the needle walk, and the mounted local shard handles.
///
/// Same reasoning as `EcChecksumScrubPlan` — `scrub_local` reads every local
/// needle's bytes, so it must not run under the store read guard.
///
/// Not `Clone`: it owns descriptors.
#[derive(Debug)]
pub struct EcLocalScrubPlan {
    volume_id: VolumeId,
    version: Version,
    data_shards: u32,
    shard_size: i64,
    large_block_size: i64,
    small_block_size: i64,
    index: EcIndexScrubPlan,
    ecx_path: String,
    ecx_walk: std::io::Result<File>,
    /// Indexed BY SHARD ID; `None` is a shard no merged runtime holds.
    shards: Vec<Option<EcLocalShard>>,
    /// Runtimes the identity fence excluded, one line each. LOCAL has no status
    /// gate, so `run()` always reports these.
    skipped: Vec<String>,
}

impl EcLocalScrubPlan {
    /// Build one plan over every per-disk runtime of a volume id. `None` only
    /// when `runtimes` is empty.
    ///
    /// The shard vector keeps its SLOT structure — index is the shard id, gaps
    /// are shards no merged runtime holds. `run()` reads that distinction to
    /// decide whether a needle can be reassembled locally at all, so compacting
    /// it would silently change which needles get verified.
    pub fn for_volumes(runtimes: &[&EcVolume]) -> Option<Self> {
        let merged = merge_ec_runtimes(runtimes)?;
        let anchor = merged.anchor;

        Some(EcLocalScrubPlan {
            volume_id: anchor.volume_id,
            version: anchor.version,
            data_shards: anchor.data_shards,
            // locate_data wants shardSize = datFileSize / DataShards when known,
            // else ecdFileSize - 1 (shards are padded to the small block size;
            // the -1 avoids an off-by-one in the large-block row count).
            //
            // The fallback takes the MAX over every merged slot, not
            // `anchor.shard_file_size()` -- which returns the anchor's FIRST
            // held shard, not a maximum. Before aggregation the plan only read
            // the anchor's own shards, so a truncated shard there mis-sized only
            // its own runtime; now one disk's truncated shard would set
            // `shard_size` for every merged sibling's shards, mis-offset
            // `locate_data` and manufacture needle corruption across the whole
            // node. `verify_ec_shards` already answers this same question the
            // same way (`if size > shard_size { shard_size = size }`), so this
            // is the in-tree convention rather than a preference. Reached only
            // on the legacy `dat_file_size == 0` path.
            //
            // `.take(..)` honors the `slots` contract: the volume's shard-id
            // range is the ANCHOR's geometry, and a same-generation runtime with
            // a disagreeing `.vif` can populate slots beyond it. Scanning those
            // would let an OUT-OF-GEOMETRY shard -- one nothing in this volume's
            // layout describes -- set the offset math for every shard that IS in
            // it, which is a narrower path to exactly the node-wide mis-sizing
            // this max exists to close.
            shard_size: if anchor.dat_file_size > 0 {
                anchor.dat_file_size / anchor.data_shards as i64
            } else {
                merged
                    .slots
                    .iter()
                    .take((anchor.data_shards + anchor.parity_shards) as usize)
                    .flatten()
                    .map(|(_, s)| s.file_size())
                    .max()
                    .unwrap_or(0)
                    - 1
            },
            large_block_size: anchor.large_block_size(),
            small_block_size: anchor.small_block_size(),
            index: anchor.scrub_index_plan(),
            ecx_path: anchor.ecx_file_name(),
            // A second descriptor: the index plan's is consumed by its own walk,
            // and both seek.
            ecx_walk: File::open(anchor.ecx_file_name()),
            shards: merged
                .slots
                .iter()
                .map(|slot| {
                    slot.map(|(_, s)| EcLocalShard {
                        file: s.try_clone_file(),
                        file_size: s.file_size(),
                        info: s.to_ec_shard_info(),
                    })
                })
                .collect(),
            skipped: merged.skipped,
        })
    }

    /// The needle walk. Filesystem only — no store, no lock.
    pub fn run(
        self,
    ) -> (
        u64,
        Vec<crate::pb::volume_server_pb::EcShardInfo>,
        Vec<String>,
    ) {
        let EcLocalScrubPlan {
            volume_id,
            version,
            data_shards,
            shard_size,
            large_block_size,
            small_block_size,
            index,
            ecx_path,
            ecx_walk,
            shards,
            skipped,
        } = self;

        // Local scan also verifies the index.
        let (_, mut errs) = index.run();

        // LOCAL has no protection status to gate on, so an excluded runtime is
        // always reported rather than silently unscanned.
        errs.extend(skipped);

        let mut broken_shards: HashSet<ShardId> = HashSet::new();
        let mut count: u64 = 0;

        let mut ecx_file = match ecx_walk {
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

            // Go recomputes this at the size check below; hoisted because Rust
            // also sizes the reassembly buffer from it. Any negative size other
            // than the -1 tombstone skipped above drives it negative.
            let want = get_actual_size(size, version);

            let locations = ec_locate::locate_data(
                offset.to_actual_offset(),
                Size(want as i32),
                shard_size,
                data_shards,
                large_block_size,
                small_block_size,
            );
            // A needle is verifiable locally only if every shard it spans is local;
            // when any is remote, skip the reassembly buffer entirely.
            let has_remote_chunks = locations.iter().any(|iv| {
                let (sid, _) =
                    iv.to_shard_id_and_offset(data_shards, large_block_size, small_block_size);
                shards.get(sid as usize).and_then(|s| s.as_ref()).is_none()
            });
            let mut read: i64 = 0;
            // `want <= 0` means the row is malformed. Go pays nothing for it:
            // it appends to a nil slice and has no capacity hint here. Rust's
            // per-needle buffer does, and `Vec::with_capacity(negative as usize)`
            // aborts the process -- inside spawn_blocking that surfaces as a
            // JoinError and takes the whole node-wide scrub RPC with it. Fall
            // through with an empty buffer instead: locate_data already returns
            // no intervals for a non-positive size, so read stays 0 and the
            // `read != want` check below reports the row, exactly as Go does.
            let mut data: Vec<u8> = if has_remote_chunks || want <= 0 {
                Vec::new()
            } else {
                Vec::with_capacity(want as usize)
            };
            let mut local_shard_ids: Vec<ShardId> = Vec::new();

            for (i, iv) in locations.iter().enumerate() {
                let (sid, soffset) =
                    iv.to_shard_id_and_offset(data_shards, large_block_size, small_block_size);
                let ssize = iv.size;
                let shard = match shards.get(sid as usize).and_then(|s| s.as_ref()) {
                    Some(s) => s,
                    None => {
                        // Shard is not local; we can't verify it without decoding.
                        read += ssize;
                        continue;
                    }
                };
                local_shard_ids.push(sid);

                if soffset + ssize > shard.file_size {
                    broken_shards.insert(sid);
                    errs.push(format!(
                        "local shard {} for needle {} is too short ({}), cannot read chunk {}/{}",
                        sid,
                        id.0,
                        shard.file_size,
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

            if read != want {
                // Like Go, returning from the walk callback aborts the scan.
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "expected {} bytes for needle {} on volume {}, got {}",
                        want, id.0, volume_id.0, read
                    ),
                ));
            }

            // Only a fully-local needle can be reassembled and CRC-checked.
            if !has_remote_chunks {
                let mut n = Needle::default();
                if let Err(e) = n.read_bytes(&data, 0, size, version) {
                    // A delete-state disagreement between the .ecx index and the reassembled
                    // on-disk header (live index vs zero header size) is not corruption.
                    let delete_state_disagrees = matches!(
                        &e,
                        NeedleError::SizeMismatch { found, .. } if size.is_deleted() != (found.0 == 0)
                    );
                    if !delete_state_disagrees {
                        errs.push(format!(
                            "needle {} on volume {}, shards {:?}: {}",
                            id.0, volume_id.0, local_shard_ids, e
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
            .filter_map(|sid| shards.get(*sid as usize).and_then(|s| s.as_ref()))
            .map(|s| s.info.clone())
            .collect();
        broken.sort_by(|a, b| a.shard_id.cmp(&b.shard_id));

        (count, broken, errs)
    }
}
