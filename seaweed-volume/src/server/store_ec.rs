//! Distributed EC read path. Mirror of `weed/storage/store_ec.go`'s
//! `readEcShardIntervals` → `readOneEcShardInterval` →
//! `readRemoteEcShardInterval` → `recoverOneRemoteEcShardInterval`
//! chain.
//!
//! The existing `EcVolume::read_ec_shard_needle` reads only locally-
//! mounted shards and returns `NotFound` if any interval requires a
//! shard held on a peer server. In a standard RS(10,4)-across-14
//! deployment each server holds one shard, so every read needs >=9
//! peer fetches. This module fills the gap by:
//!
//!   1. Locating the needle in `.ecx` (under the Store read lock) and
//!      computing the per-interval (shard_id, shard_offset, size).
//!   2. Reading the local-resident intervals while still holding the
//!      lock — same path the local-only helper uses.
//!   3. Dropping the lock and, for any remaining intervals, fetching
//!      from peer volume servers via `VolumeEcShardRead`. If the
//!      direct peer read fails, fan-out reads to other shards at the
//!      same (shard_offset, size) and rebuild the missing shard via
//!      Reed-Solomon — exactly Go's flow.
//!   4. Refreshing the per-EcVolume `shard_locations` cache from the
//!      master's `LookupEcVolume` RPC when the cached map is stale.
//!
//! All gRPC IO is async; the file IO portion runs under the sync
//! Store read lock, matching Go's `readLocalEcShardInterval`. The
//! cache write-back briefly reacquires the EcVolume's internal
//! `RwLock` so we do not contend with the Store-level lock at all.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use reed_solomon_erasure::galois_8::ReedSolomon;
use tonic::Request;

use crate::pb::master_pb::{self, seaweed_client::SeaweedClient, LookupEcVolumeRequest};
use crate::pb::volume_server_pb::{
    volume_server_client::VolumeServerClient, VolumeEcShardReadRequest,
};
use crate::server::grpc_client::{build_grpc_endpoint, GRPC_MAX_MESSAGE_SIZE};
use crate::server::request_id::outgoing_request_id_interceptor;
use crate::server::volume_server::VolumeServerState;
use crate::storage::erasure_coding::ec_locate;
use crate::storage::erasure_coding::ec_shard::ShardId;
use crate::storage::needle::needle::{get_actual_size, Needle};
use crate::storage::types::*;

/// One interval's data after Phase A.
enum IntervalResult {
    /// Already read from a locally-mounted shard.
    Local(Vec<u8>),
    /// Shard not local (or local read failed). Must be fetched.
    NeedRemote {
        shard_id: ShardId,
        shard_offset: i64,
        size: usize,
    },
}

/// Snapshot extracted under the Store read lock so Phases B/C can run
/// without holding any sync lock across `.await`.
struct Snapshot {
    data_shards: u32,
    parity_shards: u32,
    version: Version,
    actual_size: usize,
    offset: Offset,
    size_for_parse: Size,
    intervals: Vec<IntervalResult>,
    cached_locations: HashMap<ShardId, Vec<String>>,
    cache_refreshed_at: Option<Instant>,
}

/// Top-level entry point. Returns `Ok(None)` for "not found" (matches
/// Go's `ReadEcShardNeedle`); errors propagate as `io::Error`.
pub async fn read_ec_shard_needle_distributed(
    state: &Arc<VolumeServerState>,
    vid: VolumeId,
    needle_id: NeedleId,
) -> io::Result<Option<Needle>> {
    // Phase A — under the Store read lock, locate the needle, compute
    // intervals, and read any locally-mounted shard intervals. We must
    // not `.await` while holding this guard (std::sync::RwLockReadGuard
    // is !Send).
    let snapshot = match snapshot_under_lock(state, vid, needle_id)? {
        Some(s) => s,
        None => return Ok(None),
    };

    // Phase B — refresh the shard_locations cache from the master if
    // it is stale. Do this lazily: if every needed interval was read
    // locally we can skip the master RPC entirely.
    let any_remote = snapshot
        .intervals
        .iter()
        .any(|r| matches!(r, IntervalResult::NeedRemote { .. }));
    let total_shards = (snapshot.data_shards + snapshot.parity_shards) as usize;

    let mut shard_locations = snapshot.cached_locations.clone();
    if any_remote
        && needs_refresh(
            &shard_locations,
            snapshot.cache_refreshed_at,
            snapshot.data_shards as usize,
            total_shards,
        )
    {
        match cached_lookup_ec_shard_locations(state, vid).await {
            Ok(fresh) => {
                shard_locations = fresh.clone();
                write_back_shard_locations(state, vid, fresh);
            }
            Err(e) => {
                // Lookup failed — proceed with cached values. If cache
                // is empty, the remote fetch below will fail and we
                // surface a NotFound (matching Go's behavior when no
                // locations are known).
                tracing::warn!(
                    "ec lookup failed for volume {}: {} — using cached locations ({} entries)",
                    vid.0,
                    e,
                    shard_locations.len(),
                );
            }
        }
    }

    // Phase C — fetch missing intervals, reconstructing when the
    // direct peer read fails.
    let mut assembled: Vec<Vec<u8>> = Vec::with_capacity(snapshot.intervals.len());
    for res in snapshot.intervals {
        match res {
            IntervalResult::Local(buf) => assembled.push(buf),
            IntervalResult::NeedRemote {
                shard_id,
                shard_offset,
                size,
            } => {
                let buf = fetch_one_interval(
                    state,
                    vid,
                    needle_id,
                    shard_id,
                    shard_offset,
                    size,
                    &shard_locations,
                    snapshot.data_shards as usize,
                    snapshot.parity_shards as usize,
                )
                .await?;
                assembled.push(buf);
            }
        }
    }

    // Phase D — assemble and parse the Needle. Mirrors the tail of
    // `EcVolume::read_ec_shard_needle`.
    let mut bytes = Vec::with_capacity(snapshot.actual_size);
    for chunk in assembled {
        bytes.extend_from_slice(&chunk);
    }
    bytes.truncate(snapshot.actual_size);
    if bytes.len() < snapshot.actual_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "read {} bytes but need {} for needle {}",
                bytes.len(),
                snapshot.actual_size,
                needle_id
            ),
        ));
    }

    let mut n = Needle::default();
    n.id = needle_id;
    n.read_bytes(
        &bytes,
        snapshot.offset.to_actual_offset(),
        snapshot.size_for_parse,
        snapshot.version,
    )
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{}", e)))?;
    Ok(Some(n))
}

fn snapshot_under_lock(
    state: &Arc<VolumeServerState>,
    vid: VolumeId,
    needle_id: NeedleId,
) -> io::Result<Option<Snapshot>> {
    let store = state.store.read().unwrap();
    let ecv = match store.find_ec_volume(vid) {
        Some(v) => v,
        None => return Ok(None),
    };

    // Resolve needle (offset, size).
    let (offset, size) = match ecv.find_needle_from_ecx(needle_id)? {
        Some((o, s)) => (o, s),
        None => return Ok(None),
    };
    if size.is_deleted() || offset.is_zero() {
        return Ok(None);
    }

    // Compute the per-needle shard intervals. Reproduces the
    // `EcVolume::locate_needle` math so we can do everything in one
    // pass under the lock.
    let shard_size = if ecv.dat_file_size > 0 {
        ecv.dat_file_size / ecv.data_shards as i64
    } else {
        ecv.shards
            .iter()
            .find_map(|s| s.as_ref())
            .map(|s| s.file_size())
            .unwrap_or(0)
            .saturating_sub(1)
    };
    let actual = get_actual_size(size, ecv.version);
    let intervals = ec_locate::locate_data(
        offset.to_actual_offset(),
        Size(actual as i32),
        shard_size,
        ecv.data_shards,
    );
    if intervals.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no intervals for needle",
        ));
    }

    // Phase A.local: for each interval read the local shard if we
    // hold it; otherwise fall through to remote. We accumulate the
    // results in interval order so the assembly step is just a
    // concat.
    let mut interval_results = Vec::with_capacity(intervals.len());
    for interval in &intervals {
        let (shard_id, shard_offset) = interval.to_shard_id_and_offset(ecv.data_shards);
        let buf_size = interval.size as usize;
        let local = ecv
            .shards
            .get(shard_id as usize)
            .and_then(|s| s.as_ref());
        match local {
            Some(shard) => {
                let mut buf = vec![0u8; buf_size];
                match shard.read_at(&mut buf, shard_offset as u64) {
                    Ok(n) if n == buf_size => {
                        interval_results.push(IntervalResult::Local(buf));
                    }
                    _ => interval_results.push(IntervalResult::NeedRemote {
                        shard_id,
                        shard_offset,
                        size: buf_size,
                    }),
                }
            }
            None => interval_results.push(IntervalResult::NeedRemote {
                shard_id,
                shard_offset,
                size: buf_size,
            }),
        }
    }

    let cached_locations = ecv.shard_locations.read().unwrap().clone();
    let cache_refreshed_at = *ecv.shard_locations_refresh_time.lock().unwrap();

    Ok(Some(Snapshot {
        data_shards: ecv.data_shards,
        parity_shards: ecv.parity_shards,
        version: ecv.version,
        actual_size: actual as usize,
        offset,
        size_for_parse: size,
        intervals: interval_results,
        cached_locations,
        cache_refreshed_at,
    }))
}

/// Master `LookupEcVolume` freshness rules — match Go's
/// `cachedLookupEcShardLocations` thresholds in store_ec.go.
fn needs_refresh(
    locations: &HashMap<ShardId, Vec<String>>,
    refreshed_at: Option<Instant>,
    data_shards: usize,
    total_shards: usize,
) -> bool {
    let now = Instant::now();
    let age = match refreshed_at {
        Some(t) => now.saturating_duration_since(t),
        None => return true,
    };
    let shard_count = locations.len();
    if shard_count < data_shards && age < Duration::from_secs(11) {
        return false;
    }
    if shard_count == total_shards && age < Duration::from_secs(37 * 60) {
        return false;
    }
    if shard_count >= data_shards && age < Duration::from_secs(7 * 60) {
        return false;
    }
    true
}

async fn cached_lookup_ec_shard_locations(
    state: &Arc<VolumeServerState>,
    vid: VolumeId,
) -> io::Result<HashMap<ShardId, Vec<String>>> {
    let master = {
        let live = state.current_master_url.read().await.clone();
        if !live.is_empty() {
            live
        } else {
            state.master_url.clone()
        }
    };
    if master.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "no master configured for ec shard lookup",
        ));
    }

    let grpc_addr = parse_grpc_address(&master)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let endpoint = build_grpc_endpoint(&grpc_addr, state.outgoing_grpc_tls.as_ref())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let channel = endpoint
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("master connect: {}", e)))?;

    let mut client = SeaweedClient::with_interceptor(channel, outgoing_request_id_interceptor)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);

    let resp = client
        .lookup_ec_volume(Request::new(LookupEcVolumeRequest { volume_id: vid.0 }))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("lookup_ec_volume: {}", e)))?;
    let resp = resp.into_inner();

    let mut out = HashMap::new();
    for entry in resp.shard_id_locations {
        let addrs: Vec<String> = entry
            .locations
            .iter()
            .map(format_location_as_server_address)
            .collect();
        out.insert(entry.shard_id as ShardId, addrs);
    }
    Ok(out)
}

fn write_back_shard_locations(
    state: &Arc<VolumeServerState>,
    vid: VolumeId,
    locations: HashMap<ShardId, Vec<String>>,
) {
    let store = state.store.read().unwrap();
    if let Some(ecv) = store.find_ec_volume(vid) {
        let mut sl = ecv.shard_locations.write().unwrap();
        sl.clear();
        for (k, v) in locations {
            sl.insert(k, v);
        }
        *ecv.shard_locations_refresh_time.lock().unwrap() = Some(Instant::now());
    }
}

/// Build a SeaweedFS-style `host:httpPort.grpcPort` address from a
/// master `Location` so the result is what `parse_grpc_address` (and
/// the heartbeat path) already understand.
fn format_location_as_server_address(loc: &master_pb::Location) -> String {
    let raw = loc
        .url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    if loc.grpc_port > 0 {
        if let Some((host, http_port)) = raw.rsplit_once(':') {
            return format!("{}:{}.{}", host, http_port, loc.grpc_port);
        }
    }
    raw.to_string()
}

/// Try direct peer read; on failure, reconstruct via Reed-Solomon
/// from the other shards. Mirrors `readOneEcShardInterval`'s tail.
async fn fetch_one_interval(
    state: &Arc<VolumeServerState>,
    vid: VolumeId,
    needle_id: NeedleId,
    shard_id: ShardId,
    shard_offset: i64,
    size: usize,
    shard_locations: &HashMap<ShardId, Vec<String>>,
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<Vec<u8>> {
    // Direct peer read against the cached locations for this shard.
    if let Some(sources) = shard_locations.get(&shard_id) {
        if !sources.is_empty() {
            match read_remote_ec_shard_interval(
                state,
                sources,
                vid,
                needle_id,
                shard_id,
                shard_offset,
                size,
            )
            .await
            {
                Ok(buf) => return Ok(buf),
                Err(e) => {
                    tracing::debug!(
                        "direct read ec shard {}.{} from {:?} failed: {} — will reconstruct",
                        vid.0,
                        shard_id,
                        sources,
                        e
                    );
                }
            }
        }
    }

    // Reconstruct: fan-out reads to every other shard at the same
    // (shard_offset, size). Mirrors `recoverOneRemoteEcShardInterval`.
    recover_one_remote_ec_shard_interval(
        state,
        vid,
        needle_id,
        shard_id,
        shard_offset,
        size,
        shard_locations,
        data_shards,
        parity_shards,
    )
    .await
}

async fn read_remote_ec_shard_interval(
    state: &Arc<VolumeServerState>,
    sources: &[String],
    vid: VolumeId,
    needle_id: NeedleId,
    shard_id: ShardId,
    shard_offset: i64,
    size: usize,
) -> io::Result<Vec<u8>> {
    let mut last_err: Option<io::Error> = None;
    for src in sources {
        match do_read_remote_ec_shard_interval(
            state,
            src,
            vid,
            needle_id,
            shard_id,
            shard_offset,
            size,
        )
        .await
        {
            Ok(buf) => return Ok(buf),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("no source for ec shard {}.{}", vid.0, shard_id),
        )
    }))
}

async fn do_read_remote_ec_shard_interval(
    state: &Arc<VolumeServerState>,
    source: &str,
    vid: VolumeId,
    needle_id: NeedleId,
    shard_id: ShardId,
    shard_offset: i64,
    size: usize,
) -> io::Result<Vec<u8>> {
    let grpc_addr =
        parse_grpc_address(source).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let endpoint = build_grpc_endpoint(&grpc_addr, state.outgoing_grpc_tls.as_ref())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let channel = endpoint
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("connect to {}: {}", source, e),
            )
        })?;

    let mut client = VolumeServerClient::with_interceptor(channel, outgoing_request_id_interceptor)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);

    let req = VolumeEcShardReadRequest {
        volume_id: vid.0,
        shard_id: shard_id as u32,
        offset: shard_offset,
        size: size as i64,
        file_key: needle_id.0,
    };
    let resp = client
        .volume_ec_shard_read(Request::new(req))
        .await
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("volume_ec_shard_read {}.{} from {}: {}", vid.0, shard_id, source, e),
            )
        })?;
    let mut stream = resp.into_inner();

    let mut out = Vec::with_capacity(size);
    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv: {}", e)))?
    {
        if !msg.data.is_empty() {
            out.extend_from_slice(&msg.data);
        }
    }
    if out.len() < size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "short read from {} for ec shard {}.{}: got {} want {}",
                source,
                vid.0,
                shard_id,
                out.len(),
                size
            ),
        ));
    }
    out.truncate(size);
    Ok(out)
}

async fn recover_one_remote_ec_shard_interval(
    state: &Arc<VolumeServerState>,
    vid: VolumeId,
    needle_id: NeedleId,
    shard_id_to_recover: ShardId,
    shard_offset: i64,
    size: usize,
    shard_locations: &HashMap<ShardId, Vec<String>>,
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<Vec<u8>> {
    let total_shards = data_shards + parity_shards;
    let rs = ReedSolomon::new(data_shards, parity_shards).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("reed-solomon init: {:?}", e),
        )
    })?;

    // Fan-out reads. One task per known shard location (excluding the
    // one we are reconstructing). Output goes into bufs[shard_id].
    let mut tasks = Vec::new();
    for (sid, locs) in shard_locations {
        if *sid == shard_id_to_recover || locs.is_empty() {
            continue;
        }
        let sid = *sid;
        let locs = locs.clone();
        let state = state.clone();
        tasks.push(async move {
            let res = read_remote_ec_shard_interval(
                &state,
                &locs,
                vid,
                needle_id,
                sid,
                shard_offset,
                size,
            )
            .await;
            (sid, res)
        });
    }
    let results = join_all(tasks).await;

    let mut bufs: Vec<Option<Vec<u8>>> = vec![None; total_shards];
    for (sid, res) in results {
        match res {
            Ok(buf) => {
                if (sid as usize) < total_shards {
                    bufs[sid as usize] = Some(buf);
                }
            }
            Err(e) => {
                tracing::debug!(
                    "recover: read {}.{} for needle {} failed: {}",
                    vid.0,
                    sid,
                    needle_id,
                    e
                );
            }
        }
    }

    let available = bufs.iter().filter(|b| b.is_some()).count();
    if available < data_shards {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "cannot recover ec shard {}.{}: only {} shards available, need at least {}",
                vid.0, shard_id_to_recover, available, data_shards
            ),
        ));
    }

    rs.reconstruct(&mut bufs).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!(
                "reed-solomon reconstruct ec shard {}.{}: {:?}",
                vid.0, shard_id_to_recover, e
            ),
        )
    })?;

    match bufs.into_iter().nth(shard_id_to_recover as usize).flatten() {
        Some(buf) => Ok(buf),
        None => Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "reconstructed buffer for shard {}.{} missing after RS reconstruct",
                vid.0, shard_id_to_recover
            ),
        )),
    }
}

/// Local copy of grpc_server.rs's `parse_grpc_address` (not pub there
/// and we want to keep this module self-contained). Translates a
/// SeaweedFS address — `host:httpPort.grpcPort` or `host:httpPort`
/// (in which case the gRPC port is httpPort + 10000) — into the
/// `host:grpcPort` form `build_grpc_endpoint` expects.
fn parse_grpc_address(source: &str) -> Result<String, String> {
    let colon_idx = source
        .rfind(':')
        .ok_or_else(|| format!("cannot parse address: {}", source))?;
    let port_part = &source[colon_idx + 1..];
    if let Some(dot_idx) = port_part.rfind('.') {
        let host = &source[..colon_idx];
        let grpc_port = &port_part[dot_idx + 1..];
        grpc_port
            .parse::<u16>()
            .map_err(|e| format!("invalid grpc port: {}", e))?;
        return Ok(format!("{}:{}", host, grpc_port));
    }
    let port: u16 = port_part
        .parse()
        .map_err(|e| format!("invalid port: {}", e))?;
    let grpc_port = port as u32 + 10000;
    let host = &source[..colon_idx];
    Ok(format!("{}:{}", host, grpc_port))
}
