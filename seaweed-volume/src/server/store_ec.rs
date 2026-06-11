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
use crate::server::grpc_client::{build_grpc_endpoint, parse_grpc_address, GRPC_MAX_MESSAGE_SIZE};
use crate::server::request_id::outgoing_request_id_interceptor;
use crate::server::volume_server::VolumeServerState;
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
    /// This volume's encode identity, carried to peers on remote shard reads so a
    /// shard from a different encode run is rejected rather than served at a
    /// mismatched offset. 0 for a pre-feature volume (lenient).
    encode_ts_ns: i64,
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
                    snapshot.encode_ts_ns,
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

    // Reuse EcVolume::locate_needle for offset/size resolution AND
    // the per-needle shard-interval math — it's the same routine the
    // local-only read path uses, so we stay byte-identical on the
    // shard-size + interval boundaries.
    let (offset, size, intervals) = match ecv.locate_needle(needle_id)? {
        Some(v) => v,
        None => return Ok(None),
    };
    if intervals.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "no intervals for needle",
        ));
    }
    let actual = get_actual_size(size, ecv.version);

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
        encode_ts_ns: ecv.encode_ts_ns,
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
        // Atomic swap + freshness stamp so a concurrent reader sees
        // either the prior cache or the fresh one — never an
        // intermediate half-replaced map with the freshness flag
        // already flipped.
        ecv.replace_shard_locations(locations);
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
    expected_encode_ts_ns: i64,
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
                expected_encode_ts_ns,
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
        expected_encode_ts_ns,
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
    expected_encode_ts_ns: i64,
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
            expected_encode_ts_ns,
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
    expected_encode_ts_ns: i64,
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

    // TODO(grpc-jwt): clusters with `jwt.signing.key` configured will
    // reject peer-to-peer VolumeEcShardRead calls until the Rust
    // crate grows an outgoing-JWT interceptor. The gap is shared
    // with every other peer gRPC call from this binary
    // (`copy_file_from_source`, `batch_delete`, …) — handling it
    // here in isolation would split the credential plumbing across
    // call sites. Re-visit when outgoing JWT signing lands as a
    // server-wide helper.
    let mut client = VolumeServerClient::with_interceptor(channel, outgoing_request_id_interceptor)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);

    let req = VolumeEcShardReadRequest {
        volume_id: vid.0,
        shard_id: shard_id as u32,
        offset: shard_offset,
        size: size as i64,
        file_key: needle_id.0,
        encode_ts_ns: expected_encode_ts_ns,
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
        // Validate the served shard's identity client-side, so the guard holds even
        // against a pre-upgrade server that ignored the request field (returns 0).
        // A mismatch fails the read; the caller recovers from parity.
        if expected_encode_ts_ns != 0 && msg.encode_ts_ns != expected_encode_ts_ns {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "ec shard {}.{} from {} belongs to a different encode run (want {} got {})",
                    vid.0, shard_id, source, expected_encode_ts_ns, msg.encode_ts_ns
                ),
            ));
        }
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
    expected_encode_ts_ns: i64,
) -> io::Result<Vec<u8>> {
    let total_shards = data_shards + parity_shards;
    let rs = ReedSolomon::new(data_shards, parity_shards).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("reed-solomon init: {:?}", e),
        )
    })?;

    let mut bufs: Vec<Option<Vec<u8>>> = vec![None; total_shards];

    // Phase 0: seed bufs from LOCALLY mounted shards. If this node
    // already holds enough sibling shards, reconstruction completes
    // without any peer fan-out — and even with a cold/incomplete
    // shard_locations cache or a failed master lookup, local
    // survivors still contribute. Mirrors Go's
    // recoverOneRemoteEcShardInterval behaviour, which is implicitly
    // local-aware because the Store fan-out targets ALL known
    // locations (including the caller's own server address); the
    // Rust port had been remote-only, so reconstructing with a cold
    // cache failed even when enough siblings were on disk.
    {
        let store = state.store.read().unwrap();
        if let Some(ecv) = store.find_ec_volume(vid) {
            for sid in 0..total_shards {
                if sid as ShardId == shard_id_to_recover {
                    continue;
                }
                if let Some(Some(shard)) = ecv.shards.get(sid) {
                    let mut buf = vec![0u8; size];
                    if shard.read_at(&mut buf, shard_offset as u64).map(|n| n == size).unwrap_or(false) {
                        bufs[sid] = Some(buf);
                    }
                }
            }
        }
    }

    // Phase 1: remote fan-out — one task per known shard location
    // we DON'T already have locally and DON'T need to recover.
    let mut tasks = Vec::new();
    for (sid, locs) in shard_locations {
        if *sid == shard_id_to_recover || locs.is_empty() {
            continue;
        }
        if bufs[*sid as usize].is_some() {
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
                expected_encode_ts_ns,
            )
            .await;
            (sid, res)
        });
    }
    let results = join_all(tasks).await;

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

// parse_grpc_address lives in `grpc_client.rs` and is re-exported
// here via the use above so this module shares a single
// HTTP↔gRPC port-translation routine with grpc_server.rs.
