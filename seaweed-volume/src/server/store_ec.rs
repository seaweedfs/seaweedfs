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
use std::fs;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::join_all;
use reed_solomon_erasure::galois_8::ReedSolomon;
use tonic::Request;

use crate::pb::master_pb::{self, seaweed_client::SeaweedClient, LookupEcVolumeRequest};
use crate::pb::volume_server_pb::{
    volume_server_client::VolumeServerClient, CopyFileRequest, VolumeEcShardReadRequest,
};
use crate::server::grpc_client::{build_grpc_endpoint, parse_grpc_address, GRPC_MAX_MESSAGE_SIZE};
use crate::server::request_id::outgoing_request_id_interceptor;
use crate::server::volume_server::{to_http_address, VolumeServerState};
use crate::storage::erasure_coding::ec_shard::ShardId;
use crate::storage::needle::needle::{get_actual_size, Needle};
use crate::storage::store_ec_reconcile::EcVolumeMissingIndex;
use crate::storage::types::*;
use crate::storage::volume::volume_file_name;

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

// ---------------------------------------------------------------------------
// Missing-index recovery (issue #10104).
//
// Mirrors weed/server/volume_grpc_erasure_coding_recover.go. EC shards whose
// .ecx index lives only on a peer server cannot be mounted locally, so the
// master never learns about them. recover_missing_ec_indexes fetches the index
// from a peer and mounts the on-disk shards. Driven on demand by
// VolumeEcShardsMount(recover_missing_index), so an operator triggers it through
// ec.rebuild rather than a background loop.
// ---------------------------------------------------------------------------

/// Recover EC volumes whose shards sit on this server while the index lives only
/// on a peer. `filter_vid` 0 recovers every orphan on this server (including
/// volumes the master never registered); otherwise just that volume. Returns the
/// number of volumes whose index was recovered.
pub(crate) async fn recover_missing_ec_indexes(
    state: &Arc<VolumeServerState>,
    filter_vid: u32,
) -> usize {
    let missing: Vec<EcVolumeMissingIndex> = {
        let store = state.store.read().unwrap();
        store
            .collect_ec_volumes_missing_index()
            .into_iter()
            .filter(|m| filter_vid == 0 || m.vid.0 == filter_vid)
            .collect()
    };
    if missing.is_empty() {
        return 0;
    }

    let self_http = to_http_address(&state.self_url).into_owned();
    let mut recovered = 0usize;
    for m in &missing {
        let peers = match cached_lookup_ec_shard_locations(state, m.vid).await {
            Ok(map) => {
                let mut peers: Vec<String> = Vec::new();
                for addrs in map.values() {
                    for a in addrs {
                        if to_http_address(a).as_ref() == self_http.as_str() {
                            continue;
                        }
                        if !peers.contains(a) {
                            peers.push(a.clone());
                        }
                    }
                }
                peers
            }
            Err(e) => {
                tracing::warn!(
                    volume_id = m.vid.0,
                    "cannot look up peers to recover missing .ecx: {}",
                    e
                );
                continue;
            }
        };
        if peers.is_empty() {
            tracing::warn!(
                volume_id = m.vid.0,
                "shards present locally but .ecx missing and no peer holds it; leaving shards unloaded"
            );
            continue;
        }
        if fetch_ec_index_from_peers(state, m, &peers).await {
            recovered += 1;
        }
    }

    if recovered > 0 {
        state.store.write().unwrap().mount_recovered_ec_shards();
        tracing::info!(
            "recovered missing EC index for {} volume(s) from peers and mounted their shards",
            recovered
        );
    }
    recovered
}

/// Try each peer in turn, copying the `.ecx` (required) and `.ecj` / `.vif`
/// (best-effort) into m's local dirs. The `.ecx` is an immutable encode-time
/// index, identical on every holder, so any peer's copy serves. The `.ecj` is a
/// per-holder deletion journal that differs across holders; the recovered node
/// adopts the source peer's deletion view, like a balanced or rebuilt shard. The
/// first peer with a non-empty `.ecx` wins.
async fn fetch_ec_index_from_peers(
    state: &Arc<VolumeServerState>,
    m: &EcVolumeMissingIndex,
    peers: &[String],
) -> bool {
    let idx_base = volume_file_name(&m.idx_dir, &m.collection, m.vid);
    let data_base = volume_file_name(&m.data_dir, &m.collection, m.vid);
    let ecx_path = format!("{}.ecx", idx_base);
    let ecj_path = format!("{}.ecj", idx_base);
    let vif_path = format!("{}.vif", data_base);

    for peer in peers {
        match fetch_ec_index_from_one_peer(state, m, peer, &ecx_path, &ecj_path, &vif_path).await {
            Ok(()) => {
                tracing::info!(
                    volume_id = m.vid.0,
                    peer = %peer,
                    "fetched missing .ecx into {}",
                    m.idx_dir
                );
                return true;
            }
            Err(e) => {
                // Remove any partial .ecx so a later attempt is not blocked by a stub.
                let _ = fs::remove_file(&ecx_path);
                tracing::debug!(
                    volume_id = m.vid.0,
                    peer = %peer,
                    "fetch missing .ecx failed: {}",
                    e
                );
            }
        }
    }
    false
}

async fn fetch_ec_index_from_one_peer(
    state: &Arc<VolumeServerState>,
    m: &EcVolumeMissingIndex,
    peer: &str,
    ecx_path: &str,
    ecj_path: &str,
    vif_path: &str,
) -> io::Result<()> {
    let grpc_addr =
        parse_grpc_address(peer).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let channel = build_grpc_endpoint(&grpc_addr, state.outgoing_grpc_tls.as_ref())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("connect {}: {}", peer, e)))?;
    let mut client = VolumeServerClient::with_interceptor(channel, outgoing_request_id_interceptor)
        .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE)
        .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE);

    let copy_req = |ext: &str, ignore_not_found: bool| CopyFileRequest {
        volume_id: m.vid.0,
        collection: m.collection.clone(),
        is_ec_volume: true,
        ext: ext.to_string(),
        compaction_revision: u32::MAX,
        stop_offset: i64::MAX as u64,
        ignore_source_file_not_found: ignore_not_found,
        ..Default::default()
    };

    // .ecx is mandatory and written in place (create/truncate); a peer without it
    // errors and the caller moves on.
    let stream = client
        .copy_file(copy_req(".ecx", false))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("copy .ecx: {}", e)))?
        .into_inner();
    drain_copy_stream(stream, ecx_path, false).await?;

    let meta = fs::metadata(ecx_path)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("stat copied .ecx: {}", e)))?;
    if meta.is_dir() || meta.len() == 0 {
        let _ = fs::remove_file(ecx_path);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("peer {} served an unusable .ecx (size {})", peer, meta.len()),
        ));
    }

    // .ecj is the source peer's deletion journal (appended); .vif carries EC
    // params. Both are best-effort: a missing .ecj is recreated at mount and a
    // missing .vif falls back to default EC parameters. A failed .ecj append
    // leaves a partial file, so drop it.
    match client.copy_file(copy_req(".ecj", true)).await {
        Ok(resp) => {
            if let Err(e) = drain_copy_stream(resp.into_inner(), ecj_path, true).await {
                tracing::warn!(volume_id = m.vid.0, peer = %peer, "copy .ecj: {}", e);
                let _ = fs::remove_file(ecj_path);
            }
        }
        Err(e) => tracing::warn!(volume_id = m.vid.0, peer = %peer, "copy .ecj: {}", e),
    }

    match client.copy_file(copy_req(".vif", true)).await {
        Ok(resp) => {
            if let Err(e) = drain_copy_stream(resp.into_inner(), vif_path, false).await {
                tracing::warn!(volume_id = m.vid.0, peer = %peer, "copy .vif: {}", e);
            }
        }
        Err(e) => tracing::warn!(volume_id = m.vid.0, peer = %peer, "copy .vif: {}", e),
    }

    Ok(())
}

/// Drain a CopyFile stream into a local file, appending or truncating.
async fn drain_copy_stream(
    mut stream: tonic::Streaming<crate::pb::volume_server_pb::CopyFileResponse>,
    dest_path: &str,
    append: bool,
) -> io::Result<()> {
    use std::io::Write;
    let mut file = if append {
        fs::OpenOptions::new().create(true).append(true).open(dest_path)
    } else {
        fs::File::create(dest_path)
    }
    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("create {}: {}", dest_path, e)))?;
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("recv {}: {}", dest_path, e)))?
    {
        file.write_all(&chunk.file_content)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("write {}: {}", dest_path, e)))?;
    }
    Ok(())
}
