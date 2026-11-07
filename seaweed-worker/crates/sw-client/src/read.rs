//! Data plane (read): resolve a file's chunks into visible byte intervals and
//! fetch the requested range from volume servers.
//!
//! A file is a list of immutable `FileChunk`s mapping logical byte ranges to
//! needles. Overlapping chunks are resolved latest-`modified_ts_ns`-wins per
//! byte; `is_chunk_manifest` chunks are expanded (fetched + gunzipped +
//! proto-decoded) recursively; gaps read as zeros (sparse files). Encrypted
//! chunks are decoded by the caller's [`ChunkDecoder`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use prost::Message;

use crate::cache::ChunkCache;
use crate::filer::FilerClient;
use crate::filer_pb::{Entry, FileChunk, FileChunkManifest, FileId};
use crate::location_cache::LocationCache;
use crate::transport::{HttpTransport, VolumeTransport};

/// Max chunk fetches in flight per `read_file` call (the read's own chunks).
const READ_CONCURRENCY: usize = 16;
/// How many upcoming chunks to prefetch ahead of the current read position.
const PREFETCH_AHEAD: usize = 8;
/// Max background prefetch fetches in flight across all reads.
const PREFETCH_CONCURRENCY: usize = 8;
/// Whole-chunk cache capacity (bytes). Immutable-by-fileId, so coherence-safe.
const CHUNK_CACHE_CAP_BYTES: usize = 256 * 1024 * 1024;
/// A read whose offset is within this many bytes of a file's read frontier
/// counts as sequential (absorbs reordered concurrent readahead; rejects far
/// random jumps), enabling whole-chunk caching + prefetch-ahead.
const SEQ_TOLERANCE: u64 = 8 * 1024 * 1024;
/// Max distinct files tracked for sequential detection (bounds the map).
const STREAM_TRACK_CAP: usize = 4096;
/// Hysteresis depth: the mode (sequential/random) only flips after this many
/// consecutive contrary reads, so a single outlier doesn't thrash it.
const SEQ_HYSTERESIS: i8 = 3;

/// One parallel chunk fetch's result: `(dst, range bytes)`.
type ChunkFetch = Result<(usize, Vec<u8>)>;

/// Turns a chunk's stored bytes into the file's bytes, for chunks the filer
/// encrypted (`cipher_key`) or an S3 gateway encrypted (`sse_type`). Every
/// chunk a read touches goes through the decoder, manifest chunks included;
/// a plain chunk comes back as it is.
pub trait ChunkDecoder: Send + Sync {
    /// Whether `chunk` decodes only as a whole, so a ranged read fetches the
    /// entire needle.
    fn needs_whole_chunk(&self, chunk: &FileChunk) -> bool;
    /// Decodes `data`, the stored bytes of `chunk` starting `offset` bytes
    /// into it.
    fn decode(&self, chunk: &FileChunk, offset: u64, data: Vec<u8>) -> Result<Vec<u8>>;
}

/// How [`read_resolved`] fetches.
#[derive(Clone, Default)]
pub struct ReadOptions {
    /// Fetch exactly the bytes asked for, with no whole-needle caching and no
    /// prefetch: for callers with their own cache, or random reads of large
    /// files.
    pub exact: bool,
    pub decoder: Option<Arc<dyn ChunkDecoder>>,
}

fn encoded(chunk: &FileChunk) -> bool {
    !chunk.cipher_key.is_empty() || chunk.sse_type != 0
}

/// Per-file read-access state for the sequential/random gate: the read frontier
/// (max end offset seen) plus a hysteresis counter (sequential when >= 0).
#[derive(Clone, Copy)]
struct StreamPattern {
    frontier: u64,
    counter: i8,
}

/// Build the SeaweedFS fileId string for a chunk: `"<vid>,<hex(key||cookie)>"`.
pub fn file_id_string(chunk: &FileChunk) -> Option<String> {
    if !chunk.file_id.is_empty() {
        return Some(chunk.file_id.clone());
    }
    chunk.fid.as_ref().map(fid_to_string)
}

fn fid_to_string(fid: &FileId) -> String {
    format!(
        "{},{}",
        fid.volume_id,
        needle_id_cookie(fid.file_key, fid.cookie)
    )
}

/// Mirror SeaweedFS `formatNeedleIdCookie`: 8-byte big-endian key followed by
/// 4-byte big-endian cookie, with leading zero bytes (only within the key)
/// stripped, then lowercase hex.
fn needle_id_cookie(key: u64, cookie: u32) -> String {
    let mut bytes = [0u8; 12];
    bytes[0..8].copy_from_slice(&key.to_be_bytes());
    bytes[8..12].copy_from_slice(&cookie.to_be_bytes());
    let mut start = 0usize;
    while start < 8 && bytes[start] == 0 {
        start += 1;
    }
    let mut s = String::with_capacity((12 - start) * 2);
    for b in &bytes[start..] {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn chunk_volume_id(chunk: &FileChunk) -> Result<u32> {
    if let Some(fid) = &chunk.fid {
        return Ok(fid.volume_id);
    }
    if let Some((vid, _)) = chunk.file_id.split_once(',') {
        return vid.parse::<u32>().context("parse volume id from file_id");
    }
    bail!("chunk has neither fid nor file_id")
}

/// Removes its single-flight gate from the `inflight` map when the fetch scope
/// exits, on every path. Without it, a failed or early-returning fetch would
/// leave its gate entry behind, growing the map by one per distinct fileId.
///
/// It removes the entry only when the map still holds *this* gate (pointer
/// equality): a waiter can complete after a newer request has already installed
/// a fresh gate for the same fileId, and dropping that one would defeat its
/// single-flight (a redundant re-fetch, never incorrect — needles are immutable).
struct GateGuard {
    inflight: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    file_id: String,
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        let mut inflight = self.inflight.lock().unwrap();
        if inflight
            .get(&self.file_id)
            .is_some_and(|g| Arc::ptr_eq(g, &self.gate))
        {
            inflight.remove(&self.file_id);
        }
    }
}

/// Reads chunk bytes from volume servers, caching volume-id → URL locations.
/// The actual byte transfer is delegated to a pluggable [`VolumeTransport`]
/// (HTTP by default; RDMA later). Cheap to clone and safe to share across
/// concurrent reads — the location cache is interior-mutable, so all methods
/// take `&self`.
#[derive(Clone)]
pub struct VolumeReader {
    transport: Arc<dyn VolumeTransport>,
    location_cache: Arc<LocationCache>,
    /// fileId -> whole-chunk bytes (immutable needles, coherence-safe).
    chunks: Arc<Mutex<ChunkCache>>,
    /// Bounds background prefetch fetches across all in-flight reads.
    prefetch_sem: Arc<tokio::sync::Semaphore>,
    /// Per-file read-access pattern (first-chunk fileId -> frontier + hysteresis
    /// counter), for the sequential-vs-random gate. Bounded by STREAM_TRACK_CAP.
    streams: Arc<Mutex<HashMap<String, StreamPattern>>>,
    /// Single-flight gates: fileId -> per-needle async lock. The prefetch re-issues
    /// the next chunks on every read upcall; without this, each re-issue (and each
    /// concurrent sub-read) races the in-flight fetch and re-GETs the whole 4 MiB
    /// needle — measured ~5x over-fetch. The gate makes concurrent fetches of one
    /// needle share a single GET.
    inflight: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
}

impl Default for VolumeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl VolumeReader {
    pub fn new() -> Self {
        Self::with_transport(Arc::new(HttpTransport::new()))
    }

    /// Construct with a specific data-plane transport (e.g. an RDMA transport),
    /// using the default whole-chunk cache capacity.
    pub fn with_transport(transport: Arc<dyn VolumeTransport>) -> Self {
        Self::with_transport_capped(transport, CHUNK_CACHE_CAP_BYTES)
    }

    /// Construct with a specific transport and whole-chunk cache capacity (bytes).
    /// The cap bounds resident read-cache memory: a browse/metadata-heavy mount can
    /// run a much smaller cache than a streaming one. `0` disables caching.
    pub fn with_transport_capped(
        transport: Arc<dyn VolumeTransport>,
        chunk_cache_cap_bytes: usize,
    ) -> Self {
        Self {
            transport,
            location_cache: Arc::new(LocationCache::new()),
            chunks: Arc::new(Mutex::new(ChunkCache::new(chunk_cache_cap_bytes))),
            prefetch_sem: Arc::new(tokio::sync::Semaphore::new(PREFETCH_CONCURRENCY)),
            streams: Arc::new(Mutex::new(HashMap::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn locations(&self, client: &mut FilerClient, vid: u32) -> Result<Vec<String>> {
        if let Some(urls) = self.location_cache.locations(vid) {
            return Ok(urls);
        }
        let urls = client.lookup_volume(vid).await?;
        if urls.is_empty() {
            bail!("no locations for volume {vid}");
        }
        self.location_cache.learn(vid, urls.clone());
        Ok(urls)
    }

    /// Locations currently cached for `vid`, without consulting the filer.
    pub fn cached_locations(&self, vid: u32) -> Option<Vec<String>> {
        self.location_cache.locations(vid)
    }

    /// Forget where a volume lives so the next read looks it up again.
    pub fn invalidate_volume(&self, vid: u32) {
        self.location_cache.invalidate(vid);
    }

    /// Start a new location generation, for when the metadata source has to be
    /// relearned (the filer subscription reconnecting, say). What we already
    /// know stays readable until it is relearned or expires, so reads in flight
    /// are not stranded. Counterpart of the Go client's resetVidMap.
    pub fn reset_locations(&self) {
        self.location_cache.reset();
    }

    /// Drop the cached locations for the volume behind `file_id` after a failed
    /// fetch, so the next read relearns where the volume lives instead of
    /// retrying servers it may have moved off. Only the `urls` the fetch
    /// actually used are dropped: a slow read can fail long after another
    /// lookup has learned where the volume moved to, and must not throw that
    /// fresh answer away.
    fn invalidate_after_failure(&self, file_id: &str, urls: &[String]) {
        if let Some(vid) = file_id
            .split(',')
            .next()
            .and_then(|v| v.parse::<u32>().ok())
        {
            self.location_cache.invalidate_stale(vid, urls);
        }
    }

    /// Fetch a fileId from any of its volume servers, optionally a byte range,
    /// via the configured transport.
    async fn get(
        &self,
        urls: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>> {
        self.transport_read(urls, file_id, range).await
    }

    /// Every byte fetch goes through here so a failure always invalidates the
    /// locations it was made against.
    async fn transport_read(
        &self,
        urls: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>> {
        let result = self.transport.read(urls, file_id, range).await;
        if result.is_err() {
            self.invalidate_after_failure(file_id, urls);
        }
        result
    }

    /// Fetch a whole needle by fileId, served from / filling the chunk cache.
    /// Needles are immutable per fileId, so a cache hit is always valid. The
    /// mutex is only held to look up / insert — never across the network await.
    async fn fetch_and_cache_whole(&self, urls: &[String], file_id: &str) -> Result<Arc<Vec<u8>>> {
        if let Some(hit) = self.chunks.lock().unwrap().get(file_id) {
            return Ok(hit);
        }
        // Single-flight: only one fetch per needle proceeds; others wait on the
        // per-fileId gate, then hit the cache the winner just filled (instead of
        // each issuing a duplicate GET of the whole 4 MiB needle).
        let gate = self
            .inflight
            .lock()
            .unwrap()
            .entry(file_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        // Drop the gate from the map on EVERY exit path (fill, cache-hit-after-lock,
        // or a failed transport read) via this guard — an early `?` return here
        // would otherwise leak one entry per distinct fileId that ever failed to
        // fetch, growing `inflight` unboundedly on a flaky volume server. Waiters
        // re-check and hit the just-filled cache; newcomers take the fast path.
        let _gate_guard = GateGuard {
            inflight: self.inflight.clone(),
            file_id: file_id.to_string(),
            gate: gate.clone(),
        };
        let _g = gate.lock().await;
        if let Some(hit) = self.chunks.lock().unwrap().get(file_id) {
            return Ok(hit);
        }
        let bytes = self.transport_read(urls, file_id, None).await?;
        let arc = Arc::new(bytes);
        self.chunks
            .lock()
            .unwrap()
            .put(file_id.to_string(), arc.clone());
        Ok(arc)
    }

    /// Fetch exactly the needle's `[off, off+len)` bytes for one read.
    /// - `cache_whole` (sequential reads): fetch + cache the whole needle, then
    ///   slice — so later sub-reads and prefetched chunks are served from memory.
    /// - otherwise (random reads): serve from cache if already present, else
    ///   fetch only the requested range and do NOT cache — avoids over-reading a
    ///   whole 4 MB needle for a small random read.
    async fn fetch_ranged(
        &self,
        urls: &[String],
        file_id: &str,
        off: usize,
        len: usize,
        cache_whole: bool,
    ) -> Result<Vec<u8>> {
        if cache_whole {
            let whole = self.fetch_and_cache_whole(urls, file_id).await?;
            let end = (off + len).min(whole.len());
            return Ok(if off < end {
                whole[off..end].to_vec()
            } else {
                Vec::new()
            });
        }
        if let Some(whole) = self.chunks.lock().unwrap().get(file_id) {
            let end = (off + len).min(whole.len());
            return Ok(if off < end {
                whole[off..end].to_vec()
            } else {
                Vec::new()
            });
        }
        self.transport_read(urls, file_id, Some((off as u64, len as u64)))
            .await
    }

    fn copy_range_from_whole(whole: &[u8], off: usize, len: usize, dst: &mut [u8]) -> usize {
        let end = (off + len).min(whole.len());
        if off >= end {
            return 0;
        }
        let n = (end - off).min(dst.len());
        dst[..n].copy_from_slice(&whole[off..off + n]);
        n
    }

    fn copy_cached_range(
        &self,
        file_id: &str,
        off: usize,
        len: usize,
        dst: &mut [u8],
    ) -> Option<usize> {
        let whole = self.chunks.lock().unwrap().get(file_id)?;
        Some(Self::copy_range_from_whole(&whole, off, len, dst))
    }

    async fn fetch_ranged_into(
        &self,
        urls: &[String],
        file_id: &str,
        off: usize,
        len: usize,
        cache_whole: bool,
        dst: &mut [u8],
    ) -> Result<usize> {
        if cache_whole {
            let whole = self.fetch_and_cache_whole(urls, file_id).await?;
            return Ok(Self::copy_range_from_whole(&whole, off, len, dst));
        }
        if let Some(n) = self.copy_cached_range(file_id, off, len, dst) {
            return Ok(n);
        }
        let bytes = self
            .transport_read(urls, file_id, Some((off as u64, len as u64)))
            .await?;
        let n = bytes.len().min(dst.len());
        dst[..n].copy_from_slice(&bytes[..n]);
        Ok(n)
    }

    /// Update a file's read-access pattern and report whether reads should be
    /// treated as sequential — frontier + tolerance, smoothed by hysteresis:
    ///   - `near` = this read starts within SEQ_TOLERANCE of the read frontier
    ///     (the window absorbs reordered/concurrent readahead, unlike strict
    ///     contiguity, while still rejecting far random jumps);
    ///   - the hysteresis counter ramps within [-SEQ_HYSTERESIS, SEQ_HYSTERESIS],
    ///     so a lone outlier read doesn't flip the mode;
    ///   - sequential = counter >= 0. A new file starts at frontier 0 / counter 0,
    ///     so a stream from the start is sequential and a far first read is random.
    fn note_sequential(&self, key: &str, offset: u64, end: u64) -> bool {
        let mut m = self.streams.lock().unwrap();
        if !m.contains_key(key) && m.len() >= STREAM_TRACK_CAP {
            m.clear(); // crude bound; transient streams just re-confirm next read
        }
        let p = m.entry(key.to_string()).or_insert(StreamPattern {
            frontier: 0,
            counter: 0,
        });
        let near = offset.abs_diff(p.frontier) <= SEQ_TOLERANCE;
        p.frontier = p.frontier.max(end);
        if near {
            p.counter = (p.counter + 1).min(SEQ_HYSTERESIS);
        } else {
            p.counter = (p.counter - 1).max(-SEQ_HYSTERESIS);
        }
        p.counter >= 0
    }

    /// Spawn bounded background prefetches of upcoming whole chunks into the
    /// cache. Fire-and-forget: errors are ignored (the real read will retry),
    /// already-cached chunks are skipped, and the semaphore caps fan-out.
    fn prefetch(&self, ahead: Vec<(String, Vec<String>)>) {
        for (file_id, urls) in ahead {
            if self.chunks.lock().unwrap().contains(&file_id) {
                continue;
            }
            let volumes = self.clone();
            let sem = self.prefetch_sem.clone();
            tokio::spawn(async move {
                let Ok(_permit) = sem.acquire_owned().await else {
                    return;
                };
                let _ = volumes.fetch_and_cache_whole(&urls, &file_id).await;
            });
        }
    }
}

/// A non-overlapping visible byte interval `[start, stop)` of the file, backed
/// by part of one chunk.
#[derive(Clone)]
struct Visible {
    start: i64,
    stop: i64,
    file_id: String,
    vid: u32,
    offset_in_chunk: i64,
    has_cipher: bool,
    /// Index of the backing chunk in the resolved data chunks.
    chunk: usize,
}

/// Recursively expand manifest chunks into plain data chunks.
async fn expand_chunks(
    client: &mut FilerClient,
    volumes: &VolumeReader,
    chunks: &[FileChunk],
    decoder: Option<&dyn ChunkDecoder>,
) -> Result<Vec<FileChunk>> {
    let mut data = Vec::new();
    for chunk in chunks {
        if !chunk.is_chunk_manifest {
            data.push(chunk.clone());
            continue;
        }
        let fid = file_id_string(chunk).context("manifest chunk missing fileId")?;
        let vid = chunk_volume_id(chunk)?;
        let urls = volumes.locations(client, vid).await?;
        // The volume server transparently decompresses on a plain GET (we don't
        // send Accept-Encoding: gzip), so the bytes are the raw manifest proto,
        // unless the filer encrypted them.
        let raw = volumes.get(&urls, &fid, None).await?;
        let raw = match decoder {
            Some(decoder) => decoder.decode(chunk, 0, raw)?,
            None if encoded(chunk) => {
                bail!("encrypted manifest chunk {fid} needs a decoder")
            }
            None => raw,
        };
        let manifest = FileChunkManifest::decode(raw.as_slice())
            .with_context(|| format!("decode manifest {fid}"))?;
        let sub = Box::pin(expand_chunks(client, volumes, &manifest.chunks, decoder)).await?;
        data.extend(sub);
    }
    Ok(data)
}

/// Resolve data chunks into non-overlapping visible intervals (latest write wins).
fn resolve_visibles(chunks: &[FileChunk]) -> Vec<Visible> {
    if let Some(visibles) = resolve_visibles_ordered_non_overlapping(chunks) {
        return visibles;
    }

    let mut order: Vec<usize> = (0..chunks.len()).collect();
    order.sort_by_key(|&i| (chunks[i].modified_ts_ns, chunks[i].offset));

    let mut visibles: Vec<Visible> = Vec::new();
    for &i in &order {
        let c = &chunks[i];
        let start = c.offset;
        let stop = c.offset + c.size as i64;
        if stop <= start {
            continue;
        }
        let Some(file_id) = file_id_string(c) else {
            continue;
        };
        let Ok(vid) = chunk_volume_id(c) else {
            continue;
        };

        // This chunk is newer than everything already placed, so it overwrites
        // any overlapping region: trim existing intervals around [start, stop).
        let mut next: Vec<Visible> = Vec::with_capacity(visibles.len() + 1);
        for v in visibles.drain(..) {
            if v.stop <= start || v.start >= stop {
                next.push(v);
                continue;
            }
            if v.start < start {
                let mut left = v.clone();
                left.stop = start;
                next.push(left);
            }
            if v.stop > stop {
                let mut right = v.clone();
                right.offset_in_chunk += stop - v.start;
                right.start = stop;
                next.push(right);
            }
            // middle is overwritten by the newer chunk
        }
        next.push(Visible {
            start,
            stop,
            file_id,
            vid,
            offset_in_chunk: 0,
            has_cipher: !c.cipher_key.is_empty(),
            chunk: i,
        });
        next.sort_by_key(|v| v.start);
        visibles = next;
    }
    visibles
}

fn resolve_visibles_ordered_non_overlapping(chunks: &[FileChunk]) -> Option<Vec<Visible>> {
    let mut visibles = Vec::with_capacity(chunks.len());
    let mut last_stop = i64::MIN;

    for (i, c) in chunks.iter().enumerate() {
        let start = c.offset;
        let stop = c.offset + c.size as i64;
        if stop <= start || start < last_stop {
            return None;
        }
        let file_id = file_id_string(c)?;
        let vid = chunk_volume_id(c).ok()?;
        visibles.push(Visible {
            start,
            stop,
            file_id,
            vid,
            offset_in_chunk: 0,
            has_cipher: !c.cipher_key.is_empty(),
            chunk: i,
        });
        last_stop = stop;
    }

    Some(visibles)
}

/// Drop chunks that contribute no visible bytes — i.e. ones fully overwritten by
/// newer (higher `modified_ts_ns`) chunks. Cheap, metadata-only: no data is read
/// or moved. Used after a streaming flush appends overlay chunks, so a full
/// rewrite leaves the old chunks unreferenced (reclaimable) instead of leaking
/// them. Conservatively a no-op when manifest chunks are present (their offsets
/// span sub-chunks, so naive overlap resolution doesn't apply).
pub fn compact_shadowed(chunks: Vec<FileChunk>) -> Vec<FileChunk> {
    if chunks.len() < 2 || chunks.iter().any(|c| c.is_chunk_manifest) {
        return chunks;
    }
    let live: std::collections::HashSet<String> = resolve_visibles(&chunks)
        .into_iter()
        .map(|v| v.file_id)
        .collect();
    chunks
        .into_iter()
        .filter(|c| file_id_string(c).map(|f| live.contains(&f)).unwrap_or(true))
        .collect()
}

/// A file's chunks with manifests expanded and overlaps resolved, reusable
/// across reads of one entry.
#[derive(Default)]
pub struct ResolvedChunks {
    chunks: Vec<FileChunk>,
    visibles: Vec<Visible>,
}

impl ResolvedChunks {
    /// Resolves chunks holding no manifests, without touching the network.
    pub fn from_data_chunks(chunks: Vec<FileChunk>) -> Result<Self> {
        if let Some(manifest) = chunks.iter().find(|c| c.is_chunk_manifest) {
            bail!(
                "chunk {} is a manifest; use resolve_chunks",
                file_id_string(manifest).unwrap_or_default()
            );
        }
        let visibles = resolve_visibles(&chunks);
        Ok(Self { chunks, visibles })
    }

    /// One past the last byte any chunk covers.
    pub fn end(&self) -> u64 {
        self.chunks
            .iter()
            .map(|c| c.offset.max(0) as u64 + c.size)
            .max()
            .unwrap_or(0)
    }
}

/// Expands the manifests among `chunks` and resolves overlaps. Encrypted
/// manifests are decoded with `options.decoder`.
pub async fn resolve_chunks(
    client: &mut FilerClient,
    volumes: &VolumeReader,
    chunks: &[FileChunk],
    options: &ReadOptions,
) -> Result<ResolvedChunks> {
    let chunks = expand_chunks(client, volumes, chunks, options.decoder.as_deref()).await?;
    let visibles = resolve_visibles(&chunks);
    Ok(ResolvedChunks { chunks, visibles })
}

/// Read `[offset, offset+size)` of a file, clamped to its size. Gaps read as
/// zeros. Returns at most `size` bytes (fewer at EOF).
pub async fn read_file(
    client: &mut FilerClient,
    volumes: &VolumeReader,
    entry: &Entry,
    offset: u64,
    size: u64,
) -> Result<Vec<u8>> {
    read_file_with(
        client,
        volumes,
        entry,
        offset,
        size,
        &ReadOptions::default(),
    )
    .await
}

/// [`read_file`] with [`ReadOptions`].
pub async fn read_file_with(
    client: &mut FilerClient,
    volumes: &VolumeReader,
    entry: &Entry,
    offset: u64,
    size: u64,
    options: &ReadOptions,
) -> Result<Vec<u8>> {
    let resolved = resolve_chunks(client, volumes, &entry.chunks, options).await?;
    read_resolved(client, volumes, &resolved, entry, offset, size, options).await
}

/// One chunk's part of a read: `off`/`len` within the needle, `dst` in the
/// output.
struct Fetch {
    dst: usize,
    chunk: FileChunk,
    file_id: String,
    urls: Vec<String>,
    off: usize,
    len: usize,
    /// The decoder needs the whole needle.
    whole: bool,
}

/// Fetches one piece of a chunk, decoding it when the read has a decoder.
async fn fetch_piece(
    volumes: &VolumeReader,
    f: &Fetch,
    cache_whole: bool,
    decoder: Option<&dyn ChunkDecoder>,
) -> Result<Vec<u8>> {
    if f.whole {
        let raw = if cache_whole {
            volumes
                .fetch_and_cache_whole(&f.urls, &f.file_id)
                .await?
                .to_vec()
        } else {
            volumes.transport_read(&f.urls, &f.file_id, None).await?
        };
        let data = decoder
            .context("a whole-chunk fetch needs a decoder")?
            .decode(&f.chunk, 0, raw)?;
        let end = (f.off + f.len).min(data.len());
        return Ok(if f.off < end {
            data[f.off..end].to_vec()
        } else {
            Vec::new()
        });
    }
    let raw = volumes
        .fetch_ranged(&f.urls, &f.file_id, f.off, f.len, cache_whole)
        .await?;
    match decoder {
        Some(decoder) => decoder.decode(&f.chunk, f.off as u64, raw),
        None => Ok(raw),
    }
}

/// Read `[offset, offset+size)` of `entry` through its resolved chunks: at
/// most `size` bytes, fewer at the end of the file, gaps as zeros.
pub async fn read_resolved(
    client: &mut FilerClient,
    volumes: &VolumeReader,
    resolved: &ResolvedChunks,
    entry: &Entry,
    offset: u64,
    size: u64,
    options: &ReadOptions,
) -> Result<Vec<u8>> {
    let file_size = entry
        .attributes
        .as_ref()
        .map_or(0, |a| a.file_size)
        .max(resolved.end())
        .max(entry.content.len() as u64);
    if offset >= file_size || size == 0 {
        return Ok(Vec::new());
    }
    let end = offset.saturating_add(size).min(file_size);
    if end - offset > crate::MAX_READ_BYTES {
        bail!(
            "read span too large ({} bytes, cap {})",
            end - offset,
            crate::MAX_READ_BYTES
        );
    }
    let want = (end - offset) as usize;

    // Small files store their bytes inline on the entry.
    if !entry.content.is_empty() {
        let content = &entry.content;
        let s = (offset as usize).min(content.len());
        let e = (end as usize).min(content.len());
        let mut out = vec![0u8; want];
        out[..(e - s)].copy_from_slice(&content[s..e]);
        return Ok(out);
    }

    let visibles = &resolved.visibles;

    let r_start = offset as i64;
    let r_stop = end as i64;

    // Detect sequential vs random access for this file (keyed by its first
    // chunk's stable fileId) to gate prefetch + whole-chunk caching.
    let stream_key = visibles.first().map(|v| v.file_id.clone());
    let sequential = !options.exact
        && stream_key
            .as_deref()
            .map(|k| volumes.note_sequential(k, offset, end))
            .unwrap_or(false);

    // Pass 1: descriptors for the chunks this read needs. Volume location lookups
    // need `&mut client`, so they're resolved serially here (cached after first);
    // `off` is the byte offset WITHIN the needle, `dst` the offset in `out`.
    let mut fetches: Vec<Fetch> = Vec::new();
    for v in visibles {
        let cs = v.start.max(r_start);
        let ce = v.stop.min(r_stop);
        if cs >= ce {
            continue;
        }
        let chunk = &resolved.chunks[v.chunk];
        let whole = match &options.decoder {
            Some(decoder) => decoder.needs_whole_chunk(chunk),
            None if encoded(chunk) => bail!("encrypted chunk {} needs a decoder", v.file_id),
            None => false,
        };
        let urls = volumes.locations(client, v.vid).await?;
        let dst = (cs - r_start) as usize;
        let len = (ce - cs) as usize;
        fetches.push(Fetch {
            dst,
            chunk: chunk.clone(),
            file_id: v.file_id.clone(),
            urls,
            off: (v.offset_in_chunk + (cs - v.start)) as usize,
            len,
            whole,
        });
    }
    // One chunk covering the whole read, the common shape of an exact read
    // of a large file: hand its bytes back without a buffer to zero and copy
    // into.
    if options.exact && fetches.len() == 1 && fetches[0].dst == 0 && fetches[0].len == want {
        let mut bytes =
            fetch_piece(volumes, &fetches[0], false, options.decoder.as_deref()).await?;
        bytes.resize(want, 0);
        return Ok(bytes);
    }

    // Zero-initialised so gaps between chunks, sparse regions, and short-read
    // tails read back as zero. `vec![0u8; want]` lowers to `alloc_zeroed`, so a
    // large buffer is backed by pre-zeroed OS pages (no memset) that the fetches
    // below overwrite in place.
    let mut out = vec![0u8; want];

    // Prefetch-ahead ONLY for sequential streams: background whole-chunk fetches
    // of the next PREFETCH_AHEAD chunks past this read so the following (small,
    // single-chunk) upcalls hit the cache. Skipped for random access.
    if sequential {
        let mut ahead: Vec<(String, Vec<String>)> = Vec::new();
        for v in visibles {
            if v.start >= r_stop && !v.has_cipher {
                let urls = volumes.locations(client, v.vid).await?;
                ahead.push((v.file_id.clone(), urls));
                if ahead.len() >= PREFETCH_AHEAD {
                    break;
                }
            }
        }
        volumes.prefetch(ahead);
    }

    // Pass 2: fetch each chunk's requested range (cache-aware), bounded by
    // READ_CONCURRENCY. Sequential reads fetch+cache whole needles (so sub-reads
    // and prefetched chunks hit memory); random reads fetch only the exact range.
    if fetches.len() <= 1 {
        if let Some(f) = fetches.into_iter().next() {
            let dst_end = (f.dst + f.len).min(want);
            if options.decoder.is_none() {
                volumes
                    .fetch_ranged_into(
                        &f.urls,
                        &f.file_id,
                        f.off,
                        f.len,
                        sequential,
                        &mut out[f.dst..dst_end],
                    )
                    .await?;
            } else {
                let bytes =
                    fetch_piece(volumes, &f, sequential, options.decoder.as_deref()).await?;
                let n = bytes.len().min(dst_end - f.dst);
                out[f.dst..f.dst + n].copy_from_slice(&bytes[..n]);
            }
        }
    } else {
        let sem = Arc::new(tokio::sync::Semaphore::new(READ_CONCURRENCY));
        let mut set: tokio::task::JoinSet<ChunkFetch> = tokio::task::JoinSet::new();
        for f in fetches {
            let volumes = volumes.clone();
            let sem = sem.clone();
            let decoder = options.decoder.clone();
            set.spawn(async move {
                let _permit = sem.acquire_owned().await.expect("read semaphore closed");
                let bytes = fetch_piece(&volumes, &f, sequential, decoder.as_deref()).await?;
                Ok((f.dst, bytes))
            });
        }
        while let Some(joined) = set.join_next().await {
            let (dst, bytes) = joined.context("chunk fetch task panicked")??;
            let n = bytes.len().min(want - dst);
            out[dst..dst + n].copy_from_slice(&bytes[..n]);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filer_pb::FileId;

    #[test]
    fn fileid_format_strips_leading_key_zeros() {
        // vid 7, key 0x1234, cookie 0xaabbccdd
        let fid = FileId {
            volume_id: 7,
            file_key: 0x1234,
            cookie: 0xaabb_ccdd,
        };
        // key 8 bytes BE = 00 00 00 00 00 00 12 34 -> strip 6 leading zeros -> 12 34
        // cookie = aa bb cc dd  => "1234aabbccdd"
        assert_eq!(fid_to_string(&fid), "7,1234aabbccdd");
    }

    #[test]
    fn fileid_zero_key_keeps_cookie() {
        let fid = FileId {
            volume_id: 3,
            file_key: 0,
            cookie: 0x0000_00ff,
        };
        assert_eq!(fid_to_string(&fid), "3,000000ff");
    }

    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts how many actual transport reads (network fetches) happen; returns
    /// the fileId bytes so callers can check correctness.
    struct MockTransport {
        reads: AtomicUsize,
    }
    impl MockTransport {
        fn new() -> Self {
            Self {
                reads: AtomicUsize::new(0),
            }
        }
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }
    #[async_trait]
    impl VolumeTransport for MockTransport {
        async fn read(
            &self,
            _locations: &[String],
            file_id: &str,
            _range: Option<(u64, u64)>,
        ) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(file_id.as_bytes().to_vec())
        }
        async fn write(
            &self,
            _location: &str,
            _file_id: &str,
            _auth: &str,
            _filename: &str,
            _data: bytes::Bytes,
        ) -> Result<()> {
            Ok(())
        }
        fn name(&self) -> &'static str {
            "mock"
        }
    }

    async fn run_pending_prefetch(mock: &MockTransport, want: usize) {
        for _ in 0..10_000 {
            if mock.reads() >= want {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("prefetch did not complete (reads={})", mock.reads());
    }

    /// Always fails reads — to exercise the fetch error path.
    struct FailingTransport;
    #[async_trait]
    impl VolumeTransport for FailingTransport {
        async fn read(
            &self,
            _locations: &[String],
            _file_id: &str,
            _range: Option<(u64, u64)>,
        ) -> Result<Vec<u8>> {
            bail!("simulated volume read failure")
        }
        async fn write(
            &self,
            _location: &str,
            _file_id: &str,
            _auth: &str,
            _filename: &str,
            _data: bytes::Bytes,
        ) -> Result<()> {
            Ok(())
        }
        fn name(&self) -> &'static str {
            "failing"
        }
    }

    #[tokio::test]
    async fn fetch_whole_caches_by_fileid() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        let urls = vec!["h:1".to_string()];
        let a = vr.fetch_and_cache_whole(&urls, "1,aa").await.unwrap();
        let b = vr.fetch_and_cache_whole(&urls, "1,aa").await.unwrap();
        assert_eq!(&*a, b"1,aa"); // correct bytes
        assert_eq!(&*a, &*b);
        assert_eq!(mock.reads(), 1, "the second read must hit the cache");
    }

    #[test]
    fn gate_guard_only_removes_its_own_gate() {
        // A waiter's guard must not evict a newer gate installed for the same
        // fileId by a later request (which would defeat that request's
        // single-flight). Pointer-equality is what protects it.
        let inflight: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mine = Arc::new(tokio::sync::Mutex::new(()));
        inflight.lock().unwrap().insert("1,aa".into(), mine.clone());
        let guard = GateGuard {
            inflight: inflight.clone(),
            file_id: "1,aa".into(),
            gate: mine.clone(),
        };
        // A later request replaces the gate for the same fileId.
        let newer = Arc::new(tokio::sync::Mutex::new(()));
        inflight
            .lock()
            .unwrap()
            .insert("1,aa".into(), newer.clone());
        drop(guard); // must leave `newer` in place
        let current = inflight.lock().unwrap().get("1,aa").cloned();
        assert!(
            current.is_some_and(|g| Arc::ptr_eq(&g, &newer)),
            "guard evicted a newer gate belonging to another request"
        );
    }

    #[tokio::test]
    async fn single_flight_gate_is_released_on_success() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        vr.fetch_and_cache_whole(&["h:1".to_string()], "1,aa")
            .await
            .unwrap();
        assert!(
            vr.inflight.lock().unwrap().is_empty(),
            "the single-flight gate must be dropped after a completed fetch"
        );
    }

    #[tokio::test]
    async fn single_flight_gate_is_released_on_error() {
        // A failed fetch must NOT leave its gate behind: otherwise `inflight`
        // would grow by one entry per distinct fileId that ever failed to fetch.
        let vr = VolumeReader::with_transport(Arc::new(FailingTransport));
        for i in 0..100 {
            let fid = format!("1,{i:x}");
            assert!(
                vr.fetch_and_cache_whole(&["h:1".to_string()], &fid)
                    .await
                    .is_err()
            );
        }
        assert!(
            vr.inflight.lock().unwrap().is_empty(),
            "failed fetches must not leak single-flight gates"
        );
    }

    #[tokio::test]
    async fn failed_fetch_invalidates_the_volume_locations() {
        // Without this the mount would keep retrying servers a volume has moved
        // off, with no way back short of a remount.
        let vr = VolumeReader::with_transport(Arc::new(FailingTransport));
        let urls = vec!["moved-from:8080".to_string()];
        vr.location_cache.learn(7, urls.clone());
        vr.location_cache.learn(8, urls.clone());

        assert!(vr.fetch_and_cache_whole(&urls, "7,aa").await.is_err());

        assert_eq!(
            vr.cached_locations(7),
            None,
            "a failed fetch must force the next read to look the volume up again"
        );
        assert!(
            vr.cached_locations(8).is_some(),
            "only the volume that failed should be invalidated"
        );
    }

    #[tokio::test]
    async fn failed_ranged_fetch_invalidates_the_volume_locations() {
        let vr = VolumeReader::with_transport(Arc::new(FailingTransport));
        let urls = vec!["moved-from:8080".to_string()];
        vr.location_cache.learn(7, urls.clone());

        assert!(vr.fetch_ranged(&urls, "7,aa", 0, 4, false).await.is_err());

        assert_eq!(vr.cached_locations(7), None);
    }

    #[tokio::test]
    async fn failed_fetch_keeps_locations_a_newer_lookup_learned() {
        // A read can fail long after another lookup has learned where the
        // volume moved to; dropping that fresh entry would send every reader
        // back to the filer for nothing.
        let vr = VolumeReader::with_transport(Arc::new(FailingTransport));
        let stale = vec!["moved-from:8080".to_string()];
        vr.location_cache
            .learn(7, vec!["moved-to:8080".to_string()]);

        assert!(vr.fetch_and_cache_whole(&stale, "7,aa").await.is_err());

        assert_eq!(
            vr.cached_locations(7),
            Some(vec!["moved-to:8080".to_string()]),
            "a stale read's failure must not invalidate freshly learned locations"
        );
    }

    #[tokio::test]
    async fn successful_fetch_keeps_the_cached_locations() {
        let vr = VolumeReader::with_transport(Arc::new(MockTransport::new()));
        let urls = vec!["h:1".to_string()];
        vr.location_cache.learn(7, urls.clone());

        vr.fetch_and_cache_whole(&urls, "7,aa").await.unwrap();

        assert_eq!(vr.cached_locations(7), Some(urls));
    }

    #[tokio::test]
    async fn reset_locations_keeps_serving_until_relearned() {
        let vr = VolumeReader::with_transport(Arc::new(MockTransport::new()));
        let urls = vec!["h:1".to_string()];
        vr.location_cache.learn(7, urls.clone());

        vr.reset_locations();

        assert_eq!(
            vr.cached_locations(7),
            Some(urls),
            "a reset must not strand reads that are already in flight"
        );
    }

    #[tokio::test]
    async fn prefetch_fills_cache_then_serves_hit() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        let urls = vec!["h:1".to_string()];
        vr.prefetch(vec![("1,bb".to_string(), urls.clone())]);
        run_pending_prefetch(&mock, 1).await;
        // a real fetch is now served from the prefetched cache — no extra read.
        let got = vr.fetch_and_cache_whole(&urls, "1,bb").await.unwrap();
        assert_eq!(&*got, b"1,bb");
        assert_eq!(
            mock.reads(),
            1,
            "fetch_whole should hit the prefetched chunk"
        );
    }

    #[tokio::test]
    async fn prefetch_skips_already_cached() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        let urls = vec!["h:1".to_string()];
        let _ = vr.fetch_and_cache_whole(&urls, "1,cc").await.unwrap(); // reads=1, now cached
        vr.prefetch(vec![("1,cc".to_string(), urls.clone())]); // cached -> must skip
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert_eq!(mock.reads(), 1, "prefetch must not refetch a cached chunk");
    }

    const MB: u64 = 1 << 20;

    fn reader() -> VolumeReader {
        VolumeReader::with_transport(Arc::new(MockTransport::new()))
    }

    fn chunk(file_id: &str, offset: i64, size: u64, modified_ts_ns: i64) -> FileChunk {
        FileChunk {
            file_id: file_id.to_string(),
            offset,
            size,
            modified_ts_ns,
            ..Default::default()
        }
    }

    #[test]
    fn resolve_ordered_non_overlapping_chunks_without_shadow_work() {
        let chunks = vec![chunk("1,aa", 0, MB, 3), chunk("1,bb", 2 * MB as i64, MB, 1)];
        let visibles = resolve_visibles(&chunks);

        assert_eq!(visibles.len(), 2);
        assert_eq!(
            (
                visibles[0].start,
                visibles[0].stop,
                visibles[0].file_id.as_str()
            ),
            (0, MB as i64, "1,aa")
        );
        assert_eq!(
            (
                visibles[1].start,
                visibles[1].stop,
                visibles[1].file_id.as_str()
            ),
            (2 * MB as i64, 3 * MB as i64, "1,bb")
        );
    }

    #[test]
    fn resolve_overlapping_chunks_still_uses_latest_write_wins() {
        let chunks = vec![chunk("1,old", 0, 8, 1), chunk("1,new", 4, 4, 2)];
        let visibles = resolve_visibles(&chunks);

        assert_eq!(visibles.len(), 2);
        assert_eq!(
            (
                visibles[0].start,
                visibles[0].stop,
                visibles[0].file_id.as_str()
            ),
            (0, 4, "1,old")
        );
        assert_eq!(
            (
                visibles[1].start,
                visibles[1].stop,
                visibles[1].file_id.as_str()
            ),
            (4, 8, "1,new")
        );
    }

    #[test]
    fn gate_sequential_ramps_and_resists_outlier() {
        let vr = reader();
        // a stream from offset 0 is sequential and ramps the counter to the cap
        assert!(vr.note_sequential("f", 0, MB)); // near(0 vs frontier 0) -> +1
        assert!(vr.note_sequential("f", MB, 2 * MB)); // +2
        assert!(vr.note_sequential("f", 2 * MB, 3 * MB)); // +3 (capped)
        // a single far outlier does NOT flip to random (hysteresis): +3 -> +2
        assert!(vr.note_sequential("f", 900 * MB, 901 * MB));
    }

    #[test]
    fn gate_far_first_read_is_random() {
        let vr = reader();
        // first read far into the file (vs frontier 0) -> random, no prefetch
        assert!(!vr.note_sequential("f", 500 * MB, 501 * MB));
    }

    #[test]
    fn gate_tolerance_absorbs_reorder() {
        let vr = reader();
        assert!(vr.note_sequential("f", 0, MB)); // frontier -> 1MB, +1
        // a read jumping ahead by 5 MB (< tolerance) — reordered readahead — is near
        assert!(vr.note_sequential("f", 6 * MB, 7 * MB)); // |6-1|=5MB <= 8MB, frontier 7MB, +2
        // a read behind/overlapping the frontier, within tolerance, is also near
        assert!(vr.note_sequential("f", 3 * MB, 4 * MB)); // |3-7|=4MB <= 8MB, +3
    }

    #[test]
    fn gate_random_recovery_needs_hysteresis() {
        let vr = reader();
        assert!(!vr.note_sequential("f", 100 * MB, 101 * MB)); // far -> -1 (random)
        let _ = vr.note_sequential("f", 300 * MB, 301 * MB); // far -> -2
        let _ = vr.note_sequential("f", 500 * MB, 501 * MB); // far -> -3 (capped)
        // a single near read does NOT immediately flip back to sequential: -3 -> -2
        assert!(!vr.note_sequential("f", 501 * MB, 502 * MB));
    }

    #[tokio::test]
    async fn random_fetch_is_exact_and_uncached() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        let urls = vec!["h:1".to_string()];
        let _ = vr.fetch_ranged(&urls, "1,dd", 0, 16, false).await.unwrap();
        let _ = vr.fetch_ranged(&urls, "1,dd", 0, 16, false).await.unwrap();
        assert_eq!(mock.reads(), 2, "random reads must not be cached");
    }

    #[tokio::test]
    async fn sequential_fetch_caches_whole() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        let urls = vec!["h:1".to_string()];
        let a = vr.fetch_ranged(&urls, "1,ee", 0, 2, true).await.unwrap();
        assert_eq!(a, b"1,"); // sliced from the cached whole needle
        // a later sub-range of the same chunk hits the cache even in random mode.
        let b2 = vr.fetch_ranged(&urls, "1,ee", 2, 2, false).await.unwrap();
        assert_eq!(b2, b"ee");
        assert_eq!(mock.reads(), 1, "whole-chunk cache serves all sub-ranges");
    }

    /// Serves byte ranges of named needles.
    struct BytesTransport {
        needles: HashMap<String, Vec<u8>>,
        reads: AtomicUsize,
        whole_reads: AtomicUsize,
    }

    #[async_trait]
    impl VolumeTransport for BytesTransport {
        async fn read(
            &self,
            _locations: &[String],
            file_id: &str,
            range: Option<(u64, u64)>,
        ) -> Result<Vec<u8>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let needle = self.needles.get(file_id).context("no such needle")?;
            Ok(match range {
                Some((off, len)) => needle[off as usize..(off + len) as usize].to_vec(),
                None => {
                    self.whole_reads.fetch_add(1, Ordering::SeqCst);
                    needle.clone()
                }
            })
        }
        async fn write(
            &self,
            _location: &str,
            _file_id: &str,
            _auth: &str,
            _filename: &str,
            _data: bytes::Bytes,
        ) -> Result<()> {
            Ok(())
        }
        fn name(&self) -> &'static str {
            "bytes"
        }
    }

    /// Encrypted chunks are stored XORed with their stream position; chunks
    /// with a cipher key can only be undone whole.
    struct XorDecoder;

    fn xor(data: &mut [u8], position: u64) {
        for (i, b) in data.iter_mut().enumerate() {
            *b ^= (position as usize + i) as u8 ^ 0x5a;
        }
    }

    impl ChunkDecoder for XorDecoder {
        fn needs_whole_chunk(&self, chunk: &FileChunk) -> bool {
            !chunk.cipher_key.is_empty()
        }
        fn decode(&self, chunk: &FileChunk, offset: u64, mut data: Vec<u8>) -> Result<Vec<u8>> {
            if encoded(chunk) {
                xor(&mut data, offset);
            }
            Ok(data)
        }
    }

    /// A 32-byte file: a plain chunk, a manifest holding a plain chunk, an
    /// SSE chunk and a filer-encrypted chunk, 8 bytes each.
    fn encrypted_file() -> (Entry, Vec<u8>, Arc<BytesTransport>, VolumeReader) {
        let plain: Vec<u8> = (0..32u8).map(|i| i.wrapping_mul(37)).collect();
        let mut sse = plain[16..24].to_vec();
        xor(&mut sse, 0);
        let mut gcm = plain[24..32].to_vec();
        xor(&mut gcm, 0);
        let inner = FileChunkManifest {
            chunks: vec![chunk("1,bb", 8, 8, 1)],
        };
        let needles = HashMap::from([
            ("1,aa".to_string(), plain[..8].to_vec()),
            ("1,mm".to_string(), inner.encode_to_vec()),
            ("1,bb".to_string(), plain[8..16].to_vec()),
            ("1,ee".to_string(), sse),
            ("1,gg".to_string(), gcm),
        ]);
        let transport = Arc::new(BytesTransport {
            needles,
            reads: AtomicUsize::new(0),
            whole_reads: AtomicUsize::new(0),
        });
        let volumes = VolumeReader::with_transport(transport.clone());
        volumes.location_cache.learn(1, vec!["h:1".into()]);
        let mut manifest = chunk("1,mm", 8, 8, 1);
        manifest.is_chunk_manifest = true;
        let mut sse_chunk = chunk("1,ee", 16, 8, 1);
        sse_chunk.sse_type = 3;
        let mut gcm_chunk = chunk("1,gg", 24, 8, 1);
        gcm_chunk.cipher_key = vec![7; 32];
        let entry = Entry {
            attributes: Some(crate::filer_pb::FuseAttributes {
                file_size: 32,
                ..Default::default()
            }),
            chunks: vec![chunk("1,aa", 0, 8, 1), manifest, sse_chunk, gcm_chunk],
            ..Default::default()
        };
        (entry, plain, transport, volumes)
    }

    #[tokio::test]
    async fn manifests_expand_and_the_decoder_sees_every_chunk() {
        let (entry, plain, transport, volumes) = encrypted_file();
        let mut client = FilerClient::connect_lazy("127.0.0.1:1").unwrap();
        let options = ReadOptions {
            exact: true,
            decoder: Some(Arc::new(XorDecoder)),
        };
        let got = read_file_with(&mut client, &volumes, &entry, 0, 32, &options)
            .await
            .unwrap();
        assert_eq!(got, plain);

        // An unaligned read into the SSE chunk is fetched as a range and
        // decoded at its offset; the filer-encrypted chunk is fetched whole.
        let resolved = resolve_chunks(&mut client, &volumes, &entry.chunks, &options)
            .await
            .unwrap();
        assert_eq!(resolved.end(), 32);
        let before = transport.reads.load(Ordering::SeqCst);
        let whole_before = transport.whole_reads.load(Ordering::SeqCst);
        let got = read_resolved(&mut client, &volumes, &resolved, &entry, 19, 10, &options)
            .await
            .unwrap();
        assert_eq!(got, &plain[19..29]);
        assert_eq!(transport.reads.load(Ordering::SeqCst) - before, 2);
        assert_eq!(
            transport.whole_reads.load(Ordering::SeqCst) - whole_before,
            1
        );
        assert!(
            volumes.chunks.lock().unwrap().len_bytes() == 0,
            "exact reads cache nothing"
        );

        let tail = read_resolved(
            &mut client,
            &volumes,
            &resolved,
            &entry,
            19,
            u64::MAX,
            &options,
        )
        .await
        .unwrap();
        assert_eq!(tail, &plain[19..], "a size past u64::MAX reads to EOF");
    }

    #[tokio::test]
    async fn encrypted_chunks_need_a_decoder() {
        let (entry, plain, _, volumes) = encrypted_file();
        let mut client = FilerClient::connect_lazy("127.0.0.1:1").unwrap();
        let err = read_file(&mut client, &volumes, &entry, 16, 8)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("needs a decoder"), "{err:#}");
        let got = read_file(&mut client, &volumes, &entry, 0, 16)
            .await
            .unwrap();
        assert_eq!(
            got,
            &plain[..16],
            "plain chunks and manifests read without one"
        );

        let mut sse_manifest = entry.clone();
        sse_manifest.chunks[1].sse_type = 3;
        let err = read_file(&mut client, &volumes, &sse_manifest, 0, 8)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("needs a decoder"), "{err:#}");
    }

    #[tokio::test]
    async fn sequential_fetch_into_copies_from_cached_whole() {
        let mock = Arc::new(MockTransport::new());
        let vr = VolumeReader::with_transport(mock.clone());
        let urls = vec!["h:1".to_string()];

        let mut first = [0u8; 2];
        let n = vr
            .fetch_ranged_into(&urls, "1,ee", 0, 2, true, &mut first)
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(&first, b"1,");

        let mut second = [0u8; 2];
        let n = vr
            .fetch_ranged_into(&urls, "1,ee", 2, 2, false, &mut second)
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(&second, b"ee");
        assert_eq!(
            mock.reads(),
            1,
            "second copy should use the cached whole chunk"
        );
    }
}
