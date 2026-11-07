//! Filer gRPC client — the SeaweedFS metadata plane.
//!
//! Only the filer endpoint is dialed; it proxies volume assignment/lookup, so
//! the master is never contacted directly.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::filer_pb::seaweed_filer_client::SeaweedFilerClient;
use crate::filer_pb::{
    AssignVolumeRequest, AtomicRenameEntryRequest, CacheRemoteObjectToLocalClusterRequest,
    CreateEntryRequest, Entry, GetFilerConfigurationRequest, GetFilerConfigurationResponse,
    KvGetRequest, KvPutRequest, ListEntriesRequest, LockRequest, LockResponse,
    LookupDirectoryEntriesRequest, LookupDirectoryEntriesResponse, LookupDirectoryEntryRequest,
    LookupVolumeRequest, PosixLockRange, PosixLockRequest, PosixLockResponse, StatisticsRequest,
    StatisticsResponse, SubscribeMetadataRequest, SubscribeMetadataResponse, UnlockRequest,
    UpdateEntryRequest,
};

/// Convert one `LookupDirectoryEntries` reply into the address book callers
/// execute against. A result carrying an error is unusable even when the Filer
/// attached an Entry (a chunk it could not place, a Volume it could not resolve
/// while Master was unreachable), so it fails the batch closed. Only
/// `found == false` without an error is a miss.
fn batch_lookup_response(
    paths: &[(String, String)],
    response: LookupDirectoryEntriesResponse,
) -> Result<BatchEntryLookupResponse> {
    if response.results.len() != paths.len() {
        bail!(
            "LookupDirectoryEntries returned {} results for {} requests",
            response.results.len(),
            paths.len()
        );
    }

    let mut results = Vec::with_capacity(paths.len());
    for (index, result) in response.results.into_iter().enumerate() {
        if !result.error.is_empty() {
            let (directory, name) = &paths[index];
            bail!(
                "LookupDirectoryEntries result {index} ({directory}/{name}): {}",
                result.error
            );
        }
        if result.found != result.entry.is_some() {
            bail!("LookupDirectoryEntries result {index} has inconsistent found/entry state");
        }
        results.push(BatchEntryLookup {
            entry: result.entry,
            log_pos: LogPos {
                ts_ns: result.log_ts_ns,
                signature: result.log_signature,
            },
        });
    }

    let mut locations = HashMap::with_capacity(response.locations_map.len());
    for (volume_id, location_set) in response.locations_map {
        let volume_id = volume_id
            .parse::<u32>()
            .with_context(|| format!("invalid volume id {volume_id:?} in batch lookup"))?;
        let converted = location_set
            .locations
            .into_iter()
            .map(|location| VolumeLocation {
                url: location.url,
                public_url: location.public_url,
                grpc_port: location.grpc_port,
            })
            .collect::<Vec<_>>();
        if converted.is_empty() {
            bail!("LookupDirectoryEntries returned no locations for volume {volume_id}");
        }
        locations.insert(volume_id, converted);
    }

    Ok(BatchEntryLookupResponse {
        results,
        locations,
        read_auth: response.read_auth,
        source: BatchLookupSource::ExactBatch,
    })
}

/// A filer log position: the point in the metadata log that a served read or
/// an acknowledged write provably reflects. The filer stamps it under the same
/// per-path lock mutations hold across write and notify, so every event at or
/// below it is included and none above it are.
///
/// `signature` names the filer whose clock produced `ts_ns`. Each filer stamps
/// with its own wall clock, so positions are only meaningful against events
/// that same filer logged. A zero signature means the position is not
/// attributable to any filer's clock — it came from an event rather than an
/// RPC — and such a position never fences anything.
///
/// A zero `ts_ns` means the filer supplied no position (one older than the
/// field). Callers must treat that as "unknown", never as a low position.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogPos {
    pub ts_ns: i64,
    pub signature: i32,
}

/// A physical volume location as returned by the Filer. Keep the advertised
/// gRPC port: runtime RDMA endpoint discovery must dial the exact volume server
/// and must not assume the historical HTTP+10000 port convention.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VolumeLocation {
    pub url: String,
    pub public_url: String,
    pub grpc_port: u32,
}

/// One ordered exact-path result from a Filer batch lookup.
#[derive(Debug, Clone)]
pub struct BatchEntryLookup {
    pub entry: Option<Entry>,
    pub log_pos: LogPos,
}

/// How a batch lookup was resolved.
///
/// A legacy per-entry fallback preserves ordinary metadata compatibility, but
/// it cannot prove that one Filer produced the complete direct-read address
/// book and its exact-FID capabilities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchLookupSource {
    #[default]
    Unknown,
    ExactBatch,
    LegacyPerEntry,
}

/// Ordered metadata plus the physical address book needed for direct reads.
#[derive(Clone, Default)]
pub struct BatchEntryLookupResponse {
    pub results: Vec<BatchEntryLookup>,
    pub locations: HashMap<u32, Vec<VolumeLocation>>,
    /// Short-lived capabilities keyed by canonical FID. Empty when the Filer
    /// does not sign reads or when a rolling-upgrade fallback was required.
    pub read_auth: HashMap<String, String>,
    pub source: BatchLookupSource,
}

impl std::fmt::Debug for BatchEntryLookupResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchEntryLookupResponse")
            .field("result_count", &self.results.len())
            .field("volume_count", &self.locations.len())
            .field("read_auth_count", &self.read_auth.len())
            .field("source", &self.source)
            .finish()
    }
}

impl VolumeLocation {
    pub fn grpc_addr(&self) -> Result<String> {
        if self.url.is_empty() || self.grpc_port == 0 || self.grpc_port > u16::MAX as u32 {
            bail!(
                "invalid volume location url={:?} grpc_port={}",
                self.url,
                self.grpc_port
            );
        }
        let host = if self.url.starts_with('[') {
            self.url
                .split_once(']')
                .map(|(host, _)| format!("{host}]"))
                .ok_or_else(|| anyhow::anyhow!("invalid bracketed volume URL {}", self.url))?
        } else {
            self.url
                .rsplit_once(':')
                .map(|(host, _)| host.to_string())
                .unwrap_or_else(|| self.url.clone())
        };
        Ok(format!("{host}:{}", self.grpc_port))
    }
}

impl LogPos {
    /// Whether this position is known (the filer supplied one).
    pub fn is_known(&self) -> bool {
        self.ts_ns != 0
    }

    /// Whether the state at this position already accounts for an event at
    /// `ts_ns` logged by the filers in `event_signatures`.
    ///
    /// Exact on a single filer: the stamp and the mutation share that filer's
    /// per-path lock, so an event strictly below the stamp is in the value and
    /// one at or above it is not. The bound is deliberately exclusive — the
    /// stamp is taken before the read or write it fences, and a coarse clock
    /// can log a later event within the same tick, so an equal timestamp is
    /// ambiguous and must be applied rather than dropped.
    ///
    /// Across filers it is a heuristic, and knowingly so. Positions from
    /// different clocks are not comparable, so a position must name the filer
    /// that produced it and that filer must appear among the event's
    /// signatures. That test is necessary but not sufficient: the serving filer
    /// appends its own signature to every event it relays, peer events
    /// included (`filer_grpc_server_sub_meta.go`), so a peer's timestamp can
    /// still be judged against a local stamp. Under skew that can wrongly fence
    /// a peer event, leaving a handle stale until the next event for the path.
    /// Nothing in the signatures distinguishes the originating filer, so the
    /// callers that cannot afford a wrong answer — orphaning a handle, or
    /// replacing the object it holds — check entry identity instead, which
    /// travels with the value and needs no clock.
    pub fn covers(&self, ts_ns: i64, event_signatures: &[i32]) -> bool {
        if !self.is_known() || ts_ns == 0 || ts_ns >= self.ts_ns {
            return false;
        }
        self.signature != 0 && event_signatures.contains(&self.signature)
    }
}

/// One page of a directory listing (see [`FilerClient::list_entries_page`]).
#[derive(Debug, Clone, Default)]
pub struct EntryPage {
    pub entries: Vec<Entry>,
    /// Name to resume after: the last entry of this page, or the caller's own
    /// `start_from` if the page came back empty. Never rewinds, so feeding it
    /// straight back always makes progress.
    pub cursor: String,
    /// Snapshot the filer echoed for this listing. Pass it back for the next
    /// page — but it does not currently isolate the listing; see
    /// [`FilerClient::list_entries_page`].
    pub snapshot_ts_ns: i64,
    /// No entries follow this page.
    pub eof: bool,
}

/// What a paged listing must do to answer a request for a given entry index.
/// See [`seek_page`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seek {
    /// The page in hand covers it: serve from this offset within the page.
    /// An offset at or past the page's length means the listing is exhausted.
    Serve(usize),
    /// Fetch the next page; the one in hand ends before the wanted index.
    Advance,
    /// Restart from the top; the wanted index precedes the page in hand.
    Restart,
}

/// Where a listing whose page holds `len` entries starting at offset `base`
/// (`eof` = that page runs to the end of the directory) stands relative to
/// entry `index`.
///
/// Both front-ends page their readdir through this so the boundary rules have
/// one definition. The rules are asymmetric on purpose: paging only ever moves
/// forward, because the filer resumes a listing from a *name*, not an index —
/// so going back means starting over and paging up to the target again.
pub fn seek_page(base: usize, len: usize, eof: bool, index: usize) -> Seek {
    if index < base {
        return Seek::Restart;
    }
    // Past the page in hand: another page unless this one ended the directory,
    // in which case serving an empty range is what reports EOF to the kernel.
    if index >= base + len && !eof {
        return Seek::Advance;
    }
    Seek::Serve(index - base)
}

/// Result of assigning a volume for a new chunk write.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub file_id: String,
    /// Volume server URL (host:port) to upload to.
    pub url: String,
    /// Per-fileId write JWT ("" if the cluster has no write signing key).
    pub auth: String,
    /// The path's storage rule wants the needle on disk before the volume
    /// answers.
    pub fsync: bool,
}

/// Volume class hints passed to Seaweed's AssignVolume RPC.
///
/// Data-plane transports should not make replica/placement decisions. They
/// receive the assigned `file_id + location` after this metadata-plane request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AssignVolumeOptions {
    pub collection: String,
    pub replication: String,
    /// Optional explicit volume-server placement (`host:http_port`). Empty
    /// delegates placement to the master.
    pub data_node: String,
}

/// gRPC messages can be large (directory listings, fat chunk lists); raise the
/// default 4 MiB codec limits.
const MAX_MSG: usize = 256 * 1024 * 1024;

/// A connected client to a single filer's gRPC endpoint.
///
/// Cheap to clone — it shares the underlying HTTP/2 channel.
#[derive(Clone)]
pub struct FilerClient {
    inner: SeaweedFilerClient<Channel>,
    addr: String,
}

/// Cluster-wide settings fetched once at mount time.
#[derive(Debug, Clone)]
pub struct FilerConfig {
    /// At-rest encryption. When true, chunks must be read whole (no server-side
    /// range reads), which means read amplification.
    pub cipher: bool,
    /// Target chunk size in MiB for writes.
    pub max_mb: u32,
    /// This filer's signature. Metadata events we cause carry it, so the
    /// `SubscribeMetadata` follower must skip them to avoid feedback loops.
    pub signature: i32,
    pub collection: String,
    pub replication: String,
    /// Directory holding the S3 buckets, `/buckets` unless configured.
    pub dir_buckets: String,
    pub version: String,
    pub cluster_id: String,
    pub filer_group: String,
}

impl From<GetFilerConfigurationResponse> for FilerConfig {
    fn from(r: GetFilerConfigurationResponse) -> Self {
        Self {
            cipher: r.cipher,
            max_mb: r.max_mb,
            signature: r.signature,
            collection: r.collection,
            replication: r.replication,
            dir_buckets: r.dir_buckets,
            version: r.version,
            cluster_id: r.cluster_id,
            filer_group: r.filer_group,
        }
    }
}

/// TLS for the filer's gRPC, as `grpc.client` and `grpc.ca` in security.toml
/// configure it for weed processes: the filer's certificate must chain to the
/// CA and name the host dialed, and this client presents its own.
#[derive(Clone)]
pub struct GrpcTls {
    /// `grpc.ca`
    pub ca_pem: Vec<u8>,
    /// `grpc.client.cert` and `grpc.client.key`
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    /// The name the filer's certificate is checked against, when it is not
    /// the host in the filer's address.
    pub server_name: Option<String>,
}

impl std::fmt::Debug for GrpcTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTls")
            .field("server_name", &self.server_name)
            .finish()
    }
}

impl GrpcTls {
    fn config(&self, addr: &str) -> ClientTlsConfig {
        crate::transport::install_crypto_provider();
        let host = addr
            .rsplit_once(':')
            .map_or(addr, |(host, _)| host)
            .trim_matches(|c| c == '[' || c == ']');
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .identity(Identity::from_pem(&self.cert_pem, &self.key_pem))
            .domain_name(self.server_name.clone().unwrap_or_else(|| host.to_string()))
    }
}

impl FilerClient {
    /// Connect to a filer gRPC endpoint, e.g. `"10.0.0.1:18888"` (plaintext h2c).
    pub async fn connect(addr: &str) -> Result<Self> {
        Self::connect_with(addr, None).await
    }

    /// [`connect`](Self::connect) over TLS when `tls` is given.
    pub async fn connect_with(addr: &str, tls: Option<&GrpcTls>) -> Result<Self> {
        let channel = Self::endpoint(addr, tls)?
            .connect()
            .await
            .with_context(|| format!("connecting to filer {addr}"))?;
        Ok(Self::with_channel(addr, channel))
    }

    /// [`connect`](Self::connect) whose channel connects on first use, so a
    /// filer that is down right now is retried later instead of failing
    /// startup.
    pub fn connect_lazy(addr: &str) -> Result<Self> {
        Self::connect_lazy_with(addr, None)
    }

    /// [`connect_lazy`](Self::connect_lazy) over TLS when `tls` is given.
    pub fn connect_lazy_with(addr: &str, tls: Option<&GrpcTls>) -> Result<Self> {
        Ok(Self::with_channel(
            addr,
            Self::endpoint(addr, tls)?.connect_lazy(),
        ))
    }

    /// A client over a channel the caller dialed, for a TLS setup this crate
    /// does not know, such as a server allowlist.
    pub fn from_channel(addr: &str, channel: Channel) -> Self {
        Self::with_channel(addr, channel)
    }

    fn endpoint(addr: &str, tls: Option<&GrpcTls>) -> Result<Endpoint> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        let endpoint = Endpoint::from_shared(format!("{scheme}://{addr}"))
            .with_context(|| format!("invalid filer address {addr}"))?
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Some(Duration::from_secs(30)));
        match tls {
            Some(tls) => endpoint
                .tls_config(tls.config(addr))
                .with_context(|| format!("TLS for filer {addr}")),
            None => Ok(endpoint),
        }
    }

    fn with_channel(addr: &str, channel: Channel) -> Self {
        Self {
            inner: SeaweedFilerClient::new(channel)
                .max_decoding_message_size(MAX_MSG)
                .max_encoding_message_size(MAX_MSG),
            addr: addr.to_string(),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Fetch cluster configuration (cipher, chunk size, signature, ...).
    pub async fn get_configuration(&mut self) -> Result<FilerConfig> {
        let resp = self
            .inner
            .get_filer_configuration(GetFilerConfigurationRequest {})
            .await
            .context("GetFilerConfiguration RPC failed")?;
        Ok(resp.into_inner().into())
    }

    /// Read a key from the filer's key-value store. `Ok(None)` when absent (the
    /// filer answers a missing key with an empty value and no error, so an empty
    /// stored value is indistinguishable from absent — callers store non-empty
    /// values only).
    pub async fn kv_get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let req = KvGetRequest { key: key.to_vec() };
        let resp = self
            .inner
            .kv_get(req)
            .await
            .context("KvGet RPC failed")?
            .into_inner();
        if !resp.error.is_empty() {
            bail!("KvGet: {}", resp.error);
        }
        Ok((!resp.value.is_empty()).then_some(resp.value))
    }

    /// Write a key to the filer's key-value store. An empty `value` deletes the
    /// key (the filer's KvPut semantics); deleting an absent key is not an error.
    pub async fn kv_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let req = KvPutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let resp = self
            .inner
            .kv_put(req)
            .await
            .context("KvPut RPC failed")?
            .into_inner();
        if !resp.error.is_empty() {
            bail!("KvPut: {}", resp.error);
        }
        Ok(())
    }

    /// Look up a single child `name` in `directory`. Returns `Ok(None)` when the
    /// entry does not exist (the filer reports this as a generic error whose
    /// message we recognise, not a gRPC NotFound status).
    pub async fn lookup_entry(&mut self, directory: &str, name: &str) -> Result<Option<Entry>> {
        Ok(self.lookup_entry_at(directory, name).await?.0)
    }

    /// [`lookup_entry`](Self::lookup_entry) with the log position the returned
    /// entry reflects, for callers that pin it (an open file handle) and must
    /// later judge whether an event predates what they hold.
    pub async fn lookup_entry_at(
        &mut self,
        directory: &str,
        name: &str,
    ) -> Result<(Option<Entry>, LogPos)> {
        let req = LookupDirectoryEntryRequest {
            directory: directory.to_string(),
            name: name.to_string(),
        };
        match self.inner.lookup_directory_entry(req).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                let pos = LogPos {
                    ts_ns: resp.log_ts_ns,
                    signature: resp.log_signature,
                };
                Ok((resp.entry, pos))
            }
            Err(status) if is_not_found(&status) => Ok((None, LogPos::default())),
            Err(status) => Err(anyhow::Error::new(status)
                .context(format!("LookupDirectoryEntry {directory}/{name}"))),
        }
    }

    /// Look up independent exact paths in one bounded RPC.
    ///
    /// Results preserve request order. Exact misses are represented by
    /// `entry == None`; malformed entries and missing Volume locations fail the
    /// whole batch closed. Older Filers fall back to the equivalent native
    /// single-entry and Volume-location RPCs for rolling upgrades.
    pub async fn lookup_entries_batch(
        &mut self,
        paths: &[(String, String)],
    ) -> Result<BatchEntryLookupResponse> {
        self.lookup_entries_batch_with_policy(paths, false).await
    }

    /// Look up cache entries, mapping only a Master-confirmed retired Volume to
    /// a miss. An unanswered lookup remains an error.
    pub async fn lookup_cache_entries_batch(
        &mut self,
        paths: &[(String, String)],
    ) -> Result<BatchEntryLookupResponse> {
        self.lookup_entries_batch_with_policy(paths, true).await
    }

    async fn lookup_entries_batch_with_policy(
        &mut self,
        paths: &[(String, String)],
        unavailable_volume_is_miss: bool,
    ) -> Result<BatchEntryLookupResponse> {
        if paths.is_empty() {
            return Ok(BatchEntryLookupResponse {
                source: BatchLookupSource::ExactBatch,
                ..Default::default()
            });
        }
        let requests = paths
            .iter()
            .map(|(directory, name)| LookupDirectoryEntryRequest {
                directory: directory.clone(),
                name: name.clone(),
            })
            .collect();
        let response = match self
            .inner
            .lookup_directory_entries(LookupDirectoryEntriesRequest {
                requests,
                unavailable_volume_is_miss,
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::Unimplemented => {
                return self.lookup_entries_legacy(paths).await;
            }
            Err(status) => {
                return Err(anyhow::Error::new(status).context("LookupDirectoryEntries RPC failed"));
            }
        };
        batch_lookup_response(paths, response)
    }

    async fn lookup_entries_legacy(
        &mut self,
        paths: &[(String, String)],
    ) -> Result<BatchEntryLookupResponse> {
        let mut results = Vec::with_capacity(paths.len());
        let mut volume_ids = HashSet::new();
        for (directory, name) in paths {
            let (entry, log_pos) = self.lookup_entry_at(directory, name).await?;
            if let Some(entry) = entry.as_ref() {
                for chunk in &entry.chunks {
                    let fid = crate::read::file_id_string(chunk)
                        .context("legacy batch lookup entry chunk has no file id")?;
                    let (volume_id, _) = fid
                        .split_once(',')
                        .context("legacy batch lookup chunk has an invalid file id")?;
                    volume_ids.insert(volume_id.parse::<u32>().with_context(|| {
                        format!("legacy batch lookup has invalid volume id {volume_id:?}")
                    })?);
                }
            }
            results.push(BatchEntryLookup { entry, log_pos });
        }

        let mut locations = HashMap::with_capacity(volume_ids.len());
        for volume_id in volume_ids {
            let resolved = self.lookup_volume_locations(volume_id).await?;
            if resolved.is_empty() {
                bail!("legacy batch lookup returned no locations for volume {volume_id}");
            }
            locations.insert(volume_id, resolved);
        }
        Ok(BatchEntryLookupResponse {
            results,
            locations,
            read_auth: HashMap::new(),
            source: BatchLookupSource::LegacyPerEntry,
        })
    }

    /// Read one page of at most [`LIST_PAGE_SIZE`] entries from `directory`,
    /// resuming after the name `start_from` (empty = from the beginning).
    ///
    /// There is deliberately no "list the whole directory" convenience wrapper:
    /// a caller that collects every page holds the entire directory in memory,
    /// which is gigabytes for a directory of millions of entries. Readdir is
    /// served a batch at a time, so page it a batch at a time — keep the
    /// returned [`EntryPage::cursor`] and drop the entries once emitted.
    ///
    /// Pass the previous page's [`EntryPage::snapshot_ts_ns`] back in (0 for a
    /// new listing).
    ///
    /// Be careful what that buys: **the filer does not isolate the listing on
    /// it.** `ListEntries` defaults the field to `time.Now()` when unset and
    /// echoes it on the first response, but `StreamListDirectoryEntries` takes
    /// no snapshot argument, so pages are read from live state. A paged listing
    /// is therefore only as consistent as the directory is quiet: an entry
    /// created before the cursor after that page was read is missed, and one
    /// renamed from after the cursor to before it can be seen twice. Passing
    /// the value back costs nothing and makes the client correct the day the
    /// filer honours it.
    pub async fn list_entries_page(
        &mut self,
        directory: &str,
        start_from: &str,
        snapshot_ts_ns: i64,
    ) -> Result<EntryPage> {
        self.list_entries_page_prefixed(directory, "", start_from, snapshot_ts_ns)
            .await
    }

    /// [`list_entries_page`](Self::list_entries_page) of the entries whose
    /// name starts with `prefix`.
    pub async fn list_entries_page_prefixed(
        &mut self,
        directory: &str,
        prefix: &str,
        start_from: &str,
        snapshot_ts_ns: i64,
    ) -> Result<EntryPage> {
        let req = ListEntriesRequest {
            directory: directory.to_string(),
            prefix: prefix.to_string(),
            start_from_file_name: start_from.to_string(),
            inclusive_start_from: false,
            limit: LIST_PAGE_SIZE,
            snapshot_ts_ns,
            ..Default::default()
        };
        let mut stream = self
            .inner
            .list_entries(req)
            .await
            .with_context(|| format!("ListEntries {directory}"))?
            .into_inner();

        let mut entries: Vec<Entry> = Vec::new();
        let mut snapshot = snapshot_ts_ns;
        let mut cursor = start_from.to_string();
        while let Some(resp) = stream
            .message()
            .await
            .with_context(|| format!("ListEntries {directory} stream"))?
        {
            // Pin the snapshot from the first response for page consistency.
            if snapshot == 0 && resp.snapshot_ts_ns != 0 {
                snapshot = resp.snapshot_ts_ns;
            }
            if let Some(entry) = resp.entry {
                cursor.clear();
                cursor.push_str(&entry.name);
                entries.push(entry);
            }
        }

        Ok(EntryPage {
            // A short page means we've reached the end of the directory.
            eof: entries.len() < LIST_PAGE_SIZE as usize,
            entries,
            cursor,
            snapshot_ts_ns: snapshot,
        })
    }

    /// Have the filer copy a remote-only entry's bytes onto the volume servers,
    /// returning the entry with its chunks.
    pub async fn cache_remote_object(&mut self, directory: &str, name: &str) -> Result<Entry> {
        let req = CacheRemoteObjectToLocalClusterRequest {
            directory: directory.to_string(),
            name: name.to_string(),
            ..Default::default()
        };
        self.inner
            .cache_remote_object_to_local_cluster(req)
            .await
            .with_context(|| format!("CacheRemoteObjectToLocalCluster {directory}/{name}"))?
            .into_inner()
            .entry
            .with_context(|| format!("the filer cached {directory}/{name} without an entry"))
    }

    /// Resolve a volume id to full physical server locations. The filer proxies
    /// the master, so clients never dial the master directly.
    pub async fn lookup_volume_locations(&mut self, volume_id: u32) -> Result<Vec<VolumeLocation>> {
        let key = volume_id.to_string();
        let req = LookupVolumeRequest {
            volume_ids: vec![key.clone()],
        };
        let resp = self
            .inner
            .lookup_volume(req)
            .await
            .with_context(|| format!("LookupVolume {volume_id}"))?
            .into_inner();
        let locations = resp
            .locations_map
            .get(&key)
            .map(|locs| {
                locs.locations
                    .iter()
                    .map(|location| VolumeLocation {
                        url: location.url.clone(),
                        public_url: location.public_url.clone(),
                        grpc_port: location.grpc_port,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(locations)
    }

    /// Compatibility view for HTTP data-plane callers.
    pub async fn lookup_volume(&mut self, volume_id: u32) -> Result<Vec<String>> {
        Ok(self
            .lookup_volume_locations(volume_id)
            .await?
            .into_iter()
            .map(|location| location.url)
            .collect())
    }

    /// Assign a fileId + volume location for a new chunk of `size` bytes.
    pub async fn assign_volume(&mut self, path: &str, size: u64) -> Result<Assignment> {
        self.assign_volume_with_options(path, size, &AssignVolumeOptions::default())
            .await
    }

    /// Assign a fileId + volume location for a new chunk using explicit volume
    /// class hints from the mounted filer configuration.
    pub async fn assign_volume_with_options(
        &mut self,
        path: &str,
        size: u64,
        options: &AssignVolumeOptions,
    ) -> Result<Assignment> {
        let req = assign_volume_request(path, size, options);
        let resp = self
            .inner
            .assign_volume(req)
            .await
            .context("AssignVolume RPC failed")?
            .into_inner();
        if !resp.error.is_empty() {
            bail!("AssignVolume: {}", resp.error);
        }
        let url = resp.location.map(|l| l.url).unwrap_or_default();
        if resp.file_id.is_empty() || url.is_empty() {
            bail!("AssignVolume returned no fileId/location");
        }
        Ok(Assignment {
            file_id: resp.file_id,
            fsync: resp.fsync,
            url,
            auth: resp.auth,
        })
    }

    /// Create an entry (upsert unless `o_excl`). `signature` tags the resulting
    /// metadata event so our own subscription can skip it. `skip_check_parent`
    /// bypasses the parent-directory existence check (used for hard links, whose
    /// second name shares an already-validated parent).
    pub async fn create_entry(
        &mut self,
        directory: &str,
        entry: Entry,
        signature: i32,
        o_excl: bool,
        skip_check_parent: bool,
    ) -> Result<LogPos> {
        let req = CreateEntryRequest {
            directory: directory.to_string(),
            entry: Some(entry),
            o_excl,
            signatures: vec![signature],
            skip_check_parent_directory: skip_check_parent,
            ..Default::default()
        };
        let resp = self
            .inner
            .create_entry(req)
            .await
            .context("CreateEntry RPC failed")?
            .into_inner();
        if !resp.error.is_empty() {
            let error = anyhow::anyhow!("CreateEntry: {}", resp.error);
            if resp.error_code == crate::filer_pb::FilerError::EntryAlreadyExists as i32 {
                return Err(error.context(EntryExists));
            }
            return Err(error);
        }
        Ok(LogPos {
            ts_ns: resp.log_ts_ns,
            signature: resp.log_signature,
        })
    }

    /// Replace an entry (send the whole mutated entry).
    pub async fn update_entry(
        &mut self,
        directory: &str,
        entry: Entry,
        signature: i32,
    ) -> Result<LogPos> {
        let req = UpdateEntryRequest {
            directory: directory.to_string(),
            entry: Some(entry),
            signatures: vec![signature],
            ..Default::default()
        };
        let resp = self
            .inner
            .update_entry(req)
            .await
            .context("UpdateEntry RPC failed")?
            .into_inner();
        Ok(LogPos {
            ts_ns: resp.log_ts_ns,
            signature: resp.log_signature,
        })
    }

    /// Delete an entry, optionally its data and recursively.
    pub async fn delete_entry(
        &mut self,
        directory: &str,
        name: &str,
        is_recursive: bool,
        delete_data: bool,
        signature: i32,
    ) -> Result<()> {
        use crate::filer_pb::DeleteEntryRequest;
        let req = DeleteEntryRequest {
            directory: directory.to_string(),
            name: name.to_string(),
            is_recursive,
            is_delete_data: delete_data,
            ignore_recursive_error: false,
            is_from_other_cluster: false,
            signatures: vec![signature],
            if_not_modified_after: 0,
            ..Default::default()
        };
        let resp = self
            .inner
            .delete_entry(req)
            .await
            .context("DeleteEntry RPC failed")?
            .into_inner();
        if !resp.error.is_empty() {
            bail!("DeleteEntry: {}", resp.error);
        }
        Ok(())
    }

    /// Atomically rename/move an entry (handles directories recursively).
    pub async fn rename(
        &mut self,
        old_dir: &str,
        old_name: &str,
        new_dir: &str,
        new_name: &str,
        signature: i32,
    ) -> Result<()> {
        let req = AtomicRenameEntryRequest {
            old_directory: old_dir.to_string(),
            old_name: old_name.to_string(),
            new_directory: new_dir.to_string(),
            new_name: new_name.to_string(),
            signatures: vec![signature],
            ..Default::default()
        };
        self.inner
            .atomic_rename_entry(req)
            .await
            .context("AtomicRenameEntry RPC failed")?;
        Ok(())
    }

    /// Fetch cluster-wide capacity/usage for a given volume class. Mirrors the
    /// values `weed mount` reports via statfs (sizes are bytes).
    pub async fn statistics(
        &mut self,
        replication: &str,
        collection: &str,
        ttl: &str,
    ) -> Result<StatisticsResponse> {
        let req = StatisticsRequest {
            replication: replication.to_string(),
            collection: collection.to_string(),
            ttl: ttl.to_string(),
            disk_type: String::new(),
        };
        let resp = self
            .inner
            .statistics(req)
            .await
            .context("Statistics RPC failed")?
            .into_inner();
        Ok(resp)
    }

    /// Distributed advisory lock op via the filer's `PosixLock` service. `op` is
    /// a `filer_pb::PosixLockOp` (TRY_LOCK / UNLOCK / GET_LK / KEEP_ALIVE);
    /// `lock` is the byte range + owner; `keepalive` carries the full held set on
    /// a KEEP_ALIVE re-assertion (empty otherwise).
    pub async fn posix_lock(
        &mut self,
        key: &str,
        op: i32,
        lock: PosixLockRange,
        keepalive: Vec<PosixLockRange>,
    ) -> Result<PosixLockResponse> {
        let req = PosixLockRequest {
            key: key.to_string(),
            is_moved: false,
            op,
            lock: Some(lock),
            locks: keepalive,
            cooling_probe: false,
        };
        let resp = self
            .inner
            .posix_lock(req)
            .await
            .context("PosixLock RPC failed")?
            .into_inner();
        Ok(resp)
    }

    /// Acquire/renew the filer's distributed lock named `name` for
    /// `seconds_to_lock`. A fresh acquire passes an empty `renew_token`; a renew
    /// passes the token from the previous response. The response's
    /// `lock_host_moved_to` redirects to the filer that actually owns the key.
    pub async fn distributed_lock(
        &mut self,
        name: &str,
        seconds_to_lock: i64,
        renew_token: &str,
        owner: &str,
    ) -> Result<LockResponse> {
        let req = LockRequest {
            name: name.to_string(),
            seconds_to_lock,
            renew_token: renew_token.to_string(),
            is_moved: false,
            owner: owner.to_string(),
        };
        let resp = self
            .inner
            .distributed_lock(req)
            .await
            .context("DistributedLock RPC failed")?
            .into_inner();
        Ok(resp)
    }

    /// Release the distributed lock `name` held under `renew_token`.
    pub async fn distributed_unlock(&mut self, name: &str, renew_token: &str) -> Result<()> {
        let req = UnlockRequest {
            name: name.to_string(),
            renew_token: renew_token.to_string(),
            is_moved: false,
        };
        self.inner
            .distributed_unlock(req)
            .await
            .context("DistributedUnlock RPC failed")?;
        Ok(())
    }

    /// Open a metadata-change subscription stream from `since_ns` under `/`.
    ///
    /// `client_id` MUST be non-zero (the filer's keep-alive check drops a zero
    /// id and closes the stream immediately), and `client_epoch` must increase
    /// on each reconnect for the same id (or the filer rejects it as a duplicate).
    pub async fn subscribe(
        &mut self,
        client_name: &str,
        client_id: i32,
        client_epoch: i32,
        since_ns: i64,
    ) -> Result<tonic::Streaming<SubscribeMetadataResponse>> {
        let req = SubscribeMetadataRequest {
            client_name: client_name.to_string(),
            path_prefix: "/".to_string(),
            since_ns,
            client_id,
            client_epoch,
            ..Default::default()
        };
        let stream = self
            .inner
            .subscribe_metadata(req)
            .await
            .context("SubscribeMetadata RPC failed")?
            .into_inner();
        Ok(stream)
    }
}

fn assign_volume_request(
    path: &str,
    size: u64,
    options: &AssignVolumeOptions,
) -> AssignVolumeRequest {
    AssignVolumeRequest {
        count: 1,
        collection: options.collection.clone(),
        replication: options.replication.clone(),
        data_node: options.data_node.clone(),
        path: path.to_string(),
        expected_data_size: size,
        ..Default::default()
    }
}

/// Entries fetched per `ListEntries` page.
const LIST_PAGE_SIZE: u32 = 1000;

/// Marker [`FilerClient::create_entry`] attaches when `o_excl` found the entry
/// already there; detect it with [`is_entry_exists`].
#[derive(Debug)]
pub struct EntryExists;

impl std::fmt::Display for EntryExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the entry already exists")
    }
}

impl std::error::Error for EntryExists {}

/// Whether `err` is an exclusive create that found its entry in place.
pub fn is_entry_exists(err: &anyhow::Error) -> bool {
    err.downcast_ref::<EntryExists>().is_some()
}

/// Whether `err` is a filer being unreachable or slow rather than refusing
/// the request, so that another filer may answer it.
pub fn is_unavailable(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause.downcast_ref::<tonic::Status>().is_some_and(|status| {
            matches!(
                status.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Cancelled
                    | tonic::Code::Aborted
            ) || (status.code() == tonic::Code::Unknown
                && status.message().contains("transport error"))
        }) || cause.downcast_ref::<tonic::transport::Error>().is_some()
    })
}

/// SeaweedFS's filer returns a plain `ErrNotFound` Go error
/// ("filer: no entry is found in filer store"), which arrives as a gRPC status
/// with code `Unknown` rather than `NotFound` — so check the message too.
pub fn is_not_found(status: &tonic::Status) -> bool {
    // Only the filer's exact missing-entry sentinel (filer_pb.ErrNotFound =
    // "filer: no entry is found in filer store") or a proper NotFound code. A
    // looser substring like "not found" would classify unrelated failures
    // (e.g. "volume 7 not found") as a definitive miss — and the NFS resolve
    // path attaches index-record DELETIONS to definitive misses, so a false
    // positive would silently GC valid forwarding records.
    status.code() == tonic::Code::NotFound || status.message().contains("no entry is found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filer_pb::{FileChunk, Location, Locations, LookupDirectoryEntryResult};

    fn batch_paths() -> Vec<(String, String)> {
        vec![("/lm".to_string(), "key".to_string())]
    }

    fn batch_entry(fid: &str) -> Entry {
        Entry {
            name: "key".to_string(),
            chunks: vec![FileChunk {
                file_id: fid.to_string(),
                size: 4,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn batch_lookup_miss_is_found_false_without_an_error() {
        let response = LookupDirectoryEntriesResponse {
            results: vec![LookupDirectoryEntryResult::default()],
            ..Default::default()
        };
        let converted = batch_lookup_response(&batch_paths(), response).unwrap();
        assert!(converted.results[0].entry.is_none());
        assert!(converted.locations.is_empty());
        assert_eq!(converted.source, BatchLookupSource::ExactBatch);
    }

    #[test]
    fn batch_lookup_hit_carries_entry_and_locations() {
        let response = LookupDirectoryEntriesResponse {
            results: vec![LookupDirectoryEntryResult {
                found: true,
                entry: Some(batch_entry("9,01")),
                ..Default::default()
            }],
            locations_map: HashMap::from([(
                "9".to_string(),
                Locations {
                    locations: vec![Location {
                        url: "volume-a:8080".to_string(),
                        grpc_port: 18_080,
                        ..Default::default()
                    }],
                },
            )]),
            ..Default::default()
        };
        let converted = batch_lookup_response(&batch_paths(), response).unwrap();
        assert!(converted.results[0].entry.is_some());
        assert_eq!(converted.locations[&9][0].url, "volume-a:8080");
    }

    #[test]
    fn batch_lookup_debug_redacts_read_capabilities() {
        let response = BatchEntryLookupResponse {
            read_auth: HashMap::from([("9,01".to_string(), "secret-exact-fid-token".to_string())]),
            source: BatchLookupSource::ExactBatch,
            ..Default::default()
        };

        let debug = format!("{response:?}");
        assert!(debug.contains("read_auth_count: 1"));
        assert!(!debug.contains("secret-exact-fid-token"));
        assert!(!debug.contains("9,01"));
    }

    #[test]
    fn batch_lookup_unresolved_volume_is_an_error_not_a_miss() {
        // Master unreachable: the Filer keeps the Entry, reports why it could
        // not place the volume, and leaves that volume's location set empty.
        let response = LookupDirectoryEntriesResponse {
            results: vec![LookupDirectoryEntryResult {
                found: true,
                entry: Some(batch_entry("9,01")),
                error: "volume 9 has no locations: master unreachable".to_string(),
                ..Default::default()
            }],
            locations_map: HashMap::from([("9".to_string(), Locations::default())]),
            ..Default::default()
        };
        let err = batch_lookup_response(&batch_paths(), response).unwrap_err();
        assert!(
            err.to_string().contains("volume 9 has no locations"),
            "{err}"
        );
    }

    #[test]
    fn batch_lookup_malformed_entry_keeps_its_error() {
        let response = LookupDirectoryEntriesResponse {
            results: vec![LookupDirectoryEntryResult {
                found: true,
                entry: Some(batch_entry("")),
                error: "chunk has no file id".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(batch_lookup_response(&batch_paths(), response).is_err());
    }

    // A directory is listed by walking these decisions, so an off-by-one here is
    // an entry served twice or skipped — a wrong `ls`, not a crash. Page size is
    // irrelevant to the rules; these use a 10-entry page for legibility.
    #[test]
    fn seek_page_serves_from_within_the_page_in_hand() {
        // Second page of a listing: entries 10..20 are in hand.
        assert_eq!(seek_page(10, 10, false, 10), Seek::Serve(0));
        assert_eq!(seek_page(10, 10, false, 15), Seek::Serve(5));
        // The last entry of the page is still served from it, not paged past.
        assert_eq!(seek_page(10, 10, false, 19), Seek::Serve(9));
    }

    #[test]
    fn seek_page_advances_exactly_at_the_boundary() {
        // One past the page in hand is the next page, not the last entry again.
        assert_eq!(seek_page(10, 10, false, 20), Seek::Advance);
        assert_eq!(seek_page(0, 1000, false, 1000), Seek::Advance);
        // An empty window (a freshly opened listing) has nothing to serve.
        assert_eq!(seek_page(0, 0, false, 0), Seek::Advance);
    }

    #[test]
    fn seek_page_stops_at_eof_instead_of_fetching_forever() {
        // The final page ends the directory: reading past it must not ask the
        // filer for another page, it must report the end. The offset lands at or
        // past the page length, which the caller serves as an empty range.
        assert_eq!(seek_page(10, 5, true, 15), Seek::Serve(5));
        assert_eq!(seek_page(10, 5, true, 99), Seek::Serve(89));
        // Within the final page, EOF changes nothing.
        assert_eq!(seek_page(10, 5, true, 14), Seek::Serve(4));
        // An empty directory: the first page comes back empty and final.
        assert_eq!(seek_page(0, 0, true, 0), Seek::Serve(0));
    }

    #[test]
    fn seek_page_restarts_on_a_backward_seek() {
        // rewinddir, or any seekdir to a cookie before the page in hand. The
        // filer resumes by name, so there is no way back but from the top.
        assert_eq!(seek_page(10, 10, false, 9), Seek::Restart);
        assert_eq!(seek_page(10, 10, false, 0), Seek::Restart);
        // EOF does not pin the window: a rewind past a finished listing restarts.
        assert_eq!(seek_page(10, 5, true, 0), Seek::Restart);
    }

    #[test]
    fn assign_volume_request_carries_volume_class() {
        let req = assign_volume_request(
            "/models/checkpoint.bin",
            32 * 1024 * 1024,
            &AssignVolumeOptions {
                collection: "models".to_string(),
                replication: "001".to_string(),
                data_node: "volume-a:8080".to_string(),
            },
        );

        assert_eq!(req.count, 1);
        assert_eq!(req.path, "/models/checkpoint.bin");
        assert_eq!(req.expected_data_size, 32 * 1024 * 1024);
        assert_eq!(req.collection, "models");
        assert_eq!(req.replication, "001");
        assert_eq!(req.data_node, "volume-a:8080");
    }

    #[test]
    fn volume_location_uses_advertised_grpc_port() {
        let location = VolumeLocation {
            url: "volume-a.example:8080".into(),
            grpc_port: 18080,
            ..Default::default()
        };
        assert_eq!(location.grpc_addr().unwrap(), "volume-a.example:18080");
    }

    #[test]
    fn volume_location_formats_ipv6_grpc_address() {
        let location = VolumeLocation {
            url: "[2001:db8::7]:8080".into(),
            grpc_port: 18080,
            ..Default::default()
        };
        assert_eq!(location.grpc_addr().unwrap(), "[2001:db8::7]:18080");
    }

    #[test]
    fn volume_location_rejects_missing_grpc_port() {
        let location = VolumeLocation {
            url: "volume-a.example:8080".into(),
            ..Default::default()
        };
        assert!(location.grpc_addr().is_err());
    }
}
