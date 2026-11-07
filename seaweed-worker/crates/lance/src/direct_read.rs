//! Reads of a table straight from the filer and the volume servers, skipping
//! the S3 gateway: object bytes, metadata and listings alike, whatever the
//! entry looks like (inline content, chunk manifests, sparse or overlapping
//! chunks, encryption by the filer or by the gateway, objects tiered to
//! remote storage). Writes, copies and deletes still go to the gateway.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use base64::prelude::{BASE64_STANDARD, Engine};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use md5::{Digest, Md5};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result,
};
use sw_client::filer_pb::Entry;
use sw_client::{
    ChunkDecoder, FilerClient, GrpcTls, HttpTransport, HttpsTls, ReadOptions, ResolvedChunks,
    VolumeReader, VolumeTransport,
};

use crate::sse::{EntryDecoder, KEK_PATH, SseConfig, SseS3Keys};

const OBJECT_CACHE_ENTRIES: u64 = 1 << 16;
const READ_JWT_TTL_SECS: u64 = 60;
/// Bound on one filer's answer, so a filer that accepts the connection and
/// then says nothing is passed over like one that refused it.
const FILER_TIMEOUT: Duration = Duration::from_secs(30);
/// The gateway's object-level ETag, set when it differs from the entry's md5.
const ETAG_ATTRIBUTE: &str = "Seaweed-X-Amz-ETag";
const PREFIX_OBJECT_ATTRIBUTE: &str = "x-seaweedfs-prefix-object";

#[derive(Clone)]
pub struct DirectReads {
    shared: Arc<Shared>,
    /// Set when the source is the cluster's filers, for [`Self::set_filers`].
    filers: Option<Arc<Filers>>,
}

/// An object as the filer holds it, ready to read.
pub struct Object {
    entry: Entry,
    size: u64,
    etag: Option<String>,
    chunks: ResolvedChunks,
    decoder: Arc<dyn ChunkDecoder>,
}

/// Where entries and their bytes come from: the filers and volume servers.
#[async_trait::async_trait]
trait Source: Send + Sync {
    async fn lookup(&self, dir: &str, name: &str) -> anyhow::Result<Option<Entry>>;
    /// The entries of `dir` whose names start with `prefix`, in name order;
    /// empty for a missing directory.
    async fn list(&self, dir: &str, prefix: &str) -> anyhow::Result<Vec<Entry>>;
    /// Copies a remote-only entry onto the volume servers.
    async fn cache_remote(&self, dir: &str, name: &str) -> anyhow::Result<Entry>;
    async fn resolve(
        &self,
        entry: &Entry,
        decoder: Arc<dyn ChunkDecoder>,
    ) -> anyhow::Result<ResolvedChunks>;
    async fn read(&self, object: &Object, offset: u64, len: u64) -> anyhow::Result<Vec<u8>>;
    fn name(&self) -> String;
}

/// The cluster's filers as the masters list them, used one at a time; an
/// unreachable one is passed over for the next.
struct Filers {
    clients: RwLock<Vec<FilerClient>>,
    dial: FilerDialer,
    current: AtomicUsize,
    volumes: VolumeReader,
}

/// Connects to a filer by its gRPC `host:port`.
pub type FilerDialer = Arc<dyn Fn(&str) -> anyhow::Result<FilerClient> + Send + Sync>;

impl Filers {
    /// Follows the masters' list: known filers keep their connections, new
    /// ones are connected lazily, gone ones are dropped.
    fn set_addresses(&self, addresses: &[String]) -> anyhow::Result<()> {
        let known = self.clients.read().unwrap().clone();
        let mut clients = Vec::with_capacity(addresses.len());
        for addr in addresses {
            clients.push(match known.iter().find(|c| c.addr() == addr) {
                Some(client) => client.clone(),
                None => (self.dial)(addr)?,
            });
        }
        let before: Vec<&str> = known.iter().map(|c| c.addr()).collect();
        if before != addresses.iter().map(String::as_str).collect::<Vec<_>>() {
            tracing::info!("filers: {}", addresses.join(","));
        }
        *self.clients.write().unwrap() = clients;
        Ok(())
    }

    async fn with<'a, T, F>(&self, what: &str, op: F) -> anyhow::Result<T>
    where
        F: Fn(FilerClient) -> BoxFuture<'a, anyhow::Result<T>>,
    {
        let clients = self.clients.read().unwrap().clone();
        if clients.is_empty() {
            bail!("no filer is registered with the masters");
        }
        let start = self.current.load(Ordering::Relaxed);
        let mut last = None;
        for attempt in 0..clients.len() {
            let i = (start + attempt) % clients.len();
            match tokio::time::timeout(FILER_TIMEOUT, op(clients[i].clone())).await {
                Ok(Ok(value)) => {
                    self.current.store(i, Ordering::Relaxed);
                    return Ok(value);
                }
                Ok(Err(err)) if sw_client::is_unavailable(&err) => {
                    tracing::warn!(
                        "filer {} unavailable for {what}: {err:#}",
                        clients[i].addr()
                    );
                    last = Some(err);
                }
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    tracing::warn!(
                        "filer {} did not answer {what} within {FILER_TIMEOUT:?}",
                        clients[i].addr()
                    );
                    last = Some(anyhow::anyhow!(
                        "filer {} did not answer {what} within {FILER_TIMEOUT:?}",
                        clients[i].addr()
                    ));
                }
            }
        }
        Err(last.expect("at least one filer"))
    }
}

#[async_trait::async_trait]
impl Source for Filers {
    async fn lookup(&self, dir: &str, name: &str) -> anyhow::Result<Option<Entry>> {
        self.with("lookup", |mut c| {
            Box::pin(async move { c.lookup_entry(dir, name).await })
        })
        .await
    }

    async fn list(&self, dir: &str, prefix: &str) -> anyhow::Result<Vec<Entry>> {
        self.with("list", |mut c| {
            Box::pin(async move {
                let mut entries = Vec::new();
                let (mut cursor, mut snapshot) = (String::new(), 0);
                loop {
                    let page = match c
                        .list_entries_page_prefixed(dir, prefix, &cursor, snapshot)
                        .await
                    {
                        Ok(page) => page,
                        Err(err) if is_not_found(&err) => return Ok(entries),
                        Err(err) => return Err(err),
                    };
                    entries.extend(page.entries);
                    if page.eof {
                        return Ok(entries);
                    }
                    cursor = page.cursor;
                    snapshot = page.snapshot_ts_ns;
                }
            })
        })
        .await
    }

    async fn cache_remote(&self, dir: &str, name: &str) -> anyhow::Result<Entry> {
        self.with("cache remote", |mut c| {
            Box::pin(async move { c.cache_remote_object(dir, name).await })
        })
        .await
    }

    async fn resolve(
        &self,
        entry: &Entry,
        decoder: Arc<dyn ChunkDecoder>,
    ) -> anyhow::Result<ResolvedChunks> {
        let options = ReadOptions {
            exact: true,
            decoder: Some(decoder),
        };
        self.with("resolve", |mut c| {
            let options = options.clone();
            Box::pin(async move {
                sw_client::resolve_chunks(&mut c, &self.volumes, &entry.chunks, &options).await
            })
        })
        .await
    }

    async fn read(&self, object: &Object, offset: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        let options = ReadOptions {
            exact: true,
            decoder: Some(object.decoder.clone()),
        };
        let read = || {
            self.with("read", |mut c| {
                let options = options.clone();
                Box::pin(async move {
                    sw_client::read_resolved(
                        &mut c,
                        &self.volumes,
                        &object.chunks,
                        &object.entry,
                        offset,
                        len,
                        &options,
                    )
                    .await
                })
            })
        };
        match read().await {
            Ok(bytes) => Ok(bytes),
            // A failed fetch forgets the volume locations it was made
            // against, so one more attempt looks them up afresh: a volume
            // that moved is found where it is now.
            Err(first) => read()
                .await
                .map_err(|again| again.context(format!("after a first attempt failed: {first:#}"))),
        }
    }

    fn name(&self) -> String {
        self.clients
            .read()
            .unwrap()
            .iter()
            .map(|c| c.addr())
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// The filer answers a listing of a missing directory with its missing-entry
/// error; the filer client's tonic is not this crate's, so it is read as text.
fn is_not_found(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.to_string().contains("no entry is found"))
}

fn child(dir: &Path, name: &str) -> Path {
    Path::from_iter(
        dir.parts()
            .chain([object_store::path::PathPart::from(name)]),
    )
}

#[derive(Default)]
struct Stats {
    reads: AtomicU64,
    bytes: AtomicU64,
    lookups: AtomicU64,
    listings: AtomicU64,
    failures: AtomicU64,
}

struct Shared {
    source: Arc<dyn Source>,
    buckets_dir: String,
    keys: Arc<SseS3Keys>,
    /// Write-once files, resolved once; everything else is looked up per
    /// request so reads follow writes.
    objects: moka::sync::Cache<String, Arc<Object>>,
    stats: Stats,
}

impl fmt::Debug for DirectReads {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectReads")
            .field("filers", &self.shared.source.name())
            .field("buckets_dir", &self.shared.buckets_dir)
            .field("sse_s3", &self.shared.keys)
            .finish()
    }
}

/// What direct reads connect to, as the cluster's security.toml configures
/// weed processes.
#[derive(Default)]
pub struct Connection {
    /// The filers' gRPC `host:port`s as the masters list them; see
    /// [`DirectReads::set_filers`] for later changes.
    pub filers: Vec<String>,
    /// `jwt.signing.read.key`, signing each volume read.
    pub read_jwt_key: Option<String>,
    pub sse: SseConfig,
    /// `grpc.client`, when the filers' gRPC is under TLS.
    pub grpc_tls: Option<GrpcTls>,
    /// `https.client`, when volume servers serve HTTPS.
    pub https_tls: Option<HttpsTls>,
    /// Dials the filers in place of `grpc_tls`, for a TLS setup sw-client
    /// cannot express, such as a server allowlist.
    pub filer_dialer: Option<FilerDialer>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("filers", &self.filers)
            .field("read_jwt", &self.read_jwt_key.is_some())
            .field("sse", &self.sse)
            .field("grpc_tls", &self.grpc_tls)
            .field("https_tls", &self.https_tls)
            .field("filer_dialer", &self.filer_dialer.is_some())
            .finish()
    }
}

impl DirectReads {
    pub async fn connect(conn: &Connection) -> anyhow::Result<Self> {
        if conn.filers.is_empty() {
            bail!("no filer is registered with the masters");
        }
        let http = match &conn.https_tls {
            Some(tls) => HttpTransport::with_tls(tls)?,
            None => HttpTransport::new(),
        };
        let transport: Arc<dyn VolumeTransport> =
            match conn.read_jwt_key.as_deref().filter(|k| !k.is_empty()) {
                Some(key) => Arc::new(JwtTransport {
                    http,
                    key: jsonwebtoken::EncodingKey::from_secret(key.as_bytes()),
                }),
                None => Arc::new(http),
            };
        let dial: FilerDialer = match &conn.filer_dialer {
            Some(dialer) => dialer.clone(),
            None => {
                let tls = conn.grpc_tls.clone();
                Arc::new(move |addr| FilerClient::connect_lazy_with(addr, tls.as_ref()))
            }
        };
        let filers = Arc::new(Filers {
            clients: RwLock::new(Vec::new()),
            dial,
            current: AtomicUsize::new(0),
            // Lance caches above this store, so needles are not kept here.
            volumes: VolumeReader::with_transport_capped(transport, 0),
        });
        filers.set_addresses(&conn.filers)?;
        let config = filers
            .with("configuration", |mut c| {
                Box::pin(async move { c.get_configuration().await })
            })
            .await?;
        let kek_file = filers
            .lookup(KEK_PATH.0, KEK_PATH.1)
            .await
            .context("looking up the filer's SSE-S3 KEK")?
            .map(|entry| entry.content);
        let keys = SseS3Keys::load(&conn.sse, kek_file.as_deref())?;
        let mut reads = Self::new(filers.clone(), &config.dir_buckets, Arc::new(keys));
        reads.filers = Some(filers);
        reads.report_every(Duration::from_secs(30));
        Ok(reads)
    }

    fn new(source: Arc<dyn Source>, buckets_dir: &str, keys: Arc<SseS3Keys>) -> Self {
        let buckets_dir = match buckets_dir.trim_end_matches('/') {
            "" => "/buckets".to_string(),
            dir => dir.to_string(),
        };
        Self {
            shared: Arc::new(Shared {
                source,
                buckets_dir,
                keys,
                objects: moka::sync::Cache::new(OBJECT_CACHE_ENTRIES),
                stats: Stats::default(),
            }),
            filers: None,
        }
    }

    /// The filers as the masters now list them.
    pub fn set_filers(&self, addresses: &[String]) -> anyhow::Result<()> {
        match &self.filers {
            Some(filers) => filers.set_addresses(addresses),
            None => Ok(()),
        }
    }

    /// Whether SSE-S3 objects can be read.
    pub fn sse_s3_configured(&self) -> bool {
        self.shared.keys.configured()
    }

    fn report_every(&self, period: Duration) {
        let shared = Arc::downgrade(&self.shared);
        tokio::spawn(async move {
            let mut last = (0, 0, 0);
            loop {
                tokio::time::sleep(period).await;
                let Some(s) = shared.upgrade() else { return };
                let now = (
                    s.stats.reads.load(Ordering::Relaxed),
                    s.stats.lookups.load(Ordering::Relaxed),
                    s.stats.failures.load(Ordering::Relaxed),
                );
                if now != last {
                    tracing::info!(
                        "direct reads: {} reads ({} MiB), {} lookups, {} listings, {} failures",
                        now.0,
                        s.stats.bytes.load(Ordering::Relaxed) >> 20,
                        now.1,
                        s.stats.listings.load(Ordering::Relaxed),
                        now.2
                    );
                    last = now;
                }
            }
        });
    }
}

impl WrappingObjectStore for DirectReads {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        match store_prefix.strip_prefix("s3$") {
            Some(bucket) if !bucket.is_empty() => Arc::new(DirectStore {
                inner: original,
                bucket: bucket.into(),
                reads: self.shared.clone(),
            }),
            _ => original,
        }
    }
}

/// Applies wrappers innermost first.
#[derive(Debug)]
pub struct Wrappers(pub Vec<Arc<dyn WrappingObjectStore>>);

impl WrappingObjectStore for Wrappers {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.0
            .iter()
            .fold(original, |store, wrapper| wrapper.wrap(store_prefix, store))
    }
}

/// Lance names data files `data/<uuid>.lance`, deletion files
/// `_deletions/<fragment>-<version>-<random>.<ext>` and index files
/// `_indices/<uuid>/<name>`; nothing else under a table is write-once.
fn immutable(path: &Path) -> bool {
    let parts: Vec<_> = path.parts().collect();
    let Some((name, dirs)) = parts.split_last() else {
        return false;
    };
    match dirs {
        [.., dir] if dir.as_ref() == "data" => name.as_ref().ends_with(".lance"),
        [.., dir] if dir.as_ref() == "_deletions" => true,
        [.., dir, id] if dir.as_ref() == "_indices" => !id.as_ref().starts_with('_'),
        _ => false,
    }
}

fn remote_only(entry: &Entry) -> bool {
    entry.chunks.is_empty()
        && entry
            .remote_entry
            .as_ref()
            .is_some_and(|remote| remote.remote_size > 0)
}

/// The entry's size as the gateway computes it, before manifests expand.
fn entry_size(entry: &Entry) -> u64 {
    let attributes = entry.attributes.as_ref();
    let mut size = attributes.map_or(0, |a| a.file_size);
    if let Some(remote) = &entry.remote_entry
        && remote.remote_mtime > attributes.map_or(0, |a| a.mtime)
    {
        size = size.max(remote.remote_size.max(0) as u64);
    }
    let chunk_end = entry
        .chunks
        .iter()
        .map(|c| c.offset.max(0) as u64 + c.size)
        .max()
        .unwrap_or(0);
    size.max(chunk_end).max(entry.content.len() as u64)
}

/// The object's size as the gateway reports it.
fn file_size(entry: &Entry, chunks: &ResolvedChunks) -> u64 {
    entry_size(entry).max(chunks.end())
}

/// A directory with no object at its bare key. A file that a child write
/// promoted to a directory keeps its data, and a prefix object marks a
/// directory that is also the key without the trailing slash; the gateway
/// serves and lists both as objects.
fn bare_directory(entry: &Entry) -> bool {
    entry.is_directory
        && entry_size(entry) == 0
        && !entry.extended.contains_key(PREFIX_OBJECT_ATTRIBUTE)
}

/// The object's ETag as the gateway reports it: its stored S3 ETag, else the
/// remote ETag while the entry holds the remote bytes, else the entry's md5,
/// else derived from the chunks', quoted.
fn etag(entry: &Entry) -> Option<String> {
    let quoted = |etag: String| {
        if etag.starts_with('"') {
            etag
        } else {
            format!("\"{etag}\"")
        }
    };
    if let Some(stored) = entry
        .extended
        .get(ETAG_ATTRIBUTE)
        .filter(|v| !v.is_empty())
        .and_then(|v| std::str::from_utf8(v).ok())
    {
        return Some(quoted(stored.to_string()));
    }
    // A synced cache fill, or a remote-only entry; local writes clear the
    // sync stamp or add chunks or content.
    if let Some(remote) = entry
        .remote_entry
        .as_ref()
        .filter(|r| !r.remote_e_tag.is_empty())
    {
        let size = entry.attributes.as_ref().map_or(0, |a| a.file_size);
        if size == remote.remote_size.max(0) as u64
            && (remote.last_local_sync_ts_ns > 0
                || (entry.chunks.is_empty() && entry.content.is_empty()))
        {
            return Some(quoted(remote.remote_e_tag.clone()));
        }
    }
    if let Some(md5) = entry
        .attributes
        .as_ref()
        .map(|a| &a.md5)
        .filter(|m| !m.is_empty())
    {
        return Some(quoted(hex::encode(md5)));
    }
    let digests: Vec<Vec<u8>> = entry
        .chunks
        .iter()
        .map(|c| BASE64_STANDARD.decode(&c.e_tag).unwrap_or_default())
        .collect();
    match digests.as_slice() {
        [] => None,
        [one] => Some(quoted(hex::encode(one))),
        many => {
            let joined = Md5::digest(many.concat());
            Some(quoted(format!("{}-{}", hex::encode(joined), many.len())))
        }
    }
}

/// Keeps the whole cause chain in the message object_store shows.
fn generic(err: anyhow::Error) -> Error {
    Error::Generic {
        store: "DirectReads",
        source: format!("{err:#}").into(),
    }
}

fn not_found(location: &Path) -> Error {
    Error::NotFound {
        path: location.to_string(),
        source: "the filer has no such entry".into(),
    }
}

#[derive(Debug)]
struct DirectStore {
    inner: Arc<dyn ObjectStore>,
    bucket: Arc<str>,
    reads: Arc<Shared>,
}

impl fmt::Display for DirectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DirectReads({})", self.inner)
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DirectReads")
    }
}

impl Object {
    fn meta(&self, location: &Path) -> ObjectMeta {
        let mtime = self.entry.attributes.as_ref().map_or(0, |a| a.mtime);
        ObjectMeta {
            location: location.clone(),
            last_modified: DateTime::<Utc>::from_timestamp(mtime, 0).unwrap_or_default(),
            size: self.size,
            e_tag: self.etag.clone(),
            version: None,
        }
    }
}

impl DirectStore {
    fn filer_path(&self, location: &Path) -> String {
        format!("{}/{}/{}", self.reads.buckets_dir, self.bucket, location)
    }

    fn directory(&self, prefix: Option<&Path>) -> String {
        match prefix {
            Some(prefix) if !prefix.as_ref().is_empty() => self.filer_path(prefix),
            _ => format!("{}/{}", self.reads.buckets_dir, self.bucket),
        }
    }

    async fn open(&self, location: &Path) -> Result<Arc<Object>> {
        let path = self.filer_path(location);
        let cacheable = immutable(location);
        if cacheable && let Some(hit) = self.reads.objects.get(&path) {
            return Ok(hit);
        }
        let object = self.fetch(location, &path).await.map_err(|err| {
            // Lance probes for manifests that do not exist.
            if !matches!(err, Error::NotFound { .. }) {
                self.reads.stats.failures.fetch_add(1, Ordering::Relaxed);
            }
            err
        })?;
        if cacheable {
            self.reads.objects.insert(path, object.clone());
        }
        Ok(object)
    }

    async fn fetch(&self, location: &Path, path: &str) -> Result<Arc<Object>> {
        let (dir, name) = path.rsplit_once('/').expect("paths are absolute");
        self.reads.stats.lookups.fetch_add(1, Ordering::Relaxed);
        let entry = self
            .reads
            .source
            .lookup(dir, name)
            .await
            .with_context(|| format!("looking up {path}"))
            .map_err(generic)?;
        let mut entry = match entry {
            Some(entry) if !bare_directory(&entry) => entry,
            _ => return Err(not_found(location)),
        };
        if remote_only(&entry) {
            entry = self
                .reads
                .source
                .cache_remote(dir, name)
                .await
                .with_context(|| format!("caching remote {path}"))
                .map_err(generic)?;
        }
        let decoder = Arc::new(
            EntryDecoder::new(self.reads.keys.clone(), &entry)
                .with_context(|| format!("decoding {path}"))
                .map_err(generic)?,
        );
        let chunks = self
            .reads
            .source
            .resolve(&entry, decoder.clone())
            .await
            .with_context(|| format!("resolving {path}"))
            .map_err(generic)?;
        Ok(Arc::new(Object {
            size: file_size(&entry, &chunks),
            etag: etag(&entry),
            entry,
            chunks,
            decoder,
        }))
    }

    async fn read(&self, location: &Path, object: &Object, range: Range<u64>) -> Result<Bytes> {
        let want = range.end - range.start;
        let read = self
            .reads
            .source
            .read(object, range.start, want)
            .await
            .and_then(|bytes| {
                if bytes.len() as u64 == want {
                    Ok(bytes)
                } else {
                    Err(anyhow::anyhow!("read {} of {want} bytes", bytes.len()))
                }
            });
        let bytes = read.map_err(|err| {
            // The entry may have changed under a cached object.
            self.reads.objects.invalidate(&self.filer_path(location));
            self.reads.stats.failures.fetch_add(1, Ordering::Relaxed);
            generic(err.context(format!("reading {location} {range:?}")))
        })?;
        self.reads.stats.reads.fetch_add(1, Ordering::Relaxed);
        self.reads.stats.bytes.fetch_add(want, Ordering::Relaxed);
        Ok(bytes.into())
    }

    fn object_meta(&self, prefix: &Path, entry: &Entry) -> ObjectMeta {
        ObjectMeta {
            location: child(prefix, &entry.name),
            last_modified: DateTime::<Utc>::from_timestamp(
                entry.attributes.as_ref().map_or(0, |a| a.mtime),
                0,
            )
            .unwrap_or_default(),
            size: entry_size(entry),
            e_tag: etag(entry),
            version: None,
        }
    }

    async fn list_dir(&self, prefix: Option<&Path>) -> Result<Vec<Entry>> {
        self.reads.stats.listings.fetch_add(1, Ordering::Relaxed);
        let dir = self.directory(prefix);
        self.reads
            .source
            .list(&dir, "")
            .await
            .with_context(|| format!("listing {dir}"))
            .map_err(generic)
    }

    /// Every object under `prefix`, directories first-to-last in name order,
    /// after `offset` when given.
    async fn list_all(
        &self,
        prefix: Option<Path>,
        offset: Option<Path>,
    ) -> Result<Vec<ObjectMeta>> {
        let mut objects = Vec::new();
        let mut pending = vec![prefix.unwrap_or_default()];
        while let Some(dir) = pending.pop() {
            let entries = self.list_dir(Some(&dir)).await?;
            let mut subdirs = Vec::new();
            for entry in entries {
                if !bare_directory(&entry) {
                    let meta = self.object_meta(&dir, &entry);
                    if offset.as_ref().is_none_or(|o| meta.location > *o) {
                        objects.push(meta);
                    }
                }
                if entry.is_directory {
                    subdirs.push(child(&dir, &entry.name));
                }
            }
            pending.extend(subdirs.into_iter().rev());
        }
        Ok(objects)
    }

    fn stream(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = DirectStore {
            inner: self.inner.clone(),
            bucket: self.bucket.clone(),
            reads: self.reads.clone(),
        };
        let (prefix, offset) = (prefix.cloned(), offset.cloned());
        futures::stream::once(async move { store.list_all(prefix, offset).await })
            .flat_map(|listed| match listed {
                Ok(objects) => futures::stream::iter(objects.into_iter().map(Ok)).boxed(),
                Err(err) => futures::stream::once(async { Err(err) }).boxed(),
            })
            .boxed()
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for DirectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(Error::NotSupported {
                source: "vector buckets keep no object versions".into(),
            });
        }
        let object = self.open(location).await?;
        let meta = object.meta(location);
        options.check_preconditions(&meta)?;
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: Default::default(),
            });
        }
        let range = match &options.range {
            Some(range) => range
                .as_range(object.size)
                .map_err(|source| Error::Generic {
                    store: "DirectReads",
                    source: source.into(),
                })?,
            None => 0..object.size,
        };
        let bytes = self.read(location, &object, range.clone()).await?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::once(async { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let object = self.open(location).await?;
        // Nearby ranges share one volume request, as they do through S3.
        object_store::coalesce_ranges(
            ranges,
            |range| {
                let object = object.clone();
                async move {
                    let range =
                        GetRange::Bounded(range)
                            .as_range(object.size)
                            .map_err(|source| Error::Generic {
                                store: "DirectReads",
                                source: source.into(),
                            })?;
                    self.read(location, &object, range).await
                }
            },
            object_store::OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = DirectStore {
            inner: self.inner.clone(),
            bucket: self.bucket.clone(),
            reads: self.reads.clone(),
        };
        self.inner
            .delete_stream(locations)
            .inspect(move |deleted| {
                if let Ok(location) = deleted {
                    store.reads.objects.invalidate(&store.filer_path(location));
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.stream(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.stream(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let dir = prefix.cloned().unwrap_or_default();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for entry in self.list_dir(prefix).await? {
            if !bare_directory(&entry) {
                result.objects.push(self.object_meta(&dir, &entry));
            }
            if entry.is_directory {
                result.common_prefixes.push(child(&dir, &entry.name));
            }
        }
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Signs each volume read with a JWT naming its file id, as the filer does
/// when `jwt.signing.read.key` is set.
struct JwtTransport {
    http: HttpTransport,
    key: jsonwebtoken::EncodingKey,
}

#[derive(serde::Serialize)]
struct ReadClaims<'a> {
    fid: &'a str,
    exp: u64,
}

impl JwtTransport {
    fn token(&self, file_id: &str) -> anyhow::Result<String> {
        let exp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() + READ_JWT_TTL_SECS;
        Ok(jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &ReadClaims { fid: file_id, exp },
            &self.key,
        )?)
    }
}

#[async_trait::async_trait]
impl VolumeTransport for JwtTransport {
    async fn read(
        &self,
        locations: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
    ) -> anyhow::Result<Vec<u8>> {
        let token = self.token(file_id)?;
        self.http
            .read_with_auth(locations, file_id, range, &token)
            .await
    }

    async fn write(
        &self,
        _location: &str,
        _file_id: &str,
        _auth: &str,
        _filename: &str,
        _data: Bytes,
    ) -> anyhow::Result<()> {
        bail!("direct reads never write")
    }

    fn name(&self) -> &'static str {
        "http+jwt"
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use sw_client::filer_pb::{FileChunk, FuseAttributes, RemoteEntry};

    #[derive(Default)]
    struct Fake {
        entries: Mutex<HashMap<String, Entry>>,
        bytes: Mutex<HashMap<String, Vec<u8>>>,
        lookups: AtomicU64,
        cached: AtomicU64,
        fail_reads: bool,
    }

    #[async_trait::async_trait]
    impl Source for Fake {
        async fn lookup(&self, dir: &str, name: &str) -> anyhow::Result<Option<Entry>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .entries
                .lock()
                .unwrap()
                .get(&format!("{dir}/{name}"))
                .cloned())
        }

        async fn list(&self, dir: &str, prefix: &str) -> anyhow::Result<Vec<Entry>> {
            let entries = self.entries.lock().unwrap();
            let mut listed: Vec<Entry> = entries
                .iter()
                .filter(|(path, _)| {
                    path.rsplit_once('/')
                        .is_some_and(|(d, n)| d == dir && n.starts_with(prefix))
                })
                .map(|(_, e)| e.clone())
                .collect();
            listed.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(listed)
        }

        async fn cache_remote(&self, dir: &str, name: &str) -> anyhow::Result<Entry> {
            self.cached.fetch_add(1, Ordering::Relaxed);
            let mut entries = self.entries.lock().unwrap();
            let entry = entries
                .get_mut(&format!("{dir}/{name}"))
                .context("no such entry")?;
            let remote = entry.remote_entry.as_mut().unwrap();
            remote.last_local_sync_ts_ns = 1;
            let size = remote.remote_size as u64;
            entry.chunks = vec![FileChunk {
                file_id: "3,0c".into(),
                size,
                ..Default::default()
            }];
            Ok(entry.clone())
        }

        async fn resolve(
            &self,
            entry: &Entry,
            _decoder: Arc<dyn ChunkDecoder>,
        ) -> anyhow::Result<ResolvedChunks> {
            ResolvedChunks::from_data_chunks(entry.chunks.clone())
        }

        async fn read(&self, object: &Object, offset: u64, len: u64) -> anyhow::Result<Vec<u8>> {
            if self.fail_reads {
                bail!("volume server unreachable");
            }
            let bytes = self.bytes.lock().unwrap()[&object.entry.name].clone();
            let end = ((offset + len) as usize).min(bytes.len());
            let mut out = vec![0u8; len as usize];
            out[..end - offset as usize].copy_from_slice(&bytes[offset as usize..end]);
            Ok(out)
        }

        fn name(&self) -> String {
            "fake".into()
        }
    }

    impl Fake {
        fn add(&self, path: &str, bytes: &[u8], edit: impl FnOnce(&mut Entry)) {
            let name = path.rsplit_once('/').unwrap().1.to_string();
            let mut entry = Entry {
                name: name.clone(),
                attributes: Some(FuseAttributes {
                    file_size: bytes.len() as u64,
                    mtime: 1_700_000_000,
                    ..Default::default()
                }),
                chunks: vec![FileChunk {
                    file_id: "3,01637037d6".into(),
                    offset: 0,
                    size: bytes.len() as u64,
                    e_tag: BASE64_STANDARD.encode(Md5::digest(bytes)),
                    ..Default::default()
                }],
                ..Default::default()
            };
            edit(&mut entry);
            self.entries.lock().unwrap().insert(path.into(), entry);
            self.bytes.lock().unwrap().insert(name, bytes.to_vec());
        }

        fn add_dir(&self, path: &str) {
            let name = path.rsplit_once('/').unwrap().1.to_string();
            self.entries.lock().unwrap().insert(
                path.into(),
                Entry {
                    name,
                    is_directory: true,
                    ..Default::default()
                },
            );
        }
    }

    const DATA: &str = "vector/idx/data/0101abcd.lance";
    const MANIFEST: &str = "vector/idx/_versions/1.manifest";

    fn no_keys() -> Arc<SseS3Keys> {
        Arc::new(SseS3Keys::load(&SseConfig::default(), None).unwrap())
    }

    async fn store(fake: Arc<Fake>) -> (Arc<dyn ObjectStore>, Arc<InMemory>) {
        let inner = Arc::new(InMemory::new());
        for path in [DATA, MANIFEST] {
            inner
                .put(&Path::from(path), Bytes::from_static(b"through S3!").into())
                .await
                .unwrap();
        }
        let reads = DirectReads::new(fake, "", no_keys());
        (reads.wrap("s3$vb", inner.clone()), inner)
    }

    async fn ranges(store: &Arc<dyn ObjectStore>, path: &str, ranges: &[Range<u64>]) -> Vec<Bytes> {
        store.get_ranges(&Path::from(path), ranges).await.unwrap()
    }

    #[test]
    fn only_write_once_files_are_immutable() {
        for path in [
            "vector/x/data/0101abcd.lance",
            "vector/x/_deletions/3-7-12345.arrow",
            "vector/x/_indices/1b6f/index.idx",
        ] {
            assert!(immutable(&Path::from(path)), "{path}");
        }
        for path in [
            "vector/x/_versions/1.manifest",
            "vector/x/_latest.manifest",
            "vector/x/_refs/tags/weed-vector-query-1.json",
            "vector/x/_transactions/0-abc.txn",
            "vector/x/data/notes.txt",
            "vector/_indices/_versions/1.manifest",
        ] {
            assert!(!immutable(&Path::from(path)), "{path}");
        }
    }

    #[test]
    fn s3_keys_map_under_the_buckets_directory() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (dir, want) in [
            ("", "/buckets/vb/vector/idx/data/a.lance"),
            ("/buckets/", "/buckets/vb/vector/idx/data/a.lance"),
            ("/srv/buckets", "/srv/buckets/vb/vector/idx/data/a.lance"),
        ] {
            let reads = DirectReads::new(Arc::new(Fake::default()), dir, no_keys());
            let store = DirectStore {
                inner: inner.clone(),
                bucket: "vb".into(),
                reads: reads.shared.clone(),
            };
            assert_eq!(
                store.filer_path(&Path::from("vector/idx/data/a.lance")),
                want
            );
            assert_eq!(
                store.directory(None),
                want.rsplit_once("/vector").unwrap().0
            );
            assert_eq!(
                store.directory(Some(&Path::from("vector/idx"))),
                format!("{}/vector/idx", want.rsplit_once("/vector").unwrap().0)
            );
        }
        let reads = DirectReads::new(Arc::new(Fake::default()), "", no_keys());
        assert!(
            reads
                .wrap("s3$vb", inner.clone())
                .to_string()
                .starts_with("DirectReads(")
        );
        for prefix in ["file", "s3$", "gs$vb", "memory"] {
            assert!(
                !reads
                    .wrap(prefix, inner.clone())
                    .to_string()
                    .starts_with("DirectReads(")
            );
        }
    }

    #[tokio::test]
    async fn every_object_is_read_from_the_filer() {
        let fake = Arc::new(Fake::default());
        fake.add(&format!("/buckets/vb/{DATA}"), b"hello world", |_| {});
        fake.add(&format!("/buckets/vb/{MANIFEST}"), b"filer copy!", |_| {});
        fake.add("/buckets/vb/vector/idx/_latest.manifest", b"latest", |_| {});
        let (store, _) = store(fake.clone()).await;

        assert_eq!(
            ranges(&store, DATA, &[0..5, 6..11]).await,
            vec![&b"hello"[..], &b"world"[..]]
        );
        let whole = store.get(&Path::from(DATA)).await.unwrap();
        assert_eq!(whole.meta.size, 11);
        assert_eq!(
            whole.meta.e_tag.as_deref(),
            Some("\"5eb63bbbe01eeed093cb22bb8f5acdc3\"")
        );
        assert_eq!(whole.bytes().await.unwrap(), &b"hello world"[..]);
        let tail = store
            .get_opts(
                &Path::from(DATA),
                GetOptions {
                    range: Some(GetRange::Suffix(5)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(tail.range, 6..11);
        assert_eq!(tail.bytes().await.unwrap(), &b"world"[..]);
        assert_eq!(
            ranges(&store, MANIFEST, &[0..11]).await,
            vec![&b"filer copy!"[..]]
        );
        assert_eq!(
            store
                .get_range(&Path::from("vector/idx/_latest.manifest"), 0..6)
                .await
                .unwrap(),
            &b"latest"[..]
        );
        assert_eq!(fake.lookups.load(Ordering::Relaxed), 3);
        ranges(&store, DATA, &[0..5]).await;
        ranges(&store, MANIFEST, &[0..5]).await;
        assert_eq!(
            fake.lookups.load(Ordering::Relaxed),
            4,
            "write-once files are looked up once, the rest every time"
        );
    }

    #[tokio::test]
    async fn entries_of_every_shape_are_served() {
        let fake = Arc::new(Fake::default());
        fake.add(
            "/buckets/vb/vector/idx/data/inline.lance",
            b"hello world",
            |e| {
                e.content = b"hello world".to_vec();
                e.chunks.clear();
            },
        );
        fake.add("/buckets/vb/vector/idx/data/sparse.lance", b"hello", |e| {
            e.attributes.as_mut().unwrap().file_size = 8;
        });
        fake.add("/buckets/vb/vector/idx/data/sse.lance", b"hello", |e| {
            e.extended
                .insert("X-Amz-Server-Side-Encryption".into(), b"AES256".to_vec());
            e.extended
                .insert("x-seaweedfs-sse-s3-key".into(), b"{}".to_vec());
        });
        fake.add(
            "/buckets/vb/vector/idx/data/remote.lance",
            b"hello world",
            |e| {
                e.chunks.clear();
                e.remote_entry = Some(RemoteEntry {
                    remote_size: 11,
                    ..Default::default()
                });
            },
        );
        fake.add_dir("/buckets/vb/vector/idx/data/dir.lance");
        let (store, _) = store(fake.clone()).await;

        let inline = Path::from("vector/idx/data/inline.lance");
        assert_eq!(
            store.get_range(&inline, 6..11).await.unwrap(),
            &b"world"[..]
        );
        let sparse = Path::from("vector/idx/data/sparse.lance");
        assert_eq!(store.head(&sparse).await.unwrap().size, 8);
        assert_eq!(
            store.get_range(&sparse, 3..8).await.unwrap(),
            &b"lo\0\0\0"[..]
        );
        let remote = Path::from("vector/idx/data/remote.lance");
        assert_eq!(store.get_range(&remote, 0..5).await.unwrap(), &b"hello"[..]);
        assert_eq!(fake.cached.load(Ordering::Relaxed), 1);
        // The decoder is built per object, so an SSE-S3 object without keys
        // fails at open, with a reason.
        let err = store
            .head(&Path::from("vector/idx/data/sse.lance"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("SSE-S3"), "{err}");
        assert!(matches!(
            store.head(&Path::from("vector/idx/data/dir.lance")).await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            store
                .head(&Path::from("vector/idx/data/missing.lance"))
                .await,
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            store
                .get_opts(
                    &inline,
                    GetOptions {
                        version: Some("1".into()),
                        ..Default::default()
                    }
                )
                .await,
            Err(Error::NotSupported { .. })
        ));
    }

    #[tokio::test]
    async fn directories_holding_an_object_are_served_and_listed() {
        let fake = Arc::new(Fake::default());
        fake.add("/buckets/vb/vector/idx/data/a.lance", b"kept", |e| {
            e.is_directory = true;
        });
        fake.add("/buckets/vb/vector/idx/data/a.lance/part", b"p", |_| {});
        fake.add("/buckets/vb/vector/idx/data/marker", b"", |e| {
            e.is_directory = true;
            e.chunks.clear();
            e.extended
                .insert(PREFIX_OBJECT_ATTRIBUTE.into(), Vec::new());
        });
        fake.add_dir("/buckets/vb/vector/idx/data/empty");
        let (store, _) = store(fake).await;

        let promoted = Path::from("vector/idx/data/a.lance");
        assert_eq!(
            store.get_range(&promoted, 0..4).await.unwrap(),
            &b"kept"[..]
        );
        assert_eq!(
            store
                .head(&Path::from("vector/idx/data/marker"))
                .await
                .unwrap()
                .size,
            0
        );
        assert!(matches!(
            store.head(&Path::from("vector/idx/data/empty")).await,
            Err(Error::NotFound { .. })
        ));

        let data = Path::from("vector/idx/data");
        let all: Vec<_> = store
            .list(Some(&data))
            .map_ok(|m| (m.location.to_string(), m.size))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            all,
            vec![
                ("vector/idx/data/a.lance".to_string(), 4),
                ("vector/idx/data/marker".to_string(), 0),
                ("vector/idx/data/a.lance/part".to_string(), 1),
            ]
        );
        let level = store.list_with_delimiter(Some(&data)).await.unwrap();
        assert_eq!(
            level
                .objects
                .iter()
                .map(|m| m.location.to_string())
                .collect::<Vec<_>>(),
            vec!["vector/idx/data/a.lance", "vector/idx/data/marker"]
        );
        assert_eq!(
            level.common_prefixes,
            vec![
                promoted,
                Path::from("vector/idx/data/empty"),
                Path::from("vector/idx/data/marker"),
            ]
        );
    }

    #[tokio::test]
    async fn metadata_and_preconditions_come_from_the_entry() {
        let fake = Arc::new(Fake::default());
        fake.add(&format!("/buckets/vb/{MANIFEST}"), b"filer copy!", |e| {
            e.extended.insert(ETAG_ATTRIBUTE.into(), b"abc-2".to_vec());
        });
        fake.add("/buckets/vb/vector/idx/_versions/2.manifest", b"two", |e| {
            e.attributes.as_mut().unwrap().md5 = vec![0xab; 16];
            e.chunks[0].e_tag.clear();
        });
        fake.add(
            "/buckets/vb/vector/idx/_versions/3.manifest",
            b"three",
            |e| {
                e.chunks = vec![
                    FileChunk {
                        file_id: "3,01".into(),
                        size: 2,
                        e_tag: BASE64_STANDARD.encode([1u8; 16]),
                        ..Default::default()
                    },
                    FileChunk {
                        file_id: "3,02".into(),
                        offset: 2,
                        size: 3,
                        e_tag: BASE64_STANDARD.encode([2u8; 16]),
                        ..Default::default()
                    },
                ];
            },
        );
        fake.add(
            "/buckets/vb/vector/idx/_versions/4.manifest",
            b"four",
            |e| {
                e.chunks.clear();
                e.remote_entry = Some(RemoteEntry {
                    remote_e_tag: "r-4".into(),
                    remote_size: 4,
                    ..Default::default()
                });
            },
        );
        fake.add(
            "/buckets/vb/vector/idx/_versions/5.manifest",
            b"five",
            |e| {
                e.remote_entry = Some(RemoteEntry {
                    remote_e_tag: "r-5".into(),
                    remote_size: 4,
                    ..Default::default()
                });
            },
        );
        let (store, _) = store(fake).await;
        let path = Path::from(MANIFEST);
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.e_tag.as_deref(), Some("\"abc-2\""));
        assert_eq!(meta.last_modified.timestamp(), 1_700_000_000);
        assert_eq!(
            store
                .head(&Path::from("vector/idx/_versions/2.manifest"))
                .await
                .unwrap()
                .e_tag
                .as_deref(),
            Some("\"abababababababababababababababab\"")
        );
        let joined = hex::encode(Md5::digest([[1u8; 16], [2u8; 16]].concat()));
        assert_eq!(
            store
                .head(&Path::from("vector/idx/_versions/3.manifest"))
                .await
                .unwrap()
                .e_tag,
            Some(format!("\"{joined}-2\""))
        );
        // Cached from remote, the entry still holds the remote bytes; written
        // over locally, it no longer does.
        assert_eq!(
            store
                .head(&Path::from("vector/idx/_versions/4.manifest"))
                .await
                .unwrap()
                .e_tag
                .as_deref(),
            Some("\"r-4\"")
        );
        assert_eq!(
            store
                .head(&Path::from("vector/idx/_versions/5.manifest"))
                .await
                .unwrap()
                .e_tag,
            Some(format!("\"{}\"", hex::encode(Md5::digest(b"five"))))
        );

        let get = |options: GetOptions| store.get_opts(&path, options);
        assert!(
            get(GetOptions {
                if_match: Some("\"abc-2\"".into()),
                ..Default::default()
            })
            .await
            .is_ok()
        );
        assert!(matches!(
            get(GetOptions {
                if_match: Some("\"other\"".into()),
                ..Default::default()
            })
            .await,
            Err(Error::Precondition { .. })
        ));
        assert!(matches!(
            get(GetOptions {
                if_none_match: Some("\"abc-2\"".into()),
                ..Default::default()
            })
            .await,
            Err(Error::NotModified { .. })
        ));
        let since = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        assert!(matches!(
            get(GetOptions {
                if_modified_since: Some(since),
                ..Default::default()
            })
            .await,
            Err(Error::NotModified { .. })
        ));
        assert!(
            get(GetOptions {
                if_unmodified_since: Some(since),
                ..Default::default()
            })
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn listings_walk_the_filer_directories() {
        let fake = Arc::new(Fake::default());
        fake.add("/buckets/vb/vector/idx/_versions/1.manifest", b"1", |_| {});
        fake.add("/buckets/vb/vector/idx/_versions/2.manifest", b"22", |_| {});
        fake.add("/buckets/vb/vector/idx/_latest.manifest", b"l", |_| {});
        fake.add("/buckets/vb/vector/idx/data/a.lance", b"aaa", |_| {});
        fake.add_dir("/buckets/vb/vector/idx/_versions");
        fake.add_dir("/buckets/vb/vector/idx/data");
        let (store, _) = store(fake).await;

        let versions: Vec<_> = store
            .list(Some(&Path::from("vector/idx/_versions")))
            .map_ok(|m| (m.location.to_string(), m.size))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            versions,
            vec![
                ("vector/idx/_versions/1.manifest".to_string(), 1),
                ("vector/idx/_versions/2.manifest".to_string(), 2)
            ]
        );
        let after: Vec<_> = store
            .list_with_offset(
                Some(&Path::from("vector/idx")),
                &Path::from("vector/idx/_versions/1.manifest"),
            )
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            after,
            vec!["vector/idx/_versions/2.manifest", "vector/idx/data/a.lance"]
        );

        let level = store
            .list_with_delimiter(Some(&Path::from("vector/idx")))
            .await
            .unwrap();
        assert_eq!(
            level.common_prefixes,
            vec![
                Path::from("vector/idx/_versions"),
                Path::from("vector/idx/data")
            ]
        );
        assert_eq!(
            level
                .objects
                .iter()
                .map(|m| m.location.to_string())
                .collect::<Vec<_>>(),
            vec!["vector/idx/_latest.manifest"]
        );
        let empty: Vec<_> = store
            .list(Some(&Path::from("vector/none")))
            .try_collect()
            .await
            .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn a_failed_read_is_an_error_and_drops_the_cached_object() {
        let fake = Arc::new(Fake {
            fail_reads: true,
            ..Default::default()
        });
        fake.add(&format!("/buckets/vb/{DATA}"), b"hello world", |_| {});
        let (store, _) = store(fake.clone()).await;
        for _ in 0..2 {
            let err = store
                .get_ranges(&Path::from(DATA), &[0..11])
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("volume server unreachable"),
                "{err}"
            );
        }
        assert_eq!(fake.lookups.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn deleting_forgets_the_cached_object() {
        let fake = Arc::new(Fake::default());
        fake.add(&format!("/buckets/vb/{DATA}"), b"hello world", |_| {});
        let (store, _) = store(fake.clone()).await;
        ranges(&store, DATA, &[0..5]).await;
        store.delete(&Path::from(DATA)).await.unwrap();
        ranges(&store, DATA, &[0..5]).await;
        assert_eq!(fake.lookups.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn read_jwts_name_the_file_id() {
        let transport = JwtTransport {
            http: HttpTransport::new(),
            key: jsonwebtoken::EncodingKey::from_secret(b"secret"),
        };
        let token = transport.token("3,01637037d6").unwrap();
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.required_spec_claims.clear();
        let claims = jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &jsonwebtoken::DecodingKey::from_secret(b"secret"),
            &validation,
        )
        .unwrap()
        .claims;
        assert_eq!(claims["fid"], "3,01637037d6");
        assert!(claims["exp"].as_u64().unwrap() > 1_700_000_000);
    }

    #[tokio::test]
    async fn filers_follow_the_masters_and_unreachable_ones_are_passed_over() {
        let filers = Filers {
            clients: RwLock::new(Vec::new()),
            dial: Arc::new(|addr| FilerClient::connect_lazy_with(addr, None)),
            current: AtomicUsize::new(0),
            volumes: VolumeReader::with_transport_capped(Arc::new(HttpTransport::new()), 0),
        };
        let err = filers.lookup("/buckets", "vb").await.unwrap_err();
        assert!(err.to_string().contains("no filer"), "{err:#}");

        filers
            .set_addresses(&["127.0.0.1:1".into(), "127.0.0.1:2".into()])
            .unwrap();
        let err = filers.lookup("/buckets", "vb").await.unwrap_err();
        assert!(sw_client::is_unavailable(&err), "{err:#}");
        assert_eq!(filers.name(), "127.0.0.1:1,127.0.0.1:2");

        filers
            .set_addresses(&["127.0.0.1:2".into(), "127.0.0.1:3".into()])
            .unwrap();
        assert_eq!(filers.name(), "127.0.0.1:2,127.0.0.1:3");
        filers.set_addresses(&[]).unwrap();
        assert!(filers.lookup("/buckets", "vb").await.is_err());
    }
}
