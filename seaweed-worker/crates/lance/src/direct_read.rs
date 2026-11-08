//! A table's storage straight on the filer and the volume servers, skipping
//! the S3 gateway. Reads serve object bytes, metadata and listings alike,
//! whatever the entry looks like (inline content, chunk manifests, sparse or
//! overlapping chunks, encryption by the filer or by the gateway, objects
//! tiered to remote storage). Writes upload chunks to the volume servers the
//! filer assigns and create the entry as the gateway would, encrypting as the
//! bucket and the cluster ask; a conditional create is the filer's exclusive
//! create. Copies and renames are not needed by Lance and not offered.

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
use object_store::{Attribute, Attributes, PutMode, UploadPart};
use object_store::{
    CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result,
};
use sw_client::filer_pb::{Entry, FileChunk, FuseAttributes};
use sw_client::{
    AssignVolumeOptions, Assignment, ChunkDecoder, FilerClient, GrpcTls, HttpTransport, HttpsTls,
    ReadOptions, ResolvedChunks, VolumeReader, VolumeTransport, VolumeWriteReceipt, Writer,
};

use crate::sse::{EntryDecoder, EntryEncoder, KEK_PATH, SseConfig, SseS3Keys};

/// `filer_pb.SSEType.SSE_S3`, as the chunks of an SSE-S3 object record it.
#[cfg(test)]
const SSE_S3_TYPE: i32 = 3;

const OBJECT_CACHE_ENTRIES: u64 = 1 << 16;
const READ_JWT_TTL_SECS: u64 = 60;
/// Bound on one filer's answer, so a filer that accepts the connection and
/// then says nothing is passed over like one that refused it.
const FILER_TIMEOUT: Duration = Duration::from_secs(30);
/// The gateway's object-level ETag, set when it differs from the entry's md5.
const ETAG_ATTRIBUTE: &str = "Seaweed-X-Amz-ETag";
const PREFIX_OBJECT_ATTRIBUTE: &str = "x-seaweedfs-prefix-object";
/// What the gateway records for an object it wrote.
const OCTET_STREAM: &str = "application/octet-stream";
const FILE_MODE: u32 = 0o660;
/// Chunk size when the filer names none.
const DEFAULT_CHUNK_SIZE: usize = 4 << 20;
/// The S3 Tables record of a table bucket, on the bucket's entry.
const TABLE_BUCKET_METADATA: &str = "s3tables.metadata";

#[derive(Clone)]
pub struct DirectReads {
    shared: Arc<Shared>,
    /// Set when the source is the cluster's filers, for [`Self::set_filers`].
    filers: Option<Arc<Filers>>,
    /// Objects written are encrypted with SSE-S3, as the bucket asks.
    sse_s3: bool,
}

/// A chunk uploaded to the volume server the filer assigned.
struct Uploaded {
    chunk: FileChunk,
    assignment: Assignment,
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
    /// Asks the filer for a volume server to hold a needle of `size` bytes
    /// for the file at `path`, in `collection`.
    async fn assign(&self, path: &str, collection: &str, size: u64) -> anyhow::Result<Assignment>;
    /// Writes `data` to its assigned volume server as the chunk of `path` at
    /// `offset`.
    async fn write(
        &self,
        assignment: &Assignment,
        path: &str,
        offset: u64,
        data: Bytes,
    ) -> anyhow::Result<FileChunk>;
    /// Creates `entry` in `dir`; `exclusive` refuses an entry already there
    /// with [`sw_client::EntryExists`].
    async fn create(&self, dir: &str, entry: Entry, exclusive: bool) -> anyhow::Result<()>;
    /// Removes `name` in `dir` and its data, a directory's content too when
    /// `recursive`; a missing entry is no error.
    async fn delete(&self, dir: &str, name: &str, recursive: bool) -> anyhow::Result<()>;
    /// Removes a needle whose entry was never created.
    async fn discard(&self, assignment: &Assignment) -> anyhow::Result<()>;
    fn name(&self) -> String;
}

/// The cluster's filers as the masters list them, used one at a time; an
/// unreachable one is passed over for the next.
struct Filers {
    clients: RwLock<Vec<FilerClient>>,
    dial: FilerDialer,
    current: AtomicUsize,
    volumes: VolumeReader,
    transport: Arc<dyn VolumeTransport>,
    writer: Writer,
}

/// Marks a filer call whose outcome is unknown: the filer took it and never
/// answered. Detect it with [`outcome_uncertain`].
#[derive(Debug)]
struct NoAnswer;

impl fmt::Display for NoAnswer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the filer did not answer")
    }
}

impl std::error::Error for NoAnswer {}

/// Whether a failed filer write may have been applied all the same.
fn outcome_uncertain(err: &anyhow::Error) -> bool {
    err.downcast_ref::<NoAnswer>().is_some() || sw_client::is_unavailable(err)
}

/// Whether `name` is one path component: no separator, not empty, not `.`
/// or `..`, which a filer path would resolve elsewhere.
fn plain_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/')
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
        let mut unanswered = false;
        for attempt in 0..clients.len() {
            let i = (start + attempt) % clients.len();
            match tokio::time::timeout(FILER_TIMEOUT, op(clients[i].clone())).await {
                Ok(Ok(value)) => {
                    self.current.store(i, Ordering::Relaxed);
                    return Ok(value);
                }
                // A filer that fell away may have taken the request first.
                Ok(Err(err)) if sw_client::is_unavailable(&err) => {
                    tracing::warn!(
                        "filer {} unavailable for {what}: {err:#}",
                        clients[i].addr()
                    );
                    unanswered = true;
                    last = Some(err);
                }
                // A filer passed over without an answer may still apply it.
                Ok(Err(err)) if unanswered => return Err(err.context(NoAnswer)),
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    unanswered = true;
                    tracing::warn!(
                        "filer {} did not answer {what} within {FILER_TIMEOUT:?}",
                        clients[i].addr()
                    );
                    last = Some(
                        anyhow::anyhow!(
                            "filer {} did not answer {what} within {FILER_TIMEOUT:?}",
                            clients[i].addr()
                        )
                        .context(NoAnswer),
                    );
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
        // One filer attempt per page, so the deadline bounds a page, not the
        // whole listing of a large directory.
        let mut entries = Vec::new();
        let (mut cursor, mut snapshot) = (String::new(), 0);
        loop {
            let page = self
                .with("list", |mut c| {
                    let cursor = cursor.clone();
                    Box::pin(async move {
                        match c
                            .list_entries_page_prefixed(dir, prefix, &cursor, snapshot)
                            .await
                        {
                            Ok(page) => Ok(Some(page)),
                            Err(err) if is_not_found(&err) => Ok(None),
                            Err(err) => Err(err),
                        }
                    })
                })
                .await?;
            let Some(page) = page else {
                return Ok(entries);
            };
            entries.extend(page.entries);
            if page.eof {
                return Ok(entries);
            }
            cursor = page.cursor;
            snapshot = page.snapshot_ts_ns;
        }
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

    async fn assign(&self, path: &str, collection: &str, size: u64) -> anyhow::Result<Assignment> {
        // The filer picks replication from the path's storage rules, and the
        // collection too when none is named, as it does for the gateway.
        let options = AssignVolumeOptions {
            collection: collection.to_string(),
            ..Default::default()
        };
        self.with("assign", |mut c| {
            let (path, options) = (path.to_string(), options.clone());
            Box::pin(async move { c.assign_volume_with_options(&path, size, &options).await })
        })
        .await
    }

    async fn write(
        &self,
        assignment: &Assignment,
        path: &str,
        offset: u64,
        data: Bytes,
    ) -> anyhow::Result<FileChunk> {
        let name = path.rsplit('/').next().unwrap_or(path).to_string();
        let digest = BASE64_STANDARD.encode(Md5::digest(&data));
        let mut chunk = self
            .writer
            .upload_assigned_chunk_bytes(assignment, &name, offset as i64, data)
            .await?;
        chunk.e_tag = digest;
        Ok(chunk)
    }

    async fn create(&self, dir: &str, entry: Entry, exclusive: bool) -> anyhow::Result<()> {
        self.with("create", |mut c| {
            let (dir, entry) = (dir.to_string(), entry.clone());
            Box::pin(async move {
                c.create_entry(&dir, entry, 0, exclusive, false)
                    .await
                    .map(|_| ())
            })
        })
        .await
    }

    async fn delete(&self, dir: &str, name: &str, recursive: bool) -> anyhow::Result<()> {
        self.with("delete", |mut c| {
            let (dir, name) = (dir.to_string(), name.to_string());
            Box::pin(async move {
                match c.delete_entry(&dir, &name, recursive, true, 0).await {
                    Err(err) if is_not_found(&err) => Ok(()),
                    other => other,
                }
            })
        })
        .await
    }

    async fn discard(&self, assignment: &Assignment) -> anyhow::Result<()> {
        self.transport
            .delete(&assignment.url, &assignment.file_id, &assignment.auth)
            .await
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
/// error, `filer_pb.ErrNotFound`; the filer client's tonic is not this
/// crate's, so it is read as text. Other errors saying "not found", such as
/// an unresolved remote storage, are failures.
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
    writes: AtomicU64,
    written: AtomicU64,
    deletes: AtomicU64,
}

struct Shared {
    source: Arc<dyn Source>,
    buckets_dir: String,
    keys: Arc<SseS3Keys>,
    /// Bytes per uploaded chunk, as the filer configures writers.
    chunk_size: usize,
    /// Chunks are encrypted for the volume servers, as the cluster asks.
    cipher: bool,
    /// The filer group, which prefixes a bucket's collection as the gateway
    /// names it.
    filer_group: String,
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
    /// The filer group, which the gateway prefixes a bucket's collection with.
    pub filer_group: String,
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
            .field("filer_group", &self.filer_group)
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
            volumes: VolumeReader::with_transport_capped(transport.clone(), 0),
            writer: Writer::with_transport(transport.clone()),
            transport,
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
        let chunk_size = match config.max_mb {
            0 => DEFAULT_CHUNK_SIZE,
            mb => (mb as usize) << 20,
        };
        let mut reads = Self::new(filers.clone(), &config.dir_buckets, Arc::new(keys));
        reads.filers = Some(filers);
        {
            let shared = Arc::get_mut(&mut reads.shared).expect("just created");
            shared.chunk_size = chunk_size;
            shared.cipher = config.cipher;
            shared.filer_group = conn.filer_group.clone();
        }
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
                chunk_size: DEFAULT_CHUNK_SIZE,
                cipher: false,
                filer_group: String::new(),
                objects: moka::sync::Cache::builder()
                    .max_capacity(OBJECT_CACHE_ENTRIES)
                    .support_invalidation_closures()
                    .build(),
                stats: Stats::default(),
            }),
            filers: None,
            sse_s3: false,
        }
    }

    /// Whether `bucket` declares SSE-S3 for the files its clients write, as
    /// the S3 write path decides it from the table bucket's record: a Lance
    /// bucket with `sseAlgorithm` AES256. Read from the filer each time, so a
    /// bucket recreated with another setting is written as it now declares.
    pub async fn bucket_encrypts(&self, bucket: &str) -> anyhow::Result<bool> {
        let entry = self
            .shared
            .source
            .lookup(&self.shared.buckets_dir, bucket)
            .await
            .with_context(|| format!("looking up bucket {bucket}"))?;
        let Some(record) =
            entry.and_then(|entry| entry.extended.get(TABLE_BUCKET_METADATA).cloned())
        else {
            return Ok(false);
        };
        let record: serde_json::Value = serde_json::from_slice(&record)
            .with_context(|| format!("bucket {bucket} has an unreadable S3 Tables record"))?;
        Ok(record["format"] == "LANCE"
            && record["encryptionConfiguration"]["sseAlgorithm"] == "AES256")
    }

    /// The same storage, writing objects encrypted with SSE-S3 when `sse_s3`,
    /// as the bucket being written asks.
    pub fn encrypting(&self, sse_s3: bool) -> Self {
        Self {
            shared: self.shared.clone(),
            filers: self.filers.clone(),
            sse_s3,
        }
    }

    /// Removes the table at `location` (`s3://bucket/path`) with everything
    /// under it.
    pub async fn remove_table(&self, location: &str) -> anyhow::Result<()> {
        let rest = location
            .strip_prefix("s3://")
            .with_context(|| format!("{location} is not an s3:// location"))?
            .trim_end_matches('/');
        let (bucket, path) = rest
            .split_once('/')
            .with_context(|| format!("{location} names no table under its bucket"))?;
        if !plain_name(bucket) || !path.split('/').all(plain_name) {
            bail!("{location} is not a table path");
        }
        let (parent, name) = match path.rsplit_once('/') {
            Some((parent, name)) => (
                format!("{}/{bucket}/{parent}", self.shared.buckets_dir),
                name,
            ),
            None => (format!("{}/{bucket}", self.shared.buckets_dir), path),
        };
        self.shared.stats.deletes.fetch_add(1, Ordering::Relaxed);
        self.shared
            .source
            .delete(&parent, name, true)
            .await
            .inspect_err(|_| {
                self.shared.stats.failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let under = format!("{parent}/{name}/");
        self.shared
            .objects
            .invalidate_entries_if(move |key, _| key.starts_with(&under))
            .map_err(|err| anyhow::anyhow!("forgetting {location}: {err}"))?;
        Ok(())
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
            let mut last = (0, 0, 0, 0);
            loop {
                tokio::time::sleep(period).await;
                let Some(s) = shared.upgrade() else { return };
                let now = (
                    s.stats.reads.load(Ordering::Relaxed),
                    s.stats.writes.load(Ordering::Relaxed)
                        + s.stats.deletes.load(Ordering::Relaxed),
                    s.stats.lookups.load(Ordering::Relaxed),
                    s.stats.failures.load(Ordering::Relaxed),
                );
                if now != last {
                    tracing::info!(
                        "direct storage: {} reads ({} MiB), {} lookups, {} listings, {} writes ({} MiB), {} deletes, {} failures",
                        now.0,
                        s.stats.bytes.load(Ordering::Relaxed) >> 20,
                        now.2,
                        s.stats.listings.load(Ordering::Relaxed),
                        s.stats.writes.load(Ordering::Relaxed),
                        s.stats.written.load(Ordering::Relaxed) >> 20,
                        s.stats.deletes.load(Ordering::Relaxed),
                        now.3
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
                sse_s3: self.sse_s3,
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

/// Lance commits with a conditional put and never copies or renames.
fn not_implemented(operation: &str) -> Error {
    Error::NotImplemented {
        operation: operation.to_string(),
        implementer: "DirectStorage".to_string(),
    }
}

fn not_found(location: &Path) -> Error {
    Error::NotFound {
        path: location.to_string(),
        source: "the filer has no such entry".into(),
    }
}

#[derive(Debug, Clone)]
struct DirectStore {
    inner: Arc<dyn ObjectStore>,
    bucket: Arc<str>,
    reads: Arc<Shared>,
    sse_s3: bool,
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

    /// What a read reports besides the bytes: the entry's content type.
    fn attributes(&self) -> Attributes {
        match self.entry.attributes.as_ref().map(|a| a.mime.as_str()) {
            Some(mime) if !mime.is_empty() => {
                Attributes::from_iter([(Attribute::ContentType, mime.to_string())])
            }
            _ => Attributes::new(),
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

    /// Removes the object at `location`; a missing one is no error.
    async fn remove(&self, location: &Path) -> Result<()> {
        let path = self.filer_path(location);
        let (dir, name) = path.rsplit_once('/').expect("paths are absolute");
        self.reads.stats.deletes.fetch_add(1, Ordering::Relaxed);
        self.reads
            .source
            .delete(dir, name, false)
            .await
            .inspect_err(|_| {
                self.reads.stats.failures.fetch_add(1, Ordering::Relaxed);
            })
            .with_context(|| format!("deleting {path}"))
            .map_err(generic)?;
        self.reads.objects.invalidate(&path);
        Ok(())
    }

    fn stream(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
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
        let exclusive = match opts.mode {
            PutMode::Overwrite => false,
            PutMode::Create => true,
            PutMode::Update(_) => {
                return Err(Error::NotSupported {
                    source: "the filer keeps no object versions to update against".into(),
                });
            }
        };
        let upload = Upload::start(self.clone(), location.clone(), &opts.attributes)?;
        let data: Bytes = payload.into();
        let md5 = Md5::digest(&data);
        if let Err(err) = upload.put(data).await {
            if let Err(discard) = upload.discard_all().await {
                tracing::warn!("{location}: discarding after a failed upload: {discard:#}");
            }
            return Err(err);
        }
        upload.finish(exclusive, Some(md5.to_vec())).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(Upload::start(
            self.clone(),
            location.clone(),
            &opts.attributes,
        )?))
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
                attributes: object.attributes(),
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
            attributes: object.attributes(),
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
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store.remove(&location).await?;
                    Ok(location)
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

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
        Err(not_implemented("copy"))
    }

    async fn rename_opts(&self, _from: &Path, _to: &Path, _options: RenameOptions) -> Result<()> {
        Err(not_implemented("rename"))
    }
}

/// An object being written: its chunks go to the volume servers as they
/// arrive, the entry is created when they are all there.
struct Upload {
    store: DirectStore,
    location: Path,
    path: String,
    /// The collection the gateway names for the bucket: its filer group's,
    /// or none, so the filer's rules for the path pick it.
    collection: String,
    mime: String,
    encoder: Arc<EntryEncoder>,
    state: Arc<std::sync::Mutex<UploadState>>,
}

#[derive(Default)]
struct UploadState {
    chunks: Vec<Uploaded>,
    /// Where the next part starts.
    offset: u64,
    /// The entry was created, or may have been: nothing more is done with
    /// this upload, and nothing it uploaded is discarded.
    done: bool,
}

impl fmt::Debug for Upload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upload").field("path", &self.path).finish()
    }
}

impl Upload {
    fn start(store: DirectStore, location: Path, attributes: &Attributes) -> Result<Self> {
        let path = store.filer_path(&location);
        let encoder = EntryEncoder::new(store.reads.keys.clone(), store.sse_s3, store.reads.cipher)
            .with_context(|| format!("encoding {path}"))
            .map_err(generic)?;
        let collection = match store.reads.filer_group.as_str() {
            "" => String::new(),
            group => format!("{group}_{}", store.bucket),
        };
        let mime = attributes
            .get(&Attribute::ContentType)
            .map_or(OCTET_STREAM, |v| v.as_ref())
            .to_string();
        Ok(Self {
            store,
            location,
            path,
            collection,
            mime,
            encoder: Arc::new(encoder),
            state: Arc::default(),
        })
    }

    /// Uploads `data` as the chunks following what was put before.
    fn put(&self, data: Bytes) -> UploadPart {
        let offset = {
            let mut state = self.state.lock().unwrap();
            let offset = state.offset;
            state.offset += data.len() as u64;
            offset
        };
        let (store, path, collection, encoder, state) = (
            self.store.clone(),
            self.path.clone(),
            self.collection.clone(),
            self.encoder.clone(),
            self.state.clone(),
        );
        Box::pin(async move {
            let chunk_size = store.reads.chunk_size;
            let mut at = offset;
            for piece in data.chunks(chunk_size.max(1)) {
                let encoded = encoder
                    .encode(at, piece.to_vec())
                    .with_context(|| format!("encoding {path} at {at}"))
                    .map_err(generic)?;
                let size = piece.len() as u64;
                let data = Bytes::from(encoded.data);
                let source = &store.reads.source;
                let failed = |err: anyhow::Error, what: &str| {
                    store.reads.stats.failures.fetch_add(1, Ordering::Relaxed);
                    generic(err.context(format!("{what} {path} at {at}")))
                };
                let reserved = Reserved {
                    source: source.clone(),
                    path: path.clone(),
                    assignment: Some(
                        source
                            .assign(&path, &collection, data.len() as u64)
                            .await
                            .map_err(|err| failed(err, "assigning"))?,
                    ),
                };
                let mut chunk = match source.write(reserved.assignment(), &path, at, data).await {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        reserved.discard().await;
                        return Err(failed(err, "uploading"));
                    }
                };
                chunk.size = size;
                chunk.cipher_key = encoded.cipher_key;
                chunk.sse_type = encoded.sse_type;
                chunk.sse_metadata = encoded.sse_metadata;
                store.reads.stats.written.fetch_add(size, Ordering::Relaxed);
                state.lock().unwrap().chunks.push(Uploaded {
                    chunk,
                    assignment: reserved.disarm(),
                });
                at += size;
            }
            Ok(())
        })
    }

    /// Creates the entry over the chunks uploaded so far. `md5` is the whole
    /// object's, known for a one-shot put; a multipart object's ETag derives
    /// from its chunks, as through the gateway.
    async fn finish(&self, exclusive: bool, md5: Option<Vec<u8>>) -> Result<PutResult> {
        let mut chunks = {
            let mut state = self.state.lock().unwrap();
            if state.done {
                return Err(generic(anyhow::anyhow!(
                    "{} was already completed, or its completion is in doubt",
                    self.path
                )));
            }
            state.done = true;
            std::mem::take(&mut state.chunks)
        };
        chunks.sort_by_key(|u| u.chunk.offset);
        let size = chunks
            .iter()
            .map(|u| u.chunk.offset as u64 + u.chunk.size)
            .max()
            .unwrap_or(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()) as i64;
        let (dir, name) = self.path.rsplit_once('/').expect("paths are absolute");
        let mut entry = Entry {
            name: name.to_string(),
            attributes: Some(FuseAttributes {
                crtime: now,
                mtime: now,
                file_mode: FILE_MODE,
                mime: self.mime.clone(),
                file_size: size,
                md5: md5.clone().unwrap_or_default(),
                ..Default::default()
            }),
            chunks: chunks.iter().map(|u| u.chunk.clone()).collect(),
            ..Default::default()
        };
        if let Some(md5) = &md5 {
            entry
                .extended
                .insert(ETAG_ATTRIBUTE.to_string(), hex::encode(md5).into_bytes());
        }
        let attributes = self.encoder.attributes().map_err(generic)?;
        entry.extended.extend(attributes);
        let e_tag = etag(&entry);
        let created = self.store.reads.source.create(dir, entry, exclusive).await;
        if let Err(err) = created {
            // A create the filer took and never answered may still be applied,
            // and a retried exclusive create may find this very upload in
            // place: chunks the entry references, or may yet reference, are
            // kept. Only a create the filer refused has its chunks discarded.
            let saved = match self.store.reads.source.lookup(dir, name).await {
                Ok(Some(saved)) => saved
                    .chunks
                    .first()
                    .is_some_and(|c| chunks.iter().any(|u| u.chunk.file_id == c.file_id)),
                Ok(None) => false,
                Err(lookup) => {
                    tracing::warn!("{}: keeping its chunks: {lookup:#}", self.path);
                    self.store
                        .reads
                        .stats
                        .failures
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(generic(err.context(format!("creating {}", self.path))));
                }
            };
            if saved {
                tracing::warn!("{}: created despite {err:#}", self.path);
            } else if outcome_uncertain(&err) {
                tracing::warn!("{}: keeping its chunks: {err:#}", self.path);
                self.store
                    .reads
                    .stats
                    .failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(generic(err.context(format!("creating {}", self.path))));
            } else {
                for uploaded in &chunks {
                    if let Err(discard) =
                        self.store.reads.source.discard(&uploaded.assignment).await
                    {
                        tracing::warn!(
                            "{}: discarding {}: {discard:#}",
                            self.path,
                            uploaded.chunk.file_id
                        );
                    }
                }
                self.store
                    .reads
                    .stats
                    .failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(if sw_client::is_entry_exists(&err) {
                    Error::AlreadyExists {
                        path: self.location.to_string(),
                        source: format!("{err:#}").into(),
                    }
                } else {
                    generic(err.context(format!("creating {}", self.path)))
                });
            }
        }
        self.store
            .reads
            .stats
            .writes
            .fetch_add(1, Ordering::Relaxed);
        self.store.reads.objects.invalidate(&self.path);
        Ok(PutResult {
            e_tag,
            version: None,
        })
    }

    /// Discards every chunk uploaded; those a volume server would not give up
    /// are kept for another attempt, by a later abort or the drop.
    async fn discard_all(&self) -> Result<()> {
        let chunks = {
            let mut state = self.state.lock().unwrap();
            state.done = true;
            std::mem::take(&mut state.chunks)
        };
        let kept = discard(&*self.store.reads.source, &self.path, chunks).await;
        if kept.is_empty() {
            return Ok(());
        }
        let ids: Vec<_> = kept.iter().map(|u| u.chunk.file_id.as_str()).collect();
        let err = anyhow::anyhow!("{}: still holding {}", self.path, ids.join(", "));
        self.state.lock().unwrap().chunks.extend(kept);
        Err(generic(err))
    }
}

/// Discards `chunks` on the volume servers and returns those still there.
async fn discard(source: &dyn Source, path: &str, chunks: Vec<Uploaded>) -> Vec<Uploaded> {
    let mut kept = Vec::new();
    for uploaded in chunks {
        if let Err(err) = source.discard(&uploaded.assignment).await {
            tracing::warn!(
                "{path}: discarding {}: {err:#}",
                uploaded.assignment.file_id
            );
            kept.push(uploaded);
        }
    }
    kept
}

/// A needle assigned on a volume server while its write is in flight. The
/// volume server may hold it although the write's answer never came, and
/// nothing else will ever name it: a write abandoned, as when the request
/// is cancelled, or failed gives the needle back.
struct Reserved {
    source: Arc<dyn Source>,
    path: String,
    assignment: Option<Assignment>,
}

impl Reserved {
    fn assignment(&self) -> &Assignment {
        self.assignment.as_ref().expect("reserved until disarmed")
    }

    /// The write succeeded: the entry will name the needle.
    fn disarm(mut self) -> Assignment {
        self.assignment.take().expect("reserved until disarmed")
    }

    async fn discard(mut self) {
        if let Some(assignment) = self.assignment.take() {
            discard_needle(&*self.source, &self.path, &assignment).await;
        }
    }
}

impl Drop for Reserved {
    fn drop(&mut self) {
        let Some(assignment) = self.assignment.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (source, path) = (self.source.clone(), self.path.clone());
        runtime.spawn(async move {
            discard_needle(&*source, &path, &assignment).await;
        });
    }
}

async fn discard_needle(source: &dyn Source, path: &str, assignment: &Assignment) {
    if let Err(err) = source.discard(assignment).await {
        tracing::warn!("{path}: discarding {}: {err:#}", assignment.file_id);
    }
}

/// An upload dropped before it was completed or aborted, as when the request
/// writing it was cancelled, discards what it uploaded; one completed, or
/// whose completion is in doubt, has nothing left to discard.
impl Drop for Upload {
    fn drop(&mut self) {
        let chunks = std::mem::take(&mut self.state.lock().unwrap().chunks);
        if chunks.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (source, path) = (self.store.reads.source.clone(), self.path.clone());
        runtime.spawn(async move {
            discard(&*source, &path, chunks).await;
        });
    }
}

#[async_trait::async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.put(data.into())
    }

    async fn complete(&mut self) -> Result<PutResult> {
        self.finish(false, None).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.discard_all().await
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
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
    ) -> anyhow::Result<()> {
        self.http
            .write(location, file_id, auth, filename, data)
            .await
    }

    async fn write_synced(
        &self,
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
        fsync: bool,
    ) -> anyhow::Result<VolumeWriteReceipt> {
        self.http
            .write_synced(location, file_id, auth, filename, data, fsync)
            .await
    }

    async fn delete(&self, location: &str, file_id: &str, auth: &str) -> anyhow::Result<()> {
        self.http.delete(location, file_id, auth).await
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
        /// Needles uploaded through [`Source::upload`], by file id.
        needles: Mutex<HashMap<String, Vec<u8>>>,
        lookups: AtomicU64,
        cached: AtomicU64,
        uploads: AtomicU64,
        discarded: AtomicU64,
        fail_reads: bool,
        fail_lookups: bool,
        /// Creates save the entry and then answer with an error, as a filer
        /// whose reply was lost.
        lose_create_replies: bool,
        /// Creates never answer and never save, as a filer that took the call
        /// and went silent.
        silent_creates: bool,
        /// Assignments after this many fail.
        uploads_allowed: Option<u64>,
        /// Chunk writes never finish, as to a volume server that stopped
        /// answering.
        stall_writes: bool,
        fail_deletes: bool,
        /// This many discards fail before any succeeds.
        failing_discards: AtomicU64,
        /// The collections uploads named.
        collections: Mutex<std::collections::HashSet<String>>,
    }

    #[async_trait::async_trait]
    impl Source for Fake {
        async fn lookup(&self, dir: &str, name: &str) -> anyhow::Result<Option<Entry>> {
            self.lookups.fetch_add(1, Ordering::Relaxed);
            if self.fail_lookups {
                bail!("the filer is unreachable");
            }
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
            let bytes = match self.bytes.lock().unwrap().get(&object.entry.name) {
                Some(bytes) => bytes.clone(),
                // Written through the store: the needles, decoded as a volume
                // read would be.
                None => {
                    let needles = self.needles.lock().unwrap();
                    let mut whole = vec![0u8; object.size as usize];
                    for chunk in &object.entry.chunks {
                        let raw = needles[&chunk.file_id].clone();
                        let plain = object.decoder.decode(chunk, 0, raw)?;
                        let at = chunk.offset as usize;
                        whole[at..at + plain.len()].copy_from_slice(&plain);
                    }
                    whole
                }
            };
            let end = ((offset + len) as usize).min(bytes.len());
            let mut out = vec![0u8; len as usize];
            out[..end - offset as usize].copy_from_slice(&bytes[offset as usize..end]);
            Ok(out)
        }

        async fn assign(
            &self,
            _path: &str,
            collection: &str,
            _size: u64,
        ) -> anyhow::Result<Assignment> {
            self.collections
                .lock()
                .unwrap()
                .insert(collection.to_string());
            let n = self.uploads.fetch_add(1, Ordering::Relaxed) + 1;
            if self.uploads_allowed.is_some_and(|allowed| n > allowed) {
                bail!("volume server unreachable");
            }
            Ok(Assignment {
                file_id: format!("7,{n:08x}"),
                url: "volume:8080".into(),
                auth: String::new(),
                fsync: false,
            })
        }

        async fn write(
            &self,
            assignment: &Assignment,
            _path: &str,
            offset: u64,
            data: Bytes,
        ) -> anyhow::Result<FileChunk> {
            if self.stall_writes {
                std::future::pending::<()>().await;
            }
            self.needles
                .lock()
                .unwrap()
                .insert(assignment.file_id.clone(), data.to_vec());
            Ok(FileChunk {
                file_id: assignment.file_id.clone(),
                offset: offset as i64,
                size: data.len() as u64,
                e_tag: BASE64_STANDARD.encode(Md5::digest(&data)),
                ..Default::default()
            })
        }

        async fn create(&self, dir: &str, entry: Entry, exclusive: bool) -> anyhow::Result<()> {
            if self.silent_creates {
                return Err(
                    anyhow::anyhow!("filer took the create and went silent").context(NoAnswer)
                );
            }
            let mut entries = self.entries.lock().unwrap();
            let path = format!("{dir}/{}", entry.name);
            if exclusive && entries.contains_key(&path) {
                return Err(anyhow::anyhow!("CreateEntry: {path}: entry already exists")
                    .context(sw_client::EntryExists));
            }
            entries.insert(path, entry);
            if self.lose_create_replies {
                bail!("the filer did not answer");
            }
            Ok(())
        }

        async fn delete(&self, dir: &str, name: &str, recursive: bool) -> anyhow::Result<()> {
            if self.fail_deletes {
                bail!("the filer is unreachable");
            }
            let path = format!("{dir}/{name}");
            let under = format!("{path}/");
            self.entries
                .lock()
                .unwrap()
                .retain(|key, _| key != &path && !(recursive && key.starts_with(&under)));
            Ok(())
        }

        async fn discard(&self, assignment: &Assignment) -> anyhow::Result<()> {
            if self.failing_discards.load(Ordering::Relaxed) > 0 {
                self.failing_discards.fetch_sub(1, Ordering::Relaxed);
                bail!("volume server unreachable");
            }
            self.discarded.fetch_add(1, Ordering::Relaxed);
            self.needles.lock().unwrap().remove(&assignment.file_id);
            Ok(())
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
                sse_s3: false,
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
        // Written again at the same path: the next read looks it up afresh.
        fake.add(&format!("/buckets/vb/{DATA}"), b"hello again", |_| {});
        assert_eq!(
            ranges(&store, DATA, &[6..11]).await[0],
            Bytes::from_static(b"again")
        );
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
        let transport: Arc<dyn VolumeTransport> = Arc::new(HttpTransport::new());
        let filers = Filers {
            clients: RwLock::new(Vec::new()),
            dial: Arc::new(|addr| FilerClient::connect_lazy_with(addr, None)),
            current: AtomicUsize::new(0),
            volumes: VolumeReader::with_transport_capped(transport.clone(), 0),
            writer: Writer::with_transport(transport.clone()),
            transport,
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

    /// A filer that never answered may still apply the request, which the
    /// error says even when the next filer refuses it.
    #[tokio::test(start_paused = true)]
    async fn filers_that_never_answer_leave_the_outcome_open() {
        let transport: Arc<dyn VolumeTransport> = Arc::new(HttpTransport::new());
        let filers = Filers {
            clients: RwLock::new(Vec::new()),
            dial: Arc::new(|addr| FilerClient::connect_lazy_with(addr, None)),
            current: AtomicUsize::new(0),
            volumes: VolumeReader::with_transport_capped(transport.clone(), 0),
            writer: Writer::with_transport(transport.clone()),
            transport,
        };
        filers
            .set_addresses(&["127.0.0.1:1".into(), "127.0.0.1:2".into()])
            .unwrap();
        let silent = |c: FilerClient| -> BoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async move {
                if c.addr() == "127.0.0.1:1" {
                    std::future::pending::<()>().await;
                }
                bail!("refused")
            })
        };
        let err = filers.with("create", silent).await.unwrap_err();
        assert!(outcome_uncertain(&err), "{err:#}");
        assert!(format!("{err:#}").contains("refused"), "{err:#}");

        let fell_away = |c: FilerClient| -> BoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async move {
                if c.addr() == "127.0.0.1:1" {
                    return Err(tonic::Status::unavailable("connection reset").into());
                }
                bail!("refused")
            })
        };
        let err = filers.with("create", fell_away).await.unwrap_err();
        assert!(outcome_uncertain(&err), "{err:#}");
        assert!(format!("{err:#}").contains("refused"), "{err:#}");

        let answered = |_: FilerClient| -> BoxFuture<'static, anyhow::Result<()>> {
            Box::pin(async { bail!("refused") })
        };
        let err = filers.with("create", answered).await.unwrap_err();
        assert!(!outcome_uncertain(&err), "{err:#}");
    }

    #[test]
    fn only_the_filers_missing_entry_error_is_not_found() {
        let missing = anyhow::anyhow!("filer: no entry is found in filer store");
        assert!(is_not_found(&missing.context("ListEntries")));
        let remote = anyhow::anyhow!("resolve remote storage client for /buckets/vb/t: not found");
        assert!(!is_not_found(&remote.context("DeleteEntry")));
    }

    /// Failed uploads and deletes show in the storage report.
    #[tokio::test]
    async fn failed_writes_and_deletes_are_counted() {
        let fake = Arc::new(Fake {
            uploads_allowed: Some(0),
            fail_deletes: true,
            ..Default::default()
        });
        let reads = DirectReads::new(fake.clone(), "", no_keys());
        let store = reads.wrap("s3$vb", Arc::new(InMemory::new()));
        let failures = || reads.shared.stats.failures.load(Ordering::Relaxed);
        store
            .put(
                &Path::from("vector/idx/data/a.lance"),
                Bytes::from_static(b"x").into(),
            )
            .await
            .unwrap_err();
        assert_eq!(failures(), 1);
        store
            .delete(&Path::from("vector/idx/data/b.lance"))
            .await
            .unwrap_err();
        assert_eq!(failures(), 2);
        reads.remove_table("s3://vb/vector/idx").await.unwrap_err();
        assert_eq!(failures(), 3);
    }

    /// A store writing through `fake`, with `chunk_size` bytes per chunk.
    fn writing_store(
        fake: Arc<Fake>,
        keys: Arc<SseS3Keys>,
        chunk_size: usize,
        cipher: bool,
        sse_s3: bool,
    ) -> Arc<dyn ObjectStore> {
        let mut reads = DirectReads::new(fake, "", keys);
        {
            let shared = Arc::get_mut(&mut reads.shared).unwrap();
            shared.chunk_size = chunk_size;
            shared.cipher = cipher;
        }
        reads
            .encrypting(sse_s3)
            .wrap("s3$vb", Arc::new(InMemory::new()))
    }

    fn entry_of(fake: &Fake, path: &str) -> Entry {
        fake.entries.lock().unwrap()[&format!("/buckets/vb/{path}")].clone()
    }

    /// A put creates the entry the gateway would, readable back through the
    /// store; a conditional create of an existing object is a conflict that
    /// leaves no needle behind.
    #[tokio::test]
    async fn writes_create_the_entry_the_gateway_would() {
        let fake = Arc::new(Fake::default());
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let path = Path::from("vector/idx/_versions/2.manifest");
        let body = Bytes::from(vec![9u8; 10_000]);
        let result = store
            .put_opts(&path, body.clone().into(), PutMode::Create.into())
            .await
            .unwrap();
        let md5 = hex::encode(Md5::digest(&body));
        assert_eq!(result.e_tag.as_deref(), Some(format!("\"{md5}\"").as_str()));

        let entry = entry_of(&fake, "vector/idx/_versions/2.manifest");
        let attributes = entry.attributes.as_ref().unwrap();
        assert_eq!(attributes.file_size, 10_000);
        assert_eq!(hex::encode(&attributes.md5), md5);
        assert_eq!(attributes.mime, OCTET_STREAM);
        assert_eq!(entry.extended[ETAG_ATTRIBUTE], md5.as_bytes());
        assert_eq!(entry.chunks.len(), 3, "4096-byte chunks");
        assert_eq!(entry.chunks[2].offset, 8192);
        assert!(
            entry
                .chunks
                .iter()
                .all(|c| c.cipher_key.is_empty() && c.sse_type == 0)
        );

        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, body);
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 10_000);
        assert_eq!(meta.e_tag, result.e_tag);

        let conflict = store
            .put_opts(
                &path,
                Bytes::from_static(b"again").into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(conflict, Error::AlreadyExists { .. }),
            "{conflict}"
        );
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 1);
        assert_eq!(fake.needles.lock().unwrap().len(), 3);

        store
            .put_opts(
                &path,
                Bytes::from_static(b"again").into(),
                PutMode::Overwrite.into(),
            )
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, Bytes::from_static(b"again"));
    }

    /// A multipart upload's parts become chunks in order, its ETag derives
    /// from them as the gateway's does, and an abort leaves no needle behind.
    #[tokio::test]
    async fn multipart_uploads_become_chunks_in_order() {
        let fake = Arc::new(Fake::default());
        let store = writing_store(fake.clone(), no_keys(), 1 << 20, false, false);
        let path = Path::from("vector/idx/data/abcd.lance");
        let mut upload = store.put_multipart(&path).await.unwrap();
        let parts = [vec![1u8; 3000], vec![2u8; 3000], vec![3u8; 1000]];
        for part in &parts {
            upload
                .put_part(Bytes::from(part.clone()).into())
                .await
                .unwrap();
        }
        let result = upload.complete().await.unwrap();
        let entry = entry_of(&fake, "vector/idx/data/abcd.lance");
        assert_eq!(entry.chunks.len(), 3);
        assert_eq!(entry.chunks[1].offset, 3000);
        assert!(entry.attributes.as_ref().unwrap().md5.is_empty());
        assert!(!entry.extended.contains_key(ETAG_ATTRIBUTE));
        let digests: Vec<Vec<u8>> = parts.iter().map(|p| Md5::digest(p).to_vec()).collect();
        let want = format!("\"{}-3\"", hex::encode(Md5::digest(digests.concat())));
        assert_eq!(result.e_tag.as_deref(), Some(want.as_str()));
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got, Bytes::from(parts.concat()));

        let mut aborted = store
            .put_multipart(&Path::from("vector/idx/data/gone.lance"))
            .await
            .unwrap();
        aborted
            .put_part(Bytes::from_static(b"never").into())
            .await
            .unwrap();
        aborted.abort().await.unwrap();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 1);
        assert!(
            fake.entries
                .lock()
                .unwrap()
                .keys()
                .all(|k| !k.ends_with("gone.lance"))
        );
    }

    /// With SSE-S3 the needles hold ciphertext under a per-object key the
    /// entry carries, and with the cluster's volume encryption a per-chunk
    /// key; both read back through the store, and a chunk is never plain.
    #[tokio::test]
    async fn encrypted_writes_read_back() {
        let keys = Arc::new(
            SseS3Keys::load(
                &SseConfig {
                    key: Some("secret".into()),
                    ..Default::default()
                },
                None,
            )
            .unwrap(),
        );
        for (cipher, sse_s3) in [(false, true), (true, false), (true, true)] {
            let fake = Arc::new(Fake::default());
            let store = writing_store(fake.clone(), keys.clone(), 4096, cipher, sse_s3);
            let path = Path::from("vector/idx/_versions/3.manifest");
            let body: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
            store
                .put(&path, Bytes::from(body.clone()).into())
                .await
                .unwrap();
            let entry = entry_of(&fake, "vector/idx/_versions/3.manifest");
            for chunk in &entry.chunks {
                let needle = &fake.needles.lock().unwrap()[&chunk.file_id];
                let plain =
                    &body[chunk.offset as usize..chunk.offset as usize + chunk.size as usize];
                assert_ne!(needle, plain, "cipher {cipher} sse {sse_s3}");
                assert_eq!(!chunk.cipher_key.is_empty(), cipher);
                assert_eq!(chunk.sse_type == SSE_S3_TYPE, sse_s3);
            }
            assert_eq!(
                entry.extended.contains_key("X-Amz-Server-Side-Encryption"),
                sse_s3
            );
            if sse_s3 {
                // No two chunks share a keystream; the wrapped keys always
                // differ, so the IVs are what is compared.
                let ivs: std::collections::HashSet<String> = entry
                    .chunks
                    .iter()
                    .map(|c| {
                        let meta: serde_json::Value =
                            serde_json::from_slice(&c.sse_metadata).unwrap();
                        meta["iv"].as_str().unwrap().to_string()
                    })
                    .collect();
                assert_eq!(ivs.len(), entry.chunks.len());
            }
            let got = store.get(&path).await.unwrap().bytes().await.unwrap();
            assert_eq!(
                got,
                Bytes::from(body.clone()),
                "cipher {cipher} sse {sse_s3}"
            );

            // Parts of a multipart upload start wherever the previous one
            // ended, inside a cipher block included.
            let parts = Path::from("vector/idx/data/parts.lance");
            let mut upload = store.put_multipart(&parts).await.unwrap();
            for part in body.chunks(3000) {
                upload
                    .put_part(Bytes::from(part.to_vec()).into())
                    .await
                    .unwrap();
            }
            upload.complete().await.unwrap();
            let got = store.get(&parts).await.unwrap().bytes().await.unwrap();
            assert_eq!(
                got,
                Bytes::from(body),
                "unaligned parts, cipher {cipher} sse {sse_s3}"
            );
        }
    }

    /// An SSE-S3 bucket cannot be written without the KEK.
    #[tokio::test]
    async fn sse_s3_writes_need_the_kek() {
        let store = writing_store(Arc::new(Fake::default()), no_keys(), 4096, false, true);
        let err = store
            .put(&Path::from("vector/idx/x"), Bytes::from_static(b"x").into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("s3.sse.kek"), "{err}");
    }

    /// Deletes remove the entry and forget the cached object; a table is
    /// removed with everything under it.
    #[tokio::test]
    async fn deletes_remove_entries_and_tables() {
        let fake = Arc::new(Fake::default());
        let mut reads = DirectReads::new(fake.clone(), "", no_keys());
        Arc::get_mut(&mut reads.shared).unwrap().chunk_size = 4096;
        let store = reads.wrap("s3$vb", Arc::new(InMemory::new()));
        for name in [
            "vector/idx/data/a.lance",
            "vector/idx/data/b.lance",
            "vector/idx/_versions/1.manifest",
        ] {
            store
                .put(&Path::from(name), Bytes::from_static(b"bytes").into())
                .await
                .unwrap();
        }
        for name in ["vector/idx/data/a.lance", "vector/idx/data/b.lance"] {
            store.get(&Path::from(name)).await.unwrap();
        }
        store
            .delete(&Path::from("vector/idx/data/a.lance"))
            .await
            .unwrap();
        let err = store
            .get(&Path::from("vector/idx/data/a.lance"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound { .. }), "{err}");
        store
            .delete(&Path::from("vector/idx/data/a.lance"))
            .await
            .unwrap();

        reads.remove_table("s3://vb/vector/idx").await.unwrap();
        assert!(fake.entries.lock().unwrap().is_empty());
        for name in ["vector/idx/_versions/1.manifest", "vector/idx/data/b.lance"] {
            let err = store.get(&Path::from(name)).await.unwrap_err();
            assert!(matches!(err, Error::NotFound { .. }), "{name}: {err}");
        }
    }

    /// A create the filer saved but did not answer keeps its chunks and
    /// counts as written; a put that fails midway leaves no needle behind.
    #[tokio::test]
    async fn uncertain_and_failed_writes_leave_nothing_wrong_behind() {
        let fake = Arc::new(Fake {
            lose_create_replies: true,
            ..Default::default()
        });
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let path = Path::from("vector/idx/_versions/5.manifest");
        store
            .put_opts(
                &path,
                Bytes::from(vec![1u8; 5000]).into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 0);
        assert_eq!(fake.needles.lock().unwrap().len(), 2);
        assert_eq!(
            entry_of(&fake, "vector/idx/_versions/5.manifest")
                .chunks
                .len(),
            2
        );

        // Nor are they discarded when the filer cannot be asked.
        let fake = Arc::new(Fake {
            lose_create_replies: true,
            fail_lookups: true,
            ..Default::default()
        });
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        store
            .put_opts(
                &path,
                Bytes::from(vec![1u8; 5000]).into(),
                PutMode::Create.into(),
            )
            .await
            .unwrap_err();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 0);
        assert_eq!(fake.needles.lock().unwrap().len(), 2);

        let fake = Arc::new(Fake {
            uploads_allowed: Some(1),
            ..Default::default()
        });
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        store
            .put(
                &Path::from("vector/idx/data/half.lance"),
                Bytes::from(vec![2u8; 5000]).into(),
            )
            .await
            .unwrap_err();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 1);
        assert!(fake.needles.lock().unwrap().is_empty());
        assert!(fake.entries.lock().unwrap().is_empty());
    }

    /// A create the filer took and never answered keeps its chunks, since
    /// it may still be applied; the upload can then be neither completed
    /// again nor aborted into discarding them.
    #[tokio::test]
    async fn an_unanswered_create_keeps_its_chunks() {
        let fake = Arc::new(Fake {
            silent_creates: true,
            ..Default::default()
        });
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let path = Path::from("vector/idx/data/silent.lance");
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload
            .put_part(Bytes::from(vec![3u8; 5000]).into())
            .await
            .unwrap();
        upload.complete().await.unwrap_err();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 0);
        assert_eq!(fake.needles.lock().unwrap().len(), 2);
        upload.complete().await.unwrap_err();
        upload.abort().await.unwrap();
        drop(upload);
        tokio::task::yield_now().await;
        assert_eq!(
            fake.discarded.load(Ordering::Relaxed),
            0,
            "kept through abort and drop"
        );
    }

    /// An upload dropped unfinished, as by a cancelled request, discards
    /// what it uploaded.
    #[tokio::test]
    async fn a_dropped_upload_discards_its_chunks() {
        let fake = Arc::new(Fake::default());
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let mut upload = store
            .put_multipart(&Path::from("vector/idx/data/gone.lance"))
            .await
            .unwrap();
        upload
            .put_part(Bytes::from(vec![4u8; 5000]).into())
            .await
            .unwrap();
        drop(upload);
        for _ in 0..50 {
            if fake.discarded.load(Ordering::Relaxed) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 2);
    }

    /// An abort that cannot discard every chunk keeps the ones left for a
    /// later attempt, and the upload cannot be completed any more.
    #[tokio::test]
    async fn an_abort_keeps_what_it_could_not_discard() {
        let fake = Arc::new(Fake {
            failing_discards: AtomicU64::new(1),
            ..Default::default()
        });
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let mut upload = store
            .put_multipart(&Path::from("vector/idx/data/kept.lance"))
            .await
            .unwrap();
        upload
            .put_part(Bytes::from(vec![5u8; 10_000]).into())
            .await
            .unwrap();
        upload.abort().await.unwrap_err();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 2);
        upload.complete().await.unwrap_err();
        upload.abort().await.unwrap();
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 3);
        assert!(fake.needles.lock().unwrap().is_empty());
    }

    /// A chunk write cancelled in flight, as when the request writing it is
    /// dropped, gives its assigned needle back.
    #[tokio::test]
    async fn a_cancelled_chunk_write_gives_its_needle_back() {
        let fake = Arc::new(Fake {
            stall_writes: true,
            ..Default::default()
        });
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let put = tokio::spawn(async move {
            store
                .put(
                    &Path::from("vector/idx/data/cut.lance"),
                    Bytes::from(vec![6u8; 100]).into(),
                )
                .await
        });
        for _ in 0..100 {
            if fake.uploads.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fake.uploads.load(Ordering::Relaxed), 1, "assigned");
        put.abort();
        put.await.unwrap_err();
        for _ in 0..100 {
            if fake.discarded.load(Ordering::Relaxed) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(fake.discarded.load(Ordering::Relaxed), 1);
    }

    /// Chunks name the collection the gateway names: the filer group's for
    /// the bucket, or none, leaving it to the filer's rules for the path.
    #[tokio::test]
    async fn chunks_go_to_the_buckets_collection() {
        for (group, want) in [("", ""), ("eu", "eu_vb")] {
            let fake = Arc::new(Fake::default());
            let mut reads = DirectReads::new(fake.clone(), "", no_keys());
            {
                let shared = Arc::get_mut(&mut reads.shared).unwrap();
                shared.chunk_size = 4096;
                shared.filer_group = group.to_string();
            }
            let store = reads.wrap("s3$vb", Arc::new(InMemory::new()));
            store
                .put(&Path::from("vector/idx/x"), Bytes::from_static(b"x").into())
                .await
                .unwrap();
            let collections = fake.collections.lock().unwrap();
            assert_eq!(
                collections.iter().collect::<Vec<_>>(),
                vec![&want.to_string()]
            );
        }
    }

    /// The content type a put carries becomes the entry's mime, as it does
    /// through the gateway.
    #[tokio::test]
    async fn a_put_keeps_its_content_type() {
        let fake = Arc::new(Fake::default());
        let store = writing_store(fake.clone(), no_keys(), 4096, false, false);
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, "application/json".into());
        let opts = PutOptions {
            attributes,
            ..Default::default()
        };
        store
            .put_opts(
                &Path::from("vector/idx/typed"),
                Bytes::from_static(b"{}").into(),
                opts,
            )
            .await
            .unwrap();
        let typed = entry_of(&fake, "vector/idx/typed");
        assert_eq!(typed.attributes.unwrap().mime, "application/json");
        let read = store.get(&Path::from("vector/idx/typed")).await.unwrap();
        assert_eq!(
            read.attributes
                .get(&Attribute::ContentType)
                .map(|v| v.as_ref()),
            Some("application/json")
        );
    }

    /// A bucket encrypts when its S3 Tables record says LANCE and AES256,
    /// is read afresh each time, and refuses a record that cannot be read.
    #[tokio::test]
    async fn bucket_encryption_follows_the_buckets_record() {
        let fake = Arc::new(Fake::default());
        let reads = DirectReads::new(fake.clone(), "", no_keys());
        let record = |format: &str, sse: &str| {
            format!(
                r#"{{"format":"{format}","encryptionConfiguration":{{"sseAlgorithm":"{sse}"}}}}"#
            )
            .into_bytes()
        };
        fake.add("/buckets/plain", b"", |e| e.is_directory = true);
        fake.add("/buckets/iceberg", b"", |e| {
            e.is_directory = true;
            e.extended
                .insert(TABLE_BUCKET_METADATA.into(), record("ICEBERG", "AES256"));
        });
        fake.add("/buckets/lance", b"", |e| {
            e.is_directory = true;
            e.extended
                .insert(TABLE_BUCKET_METADATA.into(), record("LANCE", "AES256"));
        });
        fake.add("/buckets/bad", b"", |e| {
            e.is_directory = true;
            e.extended
                .insert(TABLE_BUCKET_METADATA.into(), b"{not json".to_vec());
        });
        assert!(!reads.bucket_encrypts("missing").await.unwrap());
        assert!(!reads.bucket_encrypts("plain").await.unwrap());
        assert!(!reads.bucket_encrypts("iceberg").await.unwrap());
        assert!(reads.bucket_encrypts("lance").await.unwrap());
        reads.bucket_encrypts("bad").await.unwrap_err();

        fake.add("/buckets/lance", b"", |e| {
            e.is_directory = true;
            e.extended
                .insert(TABLE_BUCKET_METADATA.into(), record("LANCE", ""));
        });
        assert!(
            !reads.bucket_encrypts("lance").await.unwrap(),
            "recreated bucket"
        );
    }

    /// A table path is removed only when every component is a plain name.
    #[tokio::test]
    async fn removing_a_table_refuses_path_tricks() {
        let reads = DirectReads::new(Arc::new(Fake::default()), "", no_keys());
        for location in [
            "s3://vb/vector/..",
            "s3://vb/./idx",
            "s3://../vector/idx",
            "s3://vb",
        ] {
            reads.remove_table(location).await.expect_err(location);
        }
    }
}
