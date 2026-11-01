//! A bounded in-memory cache of reads from the files Lance never rewrites.
//!
//! Data, index and deletion files are written once under fresh random names,
//! so a cached read cannot go stale, even when an index is dropped and
//! recreated at the same location. Manifests, tags and every other path are
//! read from the store each time, which keeps read-your-writes intact.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    store: Arc<str>,
    path: Path,
    /// `None` for a whole-object read.
    range: Option<Range<u64>>,
}

#[derive(Debug, Clone)]
struct Entry {
    bytes: Bytes,
    /// Set for a whole-object read, which answers with its metadata.
    object: Option<Arc<(ObjectMeta, Attributes)>>,
}

/// Wraps each object store Lance opens; clones share one cache.
#[derive(Clone)]
pub struct ReadCache(moka::sync::Cache<Key, Entry>);

impl ReadCache {
    pub fn new(capacity_bytes: u64) -> Self {
        Self(
            moka::sync::Cache::builder()
                .max_capacity(capacity_bytes)
                .weigher(|key: &Key, entry: &Entry| {
                    u32::try_from(key.path.as_ref().len() + entry.bytes.len()).unwrap_or(u32::MAX)
                })
                .build(),
        )
    }
}

impl fmt::Debug for ReadCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCache")
            .field("entries", &self.0.entry_count())
            .field("bytes", &self.0.weighted_size())
            .finish()
    }
}

impl WrappingObjectStore for ReadCache {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(CachedStore {
            inner: original,
            store: store_prefix.into(),
            cache: self.0.clone(),
        })
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

/// Lance reads files up to its block size whole; larger whole reads stream
/// through uncached.
const MAX_WHOLE_OBJECT: u64 = 4 << 20;

/// A read with no conditions, so its answer depends only on the object.
fn plain(options: &GetOptions) -> bool {
    options.if_match.is_none()
        && options.if_none_match.is_none()
        && options.if_modified_since.is_none()
        && options.if_unmodified_since.is_none()
        && options.range.is_none()
        && options.version.is_none()
        && !options.head
}

fn whole(bytes: Bytes, object: &(ObjectMeta, Attributes)) -> GetResult {
    GetResult {
        range: 0..bytes.len() as u64,
        payload: GetResultPayload::Stream(futures::stream::once(async { Ok(bytes) }).boxed()),
        meta: object.0.clone(),
        attributes: object.1.clone(),
    }
}

#[derive(Debug)]
struct CachedStore {
    inner: Arc<dyn ObjectStore>,
    store: Arc<str>,
    cache: moka::sync::Cache<Key, Entry>,
}

impl CachedStore {
    fn key(&self, location: &Path, range: Option<Range<u64>>) -> Key {
        Key {
            store: self.store.clone(),
            path: location.clone(),
            range,
        }
    }
}

impl fmt::Display for CachedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReadCache({})", self.inner)
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for CachedStore {
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
        if !immutable(location) || !plain(&options) {
            return self.inner.get_opts(location, options).await;
        }
        let key = self.key(location, None);
        if let Some(Entry {
            bytes,
            object: Some(object),
        }) = self.cache.get(&key)
        {
            return Ok(whole(bytes, &object));
        }
        let result = self.inner.get_opts(location, options).await?;
        if result.meta.size > MAX_WHOLE_OBJECT {
            return Ok(result);
        }
        let object = Arc::new((result.meta.clone(), result.attributes.clone()));
        let bytes = result.bytes().await?;
        self.cache.insert(
            key,
            Entry {
                bytes: bytes.clone(),
                object: Some(object.clone()),
            },
        );
        Ok(whole(bytes, &object))
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if !immutable(location) {
            return self.inner.get_ranges(location, ranges).await;
        }
        let mut out: Vec<Option<Bytes>> = ranges
            .iter()
            .map(|range| {
                self.cache
                    .get(&self.key(location, Some(range.clone())))
                    .map(|entry| entry.bytes)
            })
            .collect();
        let missing: Vec<Range<u64>> = ranges
            .iter()
            .zip(&out)
            .filter(|(_, hit)| hit.is_none())
            .map(|(range, _)| range.clone())
            .collect();
        if !missing.is_empty() {
            let mut fetched = self.inner.get_ranges(location, &missing).await?.into_iter();
            for (range, slot) in ranges.iter().zip(out.iter_mut()) {
                if slot.is_some() {
                    continue;
                }
                let Some(bytes) = fetched.next() else {
                    break;
                };
                self.cache.insert(
                    self.key(location, Some(range.clone())),
                    Entry {
                        bytes: bytes.clone(),
                        object: None,
                    },
                );
                *slot = Some(bytes);
            }
        }
        out.into_iter()
            .map(|bytes| {
                bytes.ok_or_else(|| object_store::Error::Generic {
                    store: "ReadCache",
                    source: "get_ranges returned fewer ranges than requested".into(),
                })
            })
            .collect()
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::vector as pb;
    use crate::pb::vector::vector_service_server::VectorService;
    use crate::service::VectorServer;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use tonic::Request;
    use weed_lance_worker::catalog::NamespaceClient;
    use weed_lance_worker::dataset::FallbackOptions;

    fn cached(inner: Arc<InMemory>) -> Arc<dyn ObjectStore> {
        ReadCache::new(1 << 20).wrap("memory", inner)
    }

    #[test]
    fn only_write_once_files_are_immutable() {
        for path in [
            "vector/x/data/0101abcd.lance",
            "vector/data/data/0101abcd.lance",
            "vector/x/_deletions/3-7-12345.arrow",
            "vector/x/_indices/1b6f/index.idx",
        ] {
            assert!(immutable(&Path::from(path)), "{path}");
        }
        for path in [
            "vector/data/_versions/1.manifest",
            "vector/data/_latest.manifest",
            "vector/x/_versions/18446744073709551614.manifest",
            "vector/x/_refs/tags/weed-vector-query-1.json",
            "vector/x/_transactions/0-abc.txn",
            "vector/x/data/notes.txt",
            "vector/_indices/_versions/1.manifest",
            "data/x.lance/_versions/1.manifest",
        ] {
            assert!(!immutable(&Path::from(path)), "{path}");
        }
    }

    async fn read(store: &Arc<dyn ObjectStore>, path: &Path, range: Range<u64>) -> Bytes {
        let mut got = store
            .get_ranges(path, std::slice::from_ref(&range))
            .await
            .unwrap();
        got.remove(0)
    }

    #[tokio::test]
    async fn write_once_reads_come_from_the_cache() {
        let inner = Arc::new(InMemory::new());
        let store = cached(inner.clone());
        let data = Path::from("vector/x/data/a.lance");
        let manifest = Path::from("vector/x/_versions/1.manifest");
        for path in [&data, &manifest] {
            inner
                .put(path, Bytes::from_static(b"first").into())
                .await
                .unwrap();
            assert_eq!(read(&store, path, 0..3).await, &b"fir"[..]);
            assert_eq!(
                store.get(path).await.unwrap().bytes().await.unwrap(),
                &b"first"[..]
            );
            inner
                .put(path, Bytes::from_static(b"second").into())
                .await
                .unwrap();
        }
        // The overwrite stands in for a store answering differently: the data
        // file keeps its cached bytes, the manifest is read again.
        assert_eq!(
            store.get_ranges(&data, &[0..3, 1..4]).await.unwrap(),
            [&b"fir"[..], &b"eco"[..]]
        );
        let got = store.get(&data).await.unwrap();
        assert_eq!(got.meta.size, 5);
        assert_eq!(got.bytes().await.unwrap(), &b"first"[..]);
        assert_eq!(read(&store, &manifest, 0..3).await, &b"sec"[..]);
        assert_eq!(
            store.get(&manifest).await.unwrap().bytes().await.unwrap(),
            &b"second"[..]
        );
        // A conditional or ranged get is the store's to answer.
        let ranged = GetOptions {
            range: Some((0..2).into()),
            ..Default::default()
        };
        let got = store.get_opts(&data, ranged).await.unwrap();
        assert_eq!(got.bytes().await.unwrap(), &b"se"[..]);
    }

    #[tokio::test]
    async fn the_cache_stays_within_its_capacity() {
        let cache = ReadCache::new(64 << 10);
        let inner = Arc::new(InMemory::new());
        let store = cache.wrap("memory", inner.clone());
        for i in 0..64 {
            let path = Path::from(format!("vector/x/data/{i}.lance"));
            inner
                .put(&path, Bytes::from(vec![0u8; 8 << 10]).into())
                .await
                .unwrap();
            read(&store, &path, 0..(8 << 10)).await;
        }
        cache.0.run_pending_tasks();
        assert!(cache.0.weighted_size() <= 64 << 10, "{cache:?}");
    }

    /// A catalog that answers every request with `location`.
    async fn catalog_at(location: String) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let body = serde_json::json!({ "location": location }).to_string();
        tokio::spawn(async move {
            loop {
                let (mut conn, _) = listener.accept().await.unwrap();
                let body = body.clone();
                tokio::spawn(async move {
                    let mut request = [0u8; 8192];
                    let _ = conn.read(&mut request).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = conn.write_all(response.as_bytes()).await;
                    let _ = conn.shutdown().await;
                });
            }
        });
        url
    }

    fn vector(key: &str, data: Vec<f32>) -> pb::Vector {
        pb::Vector {
            key: key.to_string(),
            data,
            metadata: None,
        }
    }

    async fn get(server: &VectorServer, table: &pb::Table, keys: &[&str]) -> Vec<pb::Vector> {
        let mut vectors = server
            .get_vectors(Request::new(pb::GetVectorsRequest {
                table: Some(table.clone()),
                keys: keys.iter().map(|k| k.to_string()).collect(),
                return_data: true,
                return_metadata: false,
            }))
            .await
            .unwrap()
            .into_inner()
            .vectors;
        vectors.sort_by(|a, b| a.key.cmp(&b.key));
        vectors
    }

    async fn query(server: &VectorServer, table: &pb::Table, v: Vec<f32>) -> Vec<String> {
        server
            .query_vectors(Request::new(pb::QueryVectorsRequest {
                table: Some(table.clone()),
                query_vector: v,
                top_k: 10,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .vectors
            .into_iter()
            .map(|r| r.key)
            .collect()
    }

    async fn create(server: &VectorServer, table: &pb::Table, dimension: u32) {
        server
            .create_index(Request::new(pb::CreateIndexRequest {
                table: Some(table.clone()),
                dimension,
                ..Default::default()
            }))
            .await
            .unwrap();
    }

    async fn put(server: &VectorServer, table: &pb::Table, vectors: Vec<pb::Vector>) {
        server
            .put_vectors(Request::new(pb::PutVectorsRequest {
                table: Some(table.clone()),
                vectors,
            }))
            .await
            .unwrap();
    }

    /// Reads through the cache see every committed write, and an index
    /// dropped and recreated at the same location answers with its new rows.
    #[tokio::test]
    async fn cached_reads_follow_writes_and_a_recreated_index() {
        let location = format!("shared-memory://cache-{}/vector/x", uuid_like(),);
        let server = VectorServer::new(
            NamespaceClient::new(catalog_at(location.clone()).await),
            FallbackOptions::new(),
        )
        .with_read_cache(ReadCache::new(16 << 20));
        let table = pb::Table {
            id: vec!["bkt".into(), "vector".into(), "x".into()],
            version: 0,
        };

        create(&server, &table, 4).await;
        put(
            &server,
            &table,
            vec![
                vector("a", vec![1.0, 0.0, 0.0, 0.0]),
                vector("b", vec![0.0, 1.0, 0.0, 0.0]),
            ],
        )
        .await;
        assert_eq!(
            query(&server, &table, vec![1.0, 0.0, 0.0, 0.0]).await,
            ["a", "b"]
        );
        assert_eq!(
            get(&server, &table, &["a"]).await[0].data,
            [1.0, 0.0, 0.0, 0.0]
        );

        put(
            &server,
            &table,
            vec![
                vector("a", vec![0.0, 0.0, 2.0, 0.0]),
                vector("c", vec![1.0, 0.1, 0.0, 0.0]),
            ],
        )
        .await;
        assert_eq!(
            query(&server, &table, vec![1.0, 0.0, 0.0, 0.0]).await,
            ["c", "b", "a"]
        );
        assert_eq!(
            get(&server, &table, &["a"]).await[0].data,
            [0.0, 0.0, 2.0, 0.0]
        );

        let (store, path) = lance::io::ObjectStore::from_uri(&location).await.unwrap();
        store.remove_dir_all(path).await.unwrap();
        create(&server, &table, 2).await;
        put(&server, &table, vec![vector("z", vec![0.5, 0.5])]).await;
        let vectors = get(&server, &table, &["a", "b", "c", "z"]).await;
        assert_eq!(vectors.len(), 1);
        assert_eq!(
            (vectors[0].key.as_str(), vectors[0].data.as_slice()),
            ("z", &[0.5, 0.5][..])
        );
        assert_eq!(query(&server, &table, vec![1.0, 0.0]).await, ["z"]);
    }

    fn uuid_like() -> String {
        format!(
            "{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
