//! A table dropped while a worker job runs on it: the job must fail without
//! writing into the dropped path, and a table created again there starts
//! empty.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};

use crate::metadata_index::tests::lifecycle::{table, upsert};
use crate::metadata_index::{self, State};

/// Removes the table directory just before the first write through it, the
/// way DeleteIndex purging the table between a job's open and its writes
/// would.
#[derive(Debug)]
struct DropOnFirstWrite {
    dir: PathBuf,
    fired: Arc<AtomicBool>,
}

impl WrappingObjectStore for DropOnFirstWrite {
    fn wrap(&self, _prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Dropping {
            inner: original,
            dir: self.dir.clone(),
            fired: self.fired.clone(),
        })
    }
}

#[derive(Debug)]
struct Dropping {
    inner: Arc<dyn ObjectStore>,
    dir: PathBuf,
    fired: Arc<AtomicBool>,
}

impl Dropping {
    fn drop_once(&self) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
}

impl fmt::Display for Dropping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dropping({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Dropping {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.drop_once();
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.drop_once();
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
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

/// Opens the table the way a job does; `during` drops it at the job's first
/// write, otherwise it is dropped right after the open.
async fn open_then_drop(dir: &tempfile::TempDir, during: bool) -> Dataset {
    let description = crate::catalog::TableDescription {
        location: dir.path().to_str().unwrap().to_string(),
        storage_options: Default::default(),
        managed_versioning: false,
    };
    let dropping: Option<Arc<dyn WrappingObjectStore>> = during.then(|| {
        Arc::new(DropOnFirstWrite {
            dir: dir.path().to_path_buf(),
            fired: Arc::new(AtomicBool::new(false)),
        }) as Arc<dyn WrappingObjectStore>
    });
    let dataset = crate::dataset::open_guarded(&description, dropping)
        .await
        .unwrap();
    if !during {
        std::fs::remove_dir_all(dir.path()).unwrap();
    }
    dataset
}

fn files(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(files(&path));
            } else {
                out.push(path.display().to_string());
            }
        }
    }
    out
}

/// Runs `job` on a table dropped right after the job opens it, or at the
/// job's first write. The job must fail, leave no manifest (nor, dropped
/// before it wrote, any file) in the dropped path, and a table created
/// there again must start empty.
async fn dropped_during<F, Fut>(
    label: &str,
    during: bool,
    prepare: impl AsyncFnOnce(&mut Dataset),
    job: F,
) where
    F: FnOnce(Dataset) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let dir = tempfile::TempDir::new().unwrap();
    let mut dataset = table(&dir, 4).await;
    prepare(&mut dataset).await;
    let result = job(open_then_drop(&dir, during).await).await;
    let left = files(dir.path());
    let error = result.expect_err(&format!(
        "{label}: a job on a dropped table succeeded, leaving {left:?}"
    ));
    eprintln!(
        "{label} (dropped {}): {error:#}",
        if during {
            "at its first write"
        } else {
            "after the open"
        }
    );
    assert!(
        !left.iter().any(|f| f.ends_with(".manifest")),
        "{label}: a manifest was written into the dropped table: {left:?}"
    );
    if !during {
        assert!(
            left.is_empty(),
            "{label}: files were written into the dropped table: {left:?}"
        );
    }
    let recreated = Dataset::write(
        arrow_array::RecordBatchIterator::new(
            Vec::<std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>::new(),
            Arc::new(crate::metadata_index::tests::lifecycle::base_schema()),
        ),
        dir.path().to_str().unwrap(),
        None,
    )
    .await
    .unwrap_or_else(|e| panic!("{label}: the table could not be created again: {e}"));
    assert_eq!(recreated.version().version, 1, "{label}");
    assert_eq!(recreated.count_rows(None).await.unwrap(), 0, "{label}");
}

/// Every job, with the table dropped after its open and at its first write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_on_a_dropped_table_fails_and_leaves_it_dropped() {
    for during in [false, true] {
        dropped_during(
            "vector index build",
            during,
            async |_| {},
            async |mut d| {
                crate::vector_index::build(
                    &mut d,
                    "vector",
                    crate::vector_index::INDEX_NAME,
                    4,
                    false,
                )
                .await
            },
        )
        .await;
        dropped_during(
            "metadata key add",
            during,
            async |d| metadata_index::request(d, "author").await.unwrap(),
            async |mut d| {
                let key = metadata_index::add_column(&mut d, "author").await?;
                metadata_index::finish_after(&mut d, &key, 1_000, std::time::Duration::ZERO)
                    .await
                    .map(|_| ())
            },
        )
        .await;
        dropped_during(
            "metadata key maintenance",
            during,
            async |d| {
                let key = metadata_index::add_column(d, "author").await.unwrap();
                metadata_index::finish_after(d, &key, 1_000, std::time::Duration::ZERO)
                    .await
                    .unwrap();
                *d = upsert(d.clone(), 4_000..4_500).await;
            },
            async |mut d| {
                let key = metadata_index::keys_of(&d).remove(0);
                metadata_index::maintain(&mut d, &key, 1_000, 0)
                    .await
                    .map(|_| ())
            },
        )
        .await;
        dropped_during(
            "metadata key remove",
            during,
            async |d| {
                let key = metadata_index::add_column(d, "author").await.unwrap();
                metadata_index::finish_after(d, &key, 1_000, std::time::Duration::ZERO)
                    .await
                    .unwrap();
                metadata_index::set_state(d, "_mk_0", State::Off)
                    .await
                    .unwrap();
            },
            async |mut d| metadata_index::remove_column(&mut d, "_mk_0").await,
        )
        .await;
    }
}

/// A job that removes old versions itself, as a purge does, removes the
/// manifest it opened: that is no drop, and its later writes go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purge_through_the_guard_completes() {
    let dir = tempfile::TempDir::new().unwrap();
    let dataset = table(&dir, 4).await;
    let mut dataset = upsert(dataset, 0..100).await;
    dataset.delete("key = 'k1' OR key = 'k2001'").await.unwrap();
    let description = crate::catalog::TableDescription {
        location: dir.path().to_str().unwrap().to_string(),
        storage_options: Default::default(),
        managed_versioning: false,
    };
    let mut guarded = crate::dataset::open_guarded(&description, None)
        .await
        .unwrap();
    // The job commits, then removes every older version, the one it opened
    // with them.
    guarded
        .update_config([("seaweedfs.test", "1")])
        .await
        .unwrap();
    let removed = lance::dataset::cleanup::cleanup_old_versions(
        &guarded,
        lance::dataset::cleanup::CleanupPolicy {
            before_timestamp: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            before_version: Some(guarded.version().version),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(removed.old_versions > 0);
    guarded
        .update_config([("seaweedfs.test", "2")])
        .await
        .unwrap();
    crate::jobs::purge::purge(&mut guarded, "t1").await.unwrap();
    assert_eq!(guarded.count_rows(None).await.unwrap(), 3_998);
}

/// The table is dropped and created again up to the version the job opened,
/// so the manifest path the job watches exists again, as another object:
/// the job must still fail and leave the new table as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_on_a_table_created_again_fails() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut dataset = table(&dir, 4).await;
    let key = metadata_index::add_column(&mut dataset, "author")
        .await
        .unwrap();
    metadata_index::finish_after(&mut dataset, &key, 1_000, std::time::Duration::ZERO)
        .await
        .unwrap();
    metadata_index::set_state(&mut dataset, "_mk_0", State::Off)
        .await
        .unwrap();
    let description = crate::catalog::TableDescription {
        location: dir.path().to_str().unwrap().to_string(),
        storage_options: Default::default(),
        managed_versioning: false,
    };
    let mut job = crate::dataset::open_guarded(&description, None)
        .await
        .unwrap();
    let opened = job.version().version;

    std::fs::remove_dir_all(dir.path()).unwrap();
    let mut recreated = table(&dir, 1).await;
    while recreated.version().version < opened {
        let version = recreated.version().version.to_string();
        recreated
            .update_config([("seaweedfs.test", version.as_str())])
            .await
            .unwrap();
    }
    assert_eq!(recreated.version().version, opened);

    let error = metadata_index::remove_column(&mut job, "_mk_0")
        .await
        .expect_err("a job on a table created again succeeded");
    assert!(format!("{error:#}").contains("DropGuard"), "{error:#}");
    recreated.checkout_latest().await.unwrap();
    assert_eq!(recreated.version().version, opened);
    assert_eq!(recreated.count_rows(None).await.unwrap(), 1_000);
}

fn guarded_description(dir: &tempfile::TempDir) -> crate::catalog::TableDescription {
    crate::catalog::TableDescription {
        location: dir.path().to_str().unwrap().to_string(),
        storage_options: Default::default(),
        managed_versioning: false,
    }
}

/// The table's directory in the store's own paths.
fn table_root(dataset: &Dataset) -> Path {
    let manifest = &dataset.manifest_location().path;
    manifest
        .parts()
        .take(manifest.parts().count() - 2)
        .collect()
}

/// A multipart upload started before the drop does not complete after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multipart_upload_does_not_complete_into_a_dropped_table() {
    let dir = tempfile::TempDir::new().unwrap();
    table(&dir, 1).await;
    let s3_like: Arc<dyn WrappingObjectStore> = Arc::new(S3Like::default());
    let job = crate::dataset::open_guarded(&guarded_description(&dir), Some(s3_like))
        .await
        .unwrap();
    let store = job.object_store(None).await.unwrap().inner.clone();
    let path = table_root(&job).child("data").child("late.lance");
    let mut upload = store.put_multipart(&path).await.unwrap();
    upload
        .put_part(PutPayload::from(vec![0u8; 16]))
        .await
        .unwrap();
    std::fs::remove_dir_all(dir.path()).unwrap();
    let completed = upload.complete().await;
    assert!(
        completed.is_err(),
        "an upload completed into a dropped table"
    );
    assert!(files(dir.path()).is_empty(), "{:?}", files(dir.path()));
}

/// An upload whose last probe fails is aborted, not left with its parts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_probe_aborts_an_upload() {
    let dir = tempfile::TempDir::new().unwrap();
    table(&dir, 1).await;
    let s3_like = S3Like::default();
    let (armed, aborted) = (s3_like.armed.clone(), s3_like.aborted.clone());
    let s3_like: Arc<dyn WrappingObjectStore> = Arc::new(s3_like);
    let job = crate::dataset::open_guarded(&guarded_description(&dir), Some(s3_like))
        .await
        .unwrap();
    let store = job.object_store(None).await.unwrap().inner.clone();
    let path = table_root(&job).child("data").child("late.lance");
    let mut upload = store.put_multipart(&path).await.unwrap();
    upload
        .put_part(PutPayload::from(vec![0u8; 16]))
        .await
        .unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(upload.complete().await.is_err());
    assert!(aborted.load(Ordering::SeqCst), "the upload was left open");
}

/// A table created again, through the version a job just wrote, between
/// that write and its check keeps its own manifest at that path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rollback_keeps_the_manifest_of_a_table_created_again() {
    let dir = tempfile::TempDir::new().unwrap();
    table(&dir, 1).await;
    let s3_like: Arc<dyn WrappingObjectStore> = Arc::new(S3Like {
        recreate: Some((dir.path().to_path_buf(), Arc::new(AtomicBool::new(false)))),
        ..Default::default()
    });
    let mut job = crate::dataset::open_guarded(&guarded_description(&dir), Some(s3_like))
        .await
        .unwrap();
    assert_eq!(job.version().version, 1);
    assert!(
        job.update_config([("seaweedfs.test", "job")])
            .await
            .is_err()
    );
    let again = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(again.version().version, 2);
    assert_eq!(
        again.config().get("seaweedfs.test").map(String::as_str),
        Some("again")
    );
}

/// A table created again whose manifest at the job's path cannot be told
/// from the job's, because the HEAD fails, keeps it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_rollback_probe_keeps_the_manifest_of_a_table_created_again() {
    let dir = tempfile::TempDir::new().unwrap();
    table(&dir, 1).await;
    let s3_like: Arc<dyn WrappingObjectStore> = Arc::new(S3Like {
        recreate: Some((dir.path().to_path_buf(), Arc::new(AtomicBool::new(false)))),
        fail_head_after_manifest: true,
        heads_before_failing: 1,
        ..Default::default()
    });
    let mut job = crate::dataset::open_guarded(&guarded_description(&dir), Some(s3_like))
        .await
        .unwrap();
    assert!(
        job.update_config([("seaweedfs.test", "job")])
            .await
            .is_err()
    );
    let again = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(again.version().version, 2);
    assert_eq!(
        again.config().get("seaweedfs.test").map(String::as_str),
        Some("again")
    );
}

/// A cleanup that picked its paths before the drop does not delete the files
/// of the table created again there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cleanup_does_not_delete_a_table_created_again() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut dataset = table(&dir, 1).await;
    dataset
        .update_config([("seaweedfs.test", "1")])
        .await
        .unwrap();
    let job = crate::dataset::open_guarded(&guarded_description(&dir), None)
        .await
        .unwrap();
    let store = job.object_store(None).await.unwrap().inner.clone();
    // The old version-1 manifest, as a cleanup would have picked it.
    let mut first = dataset.checkout_version(1).await.unwrap();
    let old = first.manifest_location().path.clone();
    std::fs::remove_dir_all(dir.path()).unwrap();
    let recreated = table(&dir, 1).await;
    assert_eq!(recreated.manifest_location().path, old);
    let deleted: Vec<_> = store
        .delete_stream(futures::stream::iter(vec![Ok(old.clone())]).boxed())
        .collect()
        .await;
    assert!(deleted.iter().any(|r| r.is_err()), "{deleted:?}");
    first = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(first.version().version, 1);
}

/// A probe that fails after a manifest was written says nothing about the
/// table: the manifest stays and the commit succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_probe_does_not_take_back_a_commit() {
    let dir = tempfile::TempDir::new().unwrap();
    table(&dir, 1).await;
    let flaky: Arc<dyn WrappingObjectStore> = Arc::new(S3Like {
        fail_head_after_manifest: true,
        ..Default::default()
    });
    let mut job = crate::dataset::open_guarded(&guarded_description(&dir), Some(flaky))
        .await
        .unwrap();
    let before = job.version().version;
    job.update_config([("seaweedfs.test", "1")]).await.unwrap();
    let reopened = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
    assert_eq!(reopened.version().version, before + 1);
}

/// Behaves as an object store, not a file system: a multipart upload holds
/// its parts until it completes, and records an abort. Optionally fails one
/// HEAD after a manifest is written, as a timeout would, or creates the table
/// again, through two versions, right after the first manifest is written.
#[derive(Debug, Default)]
struct S3Like {
    fail_head_after_manifest: bool,
    /// HEADs let through after a manifest is written before one fails.
    heads_before_failing: usize,
    /// The next HEAD fails.
    armed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
    recreate: Option<(PathBuf, Arc<AtomicBool>)>,
}

impl WrappingObjectStore for S3Like {
    fn wrap(&self, _prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(S3LikeStore {
            inner: original,
            fail_head_after_manifest: self.fail_head_after_manifest,
            heads_before_failing: self.heads_before_failing,
            heads_left: Arc::new(AtomicUsize::new(0)),
            armed: self.armed.clone(),
            aborted: self.aborted.clone(),
            recreate: self.recreate.clone(),
        })
    }
}

#[derive(Debug)]
struct S3LikeStore {
    inner: Arc<dyn ObjectStore>,
    fail_head_after_manifest: bool,
    heads_before_failing: usize,
    heads_left: Arc<AtomicUsize>,
    armed: Arc<AtomicBool>,
    aborted: Arc<AtomicBool>,
    recreate: Option<(PathBuf, Arc<AtomicBool>)>,
}

impl fmt::Display for S3LikeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "S3Like({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3LikeStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let put = self.inner.put_opts(location, payload, opts).await?;
        if location.extension() == Some("manifest") {
            if self.fail_head_after_manifest {
                self.heads_left
                    .store(self.heads_before_failing, Ordering::SeqCst);
                self.armed.store(true, Ordering::SeqCst);
            }
            if let Some((dir, fired)) = &self.recreate
                && !fired.swap(true, Ordering::SeqCst)
            {
                std::fs::remove_dir_all(dir).unwrap();
                let mut again = create_empty(dir).await;
                again
                    .update_config([("seaweedfs.test", "again")])
                    .await
                    .unwrap();
            }
        }
        Ok(put)
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(HeldParts {
            inner: self.inner.clone(),
            location: location.clone(),
            parts: Vec::new(),
            aborted: self.aborted.clone(),
        }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.head
            && self.armed.load(Ordering::SeqCst)
            && self
                .heads_left
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_err()
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "S3Like",
                source: "timed out".into(),
            });
        }
        self.inner.get_opts(location, options).await
    }
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
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

/// Parts held outside the table until the upload completes, as S3 does.
#[derive(Debug)]
struct HeldParts {
    inner: Arc<dyn ObjectStore>,
    location: Path,
    parts: Vec<Bytes>,
    aborted: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl MultipartUpload for HeldParts {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.parts.extend(data);
        Box::pin(async { Ok(()) })
    }
    async fn complete(&mut self) -> Result<PutResult> {
        let payload: PutPayload = self.parts.drain(..).collect();
        self.inner.put(&self.location, payload).await
    }
    async fn abort(&mut self) -> Result<()> {
        self.parts.clear();
        self.aborted.store(true, Ordering::SeqCst);
        Ok(())
    }
}

/// An empty table at `dir`.
async fn create_empty(dir: &std::path::Path) -> Dataset {
    Dataset::write(
        arrow_array::RecordBatchIterator::new(
            Vec::<std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>::new(),
            Arc::new(crate::metadata_index::tests::lifecycle::base_schema()),
        ),
        dir.to_str().unwrap(),
        None,
    )
    .await
    .unwrap()
}
