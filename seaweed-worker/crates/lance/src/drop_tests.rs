//! A table dropped while a worker job runs on it: the job must fail without
//! writing into the dropped path, and a table created again there starts
//! empty.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
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
