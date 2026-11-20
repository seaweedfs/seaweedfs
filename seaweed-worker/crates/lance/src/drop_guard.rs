//! Keeps a job from writing into a table dropped under it.
//!
//! DeleteIndex purges a table's directory, and an index created again under
//! the same name reuses it. A job that opened the table before the purge
//! would otherwise go on writing there: its data files would land in the
//! new table's directory, and its commit would put a manifest of the old
//! table there, so the new table could not be created, or would read it.

use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::io::WrappingObjectStore;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    UploadPart,
};

/// Fails every write once the manifest it watches is gone or is another
/// object (a table created again reuses version paths): the one of the
/// version the job opened, then the newest the job committed. Armed after the
/// open; until then it lets everything through. When the job itself deletes
/// the watched manifest, removing old versions as a purge does, it watches
/// the newest manifest left instead.
#[derive(Debug, Default)]
pub struct DropGuard {
    watched: Arc<Mutex<Watched>>,
}

#[derive(Debug, Default, Clone)]
struct Watched {
    manifest: Option<Path>,
    /// The manifest's e-tag when known, telling it from a replacement.
    e_tag: Option<String>,
    /// The job deleted the watched manifest itself.
    deleted: bool,
}

impl DropGuard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Watch `manifest`, the opened version's, with its e-tag when known.
    pub fn arm(&self, manifest: Path, e_tag: Option<String>) {
        let mut watched = self.watched.lock().unwrap();
        watched.manifest = Some(manifest);
        watched.e_tag = e_tag;
    }
}

impl WrappingObjectStore for DropGuard {
    fn wrap(&self, _prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Guarded {
            inner: original,
            watched: self.watched.clone(),
        })
    }
}

#[derive(Debug, Clone)]
struct Guarded {
    inner: Arc<dyn ObjectStore>,
    watched: Arc<Mutex<Watched>>,
}

/// What a check found: the table is there, or it was dropped, or created
/// again, with the error to fail the write with.
enum Found {
    Table,
    Dropped(object_store::Error),
    Replaced(object_store::Error),
}

impl Guarded {
    /// Whether the watched manifest is still the table's. An error is a
    /// failed probe, which says nothing about the table.
    async fn find(&self, location: &Path) -> Result<Found> {
        let watched = self.watched.lock().unwrap().clone();
        let Some(mut manifest) = watched.manifest else {
            return Ok(Found::Table);
        };
        let mut e_tag = watched.e_tag;
        if watched.deleted {
            if let Some((newest, tag)) = self.newest_manifest(&manifest).await? {
                manifest = newest;
                e_tag = tag;
            }
            let mut watched = self.watched.lock().unwrap();
            watched.manifest = Some(manifest.clone());
            watched.e_tag = e_tag.clone();
            watched.deleted = false;
        }
        let error = |why: &str| {
            object_store::Error::Generic {
                store: "DropGuard",
                source: format!(
                    "the table was dropped while this job wrote it: {manifest} {why}, so {location} is not written"
                )
                .into(),
            }
        };
        match self.inner.head(&manifest).await {
            Ok(meta) => match (&e_tag, &meta.e_tag) {
                (Some(want), Some(have)) if want != have => {
                    Ok(Found::Replaced(error("is another table's")))
                }
                _ => Ok(Found::Table),
            },
            // Gone with manifests beside it: a table created again, at an
            // earlier version than the one watched.
            Err(object_store::Error::NotFound { .. }) => {
                match self.newest_manifest(&manifest).await? {
                    Some(_) => Ok(Found::Replaced(error("is gone and another table is there"))),
                    None => Ok(Found::Dropped(error("is gone"))),
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn check(&self, location: &Path) -> Result<()> {
        match self.find(location).await? {
            Found::Table => Ok(()),
            Found::Dropped(error) | Found::Replaced(error) => Err(error),
        }
    }
}

fn is_manifest(location: &Path) -> bool {
    location.extension() == Some("manifest")
}

impl Guarded {
    /// The most recently written manifest beside `manifest`, with its e-tag.
    async fn newest_manifest(&self, manifest: &Path) -> Result<Option<(Path, Option<String>)>> {
        let dir: Path = manifest
            .parts()
            .take(manifest.parts().count() - 1)
            .collect();
        let listed = self.inner.list_with_delimiter(Some(&dir)).await?;
        Ok(listed
            .objects
            .into_iter()
            .filter(|o| is_manifest(&o.location))
            .max_by_key(|o| o.last_modified)
            .map(|o| (o.location, o.e_tag)))
    }

    fn committed(&self, location: &Path, e_tag: Option<String>) {
        if is_manifest(location) {
            let mut watched = self.watched.lock().unwrap();
            if watched.manifest.is_some() {
                watched.manifest = Some(location.clone());
                watched.e_tag = e_tag;
                watched.deleted = false;
            }
        }
    }
}

/// A multipart upload that checks the table again before it completes.
#[derive(Debug)]
struct GuardedUpload {
    inner: Box<dyn MultipartUpload>,
    guard: Guarded,
    location: Path,
}

#[async_trait::async_trait]
impl MultipartUpload for GuardedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // A failed probe aborts too: the parts would outlive the upload.
        let error = match self.guard.find(&self.location).await {
            Ok(Found::Table) => return self.inner.complete().await,
            Ok(Found::Dropped(error) | Found::Replaced(error)) | Err(error) => error,
        };
        let _ = self.inner.abort().await;
        Err(error)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl fmt::Display for Guarded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DropGuard({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Guarded {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.check(location).await?;
        let put = self.inner.put_opts(location, payload, opts).await?;
        // A drop can land between the check and the write. A manifest is
        // what makes the table readable again, so look once more and take
        // it back if the table is gone; a failed probe leaves it. A table
        // created again since may have written its own manifest there:
        // only a manifest known to be this write's is taken back.
        if is_manifest(location) {
            let (error, replaced) = match self.find(location).await {
                Ok(Found::Dropped(error)) => (error, false),
                Ok(Found::Replaced(error)) => (error, true),
                _ => {
                    self.committed(location, put.e_tag.clone());
                    return Ok(put);
                }
            };
            let ours = match (&put.e_tag, self.inner.head(location).await) {
                (Some(wrote), Ok(meta)) => meta.e_tag.as_ref() == Some(wrote),
                (_, Err(object_store::Error::NotFound { .. })) => false,
                _ => !replaced,
            };
            if ours {
                let _ = self.inner.delete(location).await;
            }
            return Err(error);
        }
        self.committed(location, put.e_tag.clone());
        Ok(put)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check(location).await?;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(GuardedUpload {
            inner,
            guard: self.clone(),
            location: location.clone(),
        }))
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
        // A cleanup picks its paths before it deletes them; a table created
        // again meanwhile reuses those paths, so look once before deleting.
        // In a dropped table there is nothing left to spare, and the job's
        // own failed commit is cleaned up.
        let watched = self.watched.clone();
        let locations = locations.inspect(move |location| {
            if let Ok(location) = location {
                let mut watched = watched.lock().unwrap();
                if watched.manifest.as_ref() == Some(location) {
                    watched.deleted = true;
                }
            }
        });
        let guard = self.clone();
        let pending = Arc::new(Mutex::new(Some(locations.boxed())));
        let checked =
            futures::stream::once(async move { guard.find(&Path::from("cleanup")).await })
                .flat_map(move |found| {
                    let locations = pending.lock().unwrap().take();
                    match (found, locations) {
                        (Ok(Found::Table | Found::Dropped(_)), Some(locations)) => locations,
                        (Ok(Found::Replaced(error)), _) | (Err(error), _) => {
                            futures::stream::once(async move { Err(error) }).boxed()
                        }
                        (Ok(_), None) => futures::stream::empty().boxed(),
                    }
                })
                .boxed();
        self.inner.delete_stream(checked)
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
        self.check(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.check(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}
