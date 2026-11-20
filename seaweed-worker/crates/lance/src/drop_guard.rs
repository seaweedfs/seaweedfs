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
};

/// Fails every write once the manifest it watches is gone: the one of the
/// version the job opened, then the newest the job committed. Armed after the
/// open; until then it lets everything through. When the job itself deletes
/// the watched manifest, removing old versions as a purge does, it watches
/// the newest manifest left instead.
#[derive(Debug, Default)]
pub struct DropGuard {
    watched: Arc<Mutex<Watched>>,
}

#[derive(Debug, Default)]
struct Watched {
    manifest: Option<Path>,
    /// The job deleted the watched manifest itself.
    deleted: bool,
}

impl DropGuard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Watch `manifest`, the opened version's.
    pub fn arm(&self, manifest: Path) {
        self.watched.lock().unwrap().manifest = Some(manifest);
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

#[derive(Debug)]
struct Guarded {
    inner: Arc<dyn ObjectStore>,
    watched: Arc<Mutex<Watched>>,
}

impl Guarded {
    async fn check(&self, location: &Path) -> Result<()> {
        let (manifest, deleted) = {
            let watched = self.watched.lock().unwrap();
            (watched.manifest.clone(), watched.deleted)
        };
        let Some(mut manifest) = manifest else {
            return Ok(());
        };
        if deleted {
            manifest = self.newest_manifest(&manifest).await?.unwrap_or(manifest);
            let mut watched = self.watched.lock().unwrap();
            watched.manifest = Some(manifest.clone());
            watched.deleted = false;
        }
        match self.inner.head(&manifest).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Err(object_store::Error::Generic {
                store: "DropGuard",
                source: format!(
                    "the table was dropped while this job wrote it: {manifest} is gone, so {location} is not written"
                )
                .into(),
            }),
            Err(e) => Err(e),
        }
    }
}

fn is_manifest(location: &Path) -> bool {
    location.extension() == Some("manifest")
}

impl Guarded {
    /// The most recently written manifest beside `manifest`.
    async fn newest_manifest(&self, manifest: &Path) -> Result<Option<Path>> {
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
            .map(|o| o.location))
    }

    fn committed(&self, location: &Path) {
        if is_manifest(location) {
            let mut watched = self.watched.lock().unwrap();
            if watched.manifest.is_some() {
                watched.manifest = Some(location.clone());
                watched.deleted = false;
            }
        }
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
        // it back.
        if is_manifest(location)
            && let Err(dropped) = self.check(location).await
        {
            let _ = self.inner.delete(location).await;
            return Err(dropped);
        }
        self.committed(location);
        Ok(put)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check(location).await?;
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
        let watched = self.watched.clone();
        let locations = locations
            .inspect(move |location| {
                if let Ok(location) = location {
                    let mut watched = watched.lock().unwrap();
                    if watched.manifest.as_ref() == Some(location) {
                        watched.deleted = true;
                    }
                }
            })
            .boxed();
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
        self.check(to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.check(to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}
