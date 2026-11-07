//! Data plane (write): upload file content as immutable chunks and persist the
//! chunk list on the filer.
//!
//! Phase B2 uses a simple whole-file model: the daemon buffers a file's content
//! and, on flush, re-uploads it as `chunk_size`-sized chunks and replaces the
//! entry's chunk list. Correct for sequential writes and full overwrites; an
//! incremental/append-merge pipeline is a later optimisation.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;

use crate::filer::{AssignVolumeOptions, Assignment, FilerClient};
use crate::filer_pb::{Entry, FileChunk};
use crate::transport::{HttpTransport, VolumeTransport};
use crate::wbuf::SealPiece;

/// Current Unix time as (seconds, nanoseconds).
pub fn now_unix() -> (i64, i32) {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i64, d.subsec_nanos() as i32)
}

/// Current Unix time in nanoseconds.
pub fn now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    d.as_nanos() as i64
}

/// Max chunk uploads in flight per flush (bounds load on the filer/volumes).
const MAX_PARALLEL_UPLOADS: usize = 8;

/// Uploads chunk data to volume servers via a pluggable [`VolumeTransport`]
/// (HTTP by default; RDMA later). Volume assignment stays on the filer.
/// Cheap to clone (shares the transport) so a flush can upload chunks in parallel.
#[derive(Clone)]
pub struct Writer {
    transport: Arc<dyn VolumeTransport>,
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

impl Writer {
    pub fn new() -> Self {
        Self::with_transport(Arc::new(HttpTransport::new()))
    }

    /// Construct with a specific data-plane transport (e.g. an RDMA transport).
    pub fn with_transport(transport: Arc<dyn VolumeTransport>) -> Self {
        Self { transport }
    }

    /// Assign a volume, upload `data` as one needle, and return the resulting
    /// `FileChunk` positioned at `offset` in the file.
    pub async fn upload_chunk(
        &self,
        client: &mut FilerClient,
        file_path: &str,
        filename: &str,
        offset: i64,
        data: &[u8],
    ) -> Result<FileChunk> {
        self.upload_chunk_with_options(
            client,
            file_path,
            filename,
            offset,
            data,
            &AssignVolumeOptions::default(),
        )
        .await
    }

    pub async fn upload_chunk_with_options(
        &self,
        client: &mut FilerClient,
        file_path: &str,
        filename: &str,
        offset: i64,
        data: &[u8],
        options: &AssignVolumeOptions,
    ) -> Result<FileChunk> {
        self.upload_chunk_with_options_bytes(
            client,
            file_path,
            filename,
            offset,
            Bytes::copy_from_slice(data),
            options,
        )
        .await
    }

    pub async fn upload_chunk_with_options_bytes(
        &self,
        client: &mut FilerClient,
        file_path: &str,
        filename: &str,
        offset: i64,
        data: Bytes,
        options: &AssignVolumeOptions,
    ) -> Result<FileChunk> {
        let size = data.len() as u64;
        let first = client
            .assign_volume_with_options(file_path, size, options)
            .await?;
        upload_reassigning_once(
            first,
            data,
            |assign, data| async move {
                self.upload_assigned_chunk_bytes(&assign, filename, offset, data)
                    .await
            },
            || client.assign_volume_with_options(file_path, size, options),
        )
        .await
    }

    /// Upload to a volume assignment that has already been resolved by the
    /// filer. This is the data-plane seam: HTTP/RDMA details live only in the
    /// transport implementation, while assignment and filer metadata stay above.
    pub async fn upload_assigned_chunk(
        &self,
        assign: &Assignment,
        filename: &str,
        offset: i64,
        data: &[u8],
    ) -> Result<FileChunk> {
        self.upload_assigned_chunk_bytes(assign, filename, offset, Bytes::copy_from_slice(data))
            .await
    }

    pub async fn upload_assigned_chunk_bytes(
        &self,
        assign: &Assignment,
        filename: &str,
        offset: i64,
        data: Bytes,
    ) -> Result<FileChunk> {
        self.upload_assigned_chunk_bytes_with_receipt(assign, filename, offset, data)
            .await
            .map(|(chunk, _)| chunk)
    }

    pub async fn upload_assigned_chunk_bytes_with_receipt(
        &self,
        assign: &Assignment,
        filename: &str,
        offset: i64,
        data: Bytes,
    ) -> Result<(FileChunk, crate::VolumeWriteReceipt)> {
        let size = data.len() as u64;
        let receipt = self
            .transport
            .write_synced(
                &assign.url,
                &assign.file_id,
                &assign.auth,
                filename,
                data,
                assign.fsync,
            )
            .await
            .with_context(|| format!("upload {} to {}", assign.file_id, assign.url))?;

        Ok((
            FileChunk {
                file_id: assign.file_id.clone(),
                offset,
                size,
                modified_ts_ns: now_ns(),
                ..Default::default()
            },
            receipt,
        ))
    }
}

/// Uploads under `first`; if the Volume refused before committing anything
/// (e.g. it was sealed for rotation after the assignment), asks `reassign` for
/// one fresh assignment and uploads once more. Any other result, a commit with
/// an unknown outcome included, returns as is, so nothing is written twice.
async fn upload_reassigning_once<U, UF, A, AF>(
    first: Assignment,
    data: Bytes,
    upload: U,
    reassign: A,
) -> Result<FileChunk>
where
    U: Fn(Assignment, Bytes) -> UF,
    UF: std::future::Future<Output = Result<FileChunk>>,
    A: FnOnce() -> AF,
    AF: std::future::Future<Output = Result<Assignment>>,
{
    match upload(first, data.clone()).await {
        Err(error) if crate::is_not_written(&error) => upload(reassign().await?, data).await,
        other => other,
    }
}

/// Upload a batch of staged write pieces concurrently (bounded), each as one
/// overlay needle at its file offset. Returns the resulting `FileChunk`s sorted
/// by file offset. Used
/// by the streaming write path to seal chunks incrementally without holding the
/// whole file in memory.
pub async fn upload_pieces(
    client: &FilerClient,
    writer: &Writer,
    file_path: &str,
    filename: &str,
    pieces: Vec<SealPiece>,
) -> Result<Vec<FileChunk>> {
    upload_pieces_with_options(
        client,
        writer,
        file_path,
        filename,
        pieces,
        &AssignVolumeOptions::default(),
    )
    .await
}

pub async fn upload_pieces_with_options(
    client: &FilerClient,
    writer: &Writer,
    file_path: &str,
    filename: &str,
    pieces: Vec<SealPiece>,
    options: &AssignVolumeOptions,
) -> Result<Vec<FileChunk>> {
    let sem = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_UPLOADS));
    let mut set = tokio::task::JoinSet::new();
    for p in pieces {
        let permit = sem.clone().acquire_owned().await.expect("semaphore open");
        let mut c = client.clone();
        let w = writer.clone();
        let fp = file_path.to_string();
        let name = filename.to_string();
        let opts = options.clone();
        set.spawn(async move {
            let _permit = permit;
            w.upload_chunk_with_options_bytes(
                &mut c,
                &fp,
                &name,
                p.offset as i64,
                Bytes::from(p.data),
                &opts,
            )
            .await
        });
    }
    let mut out = Vec::with_capacity(set.len());
    while let Some(joined) = set.join_next().await {
        out.push(joined.context("piece upload task")??);
    }
    out.sort_by(|a, b| {
        (a.offset, a.modified_ts_ns)
            .cmp(&(b.offset, b.modified_ts_ns))
            .then_with(|| a.file_id.cmp(&b.file_id))
    });
    Ok(out)
}

/// Flush a full-file buffer: upload it as `chunk_size` chunks and `UpdateEntry`
/// with the new chunk list and size/mtime. The entry is mutated in place.
///
/// `data` is taken as a shared `Arc` (not a slice) so the caller's in-memory
/// write buffer is uploaded in place — no extra full-file copy. With a large
/// file this is the difference between ~1x and ~3x its size in resident memory.
pub async fn flush_file(
    client: &mut FilerClient,
    writer: &Writer,
    directory: &str,
    entry: &mut Entry,
    data: Arc<Vec<u8>>,
    chunk_size: usize,
    signature: i32,
) -> Result<()> {
    let file_path = if directory == "/" {
        format!("/{}", entry.name)
    } else {
        format!("{directory}/{}", entry.name)
    };

    // Upload chunks concurrently (bounded), preserving order in the chunk list.
    // FilerClient/Writer are cheap clones (h2 multiplexes), so parallel uploads
    // overlap their AssignVolume + needle-POST round-trips.
    anyhow::ensure!(chunk_size > 0, "flushing {file_path}: chunk size is zero");
    let n = data.len().div_ceil(chunk_size);
    let shared = data;
    let sem = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_UPLOADS));
    let mut set = tokio::task::JoinSet::new();
    for i in 0..n {
        let start = i * chunk_size;
        let end = (start + chunk_size).min(shared.len());
        let permit = sem.clone().acquire_owned().await.expect("semaphore open");
        let mut c = client.clone();
        let w = writer.clone();
        let d = shared.clone();
        let fp = file_path.clone();
        let name = entry.name.clone();
        set.spawn(async move {
            let _permit = permit;
            let r = w
                .upload_chunk(&mut c, &fp, &name, start as i64, &d[start..end])
                .await;
            (i, r)
        });
    }
    let mut slots: Vec<Option<FileChunk>> = (0..n).map(|_| None).collect();
    while let Some(joined) = set.join_next().await {
        let (i, r) = joined.context("chunk upload task")?;
        slots[i] = Some(r?);
    }

    entry.chunks = slots
        .into_iter()
        .map(|c| c.expect("all chunks uploaded"))
        .collect();
    // Small-file inline content from a previous version no longer applies.
    entry.content = Vec::new();
    if let Some(a) = entry.attributes.as_mut() {
        a.file_size = shared.len() as u64;
        let (sec, nsec) = now_unix();
        a.mtime = sec;
        a.mtime_ns = nsec;
    }

    client
        .update_entry(directory, entry.clone(), signature)
        .await
        .context("persisting chunk list")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    fn assignment(fid: &str) -> Assignment {
        Assignment {
            file_id: fid.to_string(),
            url: "10.0.2.3:19601".to_string(),
            auth: String::new(),
            fsync: false,
        }
    }

    fn chunk(fid: &str, data: &Bytes) -> FileChunk {
        FileChunk {
            file_id: fid.to_string(),
            size: data.len() as u64,
            ..Default::default()
        }
    }

    /// Runs the helper with an upload that answers from `answers` in order
    /// and a reassign that returns `fresh`; reports the chunk and the calls.
    async fn reassign_run(
        answers: Vec<&'static str>,
        fresh: Result<Assignment>,
    ) -> (Result<FileChunk>, Vec<(String, Vec<u8>)>, bool) {
        let answers = Mutex::new(answers.into_iter());
        let uploads = Mutex::new(Vec::new());
        let reassigned = Mutex::new(false);
        let result = upload_reassigning_once(
            assignment("38,a1"),
            Bytes::from_static(b"kv-bytes"),
            |assign, data| {
                uploads
                    .lock()
                    .unwrap()
                    .push((assign.file_id.clone(), data.to_vec()));
                let answer = answers.lock().unwrap().next().unwrap();
                async move {
                    match answer {
                        "ok" => Ok(chunk(&assign.file_id, &data)),
                        "refused" => Err(anyhow::anyhow!("prepare rejected")
                            .context(crate::NotWritten)
                            .context("upload")),
                        _ => {
                            Err(anyhow::anyhow!("commit timed out").context(crate::CommitAmbiguous))
                        }
                    }
                }
            },
            || {
                *reassigned.lock().unwrap() = true;
                async move { fresh }
            },
        )
        .await;
        let uploads = uploads.into_inner().unwrap();
        (result, uploads, reassigned.into_inner().unwrap())
    }

    #[tokio::test]
    async fn a_refused_write_is_uploaded_once_more_under_a_fresh_assignment() {
        let (result, uploads, reassigned) =
            reassign_run(vec!["refused", "ok"], Ok(assignment("41,b2"))).await;
        assert!(reassigned);
        assert_eq!(result.unwrap().file_id, "41,b2");
        assert_eq!(
            uploads,
            vec![
                ("38,a1".to_string(), b"kv-bytes".to_vec()),
                ("41,b2".to_string(), b"kv-bytes".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn a_zero_chunk_size_is_an_error() {
        let mut client = FilerClient::connect_lazy("127.0.0.1:1").unwrap();
        let mut entry = Entry {
            name: "f".into(),
            ..Default::default()
        };
        let err = flush_file(
            &mut client,
            &Writer::new(),
            "/d",
            &mut entry,
            Arc::new(vec![1; 8]),
            0,
            0,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("chunk size is zero"), "{err:#}");
    }

    #[tokio::test]
    async fn the_retry_happens_at_most_once() {
        let (result, uploads, _) =
            reassign_run(vec!["refused", "refused"], Ok(assignment("41,b2"))).await;
        assert!(crate::is_not_written(&result.unwrap_err()));
        assert_eq!(uploads.len(), 2);
    }

    #[tokio::test]
    async fn an_ambiguous_commit_is_never_retried() {
        let (result, uploads, reassigned) =
            reassign_run(vec!["ambiguous"], Ok(assignment("41,b2"))).await;
        assert!(result.is_err());
        assert!(!reassigned);
        assert_eq!(uploads.len(), 1);
    }

    #[tokio::test]
    async fn a_failed_reassignment_returns_its_error_without_a_second_upload() {
        let (result, uploads, reassigned) =
            reassign_run(vec!["refused"], Err(anyhow::anyhow!("no writable volumes"))).await;
        assert!(reassigned);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no writable volumes")
        );
        assert_eq!(uploads.len(), 1);
    }

    #[tokio::test]
    async fn a_success_is_not_retried() {
        let (result, uploads, reassigned) = reassign_run(vec!["ok"], Ok(assignment("41,b2"))).await;
        assert_eq!(result.unwrap().file_id, "38,a1");
        assert!(!reassigned);
        assert_eq!(uploads.len(), 1);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct WriteCall {
        location: String,
        file_id: String,
        auth: String,
        filename: String,
        data: Vec<u8>,
    }

    #[derive(Default)]
    struct MockTransport {
        writes: Mutex<Vec<WriteCall>>,
    }

    #[async_trait]
    impl VolumeTransport for MockTransport {
        async fn read(
            &self,
            _locations: &[String],
            _file_id: &str,
            _range: Option<(u64, u64)>,
        ) -> Result<Vec<u8>> {
            anyhow::bail!("read is not used by write tests")
        }

        async fn write(
            &self,
            location: &str,
            file_id: &str,
            auth: &str,
            filename: &str,
            data: Bytes,
        ) -> Result<()> {
            self.writes.lock().unwrap().push(WriteCall {
                location: location.to_string(),
                file_id: file_id.to_string(),
                auth: auth.to_string(),
                filename: filename.to_string(),
                data: data.to_vec(),
            });
            Ok(())
        }

        fn name(&self) -> &'static str {
            "mock"
        }
    }

    #[tokio::test]
    async fn assigned_chunk_upload_uses_volume_transport_only() {
        let transport = Arc::new(MockTransport::default());
        let writer = Writer::with_transport(transport.clone());
        let assign = Assignment {
            file_id: "7,abc".to_string(),
            url: "10.0.0.2:8080".to_string(),
            auth: "write-token".to_string(),
            fsync: false,
        };

        let chunk = writer
            .upload_assigned_chunk(&assign, "part-0", 4096, b"needle-bytes")
            .await
            .unwrap();

        assert_eq!(chunk.file_id, "7,abc");
        assert_eq!(chunk.offset, 4096);
        assert_eq!(chunk.size, 12);

        let writes = transport.writes.lock().unwrap();
        assert_eq!(
            writes.as_slice(),
            [WriteCall {
                location: "10.0.0.2:8080".to_string(),
                file_id: "7,abc".to_string(),
                auth: "write-token".to_string(),
                filename: "part-0".to_string(),
                data: b"needle-bytes".to_vec(),
            }]
        );
    }
}
