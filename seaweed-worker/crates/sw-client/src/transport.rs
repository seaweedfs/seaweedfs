//! Volume data-plane transport: moves needle bytes to/from volume servers.
//!
//! Everything *above* this — resolving a file's chunks, looking up a volume's
//! locations, assigning a volume for a write — stays in the filer client. A
//! transport only transfers bytes for an already-resolved `fileId` at an
//! already-known location, so it is the single seam where the wire technology
//! lives: HTTP today ([`HttpTransport`]), RDMA (SoftRoCE / real NICs) next.
//!
//! The trait is deliberately narrow (read a range, write a needle) so an RDMA
//! implementation — registered memory regions, queue pairs, one-sided READ —
//! drops in without touching `read.rs` / `write.rs` or the kernel daemon.

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::header::{AUTHORIZATION, RANGE};
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VolumeWriteReceipt {
    /// SHA-256 computed by the volume over the bytes it committed. `None`
    /// means the transport/server pair does not advertise that proof.
    pub committed_sha256: Option<String>,
    /// Exact volume endpoint that accepted this commit. Fenced capacity writes
    /// persist it with their ownership receipt so GC can retire the original
    /// writer even after the volume topology changes.
    pub committed_location: Option<String>,
}

/// Transfers needle bytes for a resolved `fileId` + location. Implementations
/// must be cheap to clone/share (wrapped in `Arc`) and safe for concurrent use.
#[async_trait]
pub trait VolumeTransport: Send + Sync {
    /// Read `file_id` from any one of `locations` (`host:port`), optionally the
    /// logical byte range `(offset, len)`. Returns the (server-decompressed)
    /// needle bytes. Implementations should try locations in order until one
    /// succeeds.
    async fn read(
        &self,
        locations: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>>;

    /// Read with a short-lived exact-FID capability. Transports which cannot
    /// carry read authorization fail closed instead of silently dropping it.
    async fn read_with_auth(
        &self,
        locations: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
        auth: &str,
    ) -> Result<Vec<u8>> {
        if !auth.is_empty() {
            bail!(
                "{} transport cannot carry an exact-FID read capability",
                self.name()
            );
        }
        self.read(locations, file_id, range).await
    }

    /// Upload `data` as needle `file_id` to `location` (`host:port`). `filename`
    /// names the needle (volume servers use it for content-type/compression
    /// heuristics); `auth` is the per-fileId write JWT (`""` if none).
    async fn write(
        &self,
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
    ) -> Result<()>;

    /// Upload and return any proof produced by the volume commit. The default
    /// keeps existing transports source-compatible without claiming a digest.
    async fn write_with_receipt(
        &self,
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
    ) -> Result<VolumeWriteReceipt> {
        self.write(location, file_id, auth, filename, data).await?;
        Ok(VolumeWriteReceipt::default())
    }

    /// [`Self::write_with_receipt`] with the volume told to sync the needle to
    /// disk before it answers, when `fsync` is set. A transport that cannot ask
    /// for that fails closed rather than acknowledge a write still in memory.
    async fn write_synced(
        &self,
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
        fsync: bool,
    ) -> Result<VolumeWriteReceipt> {
        if fsync {
            bail!("{} transport cannot request fsync", self.name());
        }
        self.write_with_receipt(location, file_id, auth, filename, data)
            .await
    }

    /// Delete needle `file_id` at `location`; `auth` is the write JWT its
    /// assignment came with (`""` if none). A needle already gone is no error.
    async fn delete(&self, location: &str, file_id: &str, auth: &str) -> Result<()> {
        let _ = (location, file_id, auth);
        bail!("{} transport cannot delete needles", self.name())
    }

    /// Short label for logs/diagnostics (e.g. `"http"`, `"rdma"`).
    fn name(&self) -> &'static str;
}

/// Marker a [`VolumeTransport::write`] attaches when the write may have already
/// been committed but its outcome is unknown, so retrying or falling back could
/// duplicate data. Callers should fence the write instead of retrying; detect it
/// with `err.chain().any(|c| c.is::<CommitAmbiguous>())`.
#[derive(Debug)]
pub struct CommitAmbiguous;

impl std::fmt::Display for CommitAmbiguous {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "write commit status unknown; no safe fallback")
    }
}

impl std::error::Error for CommitAmbiguous {}

/// Marker a [`VolumeTransport::write`] attaches when the Volume refused the
/// write before any of it was committed, for example a required RDMA write to
/// a Volume that was sealed after the assignment. The needle does not exist,
/// so the caller may write it again under a fresh assignment; detect it with
/// [`is_not_written`].
#[derive(Debug)]
pub struct NotWritten;

impl std::fmt::Display for NotWritten {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "write refused before commit; nothing was written")
    }
}

impl std::error::Error for NotWritten {}

/// Whether `error` carries [`NotWritten`] anywhere in its context chain.
pub fn is_not_written(error: &anyhow::Error) -> bool {
    error.downcast_ref::<NotWritten>().is_some()
}

/// Connect bound for every volume request: a server that never answers a SYN
/// (silently partitioned) must not hold a read or a write.
pub const VOLUME_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on one GET from one location, connect to end of body. Two locations
/// fit inside a caller's 30 s op budget, so a silent first location fails
/// over to the next instead of holding the read (chaos C3).
pub const VOLUME_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound on one upload. Longer than the volume server's own 10 s bound on its
/// replica forwards, so a dead replica comes back as the server's error
/// first; still inside a caller's 30 s op budget.
pub const VOLUME_WRITE_TIMEOUT: Duration = Duration::from_secs(20);

/// TLS for volume servers, as `https.client` in security.toml configures it
/// for weed processes: enabled, volume servers are dialed over HTTPS, their
/// certificates checked against `ca` (the system roots without one), and
/// this client presents `cert`/`key` when set.
#[derive(Clone, Default)]
pub struct HttpsTls {
    pub ca_pem: Option<Vec<u8>>,
    pub cert_pem: Option<Vec<u8>>,
    pub key_pem: Option<Vec<u8>>,
    /// `https.client.insecure_skip_verify`
    pub insecure_skip_verify: bool,
}

impl std::fmt::Debug for HttpsTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpsTls")
            .field("ca", &self.ca_pem.is_some())
            .field("identity", &self.cert_pem.is_some())
            .field("insecure_skip_verify", &self.insecure_skip_verify)
            .finish()
    }
}

/// Both tonic and reqwest link rustls; with more than one crypto provider
/// compiled in, rustls needs one picked before TLS is configured.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Default transport: the SeaweedFS volume HTTP API — `GET /<fileId>` with an
/// optional `Range` header, and a multipart `POST` to upload a needle.
pub struct HttpTransport {
    http: reqwest::Client,
    scheme: &'static str,
    read_timeout: Duration,
    write_timeout: Duration,
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpTransport {
    pub fn new() -> Self {
        Self::with_timeouts(VOLUME_READ_TIMEOUT, VOLUME_WRITE_TIMEOUT)
    }

    fn with_timeouts(read_timeout: Duration, write_timeout: Duration) -> Self {
        Self {
            http: reqwest::Client::builder()
                .connect_timeout(VOLUME_CONNECT_TIMEOUT)
                .build()
                .expect("volume HTTP client"),
            scheme: "http",
            read_timeout,
            write_timeout,
        }
    }

    /// A transport dialing volume servers over HTTPS.
    pub fn with_tls(tls: &HttpsTls) -> Result<Self> {
        install_crypto_provider();
        let mut builder = reqwest::Client::builder()
            .connect_timeout(VOLUME_CONNECT_TIMEOUT)
            .use_rustls_tls()
            .danger_accept_invalid_certs(tls.insecure_skip_verify);
        if let Some(ca) = &tls.ca_pem {
            builder = builder.tls_built_in_root_certs(false).add_root_certificate(
                reqwest::Certificate::from_pem(ca).context("https.client.ca")?,
            );
        }
        match (&tls.cert_pem, &tls.key_pem) {
            (Some(cert), Some(key)) => {
                let mut pem = cert.clone();
                if !pem.ends_with(b"\n") {
                    pem.push(b'\n');
                }
                pem.extend_from_slice(key);
                builder = builder.identity(
                    reqwest::Identity::from_pem(&pem).context("https.client cert and key")?,
                );
            }
            (None, None) => {}
            _ => bail!("https.client needs both cert and key, or neither"),
        }
        Ok(Self {
            http: builder.build().context("volume HTTPS client")?,
            scheme: "https",
            read_timeout: VOLUME_READ_TIMEOUT,
            write_timeout: VOLUME_WRITE_TIMEOUT,
        })
    }
}

#[async_trait]
impl VolumeTransport for HttpTransport {
    async fn read(
        &self,
        locations: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>> {
        self.read_with_auth(locations, file_id, range, "").await
    }

    async fn read_with_auth(
        &self,
        locations: &[String],
        file_id: &str,
        range: Option<(u64, u64)>,
        auth: &str,
    ) -> Result<Vec<u8>> {
        let range_header = match range {
            Some((_, 0)) => return Ok(Vec::new()),
            Some((start, len)) => {
                let end = start
                    .checked_add(len - 1)
                    .with_context(|| format!("range {start}+{len} of {file_id}"))?;
                Some(format!("bytes={start}-{end}"))
            }
            None => None,
        };
        let mut last_err = None;
        for url in locations {
            let full = format!("{}://{url}/{file_id}", self.scheme);
            let mut req = self.http.get(&full).timeout(self.read_timeout);
            if let Some(range) = &range_header {
                req = req.header(RANGE, range);
            }
            if !auth.is_empty() {
                req = req.header(AUTHORIZATION, format!("BEARER {auth}"));
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    // A 200 to a ranged GET is the whole needle.
                    let whole = resp.status() != reqwest::StatusCode::PARTIAL_CONTENT;
                    match resp.bytes().await {
                        Ok(b) => {
                            return Ok(match range {
                                Some((start, len)) if whole => {
                                    let start = start.min(b.len() as u64) as usize;
                                    let end = start + (len.min((b.len() - start) as u64) as usize);
                                    b[start..end].to_vec()
                                }
                                _ => b.to_vec(),
                            });
                        }
                        Err(e) => last_err = Some(e.into()),
                    }
                }
                Ok(resp) => last_err = Some(anyhow!("GET {full} -> HTTP {}", resp.status())),
                Err(e) => last_err = Some(e.into()),
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("no volume URL for {file_id}")))
    }

    async fn write(
        &self,
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
    ) -> Result<()> {
        self.upload(location, file_id, auth, filename, data, false)
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
    ) -> Result<VolumeWriteReceipt> {
        self.upload(location, file_id, auth, filename, data, fsync)
            .await?;
        Ok(VolumeWriteReceipt::default())
    }

    async fn delete(&self, location: &str, file_id: &str, auth: &str) -> Result<()> {
        let url = format!("{}://{location}/{file_id}", self.scheme);
        let mut req = self.http.delete(&url).timeout(self.write_timeout);
        if !auth.is_empty() {
            req = req.header(AUTHORIZATION, format!("BEARER {auth}"));
        }
        let resp = req.send().await.with_context(|| format!("delete {url}"))?;
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        bail!("delete {url} -> HTTP {}", resp.status())
    }

    fn name(&self) -> &'static str {
        "http"
    }
}

impl HttpTransport {
    async fn upload(
        &self,
        location: &str,
        file_id: &str,
        auth: &str,
        filename: &str,
        data: Bytes,
        fsync: bool,
    ) -> Result<()> {
        let mut url = format!("{}://{location}/{file_id}", self.scheme);
        if fsync {
            url.push_str("?fsync=true");
        }
        let size = data.len() as u64;
        let part = reqwest::multipart::Part::stream_with_length(data, size)
            .file_name(filename.to_string());
        let form = reqwest::multipart::Form::new().part("file", part);
        let mut req = self
            .http
            .post(&url)
            .timeout(self.write_timeout)
            .multipart(form);
        if !auth.is_empty() {
            req = req.header(AUTHORIZATION, format!("BEARER {auth}"));
        }
        let resp = match req.send().await {
            Ok(resp) => resp,
            // Timed out after the request went out: the volume may have
            // committed it, so callers must fence instead of retrying.
            Err(e) if e.is_timeout() && !e.is_connect() => {
                return Err(e)
                    .with_context(|| format!("upload to {url}"))
                    .context(CommitAmbiguous);
            }
            Err(e) => return Err(e).with_context(|| format!("upload to {url}")),
        };
        if !resp.status().is_success() {
            let error = anyhow!("upload {url} -> HTTP {}", resp.status());
            // Only an explicit pre-append refusal permits reassignment.
            if resp.status() == reqwest::StatusCode::CONFLICT
                && resp
                    .headers()
                    .get("x-seaweed-write-outcome")
                    .is_some_and(|value| value == "refused-before-append")
            {
                return Err(error.context(NotWritten));
            }
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn not_written_is_found_under_later_contexts_and_only_there() {
        use anyhow::Context;
        let refused: anyhow::Result<()> = Err(anyhow::anyhow!("prepare rejected"))
            .context(NotWritten)
            .context("required RDMA write failed in PrepareRejected");
        let wrapped = refused
            .context("upload 38,03e1 to 10.0.2.3:19601")
            .context("loader write /lmcache/k")
            .unwrap_err();
        assert!(is_not_written(&wrapped));

        let ambiguous: anyhow::Result<()> =
            Err(anyhow::anyhow!("commit rpc timed out")).context(CommitAmbiguous);
        assert!(!is_not_written(&ambiguous.context("upload").unwrap_err()));
    }

    #[tokio::test]
    async fn authenticated_read_sends_exact_fid_bearer_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let length = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..length]).to_ascii_lowercase();
            assert!(request.starts_with("get /7,abc http/1.1\r\n"));
            assert!(request.contains("\r\nauthorization: bearer signed-token\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc")
                .await
                .unwrap();
        });

        let bytes = HttpTransport::new()
            .read_with_auth(&[address.to_string()], "7,abc", None, "signed-token")
            .await
            .unwrap();
        assert_eq!(bytes, b"abc");
        server.await.unwrap();
    }

    /// A server that ignores Range and answers 200 with the whole needle
    /// yields only the bytes asked for.
    #[tokio::test]
    async fn a_whole_needle_answer_to_a_ranged_read_is_sliced() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\n0123456789",
                    )
                    .await
                    .unwrap();
            }
        });
        let http = HttpTransport::new();
        let at = std::slice::from_ref(&address);
        assert_eq!(http.read(at, "7,abc", Some((3, 4))).await.unwrap(), b"3456");
        assert_eq!(http.read(at, "7,abc", Some((8, 4))).await.unwrap(), b"89");
        assert!(
            http.read(at, "7,abc", Some((12, 4)))
                .await
                .unwrap()
                .is_empty()
        );
        server.await.unwrap();
    }

    /// An empty range needs no request, and one past the end of the address
    /// space sends none.
    #[tokio::test]
    async fn empty_and_overflowing_ranges_send_no_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed = listener.local_addr().unwrap().to_string();
        drop(listener);
        let http = HttpTransport::new();
        let read = http
            .read(std::slice::from_ref(&closed), "7,abc", Some((5, 0)))
            .await
            .unwrap();
        assert!(read.is_empty());
        let error = http
            .read(&[closed], "7,abc", Some((u64::MAX, 2)))
            .await
            .expect_err("the range cannot be expressed");
        assert!(format!("{error:#}").contains("range"), "{error:#}");
    }

    /// A server that accepts the connection and never answers: a silently
    /// partitioned volume server after the connect (chaos C3).
    async fn silent_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let held = tokio::spawn(async move {
            let mut sockets = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                sockets.push(socket);
            }
        });
        (address, held)
    }

    /// Chaos C3: a GET with the silent copy listed first must reach the
    /// healthy copy within its per-location bound, not wait on the first.
    #[tokio::test]
    async fn read_fails_over_from_a_silent_location() {
        let (silent, held) = silent_server().await;
        let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_address = healthy.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = healthy.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc")
                .await
                .unwrap();
        });
        let bound = Duration::from_millis(300);
        let started = std::time::Instant::now();
        let bytes = tokio::time::timeout(
            Duration::from_secs(10),
            HttpTransport::with_timeouts(bound, bound).read(
                &[silent, healthy_address],
                "7,abc",
                None,
            ),
        )
        .await
        .expect("a silent first location must not hold the read")
        .unwrap();
        assert_eq!(bytes, b"abc");
        assert!(started.elapsed() >= bound && started.elapsed() < bound * 10);
        server.await.unwrap();
        held.abort();
    }

    /// A write to a silent server fails at its bound instead of waiting.
    #[tokio::test]
    async fn write_to_a_silent_server_fails_at_its_bound() {
        let (silent, held) = silent_server().await;
        let bound = Duration::from_millis(300);
        let started = std::time::Instant::now();
        let written = tokio::time::timeout(
            Duration::from_secs(10),
            HttpTransport::with_timeouts(bound, bound).write(
                &silent,
                "7,abc",
                "",
                "k",
                Bytes::from(vec![0u8; 1 << 20]),
            ),
        )
        .await
        .expect("a silent server must not hold the write");
        // The request went out, so the commit is unknown: fence, never
        // reassign. Detected through the context, as sw-kd does (#918).
        let error = written.expect_err("a silent server cannot ack the write");
        assert!(
            error.downcast_ref::<CommitAmbiguous>().is_some(),
            "{error:#}"
        );
        assert!(!is_not_written(&error));
        assert!(started.elapsed() >= bound && started.elapsed() < bound * 10);
        held.abort();
    }

    /// An assignment that wants the needle on disk asks the volume for it.
    #[tokio::test]
    async fn synced_write_asks_the_volume_to_fsync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let mut lines = Vec::new();
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let length = socket.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..length]).to_string();
                lines.push(request.lines().next().unwrap_or_default().to_string());
                socket
                    .write_all(
                        b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
            lines
        });
        let http = HttpTransport::new();
        for fsync in [true, false] {
            http.write_synced(&address, "7,abc", "", "k", Bytes::from_static(b"kv"), fsync)
                .await
                .unwrap();
        }
        assert_eq!(
            server.await.unwrap(),
            ["POST /7,abc?fsync=true HTTP/1.1", "POST /7,abc HTTP/1.1"]
        );
    }

    /// A write that never reached the server (connection refused) is a plain
    /// error, not an ambiguous commit.
    #[tokio::test]
    async fn write_that_never_connected_is_not_ambiguous() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed = listener.local_addr().unwrap().to_string();
        drop(listener);
        let error = HttpTransport::new()
            .write(&closed, "7,abc", "", "k", Bytes::from_static(b"kv"))
            .await
            .expect_err("nothing listens there");
        assert!(
            error.downcast_ref::<CommitAmbiguous>().is_none(),
            "{error:#}"
        );
    }
}
