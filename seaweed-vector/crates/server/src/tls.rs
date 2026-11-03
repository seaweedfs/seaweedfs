//! The engine's gRPC listener under TLS, run the way weed servers run
//! theirs: every client must present a certificate chaining to `grpc.ca`,
//! `allowed_commonNames` and `allowed_wildcard_domain` restrict which client
//! identities are served, and the certificate, key and CA files are re-read
//! on a timer and on SIGHUP so rotated files take effect without a restart.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::Stream;
use rustls::DistinguishedName;
use rustls::RootCertStore;
use rustls::ServerConfig;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::security::GrpcServerTls;

/// `WEED_TLS_CERT_REFRESH_INTERVAL`, the cadence weed's PEM providers re-read
/// certificate files at; a Go duration such as `30m` or `500ms`.
pub const REFRESH_INTERVAL_ENV: &str = "WEED_TLS_CERT_REFRESH_INTERVAL";
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5 * 3600);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the files are re-read: the environment's value, else weed's
/// default of five hours.
pub fn refresh_interval() -> Duration {
    std::env::var(REFRESH_INTERVAL_ENV)
        .ok()
        .and_then(|s| parse_go_duration(&s))
        .filter(|d| !d.is_zero())
        .unwrap_or(DEFAULT_REFRESH_INTERVAL)
}

/// `time.ParseDuration`'s grammar for the units weed deployments use:
/// a sequence of `<number><unit>` with units `h`, `m`, `s`, `ms`.
fn parse_go_duration(s: &str) -> Option<Duration> {
    let mut rest = s.trim();
    if rest.is_empty() {
        return None;
    }
    let mut total = Duration::ZERO;
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(rest.len());
        let (number, tail) = rest.split_at(digits);
        let value: f64 = number.parse().ok()?;
        let unit_len = tail
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(tail.len());
        let (unit, tail) = tail.split_at(unit_len);
        let seconds = match unit {
            "h" => 3600.0,
            "m" => 60.0,
            "s" => 1.0,
            "ms" => 0.001,
            _ => return None,
        };
        total += Duration::from_secs_f64(value * seconds);
        rest = tail;
    }
    Some(total)
}

/// The listener's TLS configuration, rebuilt from the files whenever they
/// change.
pub struct ServerTls {
    files: GrpcServerTls,
    config: RwLock<Arc<ServerConfig>>,
    /// The file contents the current configuration was built from.
    loaded: Mutex<Vec<u8>>,
}

impl std::fmt::Debug for ServerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTls")
            .field("files", &self.files)
            .finish_non_exhaustive()
    }
}

fn read_all(files: &GrpcServerTls) -> Result<Vec<u8>> {
    let mut all = Vec::new();
    for (key, path) in [
        ("cert", &files.cert_path),
        ("key", &files.key_path),
        ("grpc.ca", &files.ca_path),
    ] {
        all.extend(std::fs::read(path).with_context(|| format!("{key} = {path:?}"))?);
        all.push(0);
    }
    Ok(all)
}

/// Builds the configuration `security.LoadServerTLS` would: the identity in
/// `cert`/`key`, every client verified against `grpc.ca`, and the client's
/// common name checked against the allowlist when one is set.
fn build_config(files: &GrpcServerTls) -> Result<ServerConfig> {
    let cert_chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&files.cert_path)
        .with_context(|| format!("cert = {:?}", files.cert_path))?
        .collect::<Result<_, _>>()
        .with_context(|| format!("cert = {:?}", files.cert_path))?;
    if cert_chain.is_empty() {
        bail!("cert = {:?} holds no certificate", files.cert_path);
    }
    let key = PrivateKeyDer::from_pem_file(&files.key_path)
        .with_context(|| format!("key = {:?}", files.key_path))?;
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&files.ca_path)
        .with_context(|| format!("grpc.ca = {:?}", files.ca_path))?
    {
        roots
            .add(cert.with_context(|| format!("grpc.ca = {:?}", files.ca_path))?)
            .with_context(|| format!("grpc.ca = {:?}", files.ca_path))?;
    }
    if roots.is_empty() {
        bail!("grpc.ca = {:?} holds no certificate", files.ca_path);
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("client certificate verifier")?;
    let verifier: Arc<dyn ClientCertVerifier> =
        if files.allowed_common_names.is_empty() && files.allowed_wildcard_domain.is_empty() {
            verifier
        } else {
            Arc::new(CommonNameVerifier {
                inner: verifier,
                allowed_common_names: files.allowed_common_names.iter().cloned().collect(),
                allowed_wildcard_domain: files.allowed_wildcard_domain.clone(),
            })
        };
    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, key)
        .context("server certificate")?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

impl ServerTls {
    pub fn load(files: GrpcServerTls) -> Result<Arc<Self>> {
        weed_lance_worker::tls::install_default_crypto_provider();
        let loaded = read_all(&files)?;
        let config = build_config(&files)?;
        Ok(Arc::new(Self {
            files,
            config: RwLock::new(Arc::new(config)),
            loaded: Mutex::new(loaded),
        }))
    }

    pub fn files(&self) -> &GrpcServerTls {
        &self.files
    }

    /// Re-reads the files; when they changed, the next handshake uses them.
    /// Returns whether they had. A file that no longer parses is an error,
    /// and the configuration in use stays.
    pub fn reload(&self) -> Result<bool> {
        let current = read_all(&self.files)?;
        let mut loaded = self.loaded.lock().unwrap_or_else(|e| e.into_inner());
        if *loaded == current {
            return Ok(false);
        }
        let config = build_config(&self.files)?;
        *self.config.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(config);
        *loaded = current;
        Ok(true)
    }

    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(
            self.config
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        )
    }

    /// Connections accepted on `listener` and taken through the TLS
    /// handshake with the configuration current at that moment. Each
    /// handshake runs on its own task under `HANDSHAKE_TIMEOUT`, so a peer
    /// that connects and stays silent holds up nobody else; a failed
    /// handshake is logged and skipped, as the Rust volume server does.
    pub fn incoming(
        self: &Arc<Self>,
        listener: TcpListener,
    ) -> impl Stream<Item = std::io::Result<TlsStream<TcpStream>>> + Send + 'static {
        let tls = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = tx.closed() => return,
                    accepted = listener.accept() => accepted,
                };
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        let _ = tx.send(Err(err)).await;
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let acceptor = tls.acceptor();
                let tx = tx.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(stream)) => {
                            let _ = tx.send(Ok(stream)).await;
                        }
                        Ok(Err(err)) => tracing::debug!("gRPC TLS handshake from {peer}: {err}"),
                        Err(_) => tracing::debug!("gRPC TLS handshake from {peer} timed out"),
                    }
                });
            }
        });
        tokio_stream::wrappers::ReceiverStream::new(rx)
    }

    /// Re-reads the files every `every`, and at once on SIGHUP.
    pub async fn refresh(self: Arc<Self>, every: Duration) {
        #[cfg(unix)]
        let mut hangup =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();
        loop {
            #[cfg(unix)]
            {
                tokio::select! {
                    _ = tokio::time::sleep(every) => {}
                    _ = async { match hangup.as_mut() { Some(h) => h.recv().await, None => std::future::pending().await } } => {
                        tracing::info!("SIGHUP: re-reading gRPC TLS files");
                    }
                }
            }
            #[cfg(not(unix))]
            tokio::time::sleep(every).await;
            match self.reload() {
                Ok(true) => tracing::info!("gRPC TLS files changed; new handshakes use them"),
                Ok(false) => {}
                Err(err) => tracing::warn!("gRPC TLS files not reloaded: {err:#}"),
            }
        }
    }
}

/// `security.Authenticator` for client certificates: after the chain is
/// verified, the subject common name must be allowed, or lie under the
/// wildcard domain.
#[derive(Debug)]
struct CommonNameVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    allowed_common_names: HashSet<String>,
    allowed_wildcard_domain: String,
}

impl ClientCertVerifier for CommonNameVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let common_name = parse_common_name(end_entity).map_err(|e| {
            rustls::Error::General(format!("parse client certificate common name failed: {e}"))
        })?;
        if common_name_is_allowed(
            &common_name,
            &self.allowed_common_names,
            &self.allowed_wildcard_domain,
        ) {
            return Ok(ClientCertVerified::assertion());
        }
        Err(rustls::Error::General(format!(
            "Authenticate: invalid subject client common name: {common_name}"
        )))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn parse_common_name(cert: &CertificateDer<'_>) -> Result<String> {
    let (_, certificate) = X509Certificate::from_der(cert.as_ref())
        .map_err(|e| anyhow::anyhow!("parse X.509 certificate failed: {e}"))?;
    Ok(certificate
        .subject()
        .iter_common_name()
        .next()
        .and_then(|common_name| common_name.as_str().ok())
        .map(str::to_string)
        .unwrap_or_default())
}

fn common_name_is_allowed(
    common_name: &str,
    allowed_common_names: &HashSet<String>,
    allowed_wildcard_domain: &str,
) -> bool {
    allowed_common_names.contains(common_name)
        || wildcard_domain_matches(common_name, allowed_wildcard_domain)
}

// Mirrors the Go wildcard check: the name must end with the domain, carry a
// non-empty label in front of it, and be a valid DNS name so a CA-signed name
// like ".example.com" cannot satisfy the suffix check. A leading "*." label
// is allowed so wildcard certs keep matching. One trailing root dot is
// ignored on both sides so absolute DNS names still match.
fn wildcard_domain_matches(name: &str, wildcard_domain: &str) -> bool {
    let name = name.strip_suffix('.').unwrap_or(name);
    let wildcard_domain = wildcard_domain.strip_suffix('.').unwrap_or(wildcard_domain);
    if wildcard_domain.is_empty()
        || name.len() <= wildcard_domain.len()
        || !name.ends_with(wildcard_domain)
    {
        return false;
    }
    if !wildcard_domain.starts_with('.')
        && name.as_bytes()[name.len() - wildcard_domain.len() - 1] != b'.'
    {
        return false;
    }
    is_domain_name(name.strip_prefix("*.").unwrap_or(name))
}

// Mirrors Go's net.IsDomainName: non-empty labels of letters, digits, '-',
// '_', each up to 63 chars and not '-'-edged, with at least one non-digit so
// a bare IP cannot pass a domain wildcard.
fn is_domain_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 253 {
        return false;
    }
    let mut non_numeric = false;
    let valid = name.split('.').all(|label| {
        non_numeric |= label.bytes().any(|c| !c.is_ascii_digit());
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
    });
    valid && non_numeric
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    /// `tests/tls`: a CA and two leaves it signed, `engine-a` and `engine-b`.
    fn fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/tls")
            .join(name)
    }

    fn install(dir: &Path, leaf: &str) -> GrpcServerTls {
        for (from, to) in [
            (format!("{leaf}.pem"), "cert.pem"),
            (format!("{leaf}.key"), "key.pem"),
            ("ca.pem".to_string(), "ca.pem"),
        ] {
            std::fs::copy(fixture(&from), dir.join(to)).unwrap();
        }
        GrpcServerTls {
            cert_path: dir.join("cert.pem").display().to_string(),
            key_path: dir.join("key.pem").display().to_string(),
            ca_path: dir.join("ca.pem").display().to_string(),
            allowed_common_names: Vec::new(),
            allowed_wildcard_domain: String::new(),
        }
    }

    #[test]
    fn rotated_files_are_picked_up_and_broken_ones_are_not() {
        let dir = tempfile::tempdir().unwrap();
        let tls = ServerTls::load(install(dir.path(), "a")).unwrap();
        assert!(!tls.reload().unwrap(), "unchanged files are not a reload");
        let before = tls.acceptor();

        install(dir.path(), "b");
        assert!(tls.reload().unwrap());
        let after = tls.acceptor();
        assert!(!Arc::ptr_eq(before.config(), after.config()));

        std::fs::write(dir.path().join("key.pem"), "not a key").unwrap();
        assert!(tls.reload().is_err());
        assert!(
            Arc::ptr_eq(after.config(), tls.acceptor().config()),
            "a broken file leaves the working configuration in place"
        );
    }

    #[tokio::test]
    async fn a_silent_peer_does_not_hold_up_the_next_handshake() {
        use futures::StreamExt;
        weed_lance_worker::tls::install_default_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let tls = ServerTls::load(install(dir.path(), "a")).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut incoming = Box::pin(tls.incoming(listener));
        let _silent = TcpStream::connect(addr).await.unwrap();

        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(fixture("ca.pem")).unwrap() {
            roots.add(cert.unwrap()).unwrap();
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_file(fixture("a.pem")).unwrap()],
                PrivateKeyDer::from_pem_file(fixture("a.key")).unwrap(),
            )
            .unwrap();
        let client = tokio::spawn(async move {
            let tcp = TcpStream::connect(addr).await.unwrap();
            tokio_rustls::TlsConnector::from(Arc::new(config))
                .connect("localhost".try_into().unwrap(), tcp)
                .await
        });
        let served = tokio::time::timeout(Duration::from_secs(5), incoming.next()).await;
        assert!(
            matches!(served, Ok(Some(Ok(_)))),
            "the handshake waited behind a silent peer"
        );
        client.await.unwrap().unwrap();
    }

    #[test]
    fn missing_or_empty_files_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = install(dir.path(), "a");
        files.ca_path = dir.path().join("absent.pem").display().to_string();
        assert!(ServerTls::load(files.clone()).is_err());
        std::fs::write(dir.path().join("empty.pem"), "").unwrap();
        files.ca_path = dir.path().join("empty.pem").display().to_string();
        assert!(ServerTls::load(files).is_err());
    }

    #[test]
    fn client_common_names_follow_the_allowlist() {
        weed_lance_worker::tls::install_default_crypto_provider();
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(fixture("ca.pem")).unwrap() {
            roots.add(cert.unwrap()).unwrap();
        }
        let inner = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .unwrap();
        let leaf = |name: &str| CertificateDer::from_pem_file(fixture(name)).unwrap();
        let now = UnixTime::now();

        // Without an allowlist any certificate the CA signed is served.
        assert!(inner.verify_client_cert(&leaf("a.pem"), &[], now).is_ok());
        assert!(inner.verify_client_cert(&leaf("b.pem"), &[], now).is_ok());

        let verifier = CommonNameVerifier {
            inner,
            allowed_common_names: ["engine-a".to_string()].into_iter().collect(),
            allowed_wildcard_domain: String::new(),
        };
        assert!(
            verifier
                .verify_client_cert(&leaf("a.pem"), &[], now)
                .is_ok()
        );
        let err = verifier
            .verify_client_cert(&leaf("b.pem"), &[], now)
            .unwrap_err();
        assert!(err.to_string().contains("engine-b"), "{err}");
        assert_eq!(parse_common_name(&leaf("b.pem")).unwrap(), "engine-b");
    }

    #[test]
    fn wildcard_domain_matches_like_go() {
        let none = HashSet::new();
        assert!(common_name_is_allowed("a.internal", &none, ".internal"));
        assert!(common_name_is_allowed("a.internal", &none, "internal"));
        assert!(common_name_is_allowed("*.internal", &none, ".internal"));
        assert!(!common_name_is_allowed("internal", &none, ".internal"));
        assert!(!common_name_is_allowed("xinternal", &none, "internal"));
        assert!(!common_name_is_allowed("10.0.0.1", &none, "0.0.1"));
        assert!(!common_name_is_allowed("a.internal", &none, ""));
        let names: HashSet<String> = ["gateway".to_string()].into_iter().collect();
        assert!(common_name_is_allowed("gateway", &names, ""));
        assert!(!common_name_is_allowed("other", &names, ""));
    }

    #[test]
    fn refresh_interval_reads_go_durations() {
        assert_eq!(parse_go_duration("30m"), Some(Duration::from_secs(1800)));
        assert_eq!(parse_go_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_go_duration("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(parse_go_duration("2.5s"), Some(Duration::from_millis(2500)));
        assert_eq!(parse_go_duration("2d"), None);
        assert_eq!(parse_go_duration(""), None);
    }
}
