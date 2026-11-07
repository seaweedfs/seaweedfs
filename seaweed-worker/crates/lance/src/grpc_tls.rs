//! gRPC TLS the way weed processes do it.
//!
//! Serving: every client must present a certificate chaining to `grpc.ca`,
//! `allowed_commonNames` and `allowed_wildcard_domain` restrict which client
//! identities are served, and the certificate, key and CA files are re-read
//! on a timer and on SIGHUP so rotated files take effect without a restart.
//!
//! Dialing: the server's certificate must chain to `grpc.ca`; it is not
//! checked against the host dialed (weed clients verify the chain only, as
//! `advancedtls.CertVerification`), and `allowed_server_commonNames` /
//! `allowed_server_wildcard_domain` restrict which server identities are
//! accepted. The files are re-read at every dial.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::Stream;
use http::Uri;
use hyper_util::rt::TokioIo;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::verify_server_cert_signed_by_trust_anchor;
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::security::{GrpcServerTls, GrpcTls};

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

/// `time.ParseDuration`, with its arithmetic: an optional `+`, then a
/// sequence of `<number><unit>`, summed in whole nanoseconds (a fraction of
/// one is dropped) up to `i64::MAX`. A negative value is `None`, as weed
/// ignores one.
fn parse_go_duration(s: &str) -> Option<Duration> {
    const LIMIT: u64 = 1 << 63;
    let digits = |s: &str| s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let mut rest = s.strip_prefix('+').unwrap_or(s);
    if rest == "0" {
        return Some(Duration::ZERO);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total: u64 = 0;
    while !rest.is_empty() {
        let (whole, tail) = rest.split_at(digits(rest));
        let mut v: u64 = 0;
        for c in whole.bytes() {
            if v > LIMIT / 10 {
                return None;
            }
            v = v * 10 + u64::from(c - b'0');
            if v > LIMIT {
                return None;
            }
        }
        let (mut fraction, mut scale) = (0u64, 1f64);
        let (fraction_digits, tail) = match tail.strip_prefix('.') {
            Some(tail) => tail.split_at(digits(tail)),
            None => ("", tail),
        };
        let mut overflow = false;
        for c in fraction_digits.bytes() {
            if overflow || fraction > (LIMIT - 1) / 10 {
                overflow = true;
                continue;
            }
            let next = fraction * 10 + u64::from(c - b'0');
            if next > LIMIT {
                overflow = true;
                continue;
            }
            fraction = next;
            scale *= 10.0;
        }
        if whole.is_empty() && fraction_digits.is_empty() {
            return None;
        }
        let unit_len = tail
            .find(|c: char| c == '.' || c.is_ascii_digit())
            .unwrap_or(tail.len());
        let (unit, tail) = tail.split_at(unit_len);
        let unit: u64 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        if v > LIMIT / unit {
            return None;
        }
        v *= unit;
        if fraction > 0 {
            v += (fraction as f64 * (unit as f64 / scale)) as u64;
            if v > LIMIT {
                return None;
            }
        }
        total = total.checked_add(v).filter(|&total| total <= LIMIT)?;
        rest = tail;
    }
    (total < LIMIT).then(|| Duration::from_nanos(total))
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

fn read(key: &str, path: &str) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("{key} = {path:?}"))
}

fn read_all(files: &GrpcServerTls) -> Result<Vec<u8>> {
    let mut all = Vec::new();
    for (key, path) in [
        ("cert", &files.cert_path),
        ("key", &files.key_path),
        ("grpc.ca", &files.ca_path),
    ] {
        all.extend(read(key, path)?);
        all.push(0);
    }
    Ok(all)
}

fn cert_chain(key: &str, path: &str, pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<_, _>>()
        .with_context(|| format!("{key} = {path:?}"))?;
    if chain.is_empty() {
        bail!("{key} = {path:?} holds no certificate");
    }
    Ok(chain)
}

fn private_key(key: &str, path: &str, pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(pem).with_context(|| format!("{key} = {path:?}"))
}

fn root_store(path: &str, pem: &[u8]) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in cert_chain("grpc.ca", path, pem)? {
        roots
            .add(cert)
            .with_context(|| format!("grpc.ca = {path:?}"))?;
    }
    Ok(roots)
}

/// Builds the configuration `security.LoadServerTLS` would: the identity in
/// `cert`/`key`, every client verified against `grpc.ca`, and the client's
/// common name checked against the allowlist when one is set.
fn build_config(files: &GrpcServerTls) -> Result<ServerConfig> {
    let cert_chain = cert_chain("cert", &files.cert_path, &read("cert", &files.cert_path)?)?;
    let key = private_key("key", &files.key_path, &read("key", &files.key_path)?)?;
    let roots = root_store(&files.ca_path, &read("grpc.ca", &files.ca_path)?)?;
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
        crate::tls::install_default_crypto_provider();
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

/// The client configuration `security.LoadClientTLS` would build, from the
/// CA, certificate and key as `pems` holds them.
fn client_config(
    files: &GrpcTls,
    (ca, cert, key): (Vec<u8>, Vec<u8>, Vec<u8>),
) -> Result<ClientConfig> {
    crate::tls::install_default_crypto_provider();
    let verifier = ServerIdentityVerifier {
        roots: Arc::new(root_store(&files.ca_path, &ca)?),
        provider: Arc::new(aws_lc_rs::default_provider()),
        allowed_names: files.allowed_server_names.iter().cloned().collect(),
        allowed_wildcard_domain: files.allowed_server_wildcard_domain.clone(),
    };
    let mut config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_client_auth_cert(
            cert_chain("cert", &files.cert_path, &cert)?,
            private_key("key", &files.key_path, &key)?,
        )
        .context("client certificate")?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

/// The host of a gRPC address, without an IPv6 address's brackets.
fn dialed_host(uri: &Uri) -> Option<&str> {
    Some(uri.host()?.trim_matches(|c| c == '[' || c == ']'))
}

/// A tonic connector for `Endpoint::connect_with_connector`: the files read
/// as they are now, so rotated credentials are picked up, then TCP to the
/// URI's host and port and the handshake under [`client_config`].
pub fn connector(
    files: GrpcTls,
) -> impl tower::Service<
    Uri,
    Response = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>,
    Error = Box<dyn std::error::Error + Send + Sync>,
    Future = impl Send,
> + Clone
+ Send
+ 'static {
    tower::service_fn(move |uri: Uri| {
        let files = files.clone();
        async move {
            let host = dialed_host(&uri).ok_or("no host in gRPC address")?;
            let port = uri.port_u16().unwrap_or(443);
            let config = Arc::new(client_config(&files, files.pems().await?)?);
            let stream = TcpStream::connect((host, port)).await?;
            let _ = stream.set_nodelay(true);
            let name = ServerName::try_from(host.to_string())?;
            let stream = TlsConnector::from(config).connect(name, stream).await?;
            Ok(TokioIo::new(stream))
        }
    })
}

/// `security.Authenticator.AuthenticateServer`: the chain must reach
/// `grpc.ca`, the host dialed is not checked, and when an allowlist is set the
/// common name or a DNS SAN must be allowed or lie under the wildcard domain,
/// while a URI SAN must be allowed exactly.
#[derive(Debug)]
struct ServerIdentityVerifier {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
    allowed_names: HashSet<String>,
    allowed_wildcard_domain: String,
}

impl ServerCertVerifier for ServerIdentityVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        if self.allowed_names.is_empty() && self.allowed_wildcard_domain.is_empty() {
            return Ok(ServerCertVerified::assertion());
        }
        let (names, uris) = server_identities(end_entity)
            .map_err(|e| rustls::Error::General(format!("parse server certificate failed: {e}")))?;
        if names.iter().any(|name| {
            self.allowed_names.contains(name)
                || wildcard_domain_matches(name, &self.allowed_wildcard_domain)
        }) || uris.iter().any(|uri| self.allowed_names.contains(uri))
        {
            return Ok(ServerCertVerified::assertion());
        }
        Err(rustls::Error::General(format!(
            "AuthenticateServer: untrusted server identity {:?}",
            names.into_iter().chain(uris).collect::<Vec<_>>()
        )))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// The common name and DNS SANs, and the URI SANs, of a certificate.
fn server_identities(cert: &CertificateDer<'_>) -> Result<(Vec<String>, Vec<String>)> {
    let (_, certificate) = X509Certificate::from_der(cert.as_ref())
        .map_err(|e| anyhow::anyhow!("parse X.509 certificate failed: {e}"))?;
    let mut names = vec![
        certificate
            .subject()
            .iter_common_name()
            .next()
            .and_then(|common_name| common_name.as_str().ok())
            .unwrap_or_default()
            .to_string(),
    ];
    let mut uris = Vec::new();
    if let Ok(Some(san)) = certificate.subject_alternative_name() {
        for name in &san.value.general_names {
            match name {
                GeneralName::DNSName(dns) => names.push(dns.to_string()),
                GeneralName::URI(uri) => uris.push(uri.to_string()),
                _ => {}
            }
        }
    }
    Ok((names, uris))
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
        crate::tls::install_default_crypto_provider();
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
        crate::tls::install_default_crypto_provider();
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
    fn server_identities_follow_the_allowlist() {
        crate::tls::install_default_crypto_provider();
        let leaf = |name: &str| CertificateDer::from_pem_file(fixture(name)).unwrap();
        let verifier = |allowed: &[&str], wildcard: &str| ServerIdentityVerifier {
            roots: Arc::new(
                root_store("ca.pem", &std::fs::read(fixture("ca.pem")).unwrap()).unwrap(),
            ),
            provider: Arc::new(aws_lc_rs::default_provider()),
            allowed_names: allowed.iter().map(|s| s.to_string()).collect(),
            allowed_wildcard_domain: wildcard.into(),
        };
        let name = ServerName::try_from("nothing.example").unwrap();
        let check = |v: &ServerIdentityVerifier, cert: &str| {
            v.verify_server_cert(&leaf(cert), &[], &name, &[], UnixTime::now())
        };
        // The chain is checked; the host dialed is not.
        assert!(check(&verifier(&[], ""), "a.pem").is_ok());
        assert!(check(&verifier(&[], ""), "b.pem").is_ok());
        assert!(
            check(&verifier(&[], ""), "ca.pem").is_err(),
            "a root is not a server"
        );
        // The common name or a DNS SAN must be allowed.
        assert!(check(&verifier(&["engine-a"], ""), "a.pem").is_ok());
        assert!(check(&verifier(&["localhost"], ""), "b.pem").is_ok());
        let err = check(&verifier(&["engine-a"], ""), "b.pem").unwrap_err();
        assert!(err.to_string().contains("engine-b"), "{err}");
        assert!(check(&verifier(&[], ".example"), "b.pem").is_err());
        assert_eq!(
            server_identities(&leaf("a.pem")).unwrap(),
            (
                vec!["engine-a".to_string(), "localhost".to_string()],
                vec![]
            )
        );
    }

    #[test]
    fn the_dialed_host_drops_ipv6_brackets() {
        for (addr, host) in [
            ("http://m1:19333", "m1"),
            ("http://master.example", "master.example"),
            ("http://127.0.0.1:19333", "127.0.0.1"),
            ("http://[::1]:19333", "::1"),
        ] {
            let uri: Uri = addr.parse().unwrap();
            assert_eq!(dialed_host(&uri), Some(host), "{addr}");
        }
    }

    fn client(leaf: &str, allowed: &[&str]) -> GrpcTls {
        GrpcTls {
            ca_path: fixture("ca.pem").display().to_string(),
            cert_path: fixture(&format!("{leaf}.pem")).display().to_string(),
            key_path: fixture(&format!("{leaf}.key")).display().to_string(),
            allowed_server_names: allowed.iter().map(|s| s.to_string()).collect(),
            allowed_server_wildcard_domain: String::new(),
        }
    }

    async fn handshake(port: u16, files: &GrpcTls) -> Result<()> {
        let config = Arc::new(client_config(files, files.pems().await?)?);
        let stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let name = ServerName::try_from("127.0.0.1").unwrap();
        let mut stream = TlsConnector::from(config).connect(name, stream).await?;
        // A server that rejected the client certificate says so after the
        // handshake, on the first read.
        let mut byte = [0u8; 1];
        match tokio::time::timeout(
            Duration::from_secs(1),
            tokio::io::AsyncReadExt::read(&mut stream, &mut byte),
        )
        .await
        {
            Ok(Err(err)) => Err(err.into()),
            _ => Ok(()),
        }
    }

    #[tokio::test]
    async fn both_sides_check_identities() {
        use futures::StreamExt;
        let dir = tempfile::tempdir().unwrap();
        let mut files = install(dir.path(), "a");
        files.allowed_common_names = vec!["engine-b".into()];
        let tls = ServerTls::load(files).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut incoming = Box::pin(tls.incoming(listener));

        // engine-b, trusting the CA, accepts engine-a by name, dialed by IP.
        let allowed = client("b", &["engine-a"]);
        let (served, dialed) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), incoming.next()),
            handshake(port, &allowed)
        );
        assert!(dialed.is_ok(), "{dialed:?}");
        assert!(matches!(served, Ok(Some(Ok(_)))));

        // The client refuses a server outside its allowlist.
        let err = handshake(port, &client("b", &["engine-z"]))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("untrusted server identity"),
            "{err}"
        );

        // The server refuses a client outside its allowlist: engine-a is not
        // engine-b.
        let outsider = client("a", &[]);
        let (served, dialed) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(1), incoming.next()),
            handshake(port, &outsider)
        );
        assert!(
            dialed.is_err(),
            "the server must cut off a client it does not allow"
        );
        assert!(served.is_err(), "nothing is served for a refused client");
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
        assert_eq!(parse_go_duration("+2s"), Some(Duration::from_secs(2)));
        assert_eq!(
            parse_go_duration("1500us"),
            Some(Duration::from_micros(1500))
        );
        assert_eq!(
            parse_go_duration("3\u{b5}s"),
            Some(Duration::from_micros(3))
        );
        assert_eq!(
            parse_go_duration("3\u{3bc}s"),
            Some(Duration::from_micros(3))
        );
        assert_eq!(parse_go_duration("1s20ns"), Some(Duration::new(1, 20)));
        assert_eq!(parse_go_duration("-5m"), None);
        assert_eq!(parse_go_duration("99999999999999999999h"), None);
        assert_eq!(
            parse_go_duration("4000000000000000h4000000000000000h"),
            None
        );
        assert_eq!(parse_go_duration("2d"), None);
        assert_eq!(parse_go_duration(""), None);
        assert_eq!(parse_go_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_go_duration("0.3s"), Some(Duration::from_millis(300)));
        assert_eq!(parse_go_duration("1.1s"), Some(Duration::from_millis(1100)));
        assert_eq!(parse_go_duration(".5h"), Some(Duration::from_secs(1800)));
        assert_eq!(parse_go_duration("5.s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_go_duration(".s"), None);
        assert_eq!(parse_go_duration("1.2.3s"), None);
        assert_eq!(parse_go_duration(" 30m"), None);
        // A fraction of a nanosecond is dropped, so this is zero and the
        // default applies, as in weed.
        assert_eq!(parse_go_duration("0.9ns"), Some(Duration::ZERO));
        assert_eq!(parse_go_duration("1.9ns"), Some(Duration::from_nanos(1)));
        // The longest time.Duration, and past it.
        assert_eq!(
            parse_go_duration("2562047h47m16.854775807s"),
            Some(Duration::from_nanos(i64::MAX as u64))
        );
        assert_eq!(
            parse_go_duration("9223372036854775807ns"),
            Some(Duration::from_nanos(i64::MAX as u64))
        );
        assert_eq!(parse_go_duration("9223372036854775808ns"), None);
        assert_eq!(parse_go_duration("2562048h"), None);
        assert_eq!(parse_go_duration("3000000h"), None);
    }
}
