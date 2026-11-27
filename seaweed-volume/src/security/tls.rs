use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock, Weak};

use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::UnixTime;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{
    CipherSuite, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig,
    SignatureScheme, SupportedCipherSuite, SupportedProtocolVersion,
};
use x509_parser::prelude::{FromDer, X509Certificate};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsPolicy {
    pub min_version: String,
    pub max_version: String,
    pub cipher_suites: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GrpcClientAuthPolicy {
    pub allowed_common_names: Vec<String>,
    pub allowed_wildcard_domain: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPolicyError(String);

impl fmt::Display for TlsPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsPolicyError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GoTlsVersion {
    Ssl3,
    Tls10,
    Tls11,
    Tls12,
    Tls13,
}

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
            rustls::Error::General(format!(
                "parse client certificate common name failed: {}",
                e
            ))
        })?;
        if common_name_is_allowed(
            &common_name,
            &self.allowed_common_names,
            &self.allowed_wildcard_domain,
        ) {
            return Ok(ClientCertVerified::assertion());
        }
        Err(rustls::Error::General(format!(
            "Authenticate: invalid subject client common name: {}",
            common_name
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

// aws-lc-rs and ring both get linked transitively, so rustls can't auto-select
// a provider and tonic's client TLS panics on first use. Pin the default to
// aws-lc-rs, matching the server config (filtered to the FIPS lists in FIPS
// mode). Idempotent. The body lives in seaweed-common so this binary and the
// Rust plugin workers cannot end up installing different providers;
// re-exported here so callers keep their import path.
pub use seaweed_common::tls::install_default_crypto_provider;

pub fn build_rustls_server_config(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
    policy: &TlsPolicy,
) -> Result<ServerConfig, TlsPolicyError> {
    build_rustls_server_config_with_client_auth(cert_path, key_path, ca_path, policy, None)
}

pub fn build_rustls_server_config_with_grpc_client_auth(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
    policy: &TlsPolicy,
    client_auth_policy: &GrpcClientAuthPolicy,
) -> Result<ServerConfig, TlsPolicyError> {
    build_rustls_server_config_with_client_auth(
        cert_path,
        key_path,
        ca_path,
        policy,
        Some(client_auth_policy),
    )
}

fn build_rustls_server_config_with_client_auth(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
    policy: &TlsPolicy,
    client_auth_policy: Option<&GrpcClientAuthPolicy>,
) -> Result<ServerConfig, TlsPolicyError> {
    let provider = build_crypto_provider(policy)?;
    let versions = build_supported_versions(policy)?;

    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions)
        .map_err(|e| TlsPolicyError(format!("invalid TLS version policy: {}", e)))?;

    let builder = if ca_path.is_empty() {
        builder.with_no_client_auth()
    } else {
        let roots = read_root_store(ca_path)?;
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                .build()
                .map_err(|e| TlsPolicyError(format!("build client verifier failed: {}", e)))?;
        let verifier: Arc<dyn ClientCertVerifier> = if let Some(client_auth_policy) =
            client_auth_policy.filter(|policy| {
                !policy.allowed_common_names.is_empty()
                    || !policy.allowed_wildcard_domain.is_empty()
            }) {
            Arc::new(CommonNameVerifier {
                inner: verifier,
                allowed_common_names: client_auth_policy
                    .allowed_common_names
                    .iter()
                    .cloned()
                    .collect(),
                allowed_wildcard_domain: client_auth_policy.allowed_wildcard_domain.clone(),
            })
        } else {
            verifier
        };
        builder.with_client_cert_verifier(verifier)
    };

    let cert = ReloadableCert::new(cert_path, key_path, provider)?;
    let mut config = builder.with_cert_resolver(cert);
    config.require_ems = config.require_ems || seaweed_common::tls::require_ems();
    Ok(config)
}

/// Serves the cert/key pair read from disk and re-reads it on `reload_all`
/// (SIGHUP), mirroring Go's certreload provider.
#[derive(Debug)]
struct ReloadableCert {
    cert_path: String,
    key_path: String,
    provider: Arc<CryptoProvider>,
    current: RwLock<Arc<CertifiedKey>>,
}

static RELOADABLE_CERTS: Mutex<Vec<Weak<ReloadableCert>>> = Mutex::new(Vec::new());

impl ReloadableCert {
    fn new(
        cert_path: &str,
        key_path: &str,
        provider: Arc<CryptoProvider>,
    ) -> Result<Arc<Self>, TlsPolicyError> {
        let current = load_certified_key(cert_path, key_path, &provider)?;
        let cert = Arc::new(Self {
            cert_path: cert_path.to_string(),
            key_path: key_path.to_string(),
            provider,
            current: RwLock::new(Arc::new(current)),
        });
        let mut certs = RELOADABLE_CERTS.lock().unwrap();
        certs.retain(|c| c.strong_count() > 0);
        certs.push(Arc::downgrade(&cert));
        Ok(cert)
    }

    fn reload(&self) -> Result<(), TlsPolicyError> {
        let next = load_certified_key(&self.cert_path, &self.key_path, &self.provider)?;
        *self.current.write().unwrap() = Arc::new(next);
        Ok(())
    }
}

impl ResolvesServerCert for ReloadableCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current.read().unwrap().clone())
    }
}

fn load_certified_key(
    cert_path: &str,
    key_path: &str,
    provider: &CryptoProvider,
) -> Result<CertifiedKey, TlsPolicyError> {
    let cert_chain = read_cert_chain(cert_path)?;
    let private_key = read_private_key(key_path)?;
    CertifiedKey::from_der(cert_chain, private_key, provider).map_err(|e| {
        TlsPolicyError(format!(
            "build rustls server config from '{}' and '{}' failed: {}",
            cert_path, key_path, e
        ))
    })
}

/// Re-reads the cert/key files of every live TLS server config. A pair that
/// fails to load keeps serving the previous one.
pub fn reload_all() {
    let certs: Vec<Arc<ReloadableCert>> = {
        let mut certs = RELOADABLE_CERTS.lock().unwrap();
        certs.retain(|c| c.strong_count() > 0);
        certs.iter().filter_map(Weak::upgrade).collect()
    };
    for cert in certs {
        match cert.reload() {
            Ok(()) => tracing::info!("reloaded TLS cert {}", cert.cert_path),
            Err(e) => tracing::warn!("keeping TLS cert {}: {}", cert.cert_path, e),
        }
    }
}

fn read_cert_chain(cert_path: &str) -> Result<Vec<CertificateDer<'static>>, TlsPolicyError> {
    let cert_pem = std::fs::read(cert_path).map_err(|e| {
        TlsPolicyError(format!(
            "Failed to read TLS cert file '{}': {}",
            cert_path, e
        ))
    })?;
    rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            TlsPolicyError(format!(
                "Failed to parse TLS cert PEM '{}': {}",
                cert_path, e
            ))
        })
}

fn read_private_key(key_path: &str) -> Result<PrivateKeyDer<'static>, TlsPolicyError> {
    let key_pem = std::fs::read(key_path).map_err(|e| {
        TlsPolicyError(format!("Failed to read TLS key file '{}': {}", key_path, e))
    })?;
    rustls_pemfile::private_key(&mut &key_pem[..])
        .map_err(|e| TlsPolicyError(format!("Failed to parse TLS key PEM '{}': {}", key_path, e)))?
        .ok_or_else(|| TlsPolicyError(format!("No private key found in '{}'", key_path)))
}

fn read_root_store(ca_path: &str) -> Result<RootCertStore, TlsPolicyError> {
    let ca_pem = std::fs::read(ca_path)
        .map_err(|e| TlsPolicyError(format!("Failed to read TLS CA file '{}': {}", ca_path, e)))?;
    let ca_certs = rustls_pemfile::certs(&mut &ca_pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsPolicyError(format!("Failed to parse TLS CA PEM '{}': {}", ca_path, e)))?;
    let mut roots = RootCertStore::empty();
    for cert in ca_certs {
        roots
            .add(cert)
            .map_err(|e| TlsPolicyError(format!("Failed to add CA cert '{}': {}", ca_path, e)))?;
    }
    Ok(roots)
}

/// The process's provider (see `seaweed_common::tls::crypto_provider`) with
/// `tls.cipher_suites` applied. In FIPS mode a configured suite outside the
/// FIPS list is an error, so the setting cannot widen what the mode allows.
fn build_crypto_provider(policy: &TlsPolicy) -> Result<Arc<CryptoProvider>, TlsPolicyError> {
    build_crypto_provider_for(seaweed_common::fips::process_mode(), policy)
}

fn build_crypto_provider_for(
    mode: seaweed_common::fips::FipsMode,
    policy: &TlsPolicy,
) -> Result<Arc<CryptoProvider>, TlsPolicyError> {
    let mut provider = seaweed_common::tls::provider_for(mode);
    if mode.is_fips() {
        refuse_non_fips_cipher_suites(mode, &policy.cipher_suites)?;
    }
    let cipher_suites = parse_cipher_suites(&provider.cipher_suites, &policy.cipher_suites)?;
    if !cipher_suites.is_empty() {
        provider.cipher_suites = cipher_suites;
    }
    Ok(Arc::new(provider))
}

/// Errors naming the first suite in `value` (`tls.cipher_suites`) that rustls
/// implements but FIPS mode does not allow, such as ChaCha20-Poly1305.
fn refuse_non_fips_cipher_suites(
    mode: seaweed_common::fips::FipsMode,
    value: &str,
) -> Result<(), TlsPolicyError> {
    for name in value.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        if let Ok(suite) = parse_cipher_suite_name(name)
            && !seaweed_common::tls::FIPS_CIPHER_SUITES.contains(&suite)
        {
            return Err(TlsPolicyError(format!(
                "tls.cipher_suites names '{}', which FIPS mode {} does not allow",
                name, mode
            )));
        }
    }
    Ok(())
}

pub fn build_supported_versions(
    policy: &TlsPolicy,
) -> Result<Vec<&'static SupportedProtocolVersion>, TlsPolicyError> {
    let min_version = parse_go_tls_version(&policy.min_version)?;
    let max_version = parse_go_tls_version(&policy.max_version)?;
    let versions = [&rustls::version::TLS13, &rustls::version::TLS12]
        .into_iter()
        .filter(|version| {
            let current = go_tls_version_for_supported(version);
            min_version.map(|min| current >= min).unwrap_or(true)
                && max_version.map(|max| current <= max).unwrap_or(true)
        })
        .collect::<Vec<_>>();

    if versions.is_empty() {
        return Err(TlsPolicyError(format!(
            "TLS version range min='{}' max='{}' is unsupported by rustls",
            policy.min_version, policy.max_version
        )));
    }

    Ok(versions)
}

fn parse_go_tls_version(value: &str) -> Result<Option<GoTlsVersion>, TlsPolicyError> {
    match value.trim() {
        "" => Ok(None),
        "SSLv3" => Ok(Some(GoTlsVersion::Ssl3)),
        "TLS 1.0" => Ok(Some(GoTlsVersion::Tls10)),
        "TLS 1.1" => Ok(Some(GoTlsVersion::Tls11)),
        "TLS 1.2" => Ok(Some(GoTlsVersion::Tls12)),
        "TLS 1.3" => Ok(Some(GoTlsVersion::Tls13)),
        other => Err(TlsPolicyError(format!("invalid TLS version {}", other))),
    }
}

fn parse_cipher_suites(
    available: &[SupportedCipherSuite],
    value: &str,
) -> Result<Vec<SupportedCipherSuite>, TlsPolicyError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    trimmed
        .split(',')
        .map(|name| {
            let suite = parse_cipher_suite_name(name.trim())?;
            available
                .iter()
                .copied()
                .find(|candidate| candidate.suite() == suite)
                .ok_or_else(|| {
                    TlsPolicyError(format!(
                        "TLS cipher suite '{}' is unsupported by the Rust implementation",
                        name.trim()
                    ))
                })
        })
        .collect()
}

fn parse_cipher_suite_name(value: &str) -> Result<CipherSuite, TlsPolicyError> {
    match value {
        "TLS_AES_128_GCM_SHA256" => Ok(CipherSuite::TLS13_AES_128_GCM_SHA256),
        "TLS_AES_256_GCM_SHA384" => Ok(CipherSuite::TLS13_AES_256_GCM_SHA384),
        "TLS_CHACHA20_POLY1305_SHA256" => Ok(CipherSuite::TLS13_CHACHA20_POLY1305_SHA256),
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => {
            Ok(CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
        }
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => {
            Ok(CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384)
        }
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => {
            Ok(CipherSuite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256)
        }
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => {
            Ok(CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256)
        }
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => {
            Ok(CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
        }
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => {
            Ok(CipherSuite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256)
        }
        other => Err(TlsPolicyError(format!(
            "TLS cipher suite '{}' is unsupported by the Rust implementation",
            other
        ))),
    }
}

fn parse_common_name(cert: &CertificateDer<'_>) -> Result<String, TlsPolicyError> {
    let (_, certificate) = X509Certificate::from_der(cert.as_ref())
        .map_err(|e| TlsPolicyError(format!("parse X.509 certificate failed: {}", e)))?;
    let common_name = certificate
        .subject()
        .iter_common_name()
        .next()
        .and_then(|common_name| common_name.as_str().ok())
        .map(str::to_string);
    match common_name {
        Some(common_name) => Ok(common_name),
        None => Ok(String::new()),
    }
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

fn go_tls_version_for_supported(version: &SupportedProtocolVersion) -> GoTlsVersion {
    match version.version {
        rustls::ProtocolVersion::TLSv1_2 => GoTlsVersion::Tls12,
        rustls::ProtocolVersion::TLSv1_3 => GoTlsVersion::Tls13,
        _ => unreachable!("rustls only exposes TLS 1.2 and 1.3"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ReloadableCert, TlsPolicy, build_crypto_provider_for, build_rustls_server_config,
        build_supported_versions, common_name_is_allowed, parse_cipher_suites, reload_all,
    };
    use rustls::crypto::aws_lc_rs;
    use seaweed_common::fips::FipsMode;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn test_build_supported_versions_defaults_to_tls12_and_tls13() {
        let versions = build_supported_versions(&TlsPolicy::default()).unwrap();
        assert_eq!(
            versions,
            vec![&rustls::version::TLS13, &rustls::version::TLS12]
        );
    }

    #[test]
    fn test_build_supported_versions_filters_to_tls13() {
        let versions = build_supported_versions(&TlsPolicy {
            min_version: "TLS 1.3".to_string(),
            max_version: "TLS 1.3".to_string(),
            cipher_suites: String::new(),
        })
        .unwrap();
        assert_eq!(versions, vec![&rustls::version::TLS13]);
    }

    #[test]
    fn test_build_supported_versions_rejects_unsupported_legacy_range() {
        let err = build_supported_versions(&TlsPolicy {
            min_version: "TLS 1.0".to_string(),
            max_version: "TLS 1.1".to_string(),
            cipher_suites: String::new(),
        })
        .unwrap_err();
        assert!(err.to_string().contains("unsupported by rustls"));
    }

    #[test]
    fn test_parse_cipher_suites_accepts_go_names() {
        let cipher_suites = parse_cipher_suites(
            &aws_lc_rs::default_provider().cipher_suites,
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_AES_128_GCM_SHA256",
        )
        .unwrap();
        assert_eq!(cipher_suites.len(), 2);
    }

    #[test]
    fn test_fips_mode_refuses_a_configured_chacha20_suite() {
        let policy = |suites: &str| TlsPolicy {
            cipher_suites: suites.to_string(),
            ..TlsPolicy::default()
        };
        let err = build_crypto_provider_for(
            FipsMode::On,
            &policy("TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256"),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "tls.cipher_suites names 'TLS_CHACHA20_POLY1305_SHA256', which FIPS mode on does not allow"
        );
        let provider = build_crypto_provider_for(
            FipsMode::Strict,
            &policy("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_AES_128_GCM_SHA256"),
        )
        .unwrap();
        assert_eq!(provider.cipher_suites.len(), 2);
        let provider =
            build_crypto_provider_for(FipsMode::Off, &policy("TLS_CHACHA20_POLY1305_SHA256"))
                .unwrap();
        assert_eq!(provider.cipher_suites.len(), 1);
    }

    #[test]
    fn test_fips_mode_provider_drops_chacha20_and_x25519() {
        let provider = build_crypto_provider_for(FipsMode::On, &TlsPolicy::default()).unwrap();
        assert!(
            provider
                .cipher_suites
                .iter()
                .all(|s| s.suite() != rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
        );
        assert!(
            provider
                .kx_groups
                .iter()
                .all(|g| g.name() != rustls::NamedGroup::X25519)
        );
        let default = build_crypto_provider_for(FipsMode::Off, &TlsPolicy::default()).unwrap();
        assert_eq!(
            default.cipher_suites.len(),
            aws_lc_rs::default_provider().cipher_suites.len()
        );
    }

    #[test]
    fn test_common_name_is_allowed_matches_exact_and_wildcard() {
        let allowed_common_names =
            HashSet::from([String::from("volume-a.internal"), String::from("worker-7")]);
        assert!(common_name_is_allowed(
            "volume-a.internal",
            &allowed_common_names,
            "",
        ));
        assert!(common_name_is_allowed(
            "node.prod.example.com",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(common_name_is_allowed(
            "node.example.com.",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(common_name_is_allowed(
            "node.example.com.",
            &allowed_common_names,
            ".example.com.",
        ));
        assert!(!common_name_is_allowed(
            "node.example.com..",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(!common_name_is_allowed(
            "node..example.com.",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(!common_name_is_allowed(
            "node.prod.other.net",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(!common_name_is_allowed(
            ".example.com",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(!common_name_is_allowed(
            "node..example.com",
            &allowed_common_names,
            ".example.com",
        ));
        assert!(!common_name_is_allowed(
            "badexample.com",
            &allowed_common_names,
            "example.com",
        ));
        assert!(common_name_is_allowed(
            "*.example.com",
            &allowed_common_names,
            ".example.com",
        ));
    }

    // Two unrelated self-signed localhost pairs used to rotate the files.
    const FIRST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBizCCATGgAwIBAgIUauDdiWDFpHLXr8Xtf8s4d4f394kwCgYIKoZIzj0EAwIw\nEDEOMAwGA1UEAwwFZmlyc3QwHhcNMjYxMTI3MjEzMDExWhcNMzYxMTI0MjEzMDEx\nWjAQMQ4wDAYDVQQDDAVmaXJzdDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABNH8\nYX6HlRaup0oiSFzhG2Vei3ffH8gX3JLYsa7GkwebmplCYvTEQIE/2lRyBy2t10k4\n9LCQl/5UXqPSGtxClsWjaTBnMB0GA1UdDgQWBBQka2xHibxVJIsJUgcjCzX08XB/\nsDAfBgNVHSMEGDAWgBQka2xHibxVJIsJUgcjCzX08XB/sDAPBgNVHRMBAf8EBTAD\nAQH/MBQGA1UdEQQNMAuCCWxvY2FsaG9zdDAKBggqhkjOPQQDAgNIADBFAiEAo5fC\nW8S7MfBHy+pa0T6eWzx3bkJx/p8EazaH2QL0bPoCIHmCoQocMMjGHWC4VvvxazeJ\nQjV6OINpIcehqJIeHEk7\n-----END CERTIFICATE-----\n";
    const FIRST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgBKOApzLisI0tXK5d\nYxiPB4BJhegPo67l3teNjLzJKIShRANCAATR/GF+h5UWrqdKIkhc4RtlXot33x/I\nF9yS2LGuxpMHm5qZQmL0xECBP9pUcgctrddJOPSwkJf+VF6j0hrcQpbF\n-----END PRIVATE KEY-----\n";
    const SECOND_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBjjCCATOgAwIBAgIUKaOd+9Eypm1Iomu9vkP70b/92bswCgYIKoZIzj0EAwIw\nETEPMA0GA1UEAwwGc2Vjb25kMB4XDTI2MTEyNzIxMzAxMVoXDTM2MTEyNDIxMzAx\nMVowETEPMA0GA1UEAwwGc2Vjb25kMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE\nqbQV86SqTM3nyKlr5jFjhZ0hjZuMemeXomV7OPcZSVovXCfHlZsuaQAXzaOZNtrd\nU2FbpFxcGXVYw+K2f1NoDaNpMGcwHQYDVR0OBBYEFDMw3Xj86fk1HYTSk4VQBQ3N\nhwkkMB8GA1UdIwQYMBaAFDMw3Xj86fk1HYTSk4VQBQ3NhwkkMA8GA1UdEwEB/wQF\nMAMBAf8wFAYDVR0RBA0wC4IJbG9jYWxob3N0MAoGCCqGSM49BAMCA0kAMEYCIQCO\nucyTGgXImXfmbDF404yiBExWDoiLdgecAeN65DtZwgIhANqJtBqN5KL/FAfdKtit\nPaDLh5HHPhJNbXinIz/7a6Nf\n-----END CERTIFICATE-----\n";
    const SECOND_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgTqvWMTK2wl1pB63u\nnJoiarARgNRRoTcULqrG8OkrvTahRANCAASptBXzpKpMzefIqWvmMWOFnSGNm4x6\nZ5eiZXs49xlJWi9cJ8eVmy5pABfNo5k22t1TYVukXFwZdVjD4rZ/U2gN\n-----END PRIVATE KEY-----\n";

    fn write_pair(dir: &tempfile::TempDir, cert: &str, key: &str) -> (String, String) {
        let cert_path = dir.path().join("tls.crt");
        let key_path = dir.path().join("tls.key");
        std::fs::write(&cert_path, cert).unwrap();
        std::fs::write(&key_path, key).unwrap();
        (
            cert_path.to_string_lossy().into_owned(),
            key_path.to_string_lossy().into_owned(),
        )
    }

    fn leaf_der(pem: &str) -> Vec<u8> {
        rustls_pemfile::certs(&mut pem.as_bytes())
            .next()
            .unwrap()
            .unwrap()
            .to_vec()
    }

    fn served_leaf(cert: &ReloadableCert) -> Vec<u8> {
        cert.current.read().unwrap().cert[0].to_vec()
    }

    #[test]
    fn test_reload_all_serves_the_rotated_pair() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_pair(&dir, FIRST_CERT_PEM, FIRST_KEY_PEM);
        let cert = ReloadableCert::new(
            &cert_path,
            &key_path,
            Arc::new(aws_lc_rs::default_provider()),
        )
        .unwrap();
        assert_eq!(served_leaf(&cert), leaf_der(FIRST_CERT_PEM));

        write_pair(&dir, SECOND_CERT_PEM, SECOND_KEY_PEM);
        reload_all();
        assert_eq!(served_leaf(&cert), leaf_der(SECOND_CERT_PEM));
    }

    #[test]
    fn test_reload_keeps_the_previous_pair_when_the_files_are_bad() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_pair(&dir, FIRST_CERT_PEM, FIRST_KEY_PEM);
        let cert = ReloadableCert::new(
            &cert_path,
            &key_path,
            Arc::new(aws_lc_rs::default_provider()),
        )
        .unwrap();

        write_pair(&dir, SECOND_CERT_PEM, FIRST_KEY_PEM);
        assert!(cert.reload().is_err(), "a key that does not match the cert");
        assert_eq!(served_leaf(&cert), leaf_der(FIRST_CERT_PEM));

        write_pair(&dir, "not a certificate", FIRST_KEY_PEM);
        assert!(cert.reload().is_err(), "a cert file without a certificate");
        assert_eq!(served_leaf(&cert), leaf_der(FIRST_CERT_PEM));
    }

    #[test]
    fn test_build_rustls_server_config_rejects_a_mismatched_pair() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_pair(&dir, SECOND_CERT_PEM, FIRST_KEY_PEM);
        assert!(
            build_rustls_server_config(&cert_path, &key_path, "", &TlsPolicy::default()).is_err()
        );
        write_pair(&dir, SECOND_CERT_PEM, SECOND_KEY_PEM);
        assert!(
            build_rustls_server_config(&cert_path, &key_path, "", &TlsPolicy::default()).is_ok()
        );
    }
}
