use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::aws_lc_rs;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::UnixTime;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::WebPkiClientVerifier;
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
    let cert_chain = read_cert_chain(cert_path)?;
    let private_key = read_private_key(key_path)?;
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

    builder
        .with_single_cert(cert_chain, private_key)
        .map_err(|e| TlsPolicyError(format!("build rustls server config failed: {}", e)))
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

fn build_crypto_provider(policy: &TlsPolicy) -> Result<Arc<CryptoProvider>, TlsPolicyError> {
    let mut provider = aws_lc_rs::default_provider();
    let cipher_suites = parse_cipher_suites(&provider.cipher_suites, &policy.cipher_suites)?;
    if !cipher_suites.is_empty() {
        provider.cipher_suites = cipher_suites;
    }
    Ok(Arc::new(provider))
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
    (!allowed_wildcard_domain.is_empty() && common_name.ends_with(allowed_wildcard_domain))
        || allowed_common_names.contains(common_name)
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
    use super::{build_supported_versions, common_name_is_allowed, parse_cipher_suites, TlsPolicy};
    use rustls::crypto::aws_lc_rs;
    use std::collections::HashSet;

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
        assert!(!common_name_is_allowed(
            "node.prod.other.net",
            &allowed_common_names,
            ".example.com",
        ));
    }
}
