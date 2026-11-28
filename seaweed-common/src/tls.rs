//! The process-wide rustls crypto provider.
//!
//! Both binaries link aws-lc-rs and ring transitively — in the volume server
//! through the AWS SDK and reqwest, in the lance worker through lance's `aws`
//! backend and reqwest — so rustls cannot auto-select a provider and tonic's
//! client TLS panics on first use. Each binary has to pin one, and it has to be
//! the same one, which is why the choice lives here rather than in either tree.
//!
//! Under [`FipsMode::On`] and [`FipsMode::Strict`] the provider is aws-lc-rs
//! filtered to what Go's `crypto/tls` allows when `GODEBUG=fips140` is `on` or
//! `only` (`crypto/tls/defaults_fips140.go`, Go 1.27.4, the version go.mod
//! requires). TLS 1.2 and 1.3 are all rustls implements, which is Go's FIPS
//! version list.

use std::sync::{Arc, OnceLock};

use rustls::crypto::{
    CryptoProvider, KeyProvider, SupportedKxGroup, WebPkiSupportedAlgorithms, aws_lc_rs,
};
use rustls::pki_types::{PrivateKeyDer, SubjectPublicKeyInfoDer, alg_id};
use rustls::sign::{Signer, SigningKey};
use rustls::{CipherSuite, NamedGroup, SignatureAlgorithm, SignatureScheme};

use crate::fips::{self, FipsMode, FipsModeError};

/// Go's `allowedCipherSuitesTLS13FIPS` and the AES-GCM part of
/// `allowedCipherSuitesFIPS`. Go also allows
/// `TLS_ECDHE_{ECDSA,RSA}_WITH_AES_128_CBC_SHA256`, which rustls does not
/// implement.
pub const FIPS_CIPHER_SUITES: &[CipherSuite] = &[
    CipherSuite::TLS13_AES_128_GCM_SHA256,
    CipherSuite::TLS13_AES_256_GCM_SHA384,
    CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
];

/// Go's `allowedCurvePreferencesFIPS` as rustls 0.23 has them and Go enables
/// by default, in Go's preference order. rustls has no SecP384r1MLKEM1024 and
/// no P-521; it has MLKEM1024, which Go allows but enables only when
/// configured, so it is left out as Go leaves it out of its defaults.
fn fips_kx_groups() -> Vec<&'static dyn SupportedKxGroup> {
    vec![
        aws_lc_rs::kx_group::X25519MLKEM768,
        aws_lc_rs::kx_group::SECP256R1MLKEM768,
        aws_lc_rs::kx_group::SECP256R1,
        aws_lc_rs::kx_group::SECP384R1,
    ]
}

/// Go's `allowedSignatureAlgorithmsFIPS`.
pub const FIPS_SIGNATURE_SCHEMES: &[SignatureScheme] = &[
    SignatureScheme::RSA_PSS_SHA256,
    SignatureScheme::ECDSA_NISTP256_SHA256,
    SignatureScheme::ED25519,
    SignatureScheme::ML_DSA_44,
    SignatureScheme::ML_DSA_65,
    SignatureScheme::ML_DSA_87,
    SignatureScheme::RSA_PSS_SHA384,
    SignatureScheme::RSA_PSS_SHA512,
    SignatureScheme::RSA_PKCS1_SHA256,
    SignatureScheme::RSA_PKCS1_SHA384,
    SignatureScheme::RSA_PKCS1_SHA512,
    SignatureScheme::ECDSA_NISTP384_SHA384,
    SignatureScheme::ECDSA_NISTP521_SHA512,
];

/// The certificate key types Go's `isCertificateAllowedFIPS` accepts: RSA,
/// ECDSA on P-256, P-384 or P-521, Ed25519, and ML-DSA (Go allows ML-DSA for
/// its native module only). The RSA algorithms rustls verifies with take keys
/// of 2048 to 8192 bits, so Go's 2048-bit floor holds.
const FIPS_CERTIFICATE_KEYS: &[rustls::pki_types::AlgorithmIdentifier] = &[
    alg_id::RSA_ENCRYPTION,
    alg_id::ECDSA_P256,
    alg_id::ECDSA_P384,
    alg_id::ECDSA_P521,
    alg_id::ED25519,
    alg_id::ML_DSA_44,
    alg_id::ML_DSA_65,
    alg_id::ML_DSA_87,
];

/// The aws-lc-rs verification algorithms limited to Go's FIPS signature
/// schemes and certificate keys. With aws-lc-rs's current set that removes
/// nothing; the filter keeps an algorithm a later rustls adds out of FIPS
/// mode until Go allows it. Built once, since rustls needs `'static` slices.
fn fips_signature_verification_algorithms() -> WebPkiSupportedAlgorithms {
    static ALGORITHMS: OnceLock<WebPkiSupportedAlgorithms> = OnceLock::new();
    *ALGORITHMS.get_or_init(|| {
        let default = aws_lc_rs::default_provider().signature_verification_algorithms;
        let all = default
            .all
            .iter()
            .filter(|alg| FIPS_CERTIFICATE_KEYS.contains(&alg.public_key_alg_id()))
            .copied()
            .collect::<Vec<_>>();
        let mapping = default
            .mapping
            .iter()
            .filter(|(scheme, _)| FIPS_SIGNATURE_SCHEMES.contains(scheme))
            .copied()
            .collect::<Vec<_>>();
        WebPkiSupportedAlgorithms {
            all: all.leak(),
            mapping: mapping.leak(),
        }
    })
}

/// Loads private keys through aws-lc-rs and passes them through
/// [`restrict_to_fips`], so a FIPS-mode listener or client signs handshakes
/// only with Go's FIPS signature schemes.
#[derive(Debug)]
struct FipsKeyProvider;

static FIPS_KEY_PROVIDER: FipsKeyProvider = FipsKeyProvider;

impl KeyProvider for FipsKeyProvider {
    fn load_private_key(
        &self,
        key_der: PrivateKeyDer<'static>,
    ) -> Result<Arc<dyn SigningKey>, rustls::Error> {
        let key = aws_lc_rs::default_provider()
            .key_provider
            .load_private_key(key_der)?;
        restrict_to_fips(key)
    }

    fn fips(&self) -> bool {
        aws_lc_rs::default_provider().key_provider.fips()
    }
}

/// Wraps `key` so it offers only [`FIPS_SIGNATURE_SCHEMES`], refusing a key
/// that can sign none of them, such as Ed448, with an error naming its type.
fn restrict_to_fips(key: Arc<dyn SigningKey>) -> Result<Arc<dyn SigningKey>, rustls::Error> {
    if key.choose_scheme(FIPS_SIGNATURE_SCHEMES).is_none() {
        return Err(rustls::Error::General(format!(
            "{} private key is not allowed in FIPS mode, which accepts RSA, ECDSA P-256/P-384/P-521, Ed25519 and ML-DSA keys",
            key_kind(key.algorithm())
        )));
    }
    Ok(Arc::new(FipsSigningKey(key)))
}

/// How a refused key is named.
fn key_kind(algorithm: SignatureAlgorithm) -> String {
    match algorithm {
        SignatureAlgorithm::ED448 => "an Ed448".to_string(),
        other => format!("a {other:?}"),
    }
}

/// A signing key that offers only [`FIPS_SIGNATURE_SCHEMES`].
#[derive(Debug)]
struct FipsSigningKey(Arc<dyn SigningKey>);

impl SigningKey for FipsSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer>> {
        let allowed = offered
            .iter()
            .copied()
            .filter(|scheme| FIPS_SIGNATURE_SCHEMES.contains(scheme))
            .collect::<Vec<_>>();
        self.0.choose_scheme(&allowed)
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        self.0.public_key()
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        self.0.algorithm()
    }
}

/// The aws-lc-rs provider with Go's FIPS-mode TLS lists applied. It still
/// runs on the non-validated aws-lc-rs build, so `CryptoProvider::fips()` is
/// false.
pub fn fips_provider() -> CryptoProvider {
    let default = aws_lc_rs::default_provider();
    CryptoProvider {
        cipher_suites: default
            .cipher_suites
            .into_iter()
            .filter(|suite| FIPS_CIPHER_SUITES.contains(&suite.suite()))
            .collect(),
        kx_groups: fips_kx_groups(),
        signature_verification_algorithms: fips_signature_verification_algorithms(),
        key_provider: &FIPS_KEY_PROVIDER,
        ..default
    }
}

/// The provider for `mode`: aws-lc-rs's default under [`FipsMode::Off`],
/// [`fips_provider`] otherwise.
pub fn provider_for(mode: FipsMode) -> CryptoProvider {
    if mode.is_fips() {
        fips_provider()
    } else {
        aws_lc_rs::default_provider()
    }
}

/// The provider for this process's mode ([`fips::process_mode`]).
pub fn crypto_provider() -> CryptoProvider {
    provider_for(fips::process_mode())
}

/// Pin rustls's process-wide default provider to [`crypto_provider`]:
/// aws-lc-rs, filtered to the FIPS lists when the process mode asks for it.
/// Idempotent: the first call wins and every later one is a no-op, so callers
/// do not have to coordinate.
pub fn install_default_crypto_provider() {
    let _ = crypto_provider().install_default();
}

/// Whether TLS 1.2 connections must use the Extended Master Secret, as Go
/// requires in FIPS mode. rustls requires it only of a validated provider,
/// so configurations SeaweedFS builds itself set `require_ems` from this.
pub fn require_ems() -> bool {
    fips::process_mode().is_fips()
}

/// Why [`install_process_provider`] failed.
#[derive(Debug)]
pub enum InstallError {
    Mode(FipsModeError),
    /// A provider other than this mode's was installed first.
    OtherProvider(FipsMode),
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mode(e) => e.fmt(f),
            Self::OtherProvider(mode) => write!(
                f,
                "a rustls crypto provider other than the one for FIPS mode {mode} is already installed"
            ),
        }
    }
}

impl std::error::Error for InstallError {}

/// Records `mode` as this process's FIPS mode and installs its provider as the
/// rustls process default. Call it once at startup, before anything builds a
/// TLS configuration; it fails if a different provider got there first, so a
/// FIPS-mode process never runs on the unfiltered one.
pub fn install_process_provider(mode: FipsMode) -> Result<(), InstallError> {
    fips::set_process_mode(mode).map_err(InstallError::Mode)?;
    install_default_crypto_provider();
    let installed = CryptoProvider::get_default().ok_or(InstallError::OtherProvider(mode))?;
    if !same_lists(installed, &provider_for(mode)) {
        return Err(InstallError::OtherProvider(mode));
    }
    Ok(())
}

/// Whether two providers offer the same suites, groups and signature schemes.
fn same_lists(a: &CryptoProvider, b: &CryptoProvider) -> bool {
    let suites = |p: &CryptoProvider| {
        p.cipher_suites
            .iter()
            .map(|s| s.suite())
            .collect::<Vec<_>>()
    };
    let groups = |p: &CryptoProvider| {
        p.kx_groups
            .iter()
            .map(|g| g.name())
            .collect::<Vec<NamedGroup>>()
    };
    suites(a) == suites(b)
        && groups(a) == groups(b)
        && a.signature_verification_algorithms.supported_schemes()
            == b.signature_verification_algorithms.supported_schemes()
        && a.signature_verification_algorithms.all.len()
            == b.signature_verification_algorithms.all.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installing_is_idempotent_and_leaves_a_default_behind() {
        install_default_crypto_provider();
        install_default_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn off_is_the_unfiltered_default() {
        assert!(same_lists(
            &provider_for(FipsMode::Off),
            &aws_lc_rs::default_provider()
        ));
    }

    #[test]
    fn fips_mode_drops_chacha20_and_plain_x25519() {
        for mode in [FipsMode::On, FipsMode::Strict] {
            let provider = provider_for(mode);
            let suites = provider
                .cipher_suites
                .iter()
                .map(|s| s.suite())
                .collect::<Vec<_>>();
            assert_eq!(
                suites,
                vec![
                    CipherSuite::TLS13_AES_256_GCM_SHA384,
                    CipherSuite::TLS13_AES_128_GCM_SHA256,
                    CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                    CipherSuite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
                    CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                    CipherSuite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                ]
            );
            let groups = provider
                .kx_groups
                .iter()
                .map(|g| g.name())
                .collect::<Vec<_>>();
            assert_eq!(
                groups,
                vec![
                    NamedGroup::X25519MLKEM768,
                    NamedGroup::secp256r1MLKEM768,
                    NamedGroup::secp256r1,
                    NamedGroup::secp384r1,
                ]
            );
        }
    }

    fn load(provider: &CryptoProvider, pem: &str) -> Result<Arc<dyn SigningKey>, rustls::Error> {
        use rustls::pki_types::pem::PemObject;
        provider
            .key_provider
            .load_private_key(PrivateKeyDer::from_pem_slice(pem.as_bytes()).unwrap())
    }

    const ML_DSA_44_KEY: &str = include_str!("testdata/mldsa44.pem");
    const ED448_KEY: &str = include_str!("testdata/ed448.pem");
    const P256_KEY: &str = include_str!("testdata/p256.pem");
    const ED25519_KEY: &str = include_str!("testdata/ed25519.pem");

    /// A key that signs only Ed448, which aws-lc-rs cannot load, to drive the
    /// refusal.
    #[derive(Debug)]
    struct Ed448Only;

    impl SigningKey for Ed448Only {
        fn choose_scheme(&self, _offered: &[SignatureScheme]) -> Option<Box<dyn Signer>> {
            None
        }

        fn algorithm(&self) -> SignatureAlgorithm {
            SignatureAlgorithm::ED448
        }
    }

    #[test]
    fn fips_mode_refuses_keys_outside_go_s_list() {
        let err = restrict_to_fips(Arc::new(Ed448Only)).unwrap_err();
        assert!(err.to_string().contains("an Ed448 private key"), "{err}");
        assert!(
            err.to_string().contains("not allowed in FIPS mode"),
            "{err}"
        );
        assert!(load(&provider_for(FipsMode::On), ED448_KEY).is_err());
    }

    #[test]
    fn fips_mode_allows_ml_dsa_as_go_1_27_does() {
        let fips = provider_for(FipsMode::On);
        let ml_dsa = load(&fips, ML_DSA_44_KEY).unwrap();
        assert!(
            ml_dsa
                .choose_scheme(&[SignatureScheme::ML_DSA_44])
                .is_some()
        );
    }

    #[test]
    fn fips_mode_keys_sign_with_fips_schemes_only() {
        let fips = provider_for(FipsMode::Strict);
        let p256 = load(&fips, P256_KEY).unwrap();
        let signer = p256
            .choose_scheme(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .unwrap();
        assert_eq!(signer.scheme(), SignatureScheme::ECDSA_NISTP256_SHA256);
        assert!(
            p256.choose_scheme(&[SignatureScheme::ECDSA_SHA1_Legacy])
                .is_none()
        );
        assert!(p256.public_key().is_some());
        let ed25519 = load(&fips, ED25519_KEY).unwrap();
        assert!(ed25519.choose_scheme(&[SignatureScheme::ED25519]).is_some());
    }

    #[test]
    fn off_keeps_the_default_key_provider() {
        let off = provider_for(FipsMode::Off);
        let ml_dsa = load(&off, ML_DSA_44_KEY).unwrap();
        assert!(
            ml_dsa
                .choose_scheme(&[SignatureScheme::ML_DSA_44])
                .is_some()
        );
    }

    #[test]
    fn fips_mode_verifies_go_s_signature_schemes_only() {
        let provider = fips_provider();
        let mut schemes = provider
            .signature_verification_algorithms
            .supported_schemes();
        let mut want = FIPS_SIGNATURE_SCHEMES.to_vec();
        schemes.sort_by_key(|s| u16::from(*s));
        want.sort_by_key(|s| u16::from(*s));
        assert_eq!(schemes, want);
        assert!(
            provider
                .signature_verification_algorithms
                .all
                .iter()
                .all(|alg| FIPS_CERTIFICATE_KEYS.contains(&alg.public_key_alg_id()))
        );
        let default = aws_lc_rs::default_provider();
        assert_eq!(
            default.signature_verification_algorithms.all.len(),
            provider.signature_verification_algorithms.all.len(),
            "Go 1.27 allows every key type aws-lc-rs verifies"
        );
    }
}
