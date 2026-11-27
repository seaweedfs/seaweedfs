// aws-lc-rs and ring both get linked transitively (lance's aws backend pulls
// aws-lc-rs, reqwest's rustls-tls pulls ring), so rustls can't auto-select a
// provider and tonic's client TLS panics on first use. Pin the default to
// aws-lc-rs (filtered to the FIPS lists in FIPS mode). The body lives in
// seaweed-common so this binary and the Rust volume server cannot end up
// installing different providers; re-exported here so callers keep their
// import path.
pub use seaweed_common::tls::{crypto_provider, install_default_crypto_provider, require_ems};

/// Resolves the FIPS mode of a process that reads `security`
/// (`WEED_FIPS_MODE`, else its `fips.mode`, read from the same parsed file
/// as the JWT keys), installs the matching rustls provider as the process
/// default and logs the mode. Call it once at startup, before anything
/// configures TLS.
pub fn install_for_fips_mode(
    security: &crate::security::Security,
) -> anyhow::Result<seaweed_common::fips::FipsMode> {
    let setting = seaweed_common::fips::resolve(
        security.path(),
        security
            .file_setting(seaweed_common::fips::SETTING)
            .as_deref(),
        &|key| std::env::var_os(key),
    )?;
    seaweed_common::tls::install_process_provider(setting.mode)?;
    if setting.mode.is_fips() {
        tracing::info!(
            "{setting}: TLS limited to the FIPS 140-3 versions, suites, groups and signature schemes Go allows; the cryptographic module is not validated"
        );
    } else {
        tracing::info!("{setting}");
    }
    Ok(setting.mode)
}

#[cfg(test)]
mod tests {
    use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

    use super::install_default_crypto_provider;

    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIBPDCB76ADAgECAhRuRPQgeAu43BT/M7EfAWSdapVdYDAFBgMrZXAwFDESMBAG\nA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcwNTE2MTUyOVoXDTM2MDcwMjE2MTUyOVow\nFDESMBAGA1UEAwwJbG9jYWxob3N0MCowBQYDK2VwAyEAr/3bNIFI+8V32oCiY6y+\nXRFmZpdNQ2g//VtRkT+nQg+jUzBRMB0GA1UdDgQWBBTsy9tLf1zPiXCQfgci6zNi\ndEzRSjAfBgNVHSMEGDAWgBTsy9tLf1zPiXCQfgci6zNidEzRSjAPBgNVHRMBAf8E\nBTADAQH/MAUGAytlcANBAIvsdw0IbvOBBkb9cd7BfMJfIP9pQQrAL03pCRWJFnFh\nSysaLVgFXI4T078IiaM874oO+iB+5vNbWEpc7CkGow4=\n-----END CERTIFICATE-----\n";
    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIHbyn71Kk+Y7KT3sBctit7uZpErpoH6qDbFj6P8qGaZH\n-----END PRIVATE KEY-----\n";

    // Without install_default_crypto_provider this panics in the lance crate,
    // where both aws-lc-rs and ring are linked. Mirrors the volume server's
    // test_build_grpc_endpoint_with_tls_resolves_crypto_provider.
    #[tokio::test]
    async fn tls_channel_builds_after_crypto_provider_installed() {
        install_default_crypto_provider();
        let config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(TEST_CERT_PEM))
            .identity(Identity::from_pem(TEST_CERT_PEM, TEST_KEY_PEM));
        let endpoint = Channel::from_shared("https://127.0.0.1:9")
            .unwrap()
            .tls_config(config)
            .unwrap();
        assert_eq!(endpoint.uri().scheme_str(), Some("https"));
    }
}
