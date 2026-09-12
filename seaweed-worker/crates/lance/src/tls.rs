use rustls::crypto::aws_lc_rs;

// aws-lc-rs and ring both get linked transitively (lance's aws backend pulls
// aws-lc-rs, reqwest's rustls-tls pulls ring), so rustls can't auto-select a
// provider and tonic's client TLS panics on first use. Pin the default to
// aws-lc-rs, matching the Rust volume server. Idempotent.
pub fn install_default_crypto_provider() {
    let _ = aws_lc_rs::default_provider().install_default();
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
