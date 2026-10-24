//! The process-wide rustls crypto provider.
//!
//! Both binaries link aws-lc-rs and ring transitively — in the volume server
//! through the AWS SDK and reqwest, in the lance worker through lance's `aws`
//! backend and reqwest — so rustls cannot auto-select a provider and tonic's
//! client TLS panics on first use. Each binary has to pin one, and it has to be
//! the same one, which is why the choice lives here rather than in either tree.

use rustls::crypto::aws_lc_rs;

/// Pin rustls's process-wide default provider to aws-lc-rs, matching the
/// volume server's TLS config. Idempotent: the first call wins and every
/// later one is a no-op, so callers do not have to coordinate.
pub fn install_default_crypto_provider() {
    let _ = aws_lc_rs::default_provider().install_default();
}

#[cfg(test)]
mod tests {
    use super::install_default_crypto_provider;

    #[test]
    fn installing_is_idempotent_and_leaves_a_default_behind() {
        install_default_crypto_provider();
        install_default_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
