use std::time::{SystemTime, UNIX_EPOCH};

use aws_lc_rs::hmac;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

/// Mints the HS256 Bearer token the admin control stream requires when
/// jwt.filer_signing.key is configured, matching the SeaweedFilerAdminClaims
/// the Go worker sends: registered claims only, no session id.
pub fn admin_token(signing_key: &[u8], expires_after_secs: u64) -> Option<String> {
    if signing_key.is_empty() {
        return None;
    }
    let claims = if expires_after_secs > 0 {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default()
            + expires_after_secs;
        format!("{{\"exp\":{exp}}}")
    } else {
        "{}".to_string()
    };
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(b"{\"alg\":\"HS256\",\"typ\":\"JWT\"}"),
        URL_SAFE_NO_PAD.encode(claims.as_bytes())
    );
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    let signature = hmac::sign(&key, signing_input.as_bytes());
    Some(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    ))
}

/// Derives the key for one use of a signing key, so a token signed with it is
/// not accepted where the signing key itself is.
pub fn derive_key(signing_key: &[u8], purpose: &str) -> Vec<u8> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    hmac::sign(&key, purpose.as_bytes()).as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use super::{admin_token, derive_key};

    #[test]
    fn derive_key_matches_the_go_derivation() {
        let derived = derive_key(b"cluster-key", "seaweedfs-lance-cluster-token");
        let hex: String = derived.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "b38c0b66da41621f2a9b6de68e4239ddd0d2dd7b4ede01aa91bd34a17edc1021"
        );
    }

    #[test]
    fn no_key_no_token() {
        assert!(admin_token(b"", 0).is_none());
    }

    #[test]
    fn minted_token_verifies_shape() {
        let token = admin_token(b"secret", 60).unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        assert!(!parts.iter().any(|p| p.is_empty()));
    }

    #[test]
    fn no_expiry_has_empty_claims() {
        let token = admin_token(b"secret", 0).unwrap();
        let payload = token.split('.').nth(1).unwrap();
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let claims = URL_SAFE_NO_PAD.decode(payload).unwrap();
        assert_eq!(claims, b"{}");
    }
}
