//! Security: JWT validation and IP whitelist checking.
//!
//! Matches Go's security/guard.go and security/jwt.go.
//! - Guard: combines whitelist IP checking with JWT token validation
//! - JWT: HS256 HMAC signing with file-id claims

pub mod tls;

use std::collections::HashSet;
use std::net::IpAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

// ============================================================================
// JWT Claims
// ============================================================================

/// Claims for volume server file access tokens.
/// Matches Go's `SeaweedFileIdClaims`.
#[derive(Debug, Serialize, Deserialize)]
pub struct FileIdClaims {
    /// File ID this token grants access to (e.g., "3,01637037d6").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fid: Option<String>,

    /// Expiration time (Unix timestamp).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,

    /// Not before (Unix timestamp).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
}

/// Signing key wrapper (empty = security disabled).
#[derive(Clone)]
pub struct SigningKey(pub Vec<u8>);

impl SigningKey {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn from_string(s: &str) -> Self {
        SigningKey(s.as_bytes().to_vec())
    }
}

/// Generate a JWT token for file access.
pub fn gen_jwt(
    signing_key: &SigningKey,
    expires_after_sec: i64,
    file_id: &str,
) -> Result<String, JwtError> {
    if signing_key.is_empty() {
        return Err(JwtError::NoSigningKey);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let claims = FileIdClaims {
        fid: Some(file_id.to_string()),
        exp: if expires_after_sec > 0 {
            Some(now + expires_after_sec as u64)
        } else {
            None
        },
        nbf: None,
    };

    let token = encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&signing_key.0),
    )?;

    Ok(token)
}

/// Decode and validate a JWT token.
pub fn decode_jwt(signing_key: &SigningKey, token: &str) -> Result<FileIdClaims, JwtError> {
    if signing_key.is_empty() {
        return Err(JwtError::NoSigningKey);
    }

    let mut validation = Validation::new(Algorithm::HS256);
    // Match Go behavior: tokens without exp are accepted (Go's jwt-go does not require exp)
    // But if exp IS present, it must be valid (not expired).
    validation.required_spec_claims.clear();
    validation.validate_exp = true;
    // Go's jwt-go/v5 validates nbf when present
    validation.validate_nbf = true;
    validation.leeway = 0;

    let data = decode::<FileIdClaims>(
        token,
        &DecodingKey::from_secret(&signing_key.0),
        &validation,
    )?;

    Ok(data.claims)
}

// ============================================================================
// Guard
// ============================================================================

/// Security guard: IP whitelist + JWT token validation.
pub struct Guard {
    whitelist_ips: HashSet<String>,
    whitelist_cidrs: Vec<(IpAddr, u8)>, // (network, prefix_len)
    pub signing_key: SigningKey,
    pub expires_after_sec: i64,
    pub read_signing_key: SigningKey,
    pub read_expires_after_sec: i64,
    /// Combined flag: true when whitelist is non-empty OR signing key is present.
    /// Matches Go's `isWriteActive = !isEmptyWhiteList || len(SigningKey) != 0`.
    is_write_active: bool,
}

impl Guard {
    pub fn new(
        whitelist: &[String],
        signing_key: SigningKey,
        expires_after_sec: i64,
        read_signing_key: SigningKey,
        read_expires_after_sec: i64,
    ) -> Self {
        let mut guard = Guard {
            whitelist_ips: HashSet::new(),
            whitelist_cidrs: Vec::new(),
            signing_key,
            expires_after_sec,
            read_signing_key,
            read_expires_after_sec,
            is_write_active: false,
        };
        guard.update_whitelist(whitelist);
        guard
    }

    /// Update the IP whitelist.
    pub fn update_whitelist(&mut self, entries: &[String]) {
        self.whitelist_ips.clear();
        self.whitelist_cidrs.clear();

        for entry in entries {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            if entry.contains('/') {
                // CIDR range
                if let Some((ip, prefix)) = parse_cidr(entry) {
                    self.whitelist_cidrs.push((ip, prefix));
                } else {
                    tracing::error!("Parse CIDR {} in whitelist failed", entry);
                }
            } else {
                // Exact IP/hostname
                self.whitelist_ips.insert(entry.to_string());
            }
        }

        // Match Go: isWriteActive = !isEmptyWhiteList || len(SigningKey) != 0
        let is_empty_whitelist = self.whitelist_ips.is_empty() && self.whitelist_cidrs.is_empty();
        self.is_write_active = !is_empty_whitelist || !self.signing_key.is_empty();
    }

    /// Check if a remote IP is in the whitelist.
    /// Returns true if write security is inactive (no whitelist and no signing key),
    /// if the whitelist is empty, or if the IP matches.
    pub fn check_whitelist(&self, remote_addr: &str) -> bool {
        if !self.is_write_active {
            return true;
        }
        if self.whitelist_ips.is_empty() && self.whitelist_cidrs.is_empty() {
            return true;
        }

        let host = extract_host(remote_addr);

        // Check exact match
        if self.whitelist_ips.contains(&host) {
            return true;
        }

        // Check CIDR ranges
        if let Ok(ip) = host.parse::<IpAddr>() {
            for &(ref network, prefix_len) in &self.whitelist_cidrs {
                if ip_in_cidr(&ip, network, prefix_len) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a read signing key is configured.
    pub fn has_read_signing_key(&self) -> bool {
        !self.read_signing_key.is_empty()
    }

    /// Validate a request's JWT token.
    /// `is_write` determines which signing key to use.
    /// Returns Ok(()) if valid, or if security is disabled.
    pub fn check_jwt(&self, token: Option<&str>, is_write: bool) -> Result<(), JwtError> {
        let key = if is_write {
            &self.signing_key
        } else {
            &self.read_signing_key
        };

        if key.is_empty() {
            return Ok(()); // Security disabled for this operation type
        }

        let token = token.ok_or(JwtError::MissingToken)?;
        decode_jwt(key, token)?;
        Ok(())
    }

    /// Check JWT and validate the file ID claim matches.
    pub fn check_jwt_for_file(
        &self,
        token: Option<&str>,
        expected_fid: &str,
        is_write: bool,
    ) -> Result<(), JwtError> {
        let key = if is_write {
            &self.signing_key
        } else {
            &self.read_signing_key
        };

        if key.is_empty() {
            return Ok(());
        }

        let token = token.ok_or(JwtError::MissingToken)?;
        let claims = decode_jwt(key, token)?;

        match claims.fid {
            None => {
                return Err(JwtError::MissingFileIdClaim);
            }
            Some(ref fid) if fid != expected_fid => {
                return Err(JwtError::FileIdMismatch {
                    expected: expected_fid.to_string(),
                    got: fid.to_string(),
                });
            }
            _ => {}
        }

        Ok(())
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract host from "host:port" or "[::1]:port" format.
fn extract_host(addr: &str) -> String {
    // Handle IPv6 with brackets
    if addr.starts_with('[') {
        if let Some(end) = addr.find(']') {
            return addr[1..end].to_string();
        }
    }
    // Handle host:port
    if let Some(pos) = addr.rfind(':') {
        return addr[..pos].to_string();
    }
    addr.to_string()
}

/// Parse CIDR notation "192.168.1.0/24" into (IpAddr, prefix_len).
fn parse_cidr(cidr: &str) -> Option<(IpAddr, u8)> {
    let parts: Vec<&str> = cidr.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let ip: IpAddr = parts[0].parse().ok()?;
    let prefix: u8 = parts[1].parse().ok()?;
    Some((ip, prefix))
}

/// Check if an IP is within a CIDR range.
fn ip_in_cidr(ip: &IpAddr, network: &IpAddr, prefix_len: u8) -> bool {
    match (ip, network) {
        (IpAddr::V4(ip), IpAddr::V4(net)) => {
            let ip_bits = u32::from(*ip);
            let net_bits = u32::from(*net);
            let mask = if prefix_len == 0 {
                0
            } else if prefix_len >= 32 {
                u32::MAX
            } else {
                u32::MAX << (32 - prefix_len)
            };
            (ip_bits & mask) == (net_bits & mask)
        }
        (IpAddr::V6(ip), IpAddr::V6(net)) => {
            let ip_bits = u128::from(*ip);
            let net_bits = u128::from(*net);
            let mask = if prefix_len == 0 {
                0
            } else if prefix_len >= 128 {
                u128::MAX
            } else {
                u128::MAX << (128 - prefix_len)
            };
            (ip_bits & mask) == (net_bits & mask)
        }
        _ => false, // V4/V6 mismatch
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("no signing key configured")]
    NoSigningKey,

    #[error("missing JWT token")]
    MissingToken,

    #[error("JWT error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("JWT token missing required fid claim")]
    MissingFileIdClaim,

    #[error("file ID mismatch: expected {expected}, got {got}")]
    FileIdMismatch { expected: String, got: String },
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_round_trip() {
        let key = SigningKey::from_string("test-secret-key");
        let token = gen_jwt(&key, 3600, "3,01637037d6").unwrap();
        let claims = decode_jwt(&key, &token).unwrap();
        assert_eq!(claims.fid, Some("3,01637037d6".to_string()));
    }

    #[test]
    fn test_jwt_no_signing_key() {
        let key = SigningKey(vec![]);
        assert!(gen_jwt(&key, 3600, "1,abc").is_err());
    }

    #[test]
    fn test_jwt_invalid_token() {
        let key = SigningKey::from_string("secret");
        let result = decode_jwt(&key, "invalid.token.here");
        assert!(result.is_err());
    }

    #[test]
    fn test_jwt_wrong_key() {
        let key1 = SigningKey::from_string("secret1");
        let key2 = SigningKey::from_string("secret2");
        let token = gen_jwt(&key1, 3600, "1,abc").unwrap();
        assert!(decode_jwt(&key2, &token).is_err());
    }

    #[test]
    fn test_guard_empty_whitelist() {
        let guard = Guard::new(&[], SigningKey(vec![]), 0, SigningKey(vec![]), 0);
        assert!(guard.check_whitelist("192.168.1.1:8080"));
    }

    #[test]
    fn test_guard_whitelist_exact() {
        let guard = Guard::new(
            &["192.168.1.1".to_string(), "10.0.0.1".to_string()],
            SigningKey(vec![]),
            0,
            SigningKey(vec![]),
            0,
        );
        assert!(guard.check_whitelist("192.168.1.1:8080"));
        assert!(guard.check_whitelist("10.0.0.1:1234"));
        assert!(!guard.check_whitelist("172.16.0.1:8080"));
    }

    #[test]
    fn test_guard_whitelist_cidr() {
        let guard = Guard::new(
            &["10.0.0.0/8".to_string()],
            SigningKey(vec![]),
            0,
            SigningKey(vec![]),
            0,
        );
        assert!(guard.check_whitelist("10.1.2.3:8080"));
        assert!(guard.check_whitelist("10.255.255.255:80"));
        assert!(!guard.check_whitelist("11.0.0.1:80"));
    }

    #[test]
    fn test_guard_check_jwt_disabled() {
        let guard = Guard::new(&[], SigningKey(vec![]), 0, SigningKey(vec![]), 0);
        // No signing key = security disabled
        assert!(guard.check_jwt(None, true).is_ok());
        assert!(guard.check_jwt(None, false).is_ok());
    }

    #[test]
    fn test_guard_check_jwt_enabled() {
        let key = SigningKey::from_string("write-secret");
        let read_key = SigningKey::from_string("read-secret");
        let guard = Guard::new(&[], key.clone(), 3600, read_key.clone(), 3600);

        // Missing token
        assert!(guard.check_jwt(None, true).is_err());

        // Valid write token
        let token = gen_jwt(&key, 3600, "1,abc").unwrap();
        assert!(guard.check_jwt(Some(&token), true).is_ok());

        // Write token for read should fail (different key)
        assert!(guard.check_jwt(Some(&token), false).is_err());

        // Valid read token
        let read_token = gen_jwt(&read_key, 3600, "1,abc").unwrap();
        assert!(guard.check_jwt(Some(&read_token), false).is_ok());
    }

    #[test]
    fn test_guard_check_jwt_file_id() {
        let key = SigningKey::from_string("secret");
        let guard = Guard::new(&[], key.clone(), 3600, SigningKey(vec![]), 0);

        let token = gen_jwt(&key, 3600, "3,01637037d6").unwrap();

        // Correct file ID
        assert!(guard
            .check_jwt_for_file(Some(&token), "3,01637037d6", true)
            .is_ok());

        // Wrong file ID
        let err = guard.check_jwt_for_file(Some(&token), "4,deadbeef", true);
        assert!(matches!(err, Err(JwtError::FileIdMismatch { .. })));
    }

    #[test]
    fn test_extract_host() {
        assert_eq!(extract_host("192.168.1.1:8080"), "192.168.1.1");
        assert_eq!(extract_host("[::1]:8080"), "::1");
        assert_eq!(extract_host("localhost"), "localhost");
    }

    #[test]
    fn test_ip_in_cidr() {
        let net: IpAddr = "10.0.0.0".parse().unwrap();
        let ip1: IpAddr = "10.1.2.3".parse().unwrap();
        let ip2: IpAddr = "11.0.0.1".parse().unwrap();
        assert!(ip_in_cidr(&ip1, &net, 8));
        assert!(!ip_in_cidr(&ip2, &net, 8));
    }
}
