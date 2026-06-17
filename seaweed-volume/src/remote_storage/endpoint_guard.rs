//! SSRF guard for caller-supplied remote storage endpoints.
//!
//! `FetchAndWriteNeedle` accepts an S3 endpoint URL chosen by the caller and
//! dials it directly from the volume server, which typically has network
//! access to cluster-internal hosts. Without validation a caller could point
//! the server at loopback / link-local / RFC 1918 / cloud-metadata addresses
//! and read internal services (SSRF). This mirrors the Go volume server's
//! `validateRemoteEndpoint` (`weed/server/volume_grpc_remote.go`); operators
//! that legitimately fetch from private hosts opt out with
//! `-volume.allowUntrustedRemoteEndpoints`.
//!
//! NOTE: the Go server additionally pins the validated endpoint against DNS
//! rebinding with a custom dialer that re-checks the resolved IP at TCP connect
//! time (`guardedDialer`). The `aws-sdk-s3` client used here builds its own
//! connector, so that connect-time re-validation is not yet ported; the
//! up-front resolve-and-check below still blocks the common SSRF vectors. A
//! narrow TOCTOU window (a hostname that resolves to a public IP here and then
//! flips to a blocked one when the SDK dials) remains as a follow-up.

use std::net::{IpAddr, Ipv4Addr};

/// AWS/Azure/GCP IPv4 instance-metadata-service (IMDS) address. It is
/// link-local and thus already covered by [`is_link_local`], but is named
/// explicitly so the rejection reason is unambiguous in logs.
const IMDS_IPV4: IpAddr = IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254));

/// Hostnames that target cloud instance metadata services, blocked regardless
/// of how they resolve because some environments alias the IMDS address under
/// a name.
fn is_blocked_imds_host(host: &str) -> bool {
    matches!(host, "metadata.google.internal" | "metadata")
}

/// Whether `ip` is in a link-local range: IPv4 169.254.0.0/16, IPv6 fe80::/10
/// (unicast) and interface-local / link-local multicast (scope 1 and 2).
fn is_link_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_link_local(),
        IpAddr::V6(v6) => {
            let seg0 = v6.segments()[0];
            if (seg0 & 0xffc0) == 0xfe80 {
                return true; // fe80::/10 link-local unicast
            }
            if (seg0 & 0xff00) == 0xff00 {
                // ffXS:: multicast; reject interface-local (scope 1) and
                // link-local (scope 2) the way Go's IsInterfaceLocalMulticast /
                // IsLinkLocalMulticast do.
                let scope = seg0 & 0x000f;
                return scope == 1 || scope == 2;
            }
            false
        }
    }
}

/// Whether `ip` is in a private range: IPv4 RFC 1918, IPv6 fc00::/7 unique-local
/// (matching Go's `net.IP.IsPrivate`).
fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private(),
        IpAddr::V6(v6) => (v6.segments()[0] & 0xfe00) == 0xfc00,
    }
}

/// Whether `ip` is in the RFC 6598 carrier-grade NAT range (100.64.0.0/10).
/// The IPv4 private check does not cover CGNAT, so check it explicitly.
fn is_cgnat(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            o[0] == 100 && (o[1] & 0xc0) == 0x40 // second octet 64..=127
        }
        IpAddr::V6(_) => false,
    }
}

/// Returns an error if `ip` is not safe to dial from a server that can reach
/// cluster-internal hosts. Mirrors Go's `checkBlockedIP`.
pub fn check_blocked_ip(endpoint: &str, ip: IpAddr) -> Result<(), String> {
    // Normalize IPv4-mapped IPv6 (`::ffff:a.b.c.d`) to its IPv4 form so the
    // IPv4 deny rules apply. The OS routes these to the embedded IPv4 address,
    // so without this `::ffff:127.0.0.1` / `::ffff:169.254.169.254` would slip
    // past the IPv6-only checks. Mirrors Go's reliance on net.IP.To4().
    let ip = match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    };
    if ip == IMDS_IPV4 {
        return Err(format!(
            "remote endpoint {:?} targets instance metadata service {}",
            endpoint, ip
        ));
    }
    if ip.is_loopback() {
        return Err(format!(
            "remote endpoint {:?} resolves to loopback address {}",
            endpoint, ip
        ));
    }
    if ip.is_unspecified() {
        return Err(format!(
            "remote endpoint {:?} resolves to unspecified address {}",
            endpoint, ip
        ));
    }
    if is_link_local(ip) {
        return Err(format!(
            "remote endpoint {:?} resolves to link-local address {}",
            endpoint, ip
        ));
    }
    if is_private(ip) {
        return Err(format!(
            "remote endpoint {:?} resolves to private address {}",
            endpoint, ip
        ));
    }
    if is_cgnat(ip) {
        return Err(format!(
            "remote endpoint {:?} resolves to CGNAT address {}",
            endpoint, ip
        ));
    }
    Ok(())
}

/// Outcome of the synchronous, DNS-free portion of endpoint validation.
#[derive(Debug)]
enum HostCheck {
    /// Host was a literal IP and already passed [`check_blocked_ip`].
    Validated,
    /// Host is a name that must be resolved and each address checked.
    NeedsResolution(String),
}

/// Validate the scheme and host without touching DNS. Pure and unit-testable.
fn precheck_endpoint(endpoint: &str) -> Result<HostCheck, String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return Err("remote endpoint is empty".to_string());
    }

    let Some(scheme_end) = trimmed.find("://") else {
        return Err(format!(
            "remote endpoint {:?} must use http or https",
            endpoint
        ));
    };
    let scheme = trimmed[..scheme_end].to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(format!(
            "remote endpoint {:?} must use http or https, got {:?}",
            endpoint,
            &trimmed[..scheme_end]
        ));
    }

    // Authority is everything up to the first '/', '?', or '#'.
    let after = &trimmed[scheme_end + 3..];
    let authority_end = after
        .find(|c| c == '/' || c == '?' || c == '#')
        .unwrap_or(after.len());
    let authority = &after[..authority_end];

    // Strip optional userinfo ("user:pass@").
    let host_port = match authority.rfind('@') {
        Some(at) => &authority[at + 1..],
        None => authority,
    };

    // Extract the host, handling bracketed IPv6 literals and host:port.
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        match rest.find(']') {
            Some(end) => &rest[..end],
            None => {
                return Err(format!(
                    "remote endpoint {:?} has a malformed IPv6 host",
                    endpoint
                ))
            }
        }
    } else {
        match host_port.find(':') {
            Some(colon) => &host_port[..colon],
            None => host_port,
        }
    };

    if host.is_empty() {
        return Err(format!("remote endpoint {:?} has no host", endpoint));
    }

    if is_blocked_imds_host(&host.to_ascii_lowercase()) {
        return Err(format!(
            "remote endpoint {:?} targets instance metadata service",
            endpoint
        ));
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        check_blocked_ip(endpoint, ip)?;
        return Ok(HostCheck::Validated);
    }

    Ok(HostCheck::NeedsResolution(host.to_string()))
}

/// Resolve `host` to its addresses with a short timeout, matching Go's 2s
/// resolver deadline.
async fn resolve_host(host: &str) -> Result<Vec<IpAddr>, String> {
    let lookup = tokio::net::lookup_host((host, 0u16));
    let addrs = tokio::time::timeout(std::time::Duration::from_secs(2), lookup)
        .await
        .map_err(|_| format!("resolve remote endpoint host {:?}: timed out", host))?
        .map_err(|e| format!("resolve remote endpoint host {:?}: {}", host, e))?;
    Ok(addrs.map(|sock| sock.ip()).collect())
}

/// Returns an error if `endpoint` is not safe to dial from a server with access
/// to cluster-internal hosts. Rejects empty / non-http(s) endpoints,
/// loopback / unspecified / link-local / RFC 1918 / CGNAT addresses, and
/// well-known IMDS hostnames. Hostnames are resolved and every returned address
/// is checked. Mirrors Go's `validateRemoteEndpoint`.
pub async fn validate_remote_endpoint(endpoint: &str) -> Result<(), String> {
    match precheck_endpoint(endpoint)? {
        HostCheck::Validated => Ok(()),
        HostCheck::NeedsResolution(host) => {
            let addrs = resolve_host(&host).await?;
            if addrs.is_empty() {
                return Err(format!(
                    "resolve remote endpoint host {:?}: no addresses",
                    host
                ));
            }
            for ip in addrs {
                check_blocked_ip(endpoint, ip)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn rejects_blocked_literal_endpoints() {
        let cases = [
            ("http://127.0.0.1:8080", "loopback"),
            ("http://[::1]:8080", "loopback"),
            ("http://169.254.169.254/", "metadata"),
            ("http://0.0.0.0/", "unspecified"),
            ("http://[fe80::1]/", "link-local"),
            ("http://10.0.0.1/", "private"),
            ("http://172.16.5.5/", "private"),
            ("http://192.168.0.1/", "private"),
            ("http://100.64.0.1/", "CGNAT"),
        ];
        for (endpoint, want) in cases {
            let err = precheck_endpoint(endpoint).expect_err(endpoint);
            assert!(err.contains(want), "{endpoint}: {err:?} missing {want:?}");
        }
    }

    #[test]
    fn rejects_empty_and_bad_scheme() {
        assert!(precheck_endpoint("").unwrap_err().contains("empty"));
        assert!(precheck_endpoint("ftp://example.com/")
            .unwrap_err()
            .contains("http or https"));
        assert!(precheck_endpoint("example.com/")
            .unwrap_err()
            .contains("http or https"));
    }

    #[test]
    fn rejects_imds_hostnames() {
        assert!(precheck_endpoint("http://metadata.google.internal/")
            .unwrap_err()
            .contains("metadata service"));
        assert!(precheck_endpoint("http://metadata/")
            .unwrap_err()
            .contains("metadata service"));
    }

    #[test]
    fn accepts_public_literal_and_defers_hostname() {
        assert!(matches!(
            precheck_endpoint("https://52.216.10.10/"),
            Ok(HostCheck::Validated)
        ));
        match precheck_endpoint("https://s3.us-east-1.amazonaws.com/") {
            Ok(HostCheck::NeedsResolution(host)) => {
                assert_eq!(host, "s3.us-east-1.amazonaws.com")
            }
            other => panic!("expected resolution, got {other:?}"),
        }
    }

    #[test]
    fn check_blocked_ip_matches_resolved_categories() {
        // Mirror Go's "host resolves to X" cases at the address level.
        assert!(check_blocked_ip("e", ip("127.0.0.1"))
            .unwrap_err()
            .contains("loopback"));
        assert!(check_blocked_ip("e", ip("169.254.10.20"))
            .unwrap_err()
            .contains("link-local"));
        assert!(check_blocked_ip("e", ip("10.1.2.3"))
            .unwrap_err()
            .contains("private"));
        assert!(check_blocked_ip("e", ip("172.20.0.5"))
            .unwrap_err()
            .contains("private"));
        assert!(check_blocked_ip("e", ip("192.168.1.1"))
            .unwrap_err()
            .contains("private"));
        assert!(check_blocked_ip("e", ip("100.64.0.42"))
            .unwrap_err()
            .contains("CGNAT"));
        assert!(check_blocked_ip("e", ip("fc00::1"))
            .unwrap_err()
            .contains("private"));
        assert!(check_blocked_ip("e", ip("52.216.10.10")).is_ok());
        assert!(check_blocked_ip("e", ip("2606:4700:4700::1111")).is_ok());
    }

    #[test]
    fn rejects_ipv4_mapped_ipv6() {
        // IPv4-mapped IPv6 must be unmapped so the IPv4 rules catch it.
        assert!(check_blocked_ip("e", ip("::ffff:127.0.0.1"))
            .unwrap_err()
            .contains("loopback"));
        assert!(check_blocked_ip("e", ip("::ffff:169.254.169.254"))
            .unwrap_err()
            .contains("metadata"));
        assert!(check_blocked_ip("e", ip("::ffff:10.0.0.1"))
            .unwrap_err()
            .contains("private"));
        // A mapped public address still passes, and genuine IPv6 loopback is
        // still caught by the V6 path.
        assert!(check_blocked_ip("e", ip("::ffff:52.216.10.10")).is_ok());
        assert!(check_blocked_ip("e", ip("::1"))
            .unwrap_err()
            .contains("loopback"));
        // Bracketed mapped literal via the full endpoint path.
        assert!(precheck_endpoint("http://[::ffff:127.0.0.1]/")
            .unwrap_err()
            .contains("loopback"));
    }
}
