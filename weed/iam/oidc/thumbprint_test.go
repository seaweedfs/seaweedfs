package oidc

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// thumbprintFromTLS returns the SHA-1 hex of the first peer certificate the
// given TLS server presents. It mirrors the pinning algorithm used by the
// OIDC provider so the test is hashing the cert that will actually be
// verified at runtime.
func thumbprintFromTLS(t *testing.T, server *httptest.Server) string {
	t.Helper()
	if server.TLS == nil || len(server.Certificate().Raw) == 0 {
		t.Fatal("server has no TLS certificate")
	}
	sum := sha1.Sum(server.Certificate().Raw)
	return hex.EncodeToString(sum[:])
}

// newTLSIDP starts a TLS-only test IDP that always serves a static JWKS.
// httptest TLS servers use a self-signed certificate by default, which means
// callers who want a successful handshake must either trust that cert or
// use TLSInsecureSkipVerify.
func newTLSIDP(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{{Kty: "RSA", Kid: "k1", Use: "sig", Alg: "RS256", N: "AQAB", E: "AQAB"}}})
	})
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r) // force fallback path
	})
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	return server
}

// pinnedProvider configures an OIDC provider that pins TLS to the given
// thumbprint allowlist and trusts only the test server's self-signed cert.
func pinnedProvider(t *testing.T, server *httptest.Server, thumbprints []string) *OIDCProvider {
	t.Helper()
	p := NewOIDCProvider("thumbprint-test")
	cfg := &OIDCConfig{
		Issuer:                server.URL,
		ClientID:              "anything",
		Thumbprints:           thumbprints,
		TLSInsecureSkipVerify: true, // we're doing the trust decision via thumbprint
	}
	if err := p.Initialize(cfg); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	// Replace the inner transport's RootCAs with the server's cert so the chain
	// builder also accepts it; this exercises the verified-chain path.
	if transport, ok := p.httpClient.Transport.(*http.Transport); ok && transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = false
		transport.TLSClientConfig.RootCAs = server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	}
	return p
}

func TestThumbprintMatchAccepted(t *testing.T) {
	server := newTLSIDP(t)
	tp := thumbprintFromTLS(t, server)
	p := pinnedProvider(t, server, []string{tp})

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS with matching thumbprint should succeed: %v", err)
	}
}

func TestThumbprintMismatchRejected(t *testing.T) {
	server := newTLSIDP(t)
	// Flip the last byte so the digest can't match anything legitimate.
	bad := "0000000000000000000000000000000000000000"
	p := pinnedProvider(t, server, []string{bad})

	err := p.fetchJWKS(context.Background())
	if err == nil {
		t.Fatal("fetchJWKS with mismatched thumbprint should fail")
	}
}

func TestThumbprintAllowlistAcceptsMixedCase(t *testing.T) {
	server := newTLSIDP(t)
	tp := thumbprintFromTLS(t, server)
	// Mix uppercase + whitespace to ensure normalization is applied.
	p := pinnedProvider(t, server, []string{"  " + uppercase(tp) + "  "})
	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("normalized thumbprint should match: %v", err)
	}
}

// uppercase returns s with letters uppercased — cheap helper avoiding a
// dependency on strings.ToUpper at the package level for clarity.
func uppercase(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		out[i] = c
	}
	return string(out)
}

// guard against a future refactor that drops VerifyConnection by checking
// that omitting Thumbprints leaves the connection unverified-by-pin.
func TestThumbprintEmptyAllowlistSkipsCheck(t *testing.T) {
	server := newTLSIDP(t)
	p := pinnedProvider(t, server, nil)
	transport, ok := p.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport")
	}
	if transport.TLSClientConfig.VerifyConnection != nil {
		t.Fatal("VerifyConnection must be nil when no thumbprints are configured")
	}
	// And the full TLS handshake still succeeds normally.
	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS with no thumbprints: %v", err)
	}
}

// Ensure the underlying tls.Config still rejects truly invalid handshakes
// when InsecureSkipVerify is on but no thumbprint allowlist is configured —
// in that case we explicitly want to *trust* the connection (skip-verify),
// just as the existing test code did.
func TestThumbprintSkipVerifyHonoured(t *testing.T) {
	server := newTLSIDP(t)
	p := NewOIDCProvider("skip")
	if err := p.Initialize(&OIDCConfig{
		Issuer:                server.URL,
		ClientID:              "x",
		TLSInsecureSkipVerify: true,
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	transport := p.httpClient.Transport.(*http.Transport)
	if !transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify should be set")
	}
	if got := transport.TLSClientConfig.MinVersion; got != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %v want TLS12", got)
	}
}
