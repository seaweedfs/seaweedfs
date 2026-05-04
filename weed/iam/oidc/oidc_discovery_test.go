package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// fakeIDP wraps an httptest.Server and counts how many times each well-known
// endpoint is hit. Tests use it to assert discovery vs. fallback behaviour.
type fakeIDP struct {
	server               *httptest.Server
	discoveryHits        atomic.Int32
	jwksHits             atomic.Int32
	customJWKSHits       atomic.Int32
	disableDiscovery     bool
	discoveryStatusCode  int
	discoveryIssuer      string
	customJWKSPathSuffix string // optional suffix that fakeIDP serves at /custom/<suffix>
	jwks                 JWKS
}

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()
	idp := &fakeIDP{
		discoveryStatusCode: http.StatusOK,
		jwks:                JWKS{Keys: []JWK{{Kty: "RSA", Kid: "k1", Use: "sig", Alg: "RS256", N: "AQAB", E: "AQAB"}}},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		idp.discoveryHits.Add(1)
		if idp.disableDiscovery {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(idp.discoveryStatusCode)
		issuer := idp.discoveryIssuer
		if issuer == "" {
			issuer = idp.server.URL
		}
		jwksURI := idp.server.URL + "/discovered/jwks"
		if idp.customJWKSPathSuffix != "" {
			jwksURI = idp.server.URL + "/custom/" + idp.customJWKSPathSuffix
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":   issuer,
			"jwks_uri": jwksURI,
		})
	})
	mux.HandleFunc("/discovered/jwks", func(w http.ResponseWriter, r *http.Request) {
		idp.jwksHits.Add(1)
		_ = json.NewEncoder(w).Encode(idp.jwks)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		idp.jwksHits.Add(1)
		_ = json.NewEncoder(w).Encode(idp.jwks)
	})
	mux.HandleFunc("/custom/", func(w http.ResponseWriter, r *http.Request) {
		idp.customJWKSHits.Add(1)
		_ = json.NewEncoder(w).Encode(idp.jwks)
	})
	idp.server = httptest.NewServer(mux)
	t.Cleanup(idp.server.Close)
	return idp
}

func newProviderForIDP(t *testing.T, idp *fakeIDP, jwksURIOverride string) *OIDCProvider {
	t.Helper()
	p := NewOIDCProvider("test")
	cfg := &OIDCConfig{
		Issuer:   idp.server.URL,
		ClientID: "test-client",
		JWKSUri:  jwksURIOverride,
	}
	if err := p.Initialize(cfg); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return p
}

func TestDiscoveryHappyPath(t *testing.T) {
	idp := newFakeIDP(t)
	p := newProviderForIDP(t, idp, "")

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("expected 1 discovery hit, got %d", got)
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Fatalf("expected 1 JWKS hit at discovered uri, got %d", got)
	}

	// A second fetch reuses the cached jwks_uri without re-discovering.
	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS second: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("discovery should be cached, got %d hits", got)
	}
}

func TestDiscoveryFallback404(t *testing.T) {
	idp := newFakeIDP(t)
	idp.disableDiscovery = true
	p := newProviderForIDP(t, idp, "")

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("expected 1 discovery probe, got %d", got)
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Fatalf("expected 1 JWKS hit at fallback uri, got %d", got)
	}

	// Subsequent fetches skip discovery — discoveryFailed is sticky.
	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS second: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("discovery probe should not retry after failure, got %d hits", got)
	}
}

func TestDiscoveryDisabledByExplicitJWKSUri(t *testing.T) {
	idp := newFakeIDP(t)
	override := idp.server.URL + "/custom/explicit"
	idp.customJWKSPathSuffix = "explicit"
	p := newProviderForIDP(t, idp, override)

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 0 {
		t.Fatalf("explicit JWKSUri should bypass discovery, got %d hits", got)
	}
	if got := idp.customJWKSHits.Load(); got != 1 {
		t.Fatalf("expected 1 custom JWKS hit, got %d", got)
	}
}

func TestDiscoveryRejectsIssuerMismatch(t *testing.T) {
	idp := newFakeIDP(t)
	idp.discoveryIssuer = "https://attacker.example/"
	p := newProviderForIDP(t, idp, "")

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS should fall back to /.well-known/jwks.json, got error: %v", err)
	}
	// Discovery probe was tried once, rejected, then fell through to fallback path.
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("expected 1 discovery probe, got %d", got)
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Fatalf("expected 1 fallback JWKS hit, got %d", got)
	}
}
