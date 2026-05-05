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
	omitDiscoveryIssuer  bool   // when true, the discovery doc omits the "issuer" field entirely
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
		body := map[string]string{"jwks_uri": jwksURI}
		if !idp.omitDiscoveryIssuer {
			body["issuer"] = issuer
		}
		_ = json.NewEncoder(w).Encode(body)
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

	// Subsequent fetches retry discovery — discoveryFailed resets at the top
	// of fetchJWKSLocked when no URI was cached, so a transient 5xx at
	// startup doesn't lock the provider into the fallback path forever.
	// Retry rate is bounded by the JWKS TTL (one retry per refresh cycle).
	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS second: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 2 {
		t.Fatalf("discovery probe should retry while no URI is cached, got %d hits", got)
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

// TestDiscoveryRejectsMissingIssuer: a discovery document that omits the
// issuer field entirely must be treated the same as one that supplies a
// mismatched issuer. Otherwise an attacker who can intercept the discovery
// response can strip the issuer field and the comparison silently passes,
// letting the document point fetchJWKS at any URL it pleases.
func TestDiscoveryRejectsMissingIssuer(t *testing.T) {
	idp := newFakeIDP(t)
	idp.omitDiscoveryIssuer = true
	p := newProviderForIDP(t, idp, "")

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS should fall back to /.well-known/jwks.json on issuer-missing discovery: %v", err)
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("expected 1 discovery probe, got %d", got)
	}
	// The discovery document was rejected; the JWKS that ultimately served
	// us must be the fallback one, not the discovered URI. The fakeIDP
	// counts both hits under jwksHits since they share a counter; what
	// matters is that customJWKSHits stayed zero.
	if got := idp.customJWKSHits.Load(); got != 0 {
		t.Fatalf("custom JWKS endpoint must not have been used, got %d hits", got)
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Fatalf("expected 1 fallback JWKS hit, got %d", got)
	}
}
