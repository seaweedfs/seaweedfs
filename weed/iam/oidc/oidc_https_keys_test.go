package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// plainKeyServer serves a JWKS over plain http and counts the fetches: an
// https issuer's keys must never be read from it.
func plainKeyServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{{Kty: "RSA", Kid: "attacker", Use: "sig", Alg: "RS256", N: "AQAB", E: "AQAB"}}})
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

// httpsIssuer starts a TLS issuer whose handlers the test supplies, and a
// provider for it.
func httpsIssuer(t *testing.T, mux *http.ServeMux) (*httptest.Server, *OIDCProvider) {
	t.Helper()
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	p := NewOIDCProvider("https-keys")
	if err := p.Initialize(&OIDCConfig{Issuer: server.URL, ClientID: "c", TLSInsecureSkipVerify: true}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return server, p
}

// Discovery naming a plain-http jwks_uri for an https issuer is refused; the
// keys come from the issuer's own https host instead.
func TestAnHTTPSIssuersDiscoveredKeysMustBeHTTPS(t *testing.T) {
	plain, plainHits := plainKeyServer(t)
	var server *httptest.Server
	var ownKeyHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": server.URL, "jwks_uri": plain.URL + "/jwks"})
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		ownKeyHits.Add(1)
		_ = json.NewEncoder(w).Encode(JWKS{Keys: []JWK{{Kty: "RSA", Kid: "k1", Use: "sig", Alg: "RS256", N: "AQAB", E: "AQAB"}}})
	})
	server, p := httpsIssuer(t, mux)

	if err := p.fetchJWKS(context.Background()); err != nil {
		t.Fatalf("fetchJWKS: %v", err)
	}
	if got := plainHits.Load(); got != 0 {
		t.Fatalf("keys were fetched over http %d time(s)", got)
	}
	if got := ownKeyHits.Load(); got != 1 {
		t.Fatalf("expected the issuer's own https jwks to be used, got %d hits", got)
	}
}

// An https key fetch redirected to plain http fails rather than follow it.
func TestAnHTTPSKeyFetchIsNotRedirectedToHTTP(t *testing.T) {
	plain, plainHits := plainKeyServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", http.NotFound)
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/jwks", http.StatusFound)
	})
	_, p := httpsIssuer(t, mux)

	if err := p.fetchJWKS(context.Background()); err == nil {
		t.Fatal("fetchJWKS followed a redirect to http")
	}
	if got := plainHits.Load(); got != 0 {
		t.Fatalf("keys were fetched over http %d time(s)", got)
	}
}
