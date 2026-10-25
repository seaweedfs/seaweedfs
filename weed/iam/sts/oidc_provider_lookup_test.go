package sts

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// stubIdentityProvider is the minimal IdentityProvider needed to drive
// lookupOIDCProviderForAccount; the lookup never calls Authenticate.
type stubIdentityProvider struct{ name string }

func (s *stubIdentityProvider) Name() string                 { return s.name }
func (s *stubIdentityProvider) Initialize(interface{}) error { return nil }
func (s *stubIdentityProvider) Authenticate(context.Context, string) (*providers.ExternalIdentity, error) {
	return nil, nil
}
func (s *stubIdentityProvider) GetUserInfo(context.Context, string) (*providers.ExternalIdentity, error) {
	return nil, nil
}
func (s *stubIdentityProvider) ValidateToken(context.Context, string) (*providers.TokenClaims, error) {
	return nil, nil
}

func TestLookupOIDCProviderForAccountPrefersAccountMatch(t *testing.T) {
	const issuer = "https://example.com"
	accountA := &stubIdentityProvider{name: "A"}
	accountB := &stubIdentityProvider{name: "B"}

	s := &STSService{issuerToProvider: map[string]providers.IdentityProvider{}}
	s.SetIAMManagedOIDCProviders(map[string][]ScopedOIDCProvider{
		issuer: {
			{AccountID: "111111111111", Provider: accountA},
			{AccountID: "222222222222", Provider: accountB},
		},
	})

	got, ok := s.lookupOIDCProviderForAccount(issuer, "222222222222")
	if !ok || got != accountB {
		t.Fatalf("expected accountB provider for matching account, got %v ok=%v", got, ok)
	}
	got, ok = s.lookupOIDCProviderForAccount(issuer, "111111111111")
	if !ok || got != accountA {
		t.Fatalf("expected accountA provider for matching account, got %v ok=%v", got, ok)
	}
}

func TestLookupOIDCProviderForAccountFallsBackToGlobal(t *testing.T) {
	const issuer = "https://example.com"
	global := &stubIdentityProvider{name: "global"}
	accountA := &stubIdentityProvider{name: "A"}

	s := &STSService{issuerToProvider: map[string]providers.IdentityProvider{}}
	s.SetIAMManagedOIDCProviders(map[string][]ScopedOIDCProvider{
		issuer: {
			{AccountID: "", Provider: global},
			{AccountID: "111111111111", Provider: accountA},
		},
	})

	// Account not represented in records → global match.
	got, ok := s.lookupOIDCProviderForAccount(issuer, "999999999999")
	if !ok || got != global {
		t.Fatalf("expected global provider as fallback, got %v ok=%v", got, ok)
	}
	// Empty account hint → never picks an account-scoped record arbitrarily;
	// only the global record is eligible.
	got, ok = s.lookupOIDCProviderForAccount(issuer, "")
	if !ok || got != global {
		t.Fatalf("expected global provider when account unknown, got %v ok=%v", got, ok)
	}
}

func TestLookupOIDCProviderForAccountSkipsAccountSpecificWhenAccountUnknown(t *testing.T) {
	const issuer = "https://example.com"
	accountA := &stubIdentityProvider{name: "A"}

	s := &STSService{issuerToProvider: map[string]providers.IdentityProvider{}}
	s.SetIAMManagedOIDCProviders(map[string][]ScopedOIDCProvider{
		issuer: {
			{AccountID: "111111111111", Provider: accountA},
		},
	})

	// No account hint AND no global record → must fall through (return false),
	// not silently pick the account-A entry. Picking an arbitrary entry is the
	// pre-fix bug that lets a token be validated by the wrong tenant's record.
	if _, ok := s.lookupOIDCProviderForAccount(issuer, ""); ok {
		t.Fatalf("expected no match when account unknown and only account-scoped records exist")
	}
}

func TestLookupOIDCProviderForAccountFallsBackToStatic(t *testing.T) {
	const issuer = "https://example.com"
	static := &stubIdentityProvider{name: "static"}

	s := &STSService{issuerToProvider: map[string]providers.IdentityProvider{issuer: static}}
	s.SetIAMManagedOIDCProviders(nil)

	got, ok := s.lookupOIDCProviderForAccount(issuer, "111111111111")
	if !ok || got != static {
		t.Fatalf("expected static-config provider as last resort, got %v ok=%v", got, ok)
	}
}

// shutdownTrackingProvider records Shutdown calls so tests can observe which
// replaced providers the swap released.
type shutdownTrackingProvider struct {
	stubIdentityProvider
	shutdownCalls int32
}

func (s *shutdownTrackingProvider) Shutdown(context.Context) error {
	s.shutdownCalls++
	return nil
}

// A provider dropped from the managed map owns a background JTI cleanup
// worker; replacing the map must shut it down, while providers that stay in
// the map keep running.
func TestSetIAMManagedOIDCProvidersShutsDownDroppedProviders(t *testing.T) {
	dropped := &shutdownTrackingProvider{stubIdentityProvider{name: "dropped"}, 0}
	kept := &shutdownTrackingProvider{stubIdentityProvider{name: "kept"}, 0}
	added := &shutdownTrackingProvider{stubIdentityProvider{name: "added"}, 0}

	s := &STSService{issuerToProvider: map[string]providers.IdentityProvider{}}
	s.SetIAMManagedOIDCProviders(map[string][]ScopedOIDCProvider{
		"https://a.example.com": {{Provider: dropped}},
		"https://b.example.com": {{Provider: kept}, {AccountID: "111111111111", Provider: kept}},
	})

	s.SetIAMManagedOIDCProviders(map[string][]ScopedOIDCProvider{
		"https://b.example.com": {{Provider: kept}},
		"https://c.example.com": {{Provider: added}},
	})

	if dropped.shutdownCalls != 1 {
		t.Fatalf("dropped provider was shut down %d times, want 1", dropped.shutdownCalls)
	}
	if kept.shutdownCalls != 0 {
		t.Fatalf("kept provider was shut down %d times, want 0", kept.shutdownCalls)
	}
	if added.shutdownCalls != 0 {
		t.Fatalf("newly added provider was shut down %d times, want 0", added.shutdownCalls)
	}

	// Clearing the map releases the remaining providers.
	s.SetIAMManagedOIDCProviders(nil)
	if kept.shutdownCalls != 1 || added.shutdownCalls != 1 {
		t.Fatalf("clearing the map shut down kept=%d added=%d, want 1 each", kept.shutdownCalls, added.shutdownCalls)
	}
}
