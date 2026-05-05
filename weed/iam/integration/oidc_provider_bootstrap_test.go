package integration

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

func TestStaticConfigSeedsProviderStore(t *testing.T) {
	mgr := NewIAMManager()
	cfg := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
			AccountId:        "111122223333",
			Providers: []*sts.ProviderConfig{
				{
					Name:    "google",
					Type:    sts.ProviderTypeOIDC,
					Enabled: true,
					Config: map[string]interface{}{
						"issuer":   "https://accounts.google.com",
						"clientId": "1234.apps.googleusercontent.com",
					},
				},
				{
					Name:    "github-actions",
					Type:    sts.ProviderTypeOIDC,
					Enabled: true,
					Config: map[string]interface{}{
						"issuer":   "https://token.actions.githubusercontent.com",
						"clientId": "sts.amazonaws.com",
					},
				},
				{
					Name:    "disabled-google",
					Type:    sts.ProviderTypeOIDC,
					Enabled: false,
					Config: map[string]interface{}{
						"issuer":   "https://accounts.disabled.example",
						"clientId": "x",
					},
				},
			},
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &RoleStoreConfig{StoreType: "memory"},
	}
	if err := mgr.Initialize(cfg, func() string { return "localhost:8888" }); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	store := mgr.GetOIDCProviderStore()
	if store == nil {
		t.Fatal("expected store to be initialized")
	}

	got, err := mgr.ListOIDCProviders(context.Background())
	if err != nil {
		t.Fatalf("ListOIDCProviders: %v", err)
	}
	// Disabled providers must not be mirrored.
	if len(got) != 2 {
		t.Fatalf("expected 2 providers, got %d (%v)", len(got), got)
	}

	byArn := map[string]*OIDCProviderRecord{}
	for _, r := range got {
		byArn[r.ARN] = r
	}

	googleARN := "arn:aws:iam::111122223333:oidc-provider/accounts.google.com"
	g, ok := byArn[googleARN]
	if !ok {
		t.Fatalf("missing google ARN; have %v", byArn)
	}
	if len(g.ClientIDs) != 1 || g.ClientIDs[0] != "1234.apps.googleusercontent.com" {
		t.Fatalf("google clientIds wrong: %v", g.ClientIDs)
	}

	ghARN := "arn:aws:iam::111122223333:oidc-provider/token.actions.githubusercontent.com"
	gh, ok := byArn[ghARN]
	if !ok {
		t.Fatalf("missing github-actions ARN; have %v", byArn)
	}
	if len(gh.ClientIDs) != 1 || gh.ClientIDs[0] != "sts.amazonaws.com" {
		t.Fatalf("github clientIds wrong: %v", gh.ClientIDs)
	}

	// Spot-check the IAM read path returns the same record.
	rec, err := mgr.GetOIDCProvider(context.Background(), googleARN)
	if err != nil {
		t.Fatalf("GetOIDCProvider: %v", err)
	}
	if rec.URL != "https://accounts.google.com" {
		t.Fatalf("URL mismatch: %s", rec.URL)
	}
}

func TestStaticConfigMirrorsThumbprintsAndAdvancedFields(t *testing.T) {
	// Static config sets thumbprints, AllowedPrincipalTagKeys, and PolicyClaim;
	// the mirror into the IAM-managed store must carry all three. Without it,
	// the next RefreshOIDCProvidersFromStore rebuilds a runtime provider that
	// drops thumbprint pinning and silently disables claim-based policies and
	// principal-tag passthrough — and because IAM-managed entries take
	// precedence over the static-config map, the bootstrap provider gets
	// shadowed by this weaker rebuild.
	mgr := NewIAMManager()
	cfg := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
			AccountId:        "111122223333",
			Providers: []*sts.ProviderConfig{
				{
					Name:    "github-actions",
					Type:    sts.ProviderTypeOIDC,
					Enabled: true,
					Config: map[string]interface{}{
						"issuer":                  "https://token.actions.githubusercontent.com",
						"clientId":                "sts.amazonaws.com",
						"thumbprints":             []interface{}{"6938fd4d98bab03faadb97b34396831e3780aea1"},
						"allowedPrincipalTagKeys": []interface{}{"team", "env"},
						"policyClaim":             "policies",
					},
				},
			},
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &RoleStoreConfig{StoreType: "memory"},
	}
	if err := mgr.Initialize(cfg, func() string { return "localhost:8888" }); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	rec, err := mgr.GetOIDCProvider(context.Background(), "arn:aws:iam::111122223333:oidc-provider/token.actions.githubusercontent.com")
	if err != nil {
		t.Fatalf("GetOIDCProvider: %v", err)
	}
	if got, want := rec.Thumbprints, []string{"6938fd4d98bab03faadb97b34396831e3780aea1"}; !equalStrings(got, want) {
		t.Fatalf("thumbprints mismatch: got %v want %v", got, want)
	}
	if got, want := rec.AllowedPrincipalTagKeys, []string{"team", "env"}; !equalStrings(got, want) {
		t.Fatalf("allowedPrincipalTagKeys mismatch: got %v want %v", got, want)
	}
	if rec.PolicyClaim != "policies" {
		t.Fatalf("policyClaim mismatch: got %q want %q", rec.PolicyClaim, "policies")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestStoreNotConfiguredReturnsClearError(t *testing.T) {
	mgr := NewIAMManager()
	if _, err := mgr.GetOIDCProvider(context.Background(), "arn:..."); err == nil {
		t.Fatal("expected error when store not configured")
	}
	if _, err := mgr.ListOIDCProviders(context.Background()); err == nil {
		t.Fatal("expected error when store not configured")
	}
}
