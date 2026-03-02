package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

func TestOIDCAuthConfigValidateRequiresRoleMapping(t *testing.T) {
	config := OIDCAuthConfig{
		Enabled:      true,
		Issuer:       "https://issuer.example.com",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		RedirectURL:  "https://admin.example.com/login/oidc/callback",
	}

	if err := config.Validate(); err == nil {
		t.Fatalf("expected validation error when role_mapping is missing")
	}
}

func TestOIDCAuthConfigEffectiveScopesIncludesOpenID(t *testing.T) {
	config := OIDCAuthConfig{
		Scopes: []string{"profile", "email", "profile"},
	}

	scopes := config.EffectiveScopes()
	expected := []string{"profile", "email", "openid"}
	if len(scopes) != len(expected) {
		t.Fatalf("expected %d scopes, got %d (%v)", len(expected), len(scopes), scopes)
	}
	for i, scope := range expected {
		if scopes[i] != scope {
			t.Fatalf("expected scope[%d]=%q, got %q", i, scope, scopes[i])
		}
	}
}

func TestMapClaimsToRolesAndResolveAdminRole(t *testing.T) {
	claims := &providers.TokenClaims{
		Claims: map[string]interface{}{
			"groups": []interface{}{"seaweedfs-readers", "seaweedfs-admins"},
		},
	}

	roleMapping := &providers.RoleMapping{
		Rules: []providers.MappingRule{
			{Claim: "groups", Value: "seaweedfs-readers", Role: "readonly"},
			{Claim: "groups", Value: "seaweedfs-admins", Role: "admin"},
		},
		DefaultRole: "readonly",
	}

	roles := mapClaimsToRoles(claims, roleMapping)
	if len(roles) != 2 {
		t.Fatalf("expected 2 mapped roles, got %d (%v)", len(roles), roles)
	}

	role, err := resolveAdminRole(roles)
	if err != nil {
		t.Fatalf("expected resolved role, got error: %v", err)
	}
	if role != "admin" {
		t.Fatalf("expected admin role, got %s", role)
	}
}
