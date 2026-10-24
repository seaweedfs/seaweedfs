package dash

import (
	"testing"
)

// TestIsReadOnlyRole verifies that only the read-only session role is treated
// as view-only. The admin role and the empty role (used when auth is
// disabled) must not be treated as read-only, otherwise admins and no-auth
// deployments would lose legitimate access to credentials.
func TestIsReadOnlyRole(t *testing.T) {
	if !IsReadOnlyRole(RoleReadOnly) {
		t.Errorf("IsReadOnlyRole(%q) = false, want true", RoleReadOnly)
	}
	if !IsReadOnlyRole("readonly") {
		t.Errorf("IsReadOnlyRole(\"readonly\") = false, want true")
	}
	if IsReadOnlyRole("admin") {
		t.Errorf("IsReadOnlyRole(\"admin\") = true, want false")
	}
	if IsReadOnlyRole("") {
		t.Errorf("IsReadOnlyRole(\"\") = true, want false; no-auth mode must not be redacted")
	}
}

// TestObjectStoreUserRedactSecretKey verifies that redaction clears the
// reusable secret while preserving the public access key identifier.
func TestObjectStoreUserRedactSecretKey(t *testing.T) {
	u := ObjectStoreUser{
		Username:  "victim",
		AccessKey: "AKIAEXAMPLEKEY",
		SecretKey: "super-secret-value",
	}
	u.RedactSecretKey()
	if u.SecretKey != "" {
		t.Errorf("SecretKey = %q, want empty after redaction", u.SecretKey)
	}
	if u.AccessKey == "" {
		t.Errorf("AccessKey was emptied; only the secret should be redacted")
	}
	if u.Username == "" {
		t.Errorf("Username was emptied; only the secret should be redacted")
	}
}

// TestUserDetailsRedactSecretKeys verifies that redaction clears every
// access key's secret while preserving the access key identifiers.
func TestUserDetailsRedactSecretKeys(t *testing.T) {
	d := &UserDetails{
		Username: "victim",
		AccessKeys: []AccessKeyInfo{
			{AccessKey: "AKIAONE", SecretKey: "secret-one"},
			{AccessKey: "AKIATWO", SecretKey: "secret-two"},
		},
	}
	d.RedactSecretKeys()
	for i, ak := range d.AccessKeys {
		if ak.SecretKey != "" {
			t.Errorf("AccessKeys[%d].SecretKey = %q, want empty after redaction", i, ak.SecretKey)
		}
		if ak.AccessKey == "" {
			t.Errorf("AccessKeys[%d].AccessKey was emptied; only the secret should be redacted", i)
		}
	}
}

// TestUserDetailsRedactSecretKeys_Empty verifies redaction is a no-op
// (and does not panic) when there are no access keys.
func TestUserDetailsRedactSecretKeys_Empty(t *testing.T) {
	d := &UserDetails{Username: "victim"}
	d.RedactSecretKeys()
	if len(d.AccessKeys) != 0 {
		t.Errorf("expected no access keys, got %d", len(d.AccessKeys))
	}
}
