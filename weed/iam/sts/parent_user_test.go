package sts

import (
	"strings"
	"testing"
	"time"
)

func TestComputeParentUserStability(t *testing.T) {
	// Same (sub, iss) must produce the same hash, regardless of order or
	// whitespace mutations callers should never apply.
	a := ComputeParentUser("alice", "https://idp.example/")
	b := ComputeParentUser("alice", "https://idp.example/")
	if a == "" {
		t.Fatal("parent user should not be empty for non-empty sub")
	}
	if a != b {
		t.Fatalf("expected stable hash, got %q vs %q", a, b)
	}
}

func TestComputeParentUserDistinguishesIssuer(t *testing.T) {
	// The point of incorporating iss is that the same `sub` from two providers
	// must not collide. If this assertion ever fails, the hash input is wrong.
	a := ComputeParentUser("alice", "https://idp-a.example/")
	b := ComputeParentUser("alice", "https://idp-b.example/")
	if a == b {
		t.Fatalf("hashes for different issuers must differ, both = %q", a)
	}
}

func TestComputeParentUserDistinguishesSubject(t *testing.T) {
	a := ComputeParentUser("alice", "https://idp.example/")
	b := ComputeParentUser("bob", "https://idp.example/")
	if a == b {
		t.Fatalf("hashes for different subjects must differ, both = %q", a)
	}
}

func TestComputeParentUserEmptySub(t *testing.T) {
	if got := ComputeParentUser("", "https://idp.example/"); got != "" {
		t.Fatalf("empty sub should produce empty parent user, got %q", got)
	}
}

func TestComputeParentUserEmptyIss(t *testing.T) {
	// Without an issuer, two different IDPs that both name a user "alice"
	// would collide on the same parent_user. Refuse rather than hash.
	if got := ComputeParentUser("alice", ""); got != "" {
		t.Fatalf("empty iss should produce empty parent user, got %q", got)
	}
}

func TestComputeParentUserEncoding(t *testing.T) {
	got := ComputeParentUser("alice", "https://idp.example/")
	// Base64 RawURL has no padding and uses URL-safe alphabet — important
	// because parent_user shows up in filer paths and audit log fields.
	if strings.ContainsAny(got, "=+/") {
		t.Fatalf("parent user should be base64 raw url, got %q", got)
	}
}

func TestSessionClaimsRoundTripParentUser(t *testing.T) {
	parent := ComputeParentUser("alice", "https://idp.example/")
	claims := NewSTSSessionClaims("sid-1", "issuer", time.Now().Add(time.Hour)).
		WithRoleInfo("arn:aws:iam::123:role/r", "arn:aws:sts::123:assumed-role/r/s", "arn:aws:sts::123:assumed-role/r/s").
		WithParentUser(parent)

	info := claims.ToSessionInfo()
	if info.ParentUser != parent {
		t.Fatalf("ParentUser lost on round-trip: got %q want %q", info.ParentUser, parent)
	}
}
