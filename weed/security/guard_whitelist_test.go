package security

import "testing"

// TestIsWhiteListedEmptyConfigAllowsEverything pins the contract that the
// gRPC admin auth helper relies on: when no whitelist and no signing key
// are configured, IsWhiteListed accepts any host (including the empty
// string and unparseable peer addresses like the gRPC "@" passthrough
// form). Otherwise insecure default deployments would lock themselves out
// of every destructive admin RPC.
func TestIsWhiteListedEmptyConfigAllowsEverything(t *testing.T) {
	g := NewGuard(nil, "", 0, "", 0)
	for _, host := range []string{"", "@", "127.0.0.1", "::1", "garbage:value"} {
		if !g.IsWhiteListed(host) {
			t.Errorf("empty config should accept host=%q, got false", host)
		}
	}
}

func TestIsWhiteListedWithListRejectsUnknown(t *testing.T) {
	g := NewGuard([]string{"10.0.0.1", "192.168.1.0/24"}, "", 0, "", 0)
	cases := []struct {
		host string
		want bool
	}{
		{"10.0.0.1", true},
		{"192.168.1.42", true},
		{"192.168.2.1", false},
		{"127.0.0.1", false},
		{"", false},
		{"@", false},
	}
	for _, tc := range cases {
		if got := g.IsWhiteListed(tc.host); got != tc.want {
			t.Errorf("IsWhiteListed(%q) = %v want %v", tc.host, got, tc.want)
		}
	}
}

func TestIsWhiteListedWithSigningKeyButNoWhitelistAllowsAll(t *testing.T) {
	// JWT-only mode: signing key set, whitelist empty. Without this branch
	// the gRPC admin auth helper would falsely deny in-process callers when
	// the operator wires up signing keys but hasn't enumerated IPs.
	g := NewGuard(nil, "deadbeef", 0, "", 0)
	for _, host := range []string{"", "@", "127.0.0.1"} {
		if !g.IsWhiteListed(host) {
			t.Errorf("signing-key + empty whitelist should accept host=%q", host)
		}
	}
}
