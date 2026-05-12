package security

import "testing"

// TestIsWhiteListedEmptyConfigAllowsEverything pins the HTTP-side contract:
// when no whitelist and no signing key are configured, IsWhiteListed accepts
// any host. This is the allow-all-when-empty path that Guard.WhiteList
// middleware on read-mostly HTTP routes relies on; the destructive gRPC
// admin gate uses IsAdminAuthorized instead, which is fail-closed.
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
	// JWT-only mode: signing key set, whitelist empty. IsWhiteListed must
	// still accept every host because read-side HTTP middleware piggybacks
	// on it; admin gating uses IsAdminAuthorized, which is fail-closed.
	g := NewGuard(nil, "deadbeef", 0, "", 0)
	for _, host := range []string{"", "@", "127.0.0.1"} {
		if !g.IsWhiteListed(host) {
			t.Errorf("signing-key + empty whitelist should accept host=%q", host)
		}
	}
}

// TestIsAdminAuthorizedEmptyWhitelistDeniesNonLoopback pins the fail-closed
// contract the volume gRPC admin gate depends on: with no whitelist
// configured every off-host peer is rejected, even when a signing key is
// set. Loopback peers are still trusted (see
// TestIsAdminAuthorizedEmptyWhitelistAllowsLoopback) because a volume server
// commonly cohabits with master/filer on a single host.
func TestIsAdminAuthorizedEmptyWhitelistDeniesNonLoopback(t *testing.T) {
	cases := []*Guard{
		NewGuard(nil, "", 0, "", 0),
		NewGuard(nil, "deadbeef", 0, "", 0),
		NewGuard([]string{}, "", 0, "", 0),
	}
	for i, g := range cases {
		for _, host := range []string{"", "@", "10.0.0.1", "192.168.0.5", "8.8.8.8"} {
			if g.IsAdminAuthorized(host) {
				t.Errorf("case %d: empty whitelist should deny host=%q", i, host)
			}
		}
	}
}

// TestIsAdminAuthorizedEmptyWhitelistAllowsLoopback pins the loopback
// exception: when no whitelist is configured, in-process callers from
// 127.0.0.0/8 or ::1 are still authorized. Without this, single-host
// clusters and integration tests (which never set -whiteList) would lose
// every admin RPC the moment fail-closed semantics kicked in.
func TestIsAdminAuthorizedEmptyWhitelistAllowsLoopback(t *testing.T) {
	cases := []*Guard{
		NewGuard(nil, "", 0, "", 0),
		NewGuard(nil, "deadbeef", 0, "", 0),
		NewGuard([]string{}, "", 0, "", 0),
	}
	for i, g := range cases {
		for _, host := range []string{"127.0.0.1", "127.0.0.5", "::1"} {
			if !g.IsAdminAuthorized(host) {
				t.Errorf("case %d: empty whitelist should still allow loopback host=%q", i, host)
			}
		}
	}
}

func TestIsAdminAuthorizedWithIPList(t *testing.T) {
	g := NewGuard([]string{"10.0.0.1"}, "", 0, "", 0)
	cases := []struct {
		host string
		want bool
	}{
		{"10.0.0.1", true},
		{"10.0.0.2", false},
		{"", false},
		{"@", false},
	}
	for _, tc := range cases {
		if got := g.IsAdminAuthorized(tc.host); got != tc.want {
			t.Errorf("IsAdminAuthorized(%q) = %v want %v", tc.host, got, tc.want)
		}
	}
}

func TestIsAdminAuthorizedWithCIDR(t *testing.T) {
	g := NewGuard([]string{"10.0.0.0/24"}, "", 0, "", 0)
	cases := []struct {
		host string
		want bool
	}{
		{"10.0.0.5", true},
		{"10.0.0.255", true},
		{"192.168.0.1", false},
		{"10.0.1.1", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := g.IsAdminAuthorized(tc.host); got != tc.want {
			t.Errorf("IsAdminAuthorized(%q) = %v want %v", tc.host, got, tc.want)
		}
	}
}
