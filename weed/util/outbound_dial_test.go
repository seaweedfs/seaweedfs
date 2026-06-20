package util

import (
	"testing"
)

// resetOutbound clears the process-global outbound source address so each test
// starts from a clean slate (SetOutboundLocalIP is otherwise first-write-wins).
func resetOutbound() {
	outboundLocalAddrSet.Store(false)
	outboundLocalAddr.Store(nil)
}

func TestSetOutboundLocalIP(t *testing.T) {
	t.Cleanup(resetOutbound)

	cases := []struct {
		name   string
		ip     string
		wantIP string // empty means no binding
	}{
		{"ipv4", "10.0.0.5", "10.0.0.5"},
		{"ipv6", "fe80::1", "fe80::1"},
		{"empty", "", ""},
		{"wildcard v4", "0.0.0.0", ""},
		{"wildcard v6", "::", ""},
		{"garbage", "not-an-ip", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetOutbound()
			SetOutboundLocalIP(tc.ip)
			got := OutboundLocalAddr()
			if tc.wantIP == "" {
				if got != nil {
					t.Fatalf("expected no bound address, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected bound address %s, got nil", tc.wantIP)
			}
			if got.IP.String() != tc.wantIP {
				t.Fatalf("bound address = %s, want %s", got.IP.String(), tc.wantIP)
			}
			if got.Port != 0 {
				t.Fatalf("bound port = %d, want 0 (ephemeral)", got.Port)
			}
		})
	}
}

// TestSetOutboundLocalIPFirstWins guards the behavior that keeps `weed server`
// from letting a component's own bind setting clobber the process-wide address.
func TestSetOutboundLocalIPFirstWins(t *testing.T) {
	t.Cleanup(resetOutbound)
	resetOutbound()

	SetOutboundLocalIP("10.0.0.5") // server-level bind, applied first
	SetOutboundLocalIP("10.0.0.9") // a component's own bind: must be ignored
	SetOutboundLocalIP("")         // a component clearing: must not unbind

	got := OutboundLocalAddr()
	if got == nil || got.IP.String() != "10.0.0.5" {
		t.Fatalf("first call should win, got %v", got)
	}
}

func TestOutboundLocalAddrForDial(t *testing.T) {
	t.Cleanup(resetOutbound)

	// No bind configured: never binds a source address.
	resetOutbound()
	if got := outboundLocalAddrForDial("tcp", "10.0.0.9:8080"); got != nil {
		t.Fatalf("unconfigured dial should not bind, got %v", got)
	}

	resetOutbound()
	SetOutboundLocalIP("10.0.0.5")
	cases := []struct {
		name     string
		network  string
		address  string
		wantBind bool
	}{
		{"remote tcp binds", "tcp", "10.0.0.9:8080", true},
		{"tcp4 binds", "tcp4", "example.com:443", true},
		{"hostname binds", "tcp", "filer.internal:8888", true},
		{"loopback ip skipped", "tcp", "127.0.0.1:9333", false},
		{"loopback name skipped", "tcp", "localhost:9333", false},
		{"ipv6 loopback skipped", "tcp", "[::1]:9333", false},
		{"ipv6 literal target skipped", "tcp", "[2001:db8::1]:8080", false},
		{"unix network skipped", "unix", "/tmp/x.sock", false},
		{"udp network skipped", "udp", "10.0.0.9:53", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := outboundLocalAddrForDial(tc.network, tc.address)
			if tc.wantBind && got == nil {
				t.Fatalf("%s/%s: expected source binding, got nil", tc.network, tc.address)
			}
			if !tc.wantBind && got != nil {
				t.Fatalf("%s/%s: expected no source binding, got %v", tc.network, tc.address, got)
			}
		})
	}
}

func TestOutboundLocalAddrFamilyMismatch(t *testing.T) {
	t.Cleanup(resetOutbound)

	resetOutbound()
	SetOutboundLocalIP("10.0.0.5") // IPv4 source
	if got := outboundLocalAddrForDial("tcp", "[2001:db8::1]:8080"); got != nil {
		t.Fatalf("IPv4 source to IPv6 literal target should skip binding, got %v", got)
	}
	if got := outboundLocalAddrForDial("tcp", "10.0.0.9:8080"); got == nil {
		t.Fatalf("IPv4 source to IPv4 literal target should bind")
	}

	resetOutbound()
	SetOutboundLocalIP("2001:db8::5") // IPv6 source
	if got := outboundLocalAddrForDial("tcp", "10.0.0.9:8080"); got != nil {
		t.Fatalf("IPv6 source to IPv4 literal target should skip binding, got %v", got)
	}
	if got := outboundLocalAddrForDial("tcp", "[2001:db8::1]:8080"); got == nil {
		t.Fatalf("IPv6 source to IPv6 literal target should bind")
	}
}
