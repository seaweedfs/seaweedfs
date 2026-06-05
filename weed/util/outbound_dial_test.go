package util

import (
	"testing"
)

func TestSetOutboundLocalIP(t *testing.T) {
	t.Cleanup(func() { SetOutboundLocalIP("") })

	cases := []struct {
		name   string
		ip     string
		wantIP string // empty means cleared
	}{
		{"ipv4", "10.0.0.5", "10.0.0.5"},
		{"ipv6", "fe80::1", "fe80::1"},
		{"empty clears", "", ""},
		{"wildcard v4 clears", "0.0.0.0", ""},
		{"wildcard v6 clears", "::", ""},
		{"garbage clears", "not-an-ip", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

func TestOutboundLocalAddrForDial(t *testing.T) {
	t.Cleanup(func() { SetOutboundLocalIP("") })

	// No bind configured: never binds a source address.
	SetOutboundLocalIP("")
	if got := outboundLocalAddrForDial("tcp", "10.0.0.9:8080"); got != nil {
		t.Fatalf("unconfigured dial should not bind, got %v", got)
	}

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
