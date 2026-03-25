package blockvol

import (
	"net"
	"strings"
	"testing"
)

func TestCanonicalizeAddr_WildcardIPv4_UsesAdvertised(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4zero, Port: 5099}
	result := canonicalizeListenerAddr(addr, "192.168.1.184")
	if result != "192.168.1.184:5099" {
		t.Fatalf("expected 192.168.1.184:5099, got %q", result)
	}
}

func TestCanonicalizeAddr_WildcardIPv6_UsesAdvertised(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv6zero, Port: 5099}
	result := canonicalizeListenerAddr(addr, "10.0.0.3")
	if result != "10.0.0.3:5099" {
		t.Fatalf("expected 10.0.0.3:5099, got %q", result)
	}
}

func TestCanonicalizeAddr_NilIP_UsesAdvertised(t *testing.T) {
	addr := &net.TCPAddr{IP: nil, Port: 5099}
	result := canonicalizeListenerAddr(addr, "192.168.1.184")
	if result != "192.168.1.184:5099" {
		t.Fatalf("expected 192.168.1.184:5099, got %q", result)
	}
}

func TestCanonicalizeAddr_AlreadyCanonical_Unchanged(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.5"), Port: 5099}
	result := canonicalizeListenerAddr(addr, "10.0.0.1")
	if result != "192.168.1.5:5099" {
		t.Fatalf("expected 192.168.1.5:5099 (unchanged), got %q", result)
	}
}

func TestCanonicalizeAddr_Loopback_Unchanged(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3260}
	result := canonicalizeListenerAddr(addr, "192.168.1.184")
	if result != "127.0.0.1:3260" {
		t.Fatalf("expected 127.0.0.1:3260 (loopback intentional), got %q", result)
	}
}

func TestCanonicalizeAddr_NoAdvertised_FallsBackToOutbound(t *testing.T) {
	addr := &net.TCPAddr{IP: net.IPv4zero, Port: 5099}
	result := canonicalizeListenerAddr(addr, "")
	// Should not be wildcard.
	if strings.HasPrefix(result, "0.0.0.0:") || strings.HasPrefix(result, "[::]:") || strings.HasPrefix(result, ":") {
		t.Fatalf("fallback should produce routable addr, got %q", result)
	}
	if !strings.Contains(result, ":5099") {
		t.Fatalf("port should be preserved, got %q", result)
	}
}

func TestPreferredOutboundIP_NotEmpty(t *testing.T) {
	ip := preferredOutboundIP()
	if ip == "" {
		t.Skip("no network interface available")
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		t.Fatalf("not a valid IP: %q", ip)
	}
	if parsed.IsUnspecified() {
		t.Fatalf("returned unspecified IP: %q", ip)
	}
}
