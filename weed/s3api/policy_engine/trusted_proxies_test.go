package policy_engine

import (
	"net/http"
	"testing"
)

func newReq(remoteAddr string, xff, xRealIP string) *http.Request {
	r := &http.Request{RemoteAddr: remoteAddr, Header: http.Header{}}
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	if xRealIP != "" {
		r.Header.Set("X-Real-Ip", xRealIP)
	}
	return r
}

func TestTrustedProxies_IsTrusted(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.5", "192.168.0.0/16"})
	if !tp.IsTrusted("10.0.0.5") {
		t.Error("bare IP should be trusted")
	}
	if !tp.IsTrusted("192.168.1.100") {
		t.Error("IP in CIDR should be trusted")
	}
	if tp.IsTrusted("8.8.8.8") {
		t.Error("public IP should not be trusted")
	}
	if tp.IsTrusted("not-an-ip") {
		t.Error("invalid IP should not be trusted")
	}
}

func TestTrustedProxies_CanonicalizesBareIPv6(t *testing.T) {
	tp := NewTrustedProxies([]string{"2001:0db8::1"})
	if !tp.IsTrusted("2001:db8::1") {
		t.Error("canonical IPv6 should match non-canonical allowlist entry")
	}
}

func TestTrustedProxies_InvalidBareIPSkipped(t *testing.T) {
	tp := NewTrustedProxies([]string{"not-an-ip", "10.0.0.5"})
	if tp.IsTrusted("not-an-ip") {
		t.Error("invalid entry should not be stored")
	}
	if !tp.IsTrusted("10.0.0.5") {
		t.Error("valid entry after invalid one should still load")
	}
}

func TestTrustedProxies_NilNotTrusted(t *testing.T) {
	var tp *TrustedProxies
	if tp.IsTrusted("127.0.0.1") {
		t.Error("nil TrustedProxies should not trust any IP")
	}
}

func TestTrustedProxies_ExtractSourceIP_DirectPeerWhenUntrusted(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("203.0.113.5:1234", "8.8.8.8", "1.1.1.1")
	if got := tp.ExtractSourceIP(r); got != "203.0.113.5" {
		t.Errorf("untrusted peer: want 203.0.113.5, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_XForwardedFor(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("10.0.0.1:1234", "8.8.8.8, 10.0.0.2", "")
	if got := tp.ExtractSourceIP(r); got != "8.8.8.8" {
		t.Errorf("trusted proxy: want 8.8.8.8, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_AllTrustedReturnsLeftmost(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("10.0.0.1:1234", "10.0.0.5, 10.0.0.6", "")
	if got := tp.ExtractSourceIP(r); got != "10.0.0.5" {
		t.Errorf("all-trusted chain: want leftmost 10.0.0.5, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_MalformedXFFFallsBackToPeer(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("10.0.0.1:1234", "8.8.8.8, garbage", "")
	if got := tp.ExtractSourceIP(r); got != "10.0.0.1" {
		t.Errorf("malformed XFF: want direct peer 10.0.0.1, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_XRealIP(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("10.0.0.1:1234", "", "8.8.8.8")
	if got := tp.ExtractSourceIP(r); got != "8.8.8.8" {
		t.Errorf("X-Real-Ip: want 8.8.8.8, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_XForwardedForPreferredOverXRealIP(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("10.0.0.1:1234", "8.8.8.8", "1.1.1.1")
	if got := tp.ExtractSourceIP(r); got != "8.8.8.8" {
		t.Errorf("XFF should win over X-Real-Ip: want 8.8.8.8, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_NoTrustedProxiesUsesPeer(t *testing.T) {
	tp := NewTrustedProxies(nil)
	r := newReq("10.0.0.1:1234", "8.8.8.8", "1.1.1.1")
	if got := tp.ExtractSourceIP(r); got != "10.0.0.1" {
		t.Errorf("empty allowlist: want 10.0.0.1, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_NilRequest(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	if got := tp.ExtractSourceIP(nil); got != "" {
		t.Errorf("nil request: want empty, got %s", got)
	}
}

func TestTrustedProxies_ExtractSourceIP_UnixSocket(t *testing.T) {
	tp := NewTrustedProxies([]string{"10.0.0.0/24"})
	r := newReq("@", "", "")
	if got := tp.ExtractSourceIP(r); got != "@" {
		t.Errorf("unix socket: want @, got %s", got)
	}
}
