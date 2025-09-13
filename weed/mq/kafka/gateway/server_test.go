package gateway

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestServerStartAndClose(t *testing.T) {
	// Skip this test as it requires a real SeaweedMQ Agent
	t.Skip("This test requires SeaweedMQ Agent integration - run manually with agent available")

	srv := NewServer(Options{
		Listen:       ":0",
		AgentAddress: "localhost:17777", // Would need real agent for this test
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	// ensure listener is open and accepting
	// try to dial the actual chosen port
	// Find the actual address
	var addr string
	if srv.ln == nil {
		t.Fatalf("listener not set")
	}
	addr = srv.ln.Addr().String()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = c.Close()
	if err := srv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestGetListenerAddr(t *testing.T) {
	// Skip this test as it requires a real SeaweedMQ Agent
	t.Skip("This test requires SeaweedMQ Agent integration - run manually with agent available")

	// Test with localhost binding - should return the actual address
	srv := NewServer(Options{
		Listen:       "127.0.0.1:0",
		AgentAddress: "localhost:17777", // Would need real agent for this test
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Close()

	host, port := srv.GetListenerAddr()
	if host != "127.0.0.1" {
		t.Errorf("expected 127.0.0.1, got %s", host)
	}
	if port <= 0 {
		t.Errorf("expected valid port, got %d", port)
	}

	// Test IPv6 all interfaces binding - should resolve to non-loopback IP
	srv6 := NewServer(Options{
		Listen:       "[::]:0",
		AgentAddress: "localhost:17777", // Would need real agent for this test
	})
	if err := srv6.Start(); err != nil {
		t.Fatalf("start IPv6: %v", err)
	}
	defer srv6.Close()

	host6, port6 := srv6.GetListenerAddr()
	// Should not be localhost when bound to all interfaces
	if host6 == "localhost" {
		t.Errorf("IPv6 all interfaces should not resolve to localhost, got %s", host6)
	}
	if port6 <= 0 {
		t.Errorf("expected valid port, got %d", port6)
	}
	t.Logf("IPv6 all interfaces resolved to: %s:%d", host6, port6)
}

func TestResolveAdvertisedAddress(t *testing.T) {
	addr := resolveAdvertisedAddress()
	if addr == "" {
		t.Error("resolveAdvertisedAddress returned empty string")
	}

	// Should be a valid IP address
	ip := net.ParseIP(addr)
	if ip == nil {
		t.Errorf("resolveAdvertisedAddress returned invalid IP: %s", addr)
	}

	// Should not be IPv6 (we prefer IPv4 for Kafka compatibility)
	if strings.Contains(addr, ":") {
		t.Errorf("Expected IPv4 address, got IPv6: %s", addr)
	}

	t.Logf("Resolved advertised address: %s", addr)
}
