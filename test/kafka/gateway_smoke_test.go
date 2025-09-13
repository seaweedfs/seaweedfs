package kafka

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestGateway_StartAcceptsConnections(t *testing.T) {
	srv := gateway.NewTestServer(gateway.Options{Listen: ":0"})
	if err := srv.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	addr := srv.Addr()
	if addr == "" {
		t.Fatalf("server Addr() empty")
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	_ = conn.Close()
	if err := srv.Close(); err != nil {
		t.Fatalf("close gateway: %v", err)
	}
}

func TestGateway_RefusesAfterClose(t *testing.T) {
	srv := gateway.NewTestServer(gateway.Options{Listen: ":0"})
	if err := srv.Start(); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	addr := srv.Addr()
	if addr == "" {
		t.Fatalf("server Addr() empty")
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("close gateway: %v", err)
	}
	// give the OS a brief moment to release the port
	time.Sleep(50 * time.Millisecond)
	_, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err == nil {
		t.Fatalf("expected dial to fail after close")
	}
}
