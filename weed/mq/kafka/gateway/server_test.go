package gateway

import (
    "net"
    "testing"
    "time"
)

func TestServerStartAndClose(t *testing.T) {
    srv := NewServer(Options{Listen: ":0"})
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


