package testutil

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

func HasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

// MustFreePortPair is a convenience wrapper for tests that only need a single pair.
// Prefer MustAllocatePorts when allocating multiple pairs to guarantee uniqueness.
func MustFreePortPair(t *testing.T, name string) (int, int) {
	ports := MustAllocatePorts(t, 2)
	return ports[0], ports[1]
}

// MustAllocatePorts allocates count unique free ports atomically.
// All listeners are held open until every port is obtained, preventing
// the OS from recycling a port between successive allocations.
func MustAllocatePorts(t *testing.T, count int) []int {
	t.Helper()
	ports, err := AllocatePorts(count)
	if err != nil {
		t.Fatalf("Failed to allocate %d free ports: %v", count, err)
	}
	return ports
}

// AllocatePorts allocates count unique free ports atomically.
func AllocatePorts(count int) ([]int, error) {
	listeners := make([]net.Listener, 0, count)
	ports := make([]int, 0, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			for _, ll := range listeners {
				_ = ll.Close()
			}
			return nil, err
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports, nil
}

func WaitForService(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			resp, err := client.Get(url)
			if err == nil {
				resp.Body.Close()
				return true
			}
		}
	}
}

func WaitForPort(port int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	address := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
