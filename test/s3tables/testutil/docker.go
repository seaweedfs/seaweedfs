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

func MustFreePortPair(t *testing.T, name string) (int, int) {
	httpPort, grpcPort, err := findAvailablePortPair()
	if err != nil {
		t.Fatalf("Failed to get free port pair for %s: %v", name, err)
	}
	return httpPort, grpcPort
}

func findAvailablePortPair() (int, int, error) {
	httpPort, err := GetFreePort()
	if err != nil {
		return 0, 0, err
	}
	grpcPort, err := GetFreePort()
	if err != nil {
		return 0, 0, err
	}
	return httpPort, grpcPort, nil
}

func GetFreePort() (int, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
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
