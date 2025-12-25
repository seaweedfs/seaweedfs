package testutil

import (
	"testing"
	"time"
)

func TestServerStartStop(t *testing.T) {
	config := DefaultServerConfig(nil)
	config.StartupWait = 30 * time.Second

	// Start server
	server, err := StartServer(t, config)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Verify server is running
	if server.cmd.Process == nil {
		t.Fatal("Server process not started")
	}

	// Stop server
	if err := server.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}
