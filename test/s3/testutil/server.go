package testutil

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// TestServer represents a running weed mini server for testing
type TestServer struct {
	cmd    *exec.Cmd
	config ServerConfig
	done   chan error
}

// ServerConfig holds configuration for starting a weed mini server
type ServerConfig struct {
	DataDir     string
	S3Port      int
	S3Config    string
	AccessKey   string
	SecretKey   string
	LogFile     string
	PIDFile     string
	StartupWait time.Duration
	WeedBinary  string
}

// DefaultServerConfig creates a default server configuration
func DefaultServerConfig(dataDir *string) ServerConfig {
	if dataDir == nil {
		dataDir = &[]string{filepath.Join(os.TempDir(), "weed-test-data")}[0]
	}
	return ServerConfig{
		DataDir:     *dataDir,
		S3Port:      8333,
		AccessKey:   "test",
		SecretKey:   "test",
		LogFile:     "weed-test.log",
		PIDFile:     "weed-test.pid",
		StartupWait: 30 * time.Second,
	}
}

// StartServer starts a weed mini server with the given configuration
func StartServer(config ServerConfig) (*TestServer, error) {
	// Find weed binary
	weedBinary, err := findWeedBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to find weed binary: %w", err)
	}
	config.WeedBinary = weedBinary

	// Create data directory
	os.MkdirAll(config.DataDir, 0755)

	// Build command arguments
	args := []string{
		"server",
		"-debug",
		"-s3",
		fmt.Sprintf("-s3.port=%d", config.S3Port),
		"-s3.allowDeleteBucketNotEmpty=true",
		"-filer",
		"-filer.maxMB=64",
		"-master.volumeSizeLimitMB=50",
		"-master.peers=none",
		"-volume.max=100",
		fmt.Sprintf("-dir=%s", config.DataDir),
		"-volume.preStopSeconds=1",
		"-metricsPort=9324",
	}

	if config.S3Config != "" {
		args = append(args, fmt.Sprintf("-s3.config=%s", config.S3Config))
	}

	// Start server process
	cmd := exec.Command(weedBinary, args...)
	logFile, err := os.Create(config.LogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to start server: %w", err)
	}

	// Save PID
	os.WriteFile(config.PIDFile, []byte(fmt.Sprintf("%d", cmd.Process.Pid)), 0644)

	server := &TestServer{
		cmd:    cmd,
		config: config,
		done:   make(chan error, 1),
	}

	// Wait for server to be ready
	if err := server.WaitForReady(); err != nil {
		server.Stop()
		return nil, fmt.Errorf("server failed to start: %w", err)
	}

	return server, nil
}

// WaitForReady polls the server health endpoint until it responds
func (s *TestServer) WaitForReady() error {
	deadline := time.Now().Add(s.config.StartupWait)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("server startup timeout after %v", s.config.StartupWait)
		}

		resp, err := http.Get(fmt.Sprintf("http://localhost:%d", s.config.S3Port))
		if err == nil {
			resp.Body.Close()
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// Stop stops the server and cleans up resources
func (s *TestServer) Stop() error {
	if s.cmd.Process == nil {
		return nil
	}

	// Try graceful shutdown
	s.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() {
		done <- s.cmd.Wait()
	}()

	select {
	case <-time.After(5 * time.Second):
		// Force kill if graceful shutdown takes too long
		s.cmd.Process.Kill()
		<-done
	case <-done:
	}

	// Clean up files
	os.Remove(s.config.PIDFile)
	os.RemoveAll(s.config.DataDir)

	return nil
}

// findWeedBinary searches for the weed binary in common locations
func findWeedBinary() (string, error) {
	// Try ../../../weed (from test/s3/testutil directory)
	paths := []string{
		"../../../weed",
		"../../../../weed",
		"../weed",
		"./weed",
		"/usr/local/bin/weed",
		"/usr/bin/weed",
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	// Try PATH
	if path, err := exec.LookPath("weed"); err == nil {
		return path, nil
	}

	return "", fmt.Errorf("weed binary not found in PATH or common locations")
}
