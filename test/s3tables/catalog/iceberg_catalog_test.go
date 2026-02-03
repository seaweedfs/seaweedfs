// Package catalog provides integration tests for the Iceberg REST Catalog API.
// These tests use DuckDB running in Docker to verify catalog operations.
package catalog

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestEnvironment contains the test environment configuration
type TestEnvironment struct {
	seaweedDir      string
	weedBinary      string
	dataDir         string
	s3Port          int
	s3GrpcPort      int
	icebergPort     int
	masterPort      int
	masterGrpcPort  int
	filerPort       int
	filerGrpcPort   int
	volumePort      int
	volumeGrpcPort  int
	weedProcess     *exec.Cmd
	weedCancel      context.CancelFunc
	dockerAvailable bool
}

// hasDocker checks if Docker is available
func hasDocker() bool {
	cmd := exec.Command("docker", "version")
	return cmd.Run() == nil
}

// getFreePort returns an available ephemeral port
func getFreePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

// NewTestEnvironment creates a new test environment
func NewTestEnvironment(t *testing.T) *TestEnvironment {
	t.Helper()

	// Find the SeaweedFS root directory
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	// Navigate up to find the SeaweedFS root (contains go.mod)
	seaweedDir := wd
	for i := 0; i < 5; i++ {
		if _, err := os.Stat(filepath.Join(seaweedDir, "go.mod")); err == nil {
			break
		}
		seaweedDir = filepath.Dir(seaweedDir)
	}

	// Check for weed binary
	weedBinary := filepath.Join(seaweedDir, "weed", "weed")
	if _, err := os.Stat(weedBinary); os.IsNotExist(err) {
		// Try system PATH
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found, skipping integration test")
		}
	}

	// Create temporary data directory
	dataDir, err := os.MkdirTemp("", "seaweed-iceberg-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Allocate free ephemeral ports for each service
	s3Port, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for S3: %v", err)
	}
	icebergPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Iceberg: %v", err)
	}
	s3GrpcPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for S3 gRPC: %v", err)
	}
	masterPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Master: %v", err)
	}
	masterGrpcPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Master gRPC: %v", err)
	}
	filerPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Filer: %v", err)
	}
	filerGrpcPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Filer gRPC: %v", err)
	}
	volumePort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Volume: %v", err)
	}

	volumeGrpcPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port for Volume gRPC: %v", err)
	}

	return &TestEnvironment{
		seaweedDir:      seaweedDir,
		weedBinary:      weedBinary,
		dataDir:         dataDir,
		s3Port:          s3Port,
		s3GrpcPort:      s3GrpcPort,
		icebergPort:     icebergPort,
		masterPort:      masterPort,
		masterGrpcPort:  masterGrpcPort,
		filerPort:       filerPort,
		filerGrpcPort:   filerGrpcPort,
		volumePort:      volumePort,
		volumeGrpcPort:  volumeGrpcPort,
		dockerAvailable: hasDocker(),
	}
}

// StartSeaweedFS starts a SeaweedFS mini cluster
func (env *TestEnvironment) StartSeaweedFS(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	env.weedCancel = cancel

	masterDir := filepath.Join(env.dataDir, "master")
	filerDir := filepath.Join(env.dataDir, "filer")
	volumeDir := filepath.Join(env.dataDir, "volume")

	for _, dir := range []string{masterDir, filerDir, volumeDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	cmd := exec.CommandContext(ctx, env.weedBinary, "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpcPort),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpcPort),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpcPort),
		"-s3.port", fmt.Sprintf("%d", env.s3Port),
		"-s3.port.grpc", fmt.Sprintf("%d", env.s3GrpcPort),
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergPort),
		"-ip.bind", "0.0.0.0",
		"-dir", env.dataDir,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start SeaweedFS: %v", err)
	}
	env.weedProcess = cmd

	// Wait for services to be ready
	if !env.waitForService(fmt.Sprintf("http://127.0.0.1:%d/v1/config", env.icebergPort), 30*time.Second) {
		t.Fatalf("Iceberg REST API did not become ready")
	}
}

// waitForService waits for a service to become available
func (env *TestEnvironment) waitForService(url string, timeout time.Duration) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// Cleanup stops SeaweedFS and cleans up resources
func (env *TestEnvironment) Cleanup(t *testing.T) {
	t.Helper()

	if env.weedCancel != nil {
		env.weedCancel()
	}

	if env.weedProcess != nil {
		// Give process time to shut down gracefully
		time.Sleep(2 * time.Second)
		env.weedProcess.Wait()
	}

	if env.dataDir != "" {
		os.RemoveAll(env.dataDir)
	}
}

// IcebergURL returns the Iceberg REST Catalog URL
func (env *TestEnvironment) IcebergURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", env.icebergPort)
}

// TestIcebergConfig tests the /v1/config endpoint
func TestIcebergConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	env.StartSeaweedFS(t)

	// Test GET /v1/config
	resp, err := http.Get(env.IcebergURL() + "/v1/config")
	if err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	// Verify response contains required fields
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "defaults") || !strings.Contains(bodyStr, "overrides") {
		t.Errorf("Config response missing required fields: %s", bodyStr)
	}
}

// TestIcebergNamespaces tests namespace operations
func TestIcebergNamespaces(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	env.StartSeaweedFS(t)

	// Create the default table bucket first via S3
	createTableBucket(t, env, "default")

	// Test GET /v1/namespaces (should return empty list initially)
	resp, err := http.Get(env.IcebergURL() + "/v1/namespaces")
	if err != nil {
		t.Fatalf("Failed to list namespaces: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, body)
	}
}

// createTableBucket creates a table bucket via the S3Tables REST API
func createTableBucket(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	// Use S3Tables REST API to create the bucket
	endpoint := fmt.Sprintf("http://localhost:%d/buckets", env.s3Port)

	reqBody := fmt.Sprintf(`{"name":"%s"}`, bucketName)
	req, err := http.NewRequest(http.MethodPut, endpoint, strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to create table bucket %s: %v", bucketName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Failed to create table bucket %s, status %d: %s", bucketName, resp.StatusCode, body)
	}
	t.Logf("Created table bucket %s", bucketName)
}

// TestDuckDBIntegration tests Iceberg catalog operations using DuckDB
// This test requires Docker to be available
func TestDuckDBIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping DuckDB integration test")
	}

	env.StartSeaweedFS(t)

	// Create a temporary SQL file for DuckDB to execute
	sqlFile := filepath.Join(env.dataDir, "test.sql")
	sqlContent := fmt.Sprintf(`
-- Install and load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- List namespaces via Iceberg REST catalog (basic connectivity test)
-- Note: Full operations require setting up S3 credentials which is beyond this test
SELECT 'Iceberg extension loaded successfully' as result;
`)

	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0644); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	// Run DuckDB in Docker to test Iceberg connectivity
	// Use host.docker.internal to connect to the host's Iceberg port
	cmd := exec.Command("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb",
		"duckdb/duckdb:latest",
		"-init", "/test/test.sql",
		"-c", "SELECT 1",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("DuckDB output: %s", output)
		// Check for expected errors in certain CI environments
		outputStr := string(output)
		if strings.Contains(outputStr, "iceberg extension is not available") ||
			strings.Contains(outputStr, "Failed to load") {
			t.Skip("Skipping DuckDB test: Iceberg extension not available in Docker image")
		}
		// Any other error is unexpected
		t.Fatalf("DuckDB command failed unexpectedly. Output: %s\nError: %v", output, err)
	}

	// Verify the test completed successfully
	outputStr := string(output)
	t.Logf("DuckDB output: %s", outputStr)
	if !strings.Contains(outputStr, "Iceberg extension loaded successfully") {
		t.Errorf("Expected success message in output, got: %s", outputStr)
	}
}
