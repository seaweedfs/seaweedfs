package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/weed/telemetry"
	protobuf "google.golang.org/protobuf/proto"
)

const (
	serverPort = "18080" // Use different port to avoid conflicts
	serverURL  = "http://localhost:" + serverPort
)

func main() {
	fmt.Println("Starting SeaweedFS Telemetry Integration Test")

	// Start telemetry server
	fmt.Println("Starting telemetry server...")
	serverCmd, err := startTelemetryServer()
	if err != nil {
		log.Fatalf("Failed to start telemetry server: %v", err)
	}
	defer stopServer(serverCmd)

	// Wait for server to start
	if !waitForServer(serverURL+"/health", 15*time.Second) {
		log.Fatal("Telemetry server failed to start")
	}
	fmt.Println("Telemetry server started successfully")

	// Test protobuf marshaling first
	fmt.Println("Testing protobuf marshaling...")
	if err := testProtobufMarshaling(); err != nil {
		log.Fatalf("Protobuf marshaling test failed: %v", err)
	}
	fmt.Println("Protobuf marshaling test passed")

	// Test protobuf client
	fmt.Println("Testing protobuf telemetry client...")
	if err := testTelemetryClient(); err != nil {
		log.Fatalf("Telemetry client test failed: %v", err)
	}
	fmt.Println("Telemetry client test passed")

	// Test server metrics endpoint
	fmt.Println("Testing Prometheus metrics endpoint...")
	if err := testMetricsEndpoint(); err != nil {
		log.Fatalf("Metrics endpoint test failed: %v", err)
	}
	fmt.Println("Metrics endpoint test passed")

	// Test stats API
	fmt.Println("Testing stats API...")
	if err := testStatsAPI(); err != nil {
		log.Fatalf("Stats API test failed: %v", err)
	}
	fmt.Println("Stats API test passed")

	// Test instances API
	fmt.Println("Testing instances API...")
	if err := testInstancesAPI(); err != nil {
		log.Fatalf("Instances API test failed: %v", err)
	}
	fmt.Println("Instances API test passed")

	fmt.Println("All telemetry integration tests passed!")
}

func startTelemetryServer() (*exec.Cmd, error) {
	// Get the directory where this test is running
	testDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get working directory: %v", err)
	}

	// Navigate to the server directory (from main seaweedfs directory)
	serverDir := filepath.Join(testDir, "telemetry", "server")

	cmd := exec.Command("go", "run", ".",
		"-port="+serverPort,
		"-dashboard=false",
		"-cleanup=1m",
		"-max-age=1h")

	cmd.Dir = serverDir

	// Create log files for server output
	logFile, err := os.Create("telemetry-server-test.log")
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %v", err)
	}

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start server: %v", err)
	}

	return cmd, nil
}

func stopServer(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()

		// Clean up log file
		os.Remove("telemetry-server-test.log")
	}
}

func waitForServer(url string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	fmt.Printf("Waiting for server at %s...\n", url)

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			resp, err := http.Get(url)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return true
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func testProtobufMarshaling() error {
	// Test protobuf marshaling/unmarshaling
	testData := &proto.TelemetryData{
		ClusterId:         "test-cluster-12345",
		Version:           "test-3.45",
		Os:                "linux/amd64",
		VolumeServerCount: 2,
		TotalDiskBytes:    1000000,
		TotalVolumeCount:  10,
		FilerCount:        1,
		BrokerCount:       1,
		Timestamp:         time.Now().Unix(),
	}

	// Marshal
	data, err := protobuf.Marshal(testData)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %v", err)
	}

	fmt.Printf("   Protobuf size: %d bytes\n", len(data))

	// Unmarshal
	testData2 := &proto.TelemetryData{}
	if err := protobuf.Unmarshal(data, testData2); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %v", err)
	}

	// Verify data
	if testData2.ClusterId != testData.ClusterId {
		return fmt.Errorf("protobuf data mismatch: expected %s, got %s",
			testData.ClusterId, testData2.ClusterId)
	}

	if testData2.VolumeServerCount != testData.VolumeServerCount {
		return fmt.Errorf("volume server count mismatch: expected %d, got %d",
			testData.VolumeServerCount, testData2.VolumeServerCount)
	}

	return nil
}

func testTelemetryClient() error {
	// Create telemetry client
	client := telemetry.NewClient(serverURL+"/api/collect", true)

	// Create test data using protobuf format
	testData := &proto.TelemetryData{
		Version:           "test-3.45",
		Os:                "linux/amd64",
		VolumeServerCount: 3,
		TotalDiskBytes:    1073741824, // 1GB
		TotalVolumeCount:  50,
		FilerCount:        2,
		BrokerCount:       1,
		Timestamp:         time.Now().Unix(),
	}

	// Send telemetry data
	if err := client.SendTelemetry(testData); err != nil {
		return fmt.Errorf("failed to send telemetry: %v", err)
	}

	fmt.Printf("   Sent telemetry for cluster: %s\n", client.GetInstanceID())

	// Wait a bit for processing
	time.Sleep(2 * time.Second)

	return nil
}

func testMetricsEndpoint() error {
	resp, err := http.Get(serverURL + "/metrics")
	if err != nil {
		return fmt.Errorf("failed to get metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}

	// Read response and check for expected metrics
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read metrics response: %v", err)
	}

	contentStr := string(content)
	expectedMetrics := []string{
		"seaweedfs_telemetry_total_clusters",
		"seaweedfs_telemetry_active_clusters",
		"seaweedfs_telemetry_reports_received_total",
		"seaweedfs_telemetry_volume_servers",
		"seaweedfs_telemetry_disk_bytes",
		"seaweedfs_telemetry_volume_count",
		"seaweedfs_telemetry_filer_count",
		"seaweedfs_telemetry_broker_count",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(contentStr, metric) {
			return fmt.Errorf("missing expected metric: %s", metric)
		}
	}

	// Check that we have at least one report received
	if !strings.Contains(contentStr, "seaweedfs_telemetry_reports_received_total 1") {
		fmt.Printf("   Warning: Expected at least 1 report received, metrics content:\n%s\n", contentStr)
	}

	fmt.Printf("   Found %d expected metrics\n", len(expectedMetrics))

	return nil
}

func testStatsAPI() error {
	resp, err := http.Get(serverURL + "/api/stats")
	if err != nil {
		return fmt.Errorf("failed to get stats: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stats API returned status %d", resp.StatusCode)
	}

	// Read and verify JSON response
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read stats response: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "total_instances") {
		return fmt.Errorf("stats response missing total_instances field")
	}

	fmt.Printf("   Stats response: %s\n", contentStr)

	return nil
}

func testInstancesAPI() error {
	resp, err := http.Get(serverURL + "/api/instances?limit=10")
	if err != nil {
		return fmt.Errorf("failed to get instances: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("instances API returned status %d", resp.StatusCode)
	}

	// Read response
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read instances response: %v", err)
	}

	fmt.Printf("   Instances response length: %d bytes\n", len(content))

	return nil
}
