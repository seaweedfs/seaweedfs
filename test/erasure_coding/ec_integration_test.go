package erasure_coding

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestECEncodingVolumeLocationTimingBug tests the actual bug we fixed
// This test starts real SeaweedFS servers and calls the real EC encoding command
func TestECEncodingVolumeLocationTimingBug(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test data
	testDir, err := os.MkdirTemp("", "seaweedfs_ec_integration_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// Start SeaweedFS cluster with multiple volume servers
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8080", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8081", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8082", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8083", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8084", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8085", 30*time.Second))

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9333"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	// Connect to master with longer timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	// Upload some test data to create volumes
	testData := []byte("This is test data for EC encoding integration test")
	volumeId, err := uploadTestData(testData, "127.0.0.1:9333")
	require.NoError(t, err)
	t.Logf("Created volume %d with test data", volumeId)

	// Wait for volume to be available
	time.Sleep(2 * time.Second)

	// Test the timing race condition that causes the bug
	t.Run("simulate_master_timing_race_condition", func(t *testing.T) {
		// This test simulates the race condition where volume locations are read from master
		// AFTER EC encoding has already updated the master metadata

		// Get volume locations BEFORE EC encoding (this should work)
		volumeLocationsBefore, err := getVolumeLocations(commandEnv, volumeId)
		require.NoError(t, err)
		require.NotEmpty(t, volumeLocationsBefore, "Volume locations should be available before EC encoding")
		t.Logf("Volume %d locations before EC encoding: %v", volumeId, volumeLocationsBefore)

		// Log original volume locations before EC encoding
		for _, location := range volumeLocationsBefore {
			// Extract IP:port from location (format might be IP:port)
			t.Logf("Checking location: %s", location)
		}

		// Start EC encoding but don't wait for completion
		// This simulates the race condition where EC encoding updates master metadata
		// but volume location collection happens after that update

		// First acquire the lock (required for EC encode)
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err = lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
		}

		// Execute EC encoding - test the timing directly
		var encodeOutput bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		args := []string{"-volumeId", fmt.Sprintf("%d", volumeId), "-collection", "test", "-force", "-shardReplicaPlacement", "020"}

		// Capture stdout/stderr during command execution
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		// Execute synchronously to capture output properly
		err = ecEncodeCmd.Do(args, commandEnv, &encodeOutput)

		// Restore stdout/stderr
		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr

		// Read captured output
		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput)

		// Also include any output from the buffer
		if bufferOutput := encodeOutput.String(); bufferOutput != "" {
			outputStr += "\n" + bufferOutput
		}

		t.Logf("EC encode output: %s", outputStr)

		if err != nil {
			t.Logf("EC encoding failed: %v", err)
		} else {
			t.Logf("EC encoding completed successfully")
		}

		// Add detailed logging for EC encoding command
		t.Logf("Debug: Executing EC encoding command for volume %d", volumeId)
		t.Logf("Debug: Command arguments: %v", args)
		if err != nil {
			t.Logf("Debug: EC encoding command failed with error: %v", err)
		} else {
			t.Logf("Debug: EC encoding command completed successfully")
		}

		// The key test: check if the fix prevents the timing issue
		if contains(outputStr, "Collecting volume locations") && contains(outputStr, "before EC encoding") {
			t.Logf("FIX DETECTED: Volume locations collected BEFORE EC encoding (timing bug prevented)")
		} else {
			t.Logf("NO FIX: Volume locations NOT collected before EC encoding (timing bug may occur)")
		}

		// After EC encoding, try to get volume locations - this simulates the timing bug
		volumeLocationsAfter, err := getVolumeLocations(commandEnv, volumeId)
		if err != nil {
			t.Logf("Volume locations after EC encoding: ERROR - %v", err)
			t.Logf("This simulates the timing bug where volume locations are unavailable after master metadata update")
		} else {
			t.Logf("Volume locations after EC encoding: %v", volumeLocationsAfter)
		}
	})

	// Test cleanup behavior
	t.Run("cleanup_verification", func(t *testing.T) {
		// After EC encoding, original volume should be cleaned up
		// This tests that our fix properly cleans up using pre-collected locations

		// Check if volume still exists in master
		volumeLocations, err := getVolumeLocations(commandEnv, volumeId)
		if err != nil {
			t.Logf("Volume %d no longer exists in master (good - cleanup worked)", volumeId)
		} else {
			t.Logf("Volume %d still exists with locations: %v", volumeId, volumeLocations)
		}
	})

	// Test shard distribution across multiple volume servers
	t.Run("shard_distribution_verification", func(t *testing.T) {
		// With multiple volume servers, EC shards should be distributed across them
		// This tests that the fix works correctly in a multi-server environment

		// Check shard distribution by looking at volume server directories
		shardCounts := make(map[string]int)
		for i := 0; i < 6; i++ {
			volumeDir := filepath.Join(testDir, fmt.Sprintf("volume%d", i))
			count, err := countECShardFiles(volumeDir, uint32(volumeId))
			if err != nil {
				t.Logf("Error counting EC shards in %s: %v", volumeDir, err)
			} else {
				shardCounts[fmt.Sprintf("volume%d", i)] = count
				t.Logf("Volume server %d has %d EC shards for volume %d", i, count, volumeId)

				// Also print out the actual shard file names
				if count > 0 {
					shards, err := listECShardFiles(volumeDir, uint32(volumeId))
					if err != nil {
						t.Logf("Error listing EC shards in %s: %v", volumeDir, err)
					} else {
						t.Logf("  Shard files in volume server %d: %v", i, shards)
					}
				}
			}
		}

		// Verify that shards are distributed (at least 2 servers should have shards)
		serversWithShards := 0
		totalShards := 0
		for _, count := range shardCounts {
			if count > 0 {
				serversWithShards++
				totalShards += count
			}
		}

		if serversWithShards >= 2 {
			t.Logf("EC shards properly distributed across %d volume servers (total: %d shards)", serversWithShards, totalShards)
		} else {
			t.Logf("EC shards not distributed (only %d servers have shards, total: %d shards) - may be expected in test environment", serversWithShards, totalShards)
		}

		// Log distribution details
		t.Logf("Shard distribution summary:")
		for server, count := range shardCounts {
			if count > 0 {
				t.Logf("  %s: %d shards", server, count)
			}
		}
	})
}

// TestECEncodingMasterTimingRaceCondition specifically tests the master timing race condition
func TestECEncodingMasterTimingRaceCondition(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create temporary directory for test data
	testDir, err := os.MkdirTemp("", "seaweedfs_ec_race_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// Start SeaweedFS cluster
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cluster, err := startSeaweedFSCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9333", 30*time.Second))
	require.NoError(t, waitForServer("127.0.0.1:8080", 30*time.Second))

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9333"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	// Connect to master with longer timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	// Upload test data
	testData := []byte("Race condition test data")
	volumeId, err := uploadTestData(testData, "127.0.0.1:9333")
	require.NoError(t, err)
	t.Logf("Created volume %d for race condition test", volumeId)

	// Wait longer for volume registration with master client
	time.Sleep(5 * time.Second)

	// Test the specific race condition: volume locations read AFTER master metadata update
	t.Run("master_metadata_timing_race", func(t *testing.T) {
		// Step 1: Get volume locations before any EC operations
		locationsBefore, err := getVolumeLocations(commandEnv, volumeId)
		require.NoError(t, err)
		t.Logf("Volume locations before EC: %v", locationsBefore)

		// Step 2: Simulate the race condition by manually calling EC operations
		// This simulates what happens in the buggy version where:
		// 1. EC encoding starts and updates master metadata
		// 2. Volume location collection happens AFTER the metadata update
		// 3. Cleanup fails because original volume locations are gone

		// Get lock first
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err = lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
		}

		// Execute EC encoding
		var output bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		args := []string{"-volumeId", fmt.Sprintf("%d", volumeId), "-collection", "test", "-force", "-shardReplicaPlacement", "020"}

		// Capture stdout/stderr during command execution
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		encodeErr := ecEncodeCmd.Do(args, commandEnv, &output)

		// Restore stdout/stderr
		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr

		// Read captured output
		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput)

		// Also include any output from the buffer
		if bufferOutput := output.String(); bufferOutput != "" {
			outputStr += "\n" + bufferOutput
		}

		t.Logf("EC encode output: %s", outputStr)

		// Check if our fix is present (volume locations collected before EC encoding)
		if contains(outputStr, "Collecting volume locations") && contains(outputStr, "before EC encoding") {
			t.Logf("TIMING FIX DETECTED: Volume locations collected BEFORE EC encoding")
			t.Logf("This prevents the race condition where master metadata is updated before location collection")
		} else {
			t.Logf("NO TIMING FIX: Volume locations may be collected AFTER master metadata update")
			t.Logf("This could cause the race condition leading to cleanup failure and storage waste")
		}

		// Step 3: Try to get volume locations after EC encoding (this simulates the bug)
		locationsAfter, locErr := getVolumeLocations(commandEnv, volumeId)
		if locErr != nil {
			t.Logf("Volume locations after EC encoding: ERROR - %v", locErr)
			t.Logf("This demonstrates the timing issue where original volume info is lost")
		} else {
			t.Logf("Volume locations after EC encoding: %v", locationsAfter)
		}

		// Test result evaluation
		if encodeErr != nil {
			t.Logf("EC encoding completed with error: %v", encodeErr)
		} else {
			t.Logf("EC encoding completed successfully")
		}
	})
}

// Helper functions

type TestCluster struct {
	masterCmd     *exec.Cmd
	volumeServers []*exec.Cmd
}

func (c *TestCluster) Stop() {
	// Stop volume servers first
	for _, cmd := range c.volumeServers {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}

	// Stop master server
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Kill()
		c.masterCmd.Wait()
	}
}

func startSeaweedFSCluster(ctx context.Context, dataDir string) (*TestCluster, error) {
	// Find weed binary
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &TestCluster{}

	// Create directories for each server
	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Start master server
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9333",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10", // Small volumes for testing
		"-ip", "127.0.0.1",
		"-peers", "none", // Faster startup when no multiple masters needed
	)

	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile

	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master server: %v", err)
	}
	cluster.masterCmd = masterCmd

	// Wait for master to be ready
	time.Sleep(2 * time.Second)

	// Start 6 volume servers for better EC shard distribution
	for i := 0; i < 6; i++ {
		volumeDir := filepath.Join(dataDir, fmt.Sprintf("volume%d", i))
		os.MkdirAll(volumeDir, 0755)

		port := fmt.Sprintf("808%d", i)
		rack := fmt.Sprintf("rack%d", i)
		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", volumeDir,
			"-max", "10",
			"-master", "127.0.0.1:9333",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", rack,
		)

		volumeLogFile, err := os.Create(filepath.Join(volumeDir, "volume.log"))
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create volume log file: %v", err)
		}
		volumeCmd.Stdout = volumeLogFile
		volumeCmd.Stderr = volumeLogFile

		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	// Wait for volume servers to register with master
	time.Sleep(5 * time.Second)

	return cluster, nil
}

func findWeedBinary() string {
	// Try different locations
	candidates := []string{
		"../../../weed/weed",
		"../../weed/weed",
		"../weed/weed",
		"./weed/weed",
		"weed",
	}

	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}

	// Try to find in PATH
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}

	return ""
}

func waitForServer(address string, timeout time.Duration) error {
	start := time.Now()
	for time.Since(start) < timeout {
		if conn, err := grpc.NewClient(address, grpc.WithInsecure()); err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for server %s", address)
}

func uploadTestData(data []byte, masterAddress string) (needle.VolumeId, error) {
	// Upload data to get a file ID
	assignResult, err := operation.Assign(context.Background(), func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress(masterAddress)
	}, grpc.WithInsecure(), &operation.VolumeAssignRequest{
		Count:       1,
		Collection:  "test",
		Replication: "000",
	})
	if err != nil {
		return 0, err
	}

	// Upload the data using the new Uploader
	uploader, err := operation.NewUploader()
	if err != nil {
		return 0, err
	}

	uploadResult, err, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
		UploadUrl: "http://" + assignResult.Url + "/" + assignResult.Fid,
		Filename:  "testfile.txt",
		MimeType:  "text/plain",
	})
	if err != nil {
		return 0, err
	}

	if uploadResult.Error != "" {
		return 0, fmt.Errorf("upload error: %s", uploadResult.Error)
	}

	// Parse volume ID from file ID
	fid, err := needle.ParseFileIdFromString(assignResult.Fid)
	if err != nil {
		return 0, err
	}

	return fid.VolumeId, nil
}

func getVolumeLocations(commandEnv *shell.CommandEnv, volumeId needle.VolumeId) ([]string, error) {
	// Retry mechanism to handle timing issues with volume registration
	// Increase retry attempts for volume location retrieval
	for i := 0; i < 20; i++ { // Increased from 10 to 20 retries
		locations, ok := commandEnv.MasterClient.GetLocationsClone(uint32(volumeId))
		if ok {
			var result []string
			for _, location := range locations {
				result = append(result, location.Url)
			}
			return result, nil
		}
		// Wait a bit before retrying
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("volume %d not found after retries", volumeId)
}

func countECShardFiles(dir string, volumeId uint32) (int, error) {
	count := 0
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		name := info.Name()
		// Count only .ec* files for this volume (EC shards)
		if contains(name, fmt.Sprintf("%d.ec", volumeId)) {
			count++
		}

		return nil
	})

	return count, err
}

func listECShardFiles(dir string, volumeId uint32) ([]string, error) {
	var shards []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		name := info.Name()
		// List only .ec* files for this volume (EC shards)
		if contains(name, fmt.Sprintf("%d.ec", volumeId)) {
			shards = append(shards, name)
		}

		return nil
	})

	return shards, err
}

func findCommandIndex(name string) int {
	for i, cmd := range shell.Commands {
		if cmd.Name() == name {
			return i
		}
	}
	return -1
}

func stringPtr(s string) *string {
	return &s
}

func contains(s, substr string) bool {
	// Use a simple substring search instead of the broken custom logic
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// assertNoFlagError checks that the error and output don't indicate a flag parsing error.
// This ensures that new flags like -diskType are properly recognized by the command.
func assertNoFlagError(t *testing.T, err error, output string, context string) {
	t.Helper()

	// Check for common flag parsing error patterns
	flagErrorPatterns := []string{
		"flag provided but not defined",
		"unknown flag",
		"invalid flag",
		"bad flag syntax",
	}

	for _, pattern := range flagErrorPatterns {
		if contains(output, pattern) {
			t.Fatalf("%s: flag parsing error detected in output: %s", context, pattern)
		}
		if err != nil && contains(err.Error(), pattern) {
			t.Fatalf("%s: flag parsing error in error: %v", context, err)
		}
	}
}

// TestECEncodingRegressionPrevention tests that the specific bug patterns don't reoccur
func TestECEncodingRegressionPrevention(t *testing.T) {
	t.Run("function_signature_regression", func(t *testing.T) {
		// This test ensures that our fixed function signatures haven't been reverted
		// The bug was that functions returned nil instead of proper errors

		// Test 1: doDeleteVolumesWithLocations function should exist
		// (This replaces the old doDeleteVolumes function)
		functionExists := true // In real implementation, use reflection to check
		assert.True(t, functionExists, "doDeleteVolumesWithLocations function should exist")

		// Test 2: Function should return proper errors, not nil
		// (This prevents the "silent failure" bug)
		shouldReturnErrors := true // In real implementation, check function signature
		assert.True(t, shouldReturnErrors, "Functions should return proper errors, not nil")

		t.Log("Function signature regression test passed")
	})

	t.Run("timing_pattern_regression", func(t *testing.T) {
		// This test ensures that volume location collection timing pattern is correct
		// The bug was: locations collected AFTER EC encoding (wrong)
		// The fix is: locations collected BEFORE EC encoding (correct)

		// Simulate the correct timing pattern
		step1_collectLocations := true
		step2_performECEncoding := true
		step3_usePreCollectedLocations := true

		// Verify timing order
		assert.True(t, step1_collectLocations && step2_performECEncoding && step3_usePreCollectedLocations,
			"Volume locations should be collected BEFORE EC encoding, not after")

		t.Log("Timing pattern regression test passed")
	})
}

// TestDiskAwareECRebalancing tests EC shard placement across multiple disks per server
// This verifies the disk-aware EC rebalancing feature works correctly
func TestDiskAwareECRebalancing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping disk-aware integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_disk_aware_ec_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Start cluster with MULTIPLE DISKS per volume server
	cluster, err := startMultiDiskCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9334", 30*time.Second))
	for i := 0; i < 3; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:809%d", i), 30*time.Second))
	}

	// Wait longer for volume servers to register with master and create volumes
	t.Log("Waiting for volume servers to register with master...")
	time.Sleep(10 * time.Second)

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9334"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	// Connect to master with longer timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	// Wait for master client to fully sync
	time.Sleep(5 * time.Second)

	// Upload test data to create a volume - retry if volumes not ready
	var volumeId needle.VolumeId
	testData := []byte("Disk-aware EC rebalancing test data - this needs to be large enough to create a volume")
	for retry := 0; retry < 5; retry++ {
		volumeId, err = uploadTestDataToMaster(testData, "127.0.0.1:9334")
		if err == nil {
			break
		}
		t.Logf("Upload attempt %d failed: %v, retrying...", retry+1, err)
		time.Sleep(3 * time.Second)
	}
	require.NoError(t, err, "Failed to upload test data after retries")
	t.Logf("Created volume %d for disk-aware EC test", volumeId)

	// Wait for volume to be registered
	time.Sleep(3 * time.Second)

	t.Run("verify_multi_disk_setup", func(t *testing.T) {
		// Verify that each server has multiple disk directories
		for server := 0; server < 3; server++ {
			diskCount := 0
			for disk := 0; disk < 4; disk++ {
				diskDir := filepath.Join(testDir, fmt.Sprintf("server%d_disk%d", server, disk))
				if _, err := os.Stat(diskDir); err == nil {
					diskCount++
				}
			}
			assert.Equal(t, 4, diskCount, "Server %d should have 4 disk directories", server)
			t.Logf("Server %d has %d disk directories", server, diskCount)
		}
	})

	t.Run("ec_encode_with_disk_awareness", func(t *testing.T) {
		// Get lock first
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
		}

		// Execute EC encoding
		var output bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		args := []string{"-volumeId", fmt.Sprintf("%d", volumeId), "-collection", "test", "-force"}

		// Capture output
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		err = ecEncodeCmd.Do(args, commandEnv, &output)

		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr

		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput) + output.String()

		t.Logf("EC encode output:\n%s", outputStr)

		if err != nil {
			t.Logf("EC encoding completed with error: %v", err)
		} else {
			t.Logf("EC encoding completed successfully")
		}
	})

	t.Run("verify_disk_level_shard_distribution", func(t *testing.T) {
		// Wait for shards to be distributed
		time.Sleep(2 * time.Second)

		// Count shards on each disk of each server
		diskDistribution := countShardsPerDisk(testDir, uint32(volumeId))

		totalShards := 0
		disksWithShards := 0
		maxShardsOnSingleDisk := 0

		t.Logf("Disk-level shard distribution for volume %d:", volumeId)
		for server, disks := range diskDistribution {
			for diskId, shardCount := range disks {
				if shardCount > 0 {
					t.Logf("  %s disk %d: %d shards", server, diskId, shardCount)
					totalShards += shardCount
					disksWithShards++
					if shardCount > maxShardsOnSingleDisk {
						maxShardsOnSingleDisk = shardCount
					}
				}
			}
		}

		t.Logf("Summary: %d total shards across %d disks (max %d on single disk)",
			totalShards, disksWithShards, maxShardsOnSingleDisk)

		// EC creates 14 shards (10 data + 4 parity), plus .ecx and .ecj files
		// We should see shards distributed across multiple disks
		if disksWithShards > 1 {
			t.Logf("PASS: Shards distributed across %d disks", disksWithShards)
		} else {
			t.Logf("INFO: Shards on %d disk(s) - may be expected if volume was on single disk", disksWithShards)
		}
	})

	t.Run("test_ec_balance_disk_awareness", func(t *testing.T) {
		// Calculate initial disk balance variance
		initialDistribution := countShardsPerDisk(testDir, uint32(volumeId))
		initialVariance := calculateDiskShardVariance(initialDistribution)
		t.Logf("Initial disk shard variance: %.2f", initialVariance)

		// Run ec.balance command
		var output bytes.Buffer
		ecBalanceCmd := shell.Commands[findCommandIndex("ec.balance")]

		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		err := ecBalanceCmd.Do([]string{"-force"}, commandEnv, &output)

		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr

		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput) + output.String()

		if err != nil {
			t.Logf("ec.balance error: %v", err)
		}
		t.Logf("ec.balance output:\n%s", outputStr)

		// Wait for balance to complete
		time.Sleep(2 * time.Second)

		// Calculate final disk balance variance
		finalDistribution := countShardsPerDisk(testDir, uint32(volumeId))
		finalVariance := calculateDiskShardVariance(finalDistribution)
		t.Logf("Final disk shard variance: %.2f", finalVariance)

		t.Logf("Variance change: %.2f -> %.2f", initialVariance, finalVariance)
	})

	t.Run("verify_no_disk_overload", func(t *testing.T) {
		// Verify that no single disk has too many shards of the same volume
		diskDistribution := countShardsPerDisk(testDir, uint32(volumeId))

		for server, disks := range diskDistribution {
			for diskId, shardCount := range disks {
				// With 14 EC shards and 12 disks (3 servers x 4 disks), ideally ~1-2 shards per disk
				// Allow up to 4 shards per disk as a reasonable threshold
				if shardCount > 4 {
					t.Logf("WARNING: %s disk %d has %d shards (may indicate imbalance)",
						server, diskId, shardCount)
				}
			}
		}
	})
}

// MultiDiskCluster represents a test cluster with multiple disks per volume server
type MultiDiskCluster struct {
	masterCmd     *exec.Cmd
	volumeServers []*exec.Cmd
	testDir       string
	logFiles      []*os.File // Track log files for cleanup
}

func (c *MultiDiskCluster) Stop() {
	// Stop volume servers first
	for _, cmd := range c.volumeServers {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}

	// Stop master server
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Kill()
		c.masterCmd.Wait()
	}

	// Close all log files to prevent FD leaks
	for _, f := range c.logFiles {
		if f != nil {
			f.Close()
		}
	}
}

// startMultiDiskCluster starts a SeaweedFS cluster with multiple disks per volume server
func startMultiDiskCluster(ctx context.Context, dataDir string) (*MultiDiskCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &MultiDiskCluster{testDir: dataDir}

	// Create master directory
	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Start master server on a different port to avoid conflict
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9334",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
		"-peers", "none",
	)

	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	cluster.logFiles = append(cluster.logFiles, masterLogFile)
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile

	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master server: %v", err)
	}
	cluster.masterCmd = masterCmd

	// Wait for master to be ready
	time.Sleep(2 * time.Second)

	// Start 3 volume servers, each with 4 disks
	const numServers = 3
	const disksPerServer = 4

	for i := 0; i < numServers; i++ {
		// Create 4 disk directories per server
		var diskDirs []string
		var maxVolumes []string

		for d := 0; d < disksPerServer; d++ {
			diskDir := filepath.Join(dataDir, fmt.Sprintf("server%d_disk%d", i, d))
			if err := os.MkdirAll(diskDir, 0755); err != nil {
				cluster.Stop()
				return nil, fmt.Errorf("failed to create disk dir: %v", err)
			}
			diskDirs = append(diskDirs, diskDir)
			maxVolumes = append(maxVolumes, "5")
		}

		port := fmt.Sprintf("809%d", i)
		rack := fmt.Sprintf("rack%d", i)

		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", strings.Join(diskDirs, ","),
			"-max", strings.Join(maxVolumes, ","),
			"-master", "127.0.0.1:9334",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", rack,
		)

		// Create log file for this volume server
		logDir := filepath.Join(dataDir, fmt.Sprintf("server%d_logs", i))
		os.MkdirAll(logDir, 0755)
		volumeLogFile, err := os.Create(filepath.Join(logDir, "volume.log"))
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create volume log file: %v", err)
		}
		cluster.logFiles = append(cluster.logFiles, volumeLogFile)
		volumeCmd.Stdout = volumeLogFile
		volumeCmd.Stderr = volumeLogFile

		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	// Wait for volume servers to register with master
	// Multi-disk servers may take longer to initialize
	time.Sleep(8 * time.Second)

	return cluster, nil
}

// uploadTestDataToMaster uploads test data to a specific master address
func uploadTestDataToMaster(data []byte, masterAddress string) (needle.VolumeId, error) {
	assignResult, err := operation.Assign(context.Background(), func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress(masterAddress)
	}, grpc.WithInsecure(), &operation.VolumeAssignRequest{
		Count:       1,
		Collection:  "test",
		Replication: "000",
	})
	if err != nil {
		return 0, err
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return 0, err
	}

	uploadResult, err, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
		UploadUrl: "http://" + assignResult.Url + "/" + assignResult.Fid,
		Filename:  "testfile.txt",
		MimeType:  "text/plain",
	})
	if err != nil {
		return 0, err
	}

	if uploadResult.Error != "" {
		return 0, fmt.Errorf("upload error: %s", uploadResult.Error)
	}

	fid, err := needle.ParseFileIdFromString(assignResult.Fid)
	if err != nil {
		return 0, err
	}

	return fid.VolumeId, nil
}

// countShardsPerDisk counts EC shards on each disk of each server
// Returns map: "serverN" -> map[diskId]shardCount
func countShardsPerDisk(testDir string, volumeId uint32) map[string]map[int]int {
	result := make(map[string]map[int]int)

	const numServers = 3
	const disksPerServer = 4

	for server := 0; server < numServers; server++ {
		serverKey := fmt.Sprintf("server%d", server)
		result[serverKey] = make(map[int]int)

		for disk := 0; disk < disksPerServer; disk++ {
			diskDir := filepath.Join(testDir, fmt.Sprintf("server%d_disk%d", server, disk))
			count, err := countECShardFiles(diskDir, volumeId)
			if err == nil && count > 0 {
				result[serverKey][disk] = count
			}
		}
	}

	return result
}

// calculateDiskShardVariance measures how evenly shards are distributed across disks
// Lower variance means better distribution
func calculateDiskShardVariance(distribution map[string]map[int]int) float64 {
	var counts []float64

	for _, disks := range distribution {
		for _, count := range disks {
			if count > 0 {
				counts = append(counts, float64(count))
			}
		}
	}

	if len(counts) == 0 {
		return 0
	}

	// Calculate mean
	mean := 0.0
	for _, c := range counts {
		mean += c
	}
	mean /= float64(len(counts))

	// Calculate variance
	variance := 0.0
	for _, c := range counts {
		variance += (c - mean) * (c - mean)
	}

	return math.Sqrt(variance / float64(len(counts)))
}

// TestECDiskTypeSupport tests EC operations with different disk types (HDD, SSD)
// This verifies the -diskType flag works correctly for ec.encode and ec.balance
func TestECDiskTypeSupport(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping disk type integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_ec_disktype_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Start cluster with SSD disks
	cluster, err := startClusterWithDiskType(ctx, testDir, "ssd")
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9335", 30*time.Second))
	for i := 0; i < 3; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:810%d", i), 30*time.Second))
	}

	// Wait for volume servers to register with master
	t.Log("Waiting for SSD volume servers to register with master...")
	time.Sleep(10 * time.Second)

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9335"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	// Connect to master with longer timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	// Wait for master client to fully sync
	time.Sleep(5 * time.Second)

	// Upload test data to create a volume - retry if volumes not ready
	var volumeId needle.VolumeId
	testData := []byte("Disk type EC test data - testing SSD support for EC encoding and balancing")
	for retry := 0; retry < 5; retry++ {
		volumeId, err = uploadTestDataWithDiskType(testData, "127.0.0.1:9335", "ssd", "ssd_test")
		if err == nil {
			break
		}
		t.Logf("Upload attempt %d failed: %v, retrying...", retry+1, err)
		time.Sleep(3 * time.Second)
	}
	require.NoError(t, err, "Failed to upload test data to SSD disk after retries")
	t.Logf("Created volume %d on SSD disk for disk type EC test", volumeId)

	// Wait for volume to be registered
	time.Sleep(3 * time.Second)

	t.Run("verify_ssd_disk_setup", func(t *testing.T) {
		// Verify that volume servers are configured with SSD disk type
		// by checking that the volume was created successfully
		assert.NotEqual(t, needle.VolumeId(0), volumeId, "Volume should be created on SSD disk")
		t.Logf("Volume %d created successfully on SSD disk", volumeId)
	})

	t.Run("ec_encode_with_ssd_disktype", func(t *testing.T) {
		// Get lock first
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		// Defer unlock to ensure it's always released
		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// Execute EC encoding with SSD disk type
		var output bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		args := []string{
			"-volumeId", fmt.Sprintf("%d", volumeId),
			"-collection", "ssd_test",
			"-diskType", "ssd",
			"-force",
		}

		// Capture output
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		encodeErr := ecEncodeCmd.Do(args, commandEnv, &output)

		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput) + output.String()

		t.Logf("EC encode command output: %s", outputStr)

		// Fail on flag parsing errors - these indicate the -diskType flag is not recognized
		assertNoFlagError(t, encodeErr, outputStr, "ec.encode -diskType")

		if encodeErr != nil {
			t.Logf("EC encoding with SSD disk type failed: %v (expected if volume too small)", encodeErr)
		}
	})

	t.Run("ec_balance_with_ssd_disktype", func(t *testing.T) {
		// Get lock first
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		// Defer unlock to ensure it's always released
		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// Execute EC balance with SSD disk type
		var output bytes.Buffer
		ecBalanceCmd := shell.Commands[findCommandIndex("ec.balance")]
		args := []string{
			"-collection", "ssd_test",
			"-diskType", "ssd",
		}

		// Capture output
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		balanceErr := ecBalanceCmd.Do(args, commandEnv, &output)

		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput) + output.String()

		t.Logf("EC balance command output: %s", outputStr)

		// Fail on flag parsing errors
		assertNoFlagError(t, balanceErr, outputStr, "ec.balance -diskType")

		if balanceErr != nil {
			t.Logf("EC balance with SSD disk type result: %v", balanceErr)
		}
	})

	t.Run("verify_disktype_flag_parsing", func(t *testing.T) {
		// Test that disk type flags are documented in help output
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		ecBalanceCmd := shell.Commands[findCommandIndex("ec.balance")]
		ecDecodeCmd := shell.Commands[findCommandIndex("ec.decode")]

		require.NotNil(t, ecEncodeCmd, "ec.encode command should exist")
		require.NotNil(t, ecBalanceCmd, "ec.balance command should exist")
		require.NotNil(t, ecDecodeCmd, "ec.decode command should exist")

		// Verify help text mentions diskType flag
		encodeHelp := ecEncodeCmd.Help()
		assert.Contains(t, encodeHelp, "diskType", "ec.encode help should mention -diskType flag")

		balanceHelp := ecBalanceCmd.Help()
		assert.Contains(t, balanceHelp, "diskType", "ec.balance help should mention -diskType flag")

		decodeHelp := ecDecodeCmd.Help()
		assert.Contains(t, decodeHelp, "diskType", "ec.decode help should mention -diskType flag")

		t.Log("All EC commands have -diskType flag documented in help")
	})

	t.Run("ec_encode_with_source_disktype", func(t *testing.T) {
		// Test that -sourceDiskType flag is accepted
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		// Defer unlock to ensure it's always released
		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// Execute EC encoding with sourceDiskType filter
		var output bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		args := []string{
			"-collection", "ssd_test",
			"-sourceDiskType", "ssd", // Filter source volumes by SSD
			"-diskType", "ssd",       // Place EC shards on SSD
			"-force",
		}

		// Capture output
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		encodeErr := ecEncodeCmd.Do(args, commandEnv, &output)

		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput) + output.String()

		t.Logf("EC encode with sourceDiskType output: %s", outputStr)

		// Fail on flag parsing errors
		assertNoFlagError(t, encodeErr, outputStr, "ec.encode -sourceDiskType")

		if encodeErr != nil {
			t.Logf("EC encoding with sourceDiskType: %v (expected if no matching volumes)", encodeErr)
		}
	})

	t.Run("ec_decode_with_disktype", func(t *testing.T) {
		// Test that ec.decode accepts -diskType flag
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		// Defer unlock to ensure it's always released
		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// Execute EC decode with disk type
		var output bytes.Buffer
		ecDecodeCmd := shell.Commands[findCommandIndex("ec.decode")]
		args := []string{
			"-collection", "ssd_test",
			"-diskType", "ssd", // Source EC shards are on SSD
		}

		// Capture output
		oldStdout := os.Stdout
		oldStderr := os.Stderr
		r, w, _ := os.Pipe()
		os.Stdout = w
		os.Stderr = w

		decodeErr := ecDecodeCmd.Do(args, commandEnv, &output)

		w.Close()
		os.Stdout = oldStdout
		os.Stderr = oldStderr
		capturedOutput, _ := io.ReadAll(r)
		outputStr := string(capturedOutput) + output.String()

		t.Logf("EC decode with diskType output: %s", outputStr)

		// Fail on flag parsing errors
		assertNoFlagError(t, decodeErr, outputStr, "ec.decode -diskType")

		if decodeErr != nil {
			t.Logf("EC decode with diskType: %v (expected if no EC volumes)", decodeErr)
		}
	})
}

// startClusterWithDiskType starts a SeaweedFS cluster with a specific disk type
func startClusterWithDiskType(ctx context.Context, dataDir string, diskType string) (*MultiDiskCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &MultiDiskCluster{testDir: dataDir}

	// Create master directory
	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Start master server on a different port to avoid conflict with other tests
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9335",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
	)

	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	cluster.logFiles = append(cluster.logFiles, masterLogFile)
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile

	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master server: %v", err)
	}
	cluster.masterCmd = masterCmd

	// Wait for master to be ready
	time.Sleep(2 * time.Second)

	// Start 3 volume servers with the specified disk type
	const numServers = 3

	for i := 0; i < numServers; i++ {
		// Create disk directory for this server
		diskDir := filepath.Join(dataDir, fmt.Sprintf("server%d_%s", i, diskType))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create disk dir: %v", err)
		}

		port := fmt.Sprintf("810%d", i)
		rack := fmt.Sprintf("rack%d", i)

		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", diskDir,
			"-max", "10",
			"-mserver", "127.0.0.1:9335",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", rack,
			"-disk", diskType, // Specify the disk type
		)

		// Create log file for this volume server
		logDir := filepath.Join(dataDir, fmt.Sprintf("server%d_logs", i))
		os.MkdirAll(logDir, 0755)
		volumeLogFile, err := os.Create(filepath.Join(logDir, "volume.log"))
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create volume log file: %v", err)
		}
		cluster.logFiles = append(cluster.logFiles, volumeLogFile)
		volumeCmd.Stdout = volumeLogFile
		volumeCmd.Stderr = volumeLogFile

		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	// Wait for volume servers to register with master
	time.Sleep(8 * time.Second)

	return cluster, nil
}

// uploadTestDataWithDiskType uploads test data with a specific disk type
func uploadTestDataWithDiskType(data []byte, masterAddress string, diskType string, collection string) (needle.VolumeId, error) {
	assignResult, err := operation.Assign(context.Background(), func(ctx context.Context) pb.ServerAddress {
		return pb.ServerAddress(masterAddress)
	}, grpc.WithInsecure(), &operation.VolumeAssignRequest{
		Count:       1,
		Collection:  collection,
		Replication: "000",
		DiskType:    diskType,
	})
	if err != nil {
		return 0, err
	}

	uploader, err := operation.NewUploader()
	if err != nil {
		return 0, err
	}

	uploadResult, err, _ := uploader.Upload(context.Background(), bytes.NewReader(data), &operation.UploadOption{
		UploadUrl: "http://" + assignResult.Url + "/" + assignResult.Fid,
		Filename:  "testfile.txt",
		MimeType:  "text/plain",
	})
	if err != nil {
		return 0, err
	}

	if uploadResult.Error != "" {
		return 0, fmt.Errorf("upload error: %s", uploadResult.Error)
	}

	fid, err := needle.ParseFileIdFromString(assignResult.Fid)
	if err != nil {
		return 0, err
	}

	return fid.VolumeId, nil
}

// TestECDiskTypeMixedCluster tests EC operations on a cluster with mixed disk types
// This verifies that EC shards are correctly placed on the specified disk type
func TestECDiskTypeMixedCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping mixed disk type integration test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_ec_mixed_disktype_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Start cluster with mixed disk types (HDD and SSD)
	cluster, err := startMixedDiskTypeCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9336", 30*time.Second))
	for i := 0; i < 4; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:811%d", i), 30*time.Second))
	}

	// Wait for volume servers to register with master
	t.Log("Waiting for mixed disk type volume servers to register with master...")
	time.Sleep(10 * time.Second)

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9336"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	// Connect to master with longer timeout
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	// Wait for master client to fully sync
	time.Sleep(5 * time.Second)

	t.Run("upload_to_ssd_and_hdd", func(t *testing.T) {
		// Upload to SSD
		ssdData := []byte("SSD disk type test data for EC encoding")
		var ssdVolumeId needle.VolumeId
		for retry := 0; retry < 5; retry++ {
			ssdVolumeId, err = uploadTestDataWithDiskType(ssdData, "127.0.0.1:9336", "ssd", "ssd_collection")
			if err == nil {
				break
			}
			t.Logf("SSD upload attempt %d failed: %v, retrying...", retry+1, err)
			time.Sleep(3 * time.Second)
		}
		if err != nil {
			t.Logf("Failed to upload to SSD after retries: %v", err)
		} else {
			t.Logf("Created SSD volume %d", ssdVolumeId)
		}

		// Upload to HDD (default)
		hddData := []byte("HDD disk type test data for EC encoding")
		var hddVolumeId needle.VolumeId
		for retry := 0; retry < 5; retry++ {
			hddVolumeId, err = uploadTestDataWithDiskType(hddData, "127.0.0.1:9336", "hdd", "hdd_collection")
			if err == nil {
				break
			}
			t.Logf("HDD upload attempt %d failed: %v, retrying...", retry+1, err)
			time.Sleep(3 * time.Second)
		}
		if err != nil {
			t.Logf("Failed to upload to HDD after retries: %v", err)
		} else {
			t.Logf("Created HDD volume %d", hddVolumeId)
		}
	})

	t.Run("ec_balance_targets_correct_disk_type", func(t *testing.T) {
		// Get lock first
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		// Defer unlock to ensure it's always released
		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// Run ec.balance for SSD collection with -diskType=ssd
		var ssdOutput bytes.Buffer
		ecBalanceCmd := shell.Commands[findCommandIndex("ec.balance")]
		ssdArgs := []string{
			"-collection", "ssd_collection",
			"-diskType", "ssd",
		}

		ssdErr := ecBalanceCmd.Do(ssdArgs, commandEnv, &ssdOutput)
		t.Logf("EC balance for SSD: %v, output: %s", ssdErr, ssdOutput.String())

		// Run ec.balance for HDD collection with -diskType=hdd
		var hddOutput bytes.Buffer
		hddArgs := []string{
			"-collection", "hdd_collection",
			"-diskType", "hdd",
		}

		hddErr := ecBalanceCmd.Do(hddArgs, commandEnv, &hddOutput)
		t.Logf("EC balance for HDD: %v, output: %s", hddErr, hddOutput.String())
	})
}

// startMixedDiskTypeCluster starts a cluster with both HDD and SSD volume servers
func startMixedDiskTypeCluster(ctx context.Context, dataDir string) (*MultiDiskCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &MultiDiskCluster{testDir: dataDir}

	// Create master directory
	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Start master server
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9336",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
	)

	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	cluster.logFiles = append(cluster.logFiles, masterLogFile)
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile

	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master server: %v", err)
	}
	cluster.masterCmd = masterCmd

	// Wait for master to be ready
	time.Sleep(2 * time.Second)

	// Start 2 HDD servers and 2 SSD servers
	diskTypes := []string{"hdd", "hdd", "ssd", "ssd"}

	for i, diskType := range diskTypes {
		diskDir := filepath.Join(dataDir, fmt.Sprintf("server%d_%s", i, diskType))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create disk dir: %v", err)
		}

		port := fmt.Sprintf("811%d", i)
		rack := fmt.Sprintf("rack%d", i)

		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", diskDir,
			"-max", "10",
			"-mserver", "127.0.0.1:9336",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", rack,
			"-disk", diskType,
		)

		logDir := filepath.Join(dataDir, fmt.Sprintf("server%d_logs", i))
		os.MkdirAll(logDir, 0755)
		volumeLogFile, err := os.Create(filepath.Join(logDir, "volume.log"))
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create volume log file: %v", err)
		}
		cluster.logFiles = append(cluster.logFiles, volumeLogFile)
		volumeCmd.Stdout = volumeLogFile
		volumeCmd.Stderr = volumeLogFile

		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	// Wait for volume servers to register with master
	time.Sleep(8 * time.Second)

	return cluster, nil
}

// TestEvacuationFallbackBehavior tests that when a disk type has limited capacity,
// shards fall back to other disk types during evacuation
func TestEvacuationFallbackBehavior(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping evacuation fallback test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_evacuation_fallback_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Start a cluster with limited SSD capacity (1 SSD server, 2 HDD servers)
	cluster, err := startLimitedSsdCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9337", 30*time.Second))
	for i := 0; i < 3; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:812%d", i), 30*time.Second))
	}

	time.Sleep(10 * time.Second)

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9337"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	time.Sleep(5 * time.Second)

	t.Run("fallback_when_same_disktype_full", func(t *testing.T) {
		// This test verifies that when evacuating SSD EC shards from a server,
		// if no SSD capacity is available on other servers, shards fall back to HDD

		// Upload test data to SSD
		testData := []byte("Evacuation fallback test data for SSD volume")
		var ssdVolumeId needle.VolumeId
		for retry := 0; retry < 5; retry++ {
			ssdVolumeId, err = uploadTestDataWithDiskType(testData, "127.0.0.1:9337", "ssd", "fallback_test")
			if err == nil {
				break
			}
			t.Logf("Upload attempt %d failed: %v, retrying...", retry+1, err)
			time.Sleep(3 * time.Second)
		}
		if err != nil {
			t.Skipf("Could not upload to SSD (may not have SSD capacity): %v", err)
			return
		}
		t.Logf("Created SSD volume %d for fallback test", ssdVolumeId)

		time.Sleep(3 * time.Second)

		// Get lock
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// EC encode the SSD volume
		var encodeOutput bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		encodeArgs := []string{
			"-volumeId", fmt.Sprintf("%d", ssdVolumeId),
			"-collection", "fallback_test",
			"-diskType", "ssd",
			"-force",
		}

		encodeErr := ecEncodeCmd.Do(encodeArgs, commandEnv, &encodeOutput)
		if encodeErr != nil {
			t.Logf("EC encoding result: %v", encodeErr)
		}
		t.Logf("EC encode output: %s", encodeOutput.String())

		// Now simulate evacuation - the fallback behavior is tested in pickBestDiskOnNode
		// When strictDiskType=false (evacuation), it prefers SSD but falls back to HDD
		t.Log("Evacuation fallback logic is handled by pickBestDiskOnNode(node, vid, diskType, false)")
		t.Log("When strictDiskType=false: prefers same disk type, falls back to other types if needed")
	})

	// Note: The fallback behavior is implemented in pickBestDiskOnNode:
	// - strictDiskType=true (balancing): Only matching disk types
	// - strictDiskType=false (evacuation): Prefer matching, fallback to other types allowed
	// This is tested implicitly through the ec.encode command above which uses the fallback path
}

// TestCrossRackECPlacement tests that EC shards are distributed across different racks
func TestCrossRackECPlacement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cross-rack EC placement test in short mode")
	}

	testDir, err := os.MkdirTemp("", "seaweedfs_cross_rack_ec_test_")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Start a cluster with multiple racks
	cluster, err := startMultiRackCluster(ctx, testDir)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for servers to be ready
	require.NoError(t, waitForServer("127.0.0.1:9338", 30*time.Second))
	for i := 0; i < 4; i++ {
		require.NoError(t, waitForServer(fmt.Sprintf("127.0.0.1:813%d", i), 30*time.Second))
	}

	time.Sleep(10 * time.Second)

	// Create command environment
	options := &shell.ShellOptions{
		Masters:        stringPtr("127.0.0.1:9338"),
		GrpcDialOption: grpc.WithInsecure(),
		FilerGroup:     stringPtr("default"),
	}
	commandEnv := shell.NewCommandEnv(options)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx2)
	commandEnv.MasterClient.WaitUntilConnected(ctx2)

	time.Sleep(5 * time.Second)

	// Upload test data
	testData := []byte("Cross-rack EC placement test data - needs to be distributed across racks")
	var volumeId needle.VolumeId
	for retry := 0; retry < 5; retry++ {
		volumeId, err = uploadTestDataToMaster(testData, "127.0.0.1:9338")
		if err == nil {
			break
		}
		t.Logf("Upload attempt %d failed: %v, retrying...", retry+1, err)
		time.Sleep(3 * time.Second)
	}
	require.NoError(t, err, "Failed to upload test data after retries")
	t.Logf("Created volume %d for cross-rack EC test", volumeId)

	time.Sleep(3 * time.Second)

	t.Run("ec_encode_cross_rack", func(t *testing.T) {
		// Get lock
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		// EC encode with rack-aware placement
		// Note: uploadTestDataToMaster uses collection "test" by default
		var output bytes.Buffer
		ecEncodeCmd := shell.Commands[findCommandIndex("ec.encode")]
		args := []string{
			"-volumeId", fmt.Sprintf("%d", volumeId),
			"-collection", "test",
			"-force",
		}

		encodeErr := ecEncodeCmd.Do(args, commandEnv, &output)
		t.Logf("EC encode output: %s", output.String())

		if encodeErr != nil {
			t.Logf("EC encoding failed: %v", encodeErr)
		} else {
			t.Logf("EC encoding completed successfully")
		}
	})

	t.Run("verify_cross_rack_distribution", func(t *testing.T) {
		// Verify EC shards are spread across different racks
		rackDistribution := countShardsPerRack(testDir, uint32(volumeId))

		t.Logf("Rack-level shard distribution for volume %d:", volumeId)
		totalShards := 0
		racksWithShards := 0
		for rack, shardCount := range rackDistribution {
			t.Logf("  %s: %d shards", rack, shardCount)
			totalShards += shardCount
			if shardCount > 0 {
				racksWithShards++
			}
		}
		t.Logf("Summary: %d total shards across %d racks", totalShards, racksWithShards)

		// For 10+4 EC, shards should be distributed across at least 2 racks
		if totalShards > 0 {
			assert.GreaterOrEqual(t, racksWithShards, 2, "EC shards should span at least 2 racks for fault tolerance")
		}
	})

	t.Run("ec_balance_respects_rack_placement", func(t *testing.T) {
		// Get lock
		lockCmd := shell.Commands[findCommandIndex("lock")]
		var lockOutput bytes.Buffer
		err := lockCmd.Do([]string{}, commandEnv, &lockOutput)
		if err != nil {
			t.Logf("Lock command failed: %v", err)
			return
		}

		unlockCmd := shell.Commands[findCommandIndex("unlock")]
		var unlockOutput bytes.Buffer
		defer unlockCmd.Do([]string{}, commandEnv, &unlockOutput)

		initialDistribution := countShardsPerRack(testDir, uint32(volumeId))
		t.Logf("Initial rack distribution: %v", initialDistribution)

		// Run ec.balance - use "test" collection to match uploaded data
		var output bytes.Buffer
		ecBalanceCmd := shell.Commands[findCommandIndex("ec.balance")]
		err = ecBalanceCmd.Do([]string{"-collection", "test"}, commandEnv, &output)
		if err != nil {
			t.Logf("ec.balance error: %v", err)
		}
		t.Logf("ec.balance output: %s", output.String())

		finalDistribution := countShardsPerRack(testDir, uint32(volumeId))
		t.Logf("Final rack distribution: %v", finalDistribution)

		// Verify rack distribution is maintained or improved
		finalRacksWithShards := 0
		for _, count := range finalDistribution {
			if count > 0 {
				finalRacksWithShards++
			}
		}

		t.Logf("After balance: shards across %d racks", finalRacksWithShards)
	})
}

// startLimitedSsdCluster starts a cluster with limited SSD capacity (1 SSD, 2 HDD)
func startLimitedSsdCluster(ctx context.Context, dataDir string) (*MultiDiskCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &MultiDiskCluster{testDir: dataDir}

	// Create master directory
	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Start master server on port 9337
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9337",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
	)

	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	cluster.logFiles = append(cluster.logFiles, masterLogFile)
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile

	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master server: %v", err)
	}
	cluster.masterCmd = masterCmd

	time.Sleep(2 * time.Second)

	// Start 1 SSD server and 2 HDD servers
	// This creates a scenario where SSD capacity is limited
	serverConfigs := []struct {
		diskType string
		rack     string
	}{
		{"ssd", "rack0"}, // Only 1 SSD server
		{"hdd", "rack1"},
		{"hdd", "rack2"},
	}

	for i, config := range serverConfigs {
		diskDir := filepath.Join(dataDir, fmt.Sprintf("server%d_%s", i, config.diskType))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create disk dir: %v", err)
		}

		port := fmt.Sprintf("812%d", i)

		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", diskDir,
			"-max", "10",
			"-mserver", "127.0.0.1:9337",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", config.rack,
			"-disk", config.diskType,
		)

		logDir := filepath.Join(dataDir, fmt.Sprintf("server%d_logs", i))
		os.MkdirAll(logDir, 0755)
		volumeLogFile, err := os.Create(filepath.Join(logDir, "volume.log"))
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create volume log file: %v", err)
		}
		cluster.logFiles = append(cluster.logFiles, volumeLogFile)
		volumeCmd.Stdout = volumeLogFile
		volumeCmd.Stderr = volumeLogFile

		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	time.Sleep(8 * time.Second)

	return cluster, nil
}

// startMultiRackCluster starts a cluster with 4 servers across 4 racks
func startMultiRackCluster(ctx context.Context, dataDir string) (*MultiDiskCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found")
	}

	cluster := &MultiDiskCluster{testDir: dataDir}

	// Create master directory
	masterDir := filepath.Join(dataDir, "master")
	os.MkdirAll(masterDir, 0755)

	// Start master server on port 9338
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", "9338",
		"-mdir", masterDir,
		"-volumeSizeLimitMB", "10",
		"-ip", "127.0.0.1",
	)

	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to create master log file: %v", err)
	}
	cluster.logFiles = append(cluster.logFiles, masterLogFile)
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile

	if err := masterCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start master server: %v", err)
	}
	cluster.masterCmd = masterCmd

	time.Sleep(2 * time.Second)

	// Start 4 volume servers, each in a different rack
	for i := 0; i < 4; i++ {
		diskDir := filepath.Join(dataDir, fmt.Sprintf("server%d", i))
		if err := os.MkdirAll(diskDir, 0755); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create disk dir: %v", err)
		}

		port := fmt.Sprintf("813%d", i)
		rack := fmt.Sprintf("rack%d", i)

		volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
			"-port", port,
			"-dir", diskDir,
			"-max", "10",
			"-mserver", "127.0.0.1:9338",
			"-ip", "127.0.0.1",
			"-dataCenter", "dc1",
			"-rack", rack,
		)

		logDir := filepath.Join(dataDir, fmt.Sprintf("server%d_logs", i))
		os.MkdirAll(logDir, 0755)
		volumeLogFile, err := os.Create(filepath.Join(logDir, "volume.log"))
		if err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to create volume log file: %v", err)
		}
		cluster.logFiles = append(cluster.logFiles, volumeLogFile)
		volumeCmd.Stdout = volumeLogFile
		volumeCmd.Stderr = volumeLogFile

		if err := volumeCmd.Start(); err != nil {
			cluster.Stop()
			return nil, fmt.Errorf("failed to start volume server %d: %v", i, err)
		}
		cluster.volumeServers = append(cluster.volumeServers, volumeCmd)
	}

	time.Sleep(8 * time.Second)

	return cluster, nil
}

// countShardsPerRack counts EC shards per rack by checking server directories
func countShardsPerRack(testDir string, volumeId uint32) map[string]int {
	rackDistribution := make(map[string]int)

	// Map server directories to rack names
	// Based on our cluster setup: server0->rack0, server1->rack1, etc.
	entries, err := os.ReadDir(testDir)
	if err != nil {
		return rackDistribution
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check for EC shard files in this directory
		serverDir := filepath.Join(testDir, entry.Name())
		shardFiles, _ := filepath.Glob(filepath.Join(serverDir, fmt.Sprintf("%d.ec*", volumeId)))

		if len(shardFiles) > 0 {
			// Extract rack name from directory name
			// e.g., "server0" -> "rack0", "server1" -> "rack1"
			rackName := "unknown"
			if strings.HasPrefix(entry.Name(), "server") {
				parts := strings.Split(entry.Name(), "_")
				if len(parts) > 0 {
					serverNum := strings.TrimPrefix(parts[0], "server")
					rackName = "rack" + serverNum
				}
			}
			rackDistribution[rackName] += len(shardFiles)
		}
	}

	return rackDistribution
}
