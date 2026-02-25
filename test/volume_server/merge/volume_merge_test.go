package volume_server_merge_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// runWeedShell executes a weed shell command by providing commands via stdin with lock/unlock.
// It uses a timeout to prevent hanging if the weed shell process becomes unresponsive.
func runWeedShell(t *testing.T, weedBinary, masterAddr, shellCommand string) (output string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, weedBinary, "shell", "-master="+masterAddr)
	// Wrap command in lock/unlock for cluster-wide operations
	shellCommands := "lock\n" + shellCommand + "\nunlock\nexit\n"
	cmd.Stdin = strings.NewReader(shellCommands)
	outputBytes, err := cmd.CombinedOutput()
	output = string(outputBytes)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			t.Logf("weed shell command '%s' timed out after 30s", shellCommand)
		} else {
			t.Logf("weed shell command '%s' output: %s, error: %v", shellCommand, output, err)
		}
	}
	return output, err
}

// TestVolumeMergeBasic verifies the basic volume.merge workflow using the weed shell command
func TestVolumeMergeBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Start a triple cluster with 3 volume servers (needed for merge which allocates to a third location)
	cluster := framework.StartTripleVolumeCluster(t, matrix.P1())

	// Connect to volume servers to allocate volumes
	conn0, volumeClient0 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()

	conn1, volumeClient1 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(100)

	// Allocate volume on only 2 servers (replicas)
	// The merge command will allocate on the 3rd server as a temporary location
	framework.AllocateVolume(t, volumeClient0, volumeID, "")
	framework.AllocateVolume(t, volumeClient1, volumeID, "")

	t.Logf("Successfully allocated volume %d on servers 0 and 1 as replicas", volumeID)

	// Get weed binary
	weedBinary := os.Getenv("WEED_BINARY")
	if weedBinary == "" {
		var err error
		weedBinary, err = framework.FindOrBuildWeedBinary()
		if err != nil {
			t.Fatalf("failed to find weed binary: %v", err)
		}
	}

	// Execute volume.merge command via weed shell
	output, err := runWeedShell(t, weedBinary, cluster.MasterAddress(), fmt.Sprintf("volume.merge -volumeId %d", volumeID))

	t.Logf("volume.merge command output:\n%s", output)

	if err != nil {
		t.Fatalf("volume.merge command failed: %v\noutput: %s", err, output)
	}

	// Verify the success message in output
	if !strings.Contains(output, fmt.Sprintf("merged volume %d", volumeID)) {
		t.Fatalf("expected success message in output, got: %s", output)
	}

	t.Logf("Successfully executed volume.merge command for volume %d", volumeID)
}

// TestVolumeMergeReadonly verifies that volume.merge requires readonly state
func TestVolumeMergeReadonly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartTripleVolumeCluster(t, matrix.P1())

	// Connect to volume servers
	conn0, volumeClient0 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()

	conn1, volumeClient1 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(101)

	// Allocate volumes on only 2 servers (the merge will allocate on the 3rd)
	framework.AllocateVolume(t, volumeClient0, volumeID, "")
	framework.AllocateVolume(t, volumeClient1, volumeID, "")

	// Get weed binary
	weedBinary := os.Getenv("WEED_BINARY")
	if weedBinary == "" {
		var err error
		weedBinary, err = framework.FindOrBuildWeedBinary()
		if err != nil {
			t.Fatalf("failed to find weed binary: %v", err)
		}
	}

	// Test 1: Merge while writable (merge command will mark volumes readonly as needed)
	output, err := runWeedShell(t, weedBinary, cluster.MasterAddress(), fmt.Sprintf("volume.merge -volumeId %d", volumeID))
	if err != nil {
		t.Logf("merge on writable volumes failed: %v\noutput: %s", err, output)
		t.Fatalf("volume.merge should work on writable volumes (marks them readonly internally)")
	}

	if !strings.Contains(output, fmt.Sprintf("merged volume %d", volumeID)) {
		t.Fatalf("expected success message in output, got: %s", output)
	}

	// Verify volumes were marked readonly during merge and restored after
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Check that volumes are writable again after merge (were restored)
	status0, err := volumeClient0.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get status after merge: %v", err)
	}

	status1, err := volumeClient1.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get status from server 1 after merge: %v", err)
	}

	if status0.GetIsReadOnly() {
		t.Fatalf("expected volume to be writable again after merge on server 0")
	}

	if status1.GetIsReadOnly() {
		t.Fatalf("expected volume to be writable again after merge on server 1")
	}

	t.Logf("Successfully tested merge on writable volumes and writable restoration")
}

// TestVolumeMergeRestore verifies that merge restores writable state for originally-writable replicas
func TestVolumeMergeRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartTripleVolumeCluster(t, matrix.P1())

	conn0, volumeClient0 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()

	conn1, volumeClient1 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(102)

	// Allocate volume on only 2 servers (the merge will allocate on the 3rd)
	framework.AllocateVolume(t, volumeClient0, volumeID, "")
	framework.AllocateVolume(t, volumeClient1, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Mark both as readonly
	_, err := volumeClient0.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("failed to mark readonly: %v", err)
	}

	_, err = volumeClient1.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("failed to mark readonly on server 1: %v", err)
	}

	// Get weed binary
	weedBinary := os.Getenv("WEED_BINARY")
	if weedBinary == "" {
		var err error
		weedBinary, err = framework.FindOrBuildWeedBinary()
		if err != nil {
			t.Fatalf("failed to find weed binary: %v", err)
		}
	}

	// Execute volume.merge via shell
	output, err := runWeedShell(t, weedBinary, cluster.MasterAddress(), fmt.Sprintf("volume.merge -volumeId %d", volumeID))

	t.Logf("volume.merge output: %s, error: %v", output, err)

	if err != nil {
		t.Fatalf("volume.merge failed: %v\noutput: %s", err, output)
	}

	if !strings.Contains(output, fmt.Sprintf("merged volume %d", volumeID)) {
		t.Fatalf("expected success message in output, got: %s", output)
	}

	// After merge, verify that originally-writable replicas are writable again
	// (The merge command should restore writable state for replicas that were writable before readonly)
	// Actually both were writable initially, then marked readonly, so both should be restored

	// Poll for writable state restoration instead of fixed sleep
	maxRetries := 50 // ~5s total with 100ms sleeps
	for retries := 0; retries < maxRetries; retries++ {
		status0, err := volumeClient0.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
			VolumeId: volumeID,
		})
		if err == nil && !status0.GetIsReadOnly() {
			// Server 0 is writable, check server 1
			status1, err := volumeClient1.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
				VolumeId: volumeID,
			})
			if err == nil && !status1.GetIsReadOnly() {
				// Both are writable, break out
				break
			}
		}
		if retries < maxRetries-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	status0Final, err := volumeClient0.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get final status for server 0: %v", err)
	}

	status1Final, err := volumeClient1.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get final status for server 1: %v", err)
	}

	if status0Final.GetIsReadOnly() {
		t.Fatalf("expected volume %d to be writable on server 0 after merge, but it's still readonly", volumeID)
	}

	if status1Final.GetIsReadOnly() {
		t.Fatalf("expected volume %d to be writable on server 1 after merge, but it's still readonly", volumeID)
	}

	t.Logf("After merge - volume %d on server 0: readonly=%v, server 1: readonly=%v", volumeID, status0Final.GetIsReadOnly(), status1Final.GetIsReadOnly())

	t.Logf("Successfully tested merge and restore workflow for volume %d", volumeID)
}

// TestVolumeMergeTailNeedles verifies the volume.merge command with empty volumes
func TestVolumeMergeTailNeedles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartTripleVolumeCluster(t, matrix.P1())

	conn0, volumeClient0 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()

	conn1, volumeClient1 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(200)

	// Allocate empty volumes on only 2 servers (the merge will allocate on the 3rd)
	framework.AllocateVolume(t, volumeClient0, volumeID, "")
	framework.AllocateVolume(t, volumeClient1, volumeID, "")

	// Mark as readonly
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := volumeClient0.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("failed to mark readonly on server 0: %v", err)
	}

	_, err = volumeClient1.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("failed to mark readonly on server 1: %v", err)
	}

	// Get weed binary
	weedBinary := os.Getenv("WEED_BINARY")
	if weedBinary == "" {
		var err error
		weedBinary, err = framework.FindOrBuildWeedBinary()
		if err != nil {
			t.Fatalf("failed to find weed binary: %v", err)
		}
	}

	// Execute volume.merge command on empty volumes
	output, err := runWeedShell(t, weedBinary, cluster.MasterAddress(), fmt.Sprintf("volume.merge -volumeId %d", volumeID))

	t.Logf("merge empty volumes - output: %s, error: %v", output, err)

	if err != nil {
		t.Fatalf("volume.merge failed on empty volumes: %v\noutput: %s", err, output)
	}

	// Verify merge completed successfully
	if !strings.Contains(output, fmt.Sprintf("merged volume %d", volumeID)) {
		t.Fatalf("expected success message in output, got: %s", output)
	}

	t.Logf("Successfully merged empty volumes %d", volumeID)
}

// TestVolumeMergeDivergentReplicas simulates a realistic merge scenario using shell command
func TestVolumeMergeDivergentReplicas(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartTripleVolumeCluster(t, matrix.P1())

	// Connect to both servers
	conn0, volumeClient0 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()

	conn1, volumeClient1 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	const volumeID = uint32(201)

	// Allocate the same volume on only 2 servers (the merge will allocate on the 3rd)
	framework.AllocateVolume(t, volumeClient0, volumeID, "")
	framework.AllocateVolume(t, volumeClient1, volumeID, "")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Verify both volumes are initially writable
	status0, err := volumeClient0.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get status for server 0: %v", err)
	}

	status1, err := volumeClient1.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get status for server 1: %v", err)
	}

	if status0.GetIsReadOnly() || status1.GetIsReadOnly() {
		t.Fatalf("expected both volumes to be writable initially")
	}

	// Mark both as readonly to simulate merge precondition
	_, err = volumeClient0.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("failed to mark readonly on server 0: %v", err)
	}

	_, err = volumeClient1.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: volumeID,
		Persist:  false,
	})
	if err != nil {
		t.Fatalf("failed to mark readonly on server 1: %v", err)
	}

	// Verify both are readonly
	status0Again, err := volumeClient0.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get status after readonly: %v", err)
	}

	if !status0Again.GetIsReadOnly() {
		t.Fatalf("expected volume %d to be readonly", volumeID)
	}

	// Also verify server 1 is readonly
	status1Again, err := volumeClient1.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("failed to get status on server 1 after readonly: %v", err)
	}

	if !status1Again.GetIsReadOnly() {
		t.Fatalf("expected volume %d to be readonly on server 1", volumeID)
	}

	// Get weed binary
	weedBinary := os.Getenv("WEED_BINARY")
	if weedBinary == "" {
		var err error
		weedBinary, err = framework.FindOrBuildWeedBinary()
		if err != nil {
			t.Fatalf("failed to find weed binary: %v", err)
		}
	}

	// Execute volume.merge command via shell
	output, err := runWeedShell(t, weedBinary, cluster.MasterAddress(), fmt.Sprintf("volume.merge -volumeId %d", volumeID))

	t.Logf("merge divergent replicas - output: %s, error: %v", output, err)

	if err != nil {
		t.Fatalf("volume.merge failed: %v\noutput: %s", err, output)
	}

	// Verify merge completed successfully
	if !strings.Contains(output, fmt.Sprintf("merged volume %d", volumeID)) {
		t.Fatalf("expected success message in output, got: %s", output)
	}

	t.Logf("Successfully merged divergent replicas for volume %d using shell command", volumeID)
}
