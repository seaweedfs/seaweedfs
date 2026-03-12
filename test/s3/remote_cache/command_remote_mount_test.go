package remote_cache

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isWeedShellBannerLine(line string) bool {
	t := strings.TrimSpace(line)
	return strings.HasPrefix(t, "master:") ||
		t == ">" || strings.TrimLeft(t, "> ") == ""
}

// TestRemoteMountBasic tests mounting a remote bucket to a local directory
func TestRemoteMountBasic(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testmount%d", time.Now().UnixNano()%1000000)

	// Mount the remote bucket
	t.Logf("Mounting remote bucket to %s...", testDir)
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket", testDir)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to mount remote")
	t.Logf("Mount output: %s", output)

	// Verify mount exists in list
	output, err = runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to list mounts")
	assert.Contains(t, output, testDir, "mount not found in list")

	// Clean up - unmount
	t.Logf("Unmounting %s...", testDir)
	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to unmount")
}

// TestRemoteMountNonEmpty tests mounting with -nonempty flag
func TestRemoteMountNonEmpty(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testnonempty%d", time.Now().UnixNano()%1000000)
	testFile := fmt.Sprintf("testfile-%d.txt", time.Now().UnixNano())

	// First mount to create the directory
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket", testDir)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to initial mount")

	// Upload a file to make it non-empty
	uploadToPrimary(t, testFile, []byte("test data"))
	time.Sleep(500 * time.Millisecond)

	// Unmount
	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to unmount")

	// Try to mount again with -nonempty flag (directory may have residual data)
	t.Logf("Mounting with -nonempty flag...")
	cmd = fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket -nonempty=true", testDir)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to mount with -nonempty")
	t.Logf("Mount output: %s", output)

	// Clean up
	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to unmount")
}

// TestRemoteMountMetadataStrategyLazy tests mounting with -metadataStrategy=lazy: no upfront metadata pull, mount mapping persisted
func TestRemoteMountMetadataStrategyLazy(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testlazy%d", time.Now().UnixNano()%1000000)

	t.Logf("Mounting with -metadataStrategy=lazy...")
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket -metadataStrategy=lazy", testDir)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to mount with -metadataStrategy=lazy")
	t.Logf("Mount output: %s", output)

	listCmd := fmt.Sprintf("fs.ls %s", testDir)
	listOutput, err := runWeedShellWithOutput(t, listCmd)
	require.NoError(t, err, "failed to list mount dir")
	listLines := strings.Split(strings.TrimSpace(listOutput), "\n")
	nonEmptyLines := 0
	for _, line := range listLines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "total ") {
			continue
		}
		if isWeedShellBannerLine(trimmed) {
			continue
		}
		nonEmptyLines++
	}
	assert.Zero(t, nonEmptyLines, "expected no upfront metadata (empty dir listing) after -metadataStrategy=lazy mount, got %d entries: %s", nonEmptyLines, listOutput)

	output, err = runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to list mounts")
	assert.Contains(t, output, testDir, "mount not found in list after -metadataStrategy=lazy mount")

	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to unmount")
}

// TestRemoteMountMetadataStrategyEager tests mounting with -metadataStrategy=eager: full upfront metadata pull
func TestRemoteMountMetadataStrategyEager(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testeager%d", time.Now().UnixNano()%1000000)

	t.Logf("Mounting with -metadataStrategy=eager...")
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket -metadataStrategy=eager", testDir)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to mount with -metadataStrategy=eager")
	t.Logf("Mount output: %s", output)

	_, err = runWeedShellWithOutput(t, fmt.Sprintf("fs.ls %s", testDir))
	require.NoError(t, err, "eager mount should allow listing (metadata pulled)")

	output, err = runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to list mounts")
	assert.Contains(t, output, testDir, "mount not found in list after -metadataStrategy=eager mount")

	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to unmount")
}

// TestRemoteMountMetadataStrategyInvalid tests that invalid metadataStrategy is rejected
func TestRemoteMountMetadataStrategyInvalid(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testinvalidstrat%d", time.Now().UnixNano()%1000000)
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket -metadataStrategy=invalid", testDir)
	output, err := runWeedShellWithOutput(t, cmd)
	require.Error(t, err, "expected error for invalid metadataStrategy")
	assert.Contains(t, strings.ToLower(output), "lazy or eager", "error should mention allowed values")
}

// TestRemoteMountInvalidRemote tests mounting with non-existent remote configuration
func TestRemoteMountInvalidRemote(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testinvalid%d", time.Now().UnixNano()%1000000)
	invalidRemote := fmt.Sprintf("nonexistent%d/bucket", time.Now().UnixNano())

	// Try to mount with invalid remote
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=%s", testDir, invalidRemote)
	output, err := runWeedShellWithOutput(t, cmd)

	// Should fail with invalid remote
	hasError := err != nil || strings.Contains(strings.ToLower(output), "invalid") || strings.Contains(strings.ToLower(output), "error") || strings.Contains(strings.ToLower(output), "not found")
	assert.True(t, hasError, "Expected error for invalid remote, got: %s", output)
	t.Logf("Invalid remote result: err=%v, output: %s", err, output)
}

// TestRemoteMountList tests listing all mounts
func TestRemoteMountList(t *testing.T) {
	checkServersRunning(t)

	// List all mounts
	output, err := runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to list mounts")
	t.Logf("Mount list: %s", output)

	// Should contain the default mount from setup
	assert.Contains(t, output, "remotemounted", "default mount not found")
}

// TestRemoteUnmountBasic tests unmounting and verifying cleanup
func TestRemoteUnmountBasic(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/testunmount%d", time.Now().UnixNano()%1000000)

	// Mount first
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket", testDir)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to mount")

	// Verify it's mounted
	output, err := runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to list mounts")
	assert.Contains(t, output, testDir, "mount not found before unmount")

	// Unmount
	t.Logf("Unmounting %s...", testDir)
	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	output, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to unmount")
	t.Logf("Unmount output: %s", output)

	// Verify it's no longer mounted
	output, err = runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to list mounts after unmount")
	assert.NotContains(t, output, testDir, "mount still exists after unmount")
}

// TestRemoteUnmountNotMounted tests unmounting a non-mounted directory
func TestRemoteUnmountNotMounted(t *testing.T) {
	checkServersRunning(t)

	testDir := fmt.Sprintf("/buckets/notmounted%d", time.Now().UnixNano()%1000000)

	// Try to unmount a directory that's not mounted
	cmd := fmt.Sprintf("remote.unmount -dir=%s", testDir)
	output, err := runWeedShellWithOutput(t, cmd)

	// Should fail or show error
	hasError := err != nil || strings.Contains(strings.ToLower(output), "not mounted") || strings.Contains(strings.ToLower(output), "error")
	assert.True(t, hasError, "Expected error for unmounting non-mounted directory, got: %s", output)
	t.Logf("Unmount non-mounted result: err=%v, output: %s", err, output)
}

// TestRemoteMountBucketsBasic tests mounting all buckets from remote
func TestRemoteMountBucketsBasic(t *testing.T) {
	checkServersRunning(t)

	// List buckets in dry-run mode (without -apply)
	t.Log("Listing buckets without -apply flag...")
	cmd := "remote.mount.buckets -remote=seaweedremote"
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to list buckets")
	t.Logf("Bucket list output: %s", output)

	// Should show the remote bucket
	assert.Contains(t, output, "remotesourcebucket", "remote bucket not found in list")
}

// TestRemoteMountBucketsWithPattern tests mounting with bucket pattern filter
func TestRemoteMountBucketsWithPattern(t *testing.T) {
	checkServersRunning(t)

	// Test with pattern matching
	t.Log("Testing bucket pattern matching...")
	cmd := "remote.mount.buckets -remote=seaweedremote -bucketPattern=remote*"
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to list buckets with pattern")
	t.Logf("Pattern match output: %s", output)

	// Should show matching buckets
	assert.Contains(t, output, "remotesourcebucket", "matching bucket not found")

	// Test with non-matching pattern
	cmd = "remote.mount.buckets -remote=seaweedremote -bucketPattern=nonexistent*"
	output, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to list buckets with non-matching pattern")
	t.Logf("Non-matching pattern output: %s", output)
}

// TestRemoteMountBucketsDryRun tests dry run mode (no -apply flag)
func TestRemoteMountBucketsDryRun(t *testing.T) {
	checkServersRunning(t)

	// Get initial mount list
	initialOutput, err := runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to get initial mount list")

	// Run mount.buckets without -apply (dry run)
	t.Log("Running mount.buckets in dry-run mode...")
	cmd := "remote.mount.buckets -remote=seaweedremote"
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to run dry-run mount.buckets")
	t.Logf("Dry-run output: %s", output)

	// Get mount list after dry run
	afterOutput, err := runWeedShellWithOutput(t, "remote.mount")
	require.NoError(t, err, "failed to get mount list after dry-run")

	// Mount list should be unchanged (dry run doesn't actually mount)
	assert.Equal(t, initialOutput, afterOutput, "mount list changed after dry-run")
}
