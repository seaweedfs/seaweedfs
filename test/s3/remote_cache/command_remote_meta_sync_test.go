package remote_cache

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRemoteMetaSyncBasic tests syncing metadata from remote
func TestRemoteMetaSyncBasic(t *testing.T) {
	checkServersRunning(t)

	// Sync metadata from remote
	t.Log("Syncing metadata from remote...")
	cmd := fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.meta.sync failed")
	t.Logf("Meta sync output: %s", output)

	// Should complete without errors
	assert.NotContains(t, strings.ToLower(output), "failed", "sync should not fail")
}

// TestRemoteMetaSyncNewFiles tests detecting new files on remote
func TestRemoteMetaSyncNewFiles(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("metasync-new-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, testKey, 1024)

	// Copy to remote
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to copy file to remote")

	// Uncache to remove local chunks
	uncacheLocal(t, testKey)
	time.Sleep(500 * time.Millisecond)

	// Sync metadata - should detect the file
	t.Log("Syncing metadata to detect new file...")
	cmd = fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.meta.sync failed")
	t.Logf("Meta sync output: %s", output)

	// File should be readable
	verifyFileContent(t, testKey, testData)
}

// TestRemoteMetaSyncSubdirectory tests syncing specific subdirectory
func TestRemoteMetaSyncSubdirectory(t *testing.T) {
	checkServersRunning(t)

	// Sync just the mounted directory
	t.Log("Syncing subdirectory metadata...")
	cmd := fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.meta.sync subdirectory failed")
	t.Logf("Subdirectory sync output: %s", output)
}

// TestRemoteMetaSyncNotMounted tests error when directory not mounted
func TestRemoteMetaSyncNotMounted(t *testing.T) {
	checkServersRunning(t)

	// Try to sync a non-mounted directory
	notMountedDir := fmt.Sprintf("/notmounted-%d", time.Now().UnixNano())

	t.Log("Testing sync on non-mounted directory...")
	cmd := fmt.Sprintf("remote.meta.sync -dir=%s", notMountedDir)
	output, err := runWeedShellWithOutput(t, cmd)

	// Should fail or show error
	hasError := err != nil || strings.Contains(strings.ToLower(output), "not mounted") || strings.Contains(strings.ToLower(output), "error")
	assert.True(t, hasError, "Expected error for non-mounted directory, got: %s", output)
	t.Logf("Non-mounted directory result: err=%v, output: %s", err, output)
}

// TestRemoteMetaSyncRepeated tests running sync multiple times
func TestRemoteMetaSyncRepeated(t *testing.T) {
	checkServersRunning(t)

	// Run sync multiple times - should be idempotent
	for i := 0; i < 3; i++ {
		t.Logf("Running sync iteration %d...", i+1)
		cmd := fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
		output, err := runWeedShellWithOutput(t, cmd)
		require.NoError(t, err, "remote.meta.sync iteration %d failed", i+1)
		t.Logf("Iteration %d output: %s", i+1, output)
		time.Sleep(500 * time.Millisecond)
	}
}

// TestRemoteMetaSyncAfterRemoteChange tests detecting changes on remote
func TestRemoteMetaSyncAfterRemoteChange(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("metasync-change-%d.txt", time.Now().UnixNano())

	// Create and sync file
	originalData := createTestFile(t, testKey, 1024)
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to copy file to remote")

	// First sync
	cmd = fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "first sync failed")

	// Simulate remote change by updating the file and copying again
	newData := []byte("Updated content after remote change")
	uploadToPrimary(t, testKey, newData)
	time.Sleep(500 * time.Millisecond)

	cmd = fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s -forceUpdate=true", testBucket, testKey)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to update remote file")

	// Sync again - should detect the change
	t.Log("Syncing after remote change...")
	cmd = fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "sync after change failed")
	t.Logf("Sync after change output: %s", output)

	// Restore original for cleanup
	uploadToPrimary(t, testKey, originalData)
}

// TestRemoteMetaSyncEmptyRemote tests syncing when remote is empty
func TestRemoteMetaSyncEmptyRemote(t *testing.T) {
	checkServersRunning(t)

	// Create a new mount point for testing
	testDir := fmt.Sprintf("/buckets/testempty%d", time.Now().UnixNano()%1000000)

	// Mount the remote bucket to new directory
	cmd := fmt.Sprintf("remote.mount -dir=%s -remote=seaweedremote/remotesourcebucket -nonempty=true", testDir)
	_, err := runWeedShellWithOutput(t, cmd)
	if err != nil {
		t.Skip("Could not create test mount for empty remote test")
	}

	// Sync metadata
	t.Log("Syncing metadata from potentially empty remote...")
	cmd = fmt.Sprintf("remote.meta.sync -dir=%s", testDir)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "sync on empty remote failed")
	t.Logf("Empty remote sync output: %s", output)

	// Clean up
	cmd = fmt.Sprintf("remote.unmount -dir=%s", testDir)
	_, err = runWeedShellWithOutput(t, cmd)
	if err != nil {
		t.Logf("Warning: failed to unmount test directory: %v", err)
	}
}
