package remote_cache

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRemoteCopyLocalBasic tests copying local-only files to remote
func TestRemoteCopyLocalBasic(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("copylocal-basic-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, testKey, 2048)

	// File is now local-only (not on remote yet)
	// Use remote.copy.local to copy it to remote
	t.Log("Copying local file to remote...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local failed")
	t.Logf("Copy output: %s", output)

	// Verify file is still readable
	verifyFileContent(t, testKey, testData)

	// Now uncache and read again - should work if copied to remote
	uncacheLocal(t, testKey)
	time.Sleep(500 * time.Millisecond)
	verifyFileContent(t, testKey, testData)
}

// TestRemoteCopyLocalDryRun tests preview mode without actual copying
func TestRemoteCopyLocalDryRun(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("copylocal-dryrun-%d.txt", time.Now().UnixNano())
	createTestFile(t, testKey, 1024)

	// Run in dry-run mode
	t.Log("Running remote.copy.local in dry-run mode...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -dryRun=true", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err)
	t.Logf("Dry-run output: %s", output)

	// Output should indicate what would be copied
	assert.Contains(t, strings.ToLower(output), "dry", "dry-run output should mention dry run")
}

// TestRemoteCopyLocalWithInclude tests copying only matching files
func TestRemoteCopyLocalWithInclude(t *testing.T) {
	checkServersRunning(t)

	// Create multiple files
	pdfFile := fmt.Sprintf("copylocal-doc-%d.pdf", time.Now().UnixNano())
	txtFile := fmt.Sprintf("copylocal-doc-%d.txt", time.Now().UnixNano())

	pdfData := createTestFile(t, pdfFile, 1024)
	txtData := createTestFile(t, txtFile, 1024)

	// Copy only PDF files
	t.Log("Copying only PDF files to remote...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=*.pdf", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local with include failed")
	t.Logf("Copy output: %s", output)

	// Verify both files are still readable
	verifyFileContent(t, pdfFile, pdfData)
	verifyFileContent(t, txtFile, txtData)
}

// TestRemoteCopyLocalWithExclude tests excluding pattern from copy
func TestRemoteCopyLocalWithExclude(t *testing.T) {
	checkServersRunning(t)

	// Create test files
	keepFile := fmt.Sprintf("copylocal-keep-%d.dat", time.Now().UnixNano())
	tmpFile := fmt.Sprintf("copylocal-temp-%d.tmp", time.Now().UnixNano())

	keepData := createTestFile(t, keepFile, 1024)
	tmpData := createTestFile(t, tmpFile, 1024)

	// Copy excluding .tmp files
	t.Log("Copying excluding .tmp files...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -exclude=*.tmp", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local with exclude failed")
	t.Logf("Copy output: %s", output)

	// Both should still be readable
	verifyFileContent(t, keepFile, keepData)
	verifyFileContent(t, tmpFile, tmpData)
}

// TestRemoteCopyLocalForceUpdate tests overwriting existing remote files
func TestRemoteCopyLocalForceUpdate(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("copylocal-force-%d.txt", time.Now().UnixNano())

	// Create and copy file to remote
	originalData := createTestFile(t, testKey, 1024)
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "initial copy failed")

	// Modify the file locally (simulate local change)
	newData := []byte("Updated content for force update test")
	uploadToPrimary(t, testKey, newData)
	time.Sleep(500 * time.Millisecond)

	// Copy again with force update
	t.Log("Copying with force update...")
	cmd = fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s -forceUpdate=true", testBucket, testKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local with forceUpdate failed")
	t.Logf("Force update output: %s", output)

	// Verify new content
	verifyFileContent(t, testKey, newData)

	// Uncache and verify it was updated on remote
	uncacheLocal(t, testKey)
	time.Sleep(500 * time.Millisecond)
	verifyFileContent(t, testKey, newData)

	// Clean up - restore original for other tests
	uploadToPrimary(t, testKey, originalData)
}

// TestRemoteCopyLocalConcurrency tests parallel copy operations
func TestRemoteCopyLocalConcurrency(t *testing.T) {
	checkServersRunning(t)

	// Create multiple files
	var files []string
	var dataMap = make(map[string][]byte)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("copylocal-concurrent-%d-%d.bin", time.Now().UnixNano(), i)
		data := createTestFile(t, key, 2048)
		files = append(files, key)
		dataMap[key] = data
		time.Sleep(10 * time.Millisecond) // Small delay to ensure unique timestamps
	}

	// Copy with high concurrency
	t.Log("Copying with concurrency=8...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -concurrent=8", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local with concurrency failed")
	t.Logf("Copy output: %s", output)

	// Verify all files are still readable
	for _, file := range files {
		verifyFileContent(t, file, dataMap[file])
	}
}

// TestRemoteCopyLocalEmptyDirectory tests handling empty directories
func TestRemoteCopyLocalEmptyDirectory(t *testing.T) {
	checkServersRunning(t)

	// Try to copy from a directory with no local-only files
	// (all files are already synced to remote)
	t.Log("Testing copy from directory with no local-only files...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)

	// Should not error, just report nothing to copy
	require.NoError(t, err, "remote.copy.local should handle empty directory gracefully")
	t.Logf("Empty directory output: %s", output)
}

// TestRemoteCopyLocalLargeFile tests copying large files
func TestRemoteCopyLocalLargeFile(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("copylocal-large-%d.bin", time.Now().UnixNano())

	// Create a 10MB file
	t.Log("Creating 10MB test file...")
	testData := createTestFile(t, testKey, 10*1024*1024)

	// Copy to remote
	t.Log("Copying large file to remote...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local large file failed")
	t.Logf("Large file copy output: %s", output)

	// Verify file integrity
	verifyFileContent(t, testKey, testData)

	// Uncache and verify it was copied to remote
	uncacheLocal(t, testKey)
	time.Sleep(1 * time.Second)
	verifyFileContent(t, testKey, testData)
}

// TestRemoteCopyLocalAlreadyExists tests skipping files already on remote
func TestRemoteCopyLocalAlreadyExists(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("copylocal-exists-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, testKey, 1024)

	// First copy
	t.Log("First copy to remote...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "first copy failed")
	t.Logf("First copy output: %s", output)

	// Second copy without forceUpdate - should skip
	t.Log("Second copy (should skip)...")
	output, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "second copy failed")
	t.Logf("Second copy output: %s", output)

	// File should still be readable
	verifyFileContent(t, testKey, testData)
}

// TestRemoteCopyLocalNotMounted tests error when directory not mounted
func TestRemoteCopyLocalNotMounted(t *testing.T) {
	checkServersRunning(t)

	// Try to copy from a non-mounted directory
	notMountedDir := fmt.Sprintf("/notmounted-%d", time.Now().UnixNano())

	t.Log("Testing copy from non-mounted directory...")
	cmd := fmt.Sprintf("remote.copy.local -dir=%s", notMountedDir)
	output, err := runWeedShellWithOutput(t, cmd)

	// Should fail or show error
	hasError := err != nil || strings.Contains(strings.ToLower(output), "not mounted") || strings.Contains(strings.ToLower(output), "error")
	assert.True(t, hasError, "Expected error or error message for non-mounted directory, got: %s", output)
	t.Logf("Non-mounted directory result: err=%v, output: %s", err, output)
}

// TestRemoteCopyLocalMinMaxSize tests size-based filtering
func TestRemoteCopyLocalMinMaxSize(t *testing.T) {
	checkServersRunning(t)

	// Create files of different sizes
	smallFile := fmt.Sprintf("copylocal-small-%d.bin", time.Now().UnixNano())
	mediumFile := fmt.Sprintf("copylocal-medium-%d.bin", time.Now().UnixNano())
	largeFile := fmt.Sprintf("copylocal-large-%d.bin", time.Now().UnixNano())

	smallData := createTestFile(t, smallFile, 500)    // 500 bytes
	mediumData := createTestFile(t, mediumFile, 5000) // 5KB
	largeData := createTestFile(t, largeFile, 50000)  // 50KB

	// Copy only files between 1KB and 10KB
	t.Log("Copying files between 1KB and 10KB...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -minSize=1024 -maxSize=10240", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.copy.local with size filters failed")
	t.Logf("Size filter output: %s", output)

	// All files should still be readable
	verifyFileContent(t, smallFile, smallData)
	verifyFileContent(t, mediumFile, mediumData)
	verifyFileContent(t, largeFile, largeData)
}
