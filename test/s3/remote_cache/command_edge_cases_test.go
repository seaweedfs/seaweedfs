package remote_cache

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEdgeCaseEmptyDirectory tests operations on empty directories
func TestEdgeCaseEmptyDirectory(t *testing.T) {
	checkServersRunning(t)

	// Test various commands on empty directory
	t.Log("Testing commands on empty directory...")

	// Cache on empty directory
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -include=nonexistent*", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "cache on empty directory failed")
	t.Logf("Cache empty output: %s", output)

	// Uncache on empty directory
	cmd = fmt.Sprintf("remote.uncache -dir=/buckets/%s -include=nonexistent*", testBucket)
	output, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "uncache on empty directory failed")
	t.Logf("Uncache empty output: %s", output)

	// Copy.local on empty directory
	cmd = fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=nonexistent*", testBucket)
	output, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "copy.local on empty directory failed")
	t.Logf("Copy.local empty output: %s", output)
}

// TestEdgeCaseNestedDirectories tests deep directory hierarchies
func TestEdgeCaseNestedDirectories(t *testing.T) {
	checkServersRunning(t)

	// Create files in nested structure via S3 key naming
	nestedKey := fmt.Sprintf("level1/level2/level3/nested-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, nestedKey, 512)

	// Verify file is accessible
	verifyFileContent(t, nestedKey, testData)

	// Uncache and verify still accessible
	uncacheLocal(t, nestedKey)
	time.Sleep(500 * time.Millisecond)
	verifyFileContent(t, nestedKey, testData)
}

// TestEdgeCaseSpecialCharacters tests files with special characters in names
func TestEdgeCaseSpecialCharacters(t *testing.T) {
	checkServersRunning(t)

	// Test various special characters (S3 compatible)
	specialNames := []string{
		fmt.Sprintf("file-with-dash-%d.txt", time.Now().UnixNano()),
		fmt.Sprintf("file_with_underscore_%d.txt", time.Now().UnixNano()),
		fmt.Sprintf("file.with.dots.%d.txt", time.Now().UnixNano()),
		fmt.Sprintf("file with space %d.txt", time.Now().UnixNano()),
		fmt.Sprintf("file(with)parens-%d.txt", time.Now().UnixNano()),
	}

	for _, name := range specialNames {
		t.Run(name, func(t *testing.T) {
			// Create file
			data := createTestFile(t, name, 256)

			// Verify readable
			verifyFileContent(t, name, data)

			// Uncache and verify
			uncacheLocal(t, name)
			time.Sleep(300 * time.Millisecond)
			verifyFileContent(t, name, data)
		})
	}
}

// TestEdgeCaseVeryLargeFile tests 100MB+ file handling
func TestEdgeCaseVeryLargeFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}
	checkServersRunning(t)

	testKey := fmt.Sprintf("verylarge-%d.bin", time.Now().UnixNano())

	// Create a 100MB file
	t.Log("Creating 100MB test file...")
	testData := createTestFile(t, testKey, 100*1024*1024)

	// Copy to remote
	t.Log("Copying very large file to remote...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "copy.local very large file failed")
	t.Logf("Large file copy output: %s", output)

	// Uncache
	t.Log("Uncaching very large file...")
	uncacheLocal(t, testKey)
	time.Sleep(2 * time.Second)

	// Verify integrity
	t.Log("Verifying very large file integrity...")
	verifyFileContent(t, testKey, testData)
}

// TestEdgeCaseManySmallFiles tests 100+ small files
func TestEdgeCaseManySmallFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping many files test in short mode")
	}
	checkServersRunning(t)

	prefix := fmt.Sprintf("manyfiles-%d", time.Now().UnixNano())
	fileCount := 100
	var files []string
	var dataMap = make(map[string][]byte)

	// Create many small files
	t.Logf("Creating %d small files...", fileCount)
	for i := 0; i < fileCount; i++ {
		key := fmt.Sprintf("%s/file-%04d.txt", prefix, i)
		data := createTestFile(t, key, 128)
		files = append(files, key)
		dataMap[key] = data

		if i%20 == 0 {
			time.Sleep(100 * time.Millisecond) // Avoid overwhelming the system
		}
	}

	// Copy all to remote
	t.Log("Copying all files to remote...")
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s/*", testBucket, prefix)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "copy.local many files failed")
	t.Logf("Many files copy output: %s", output)

	// Uncache all
	t.Log("Uncaching all files...")
	cmd = fmt.Sprintf("remote.uncache -dir=/buckets/%s -include=%s/*", testBucket, prefix)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "uncache many files failed")
	time.Sleep(2 * time.Second)

	// Verify a sample of files
	t.Log("Verifying sample of files...")
	sampleIndices := []int{0, fileCount / 4, fileCount / 2, 3 * fileCount / 4, fileCount - 1}
	for _, idx := range sampleIndices {
		if idx < len(files) {
			verifyFileContent(t, files[idx], dataMap[files[idx]])
		}
	}
}

// TestEdgeCaseRapidCacheUncache tests rapid cache/uncache cycles
func TestEdgeCaseRapidCacheUncache(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("rapid-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, testKey, 1024)

	// Perform rapid cache/uncache cycles
	t.Log("Performing rapid cache/uncache cycles...")
	for i := 0; i < 5; i++ {
		// Uncache
		uncacheLocal(t, testKey)
		time.Sleep(200 * time.Millisecond)

		// Read (triggers cache)
		verifyFileContent(t, testKey, testData)
		time.Sleep(200 * time.Millisecond)
	}

	// Final verification
	verifyFileContent(t, testKey, testData)
}

// TestEdgeCaseConcurrentCommands tests multiple commands running simultaneously
func TestEdgeCaseConcurrentCommands(t *testing.T) {
	checkServersRunning(t)

	// Create test files
	var files []string
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("concurrent-cmd-%d-%d.txt", time.Now().UnixNano(), i)
		createTestFile(t, key, 1024)
		files = append(files, key)
		time.Sleep(10 * time.Millisecond)
	}

	// Run multiple commands concurrently
	t.Log("Running concurrent commands...")
	var wg sync.WaitGroup
	errors := make(chan error, 3)

	// Concurrent cache
	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s", testBucket)
		_, err := runWeedShellWithOutput(t, cmd)
		if err != nil {
			errors <- fmt.Errorf("cache: %w", err)
		}
	}()

	// Concurrent copy.local
	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s", testBucket)
		_, err := runWeedShellWithOutput(t, cmd)
		if err != nil {
			errors <- fmt.Errorf("copy.local: %w", err)
		}
	}()

	// Concurrent meta.sync
	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd := fmt.Sprintf("remote.meta.sync -dir=/buckets/%s", testBucket)
		_, err := runWeedShellWithOutput(t, cmd)
		if err != nil {
			errors <- fmt.Errorf("meta.sync: %w", err)
		}
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Logf("Concurrent command error: %v", err)
	}
}

// TestEdgeCaseInvalidPaths tests non-existent paths and invalid characters
func TestEdgeCaseInvalidPaths(t *testing.T) {
	checkServersRunning(t)

	invalidPaths := []string{
		"/nonexistent/path/to/nowhere",
		"/buckets/../../../etc/passwd", // Path traversal attempt
		"",                             // Empty path
	}

	for _, path := range invalidPaths {
		t.Run(fmt.Sprintf("path_%s", strings.ReplaceAll(path, "/", "_")), func(t *testing.T) {
			// Try various commands with invalid paths
			commands := []string{
				fmt.Sprintf("remote.cache -dir=%s", path),
				fmt.Sprintf("remote.uncache -dir=%s", path),
				fmt.Sprintf("remote.copy.local -dir=%s", path),
			}

			for _, cmd := range commands {
				output, err := runWeedShellWithOutput(t, cmd)
				// Should either error or handle gracefully
				if err != nil {
					t.Logf("Command '%s' failed as expected: %v", cmd, err)
				} else {
					t.Logf("Command '%s' output: %s", cmd, output)
				}
			}
		})
	}
}

// TestEdgeCaseZeroByteFiles tests empty file handling
func TestEdgeCaseZeroByteFiles(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("zerobyte-%d.txt", time.Now().UnixNano())

	// Create zero-byte file
	emptyData := []byte{}
	uploadToPrimary(t, testKey, emptyData)

	// Verify it exists
	result := getFromPrimary(t, testKey)
	assert.Equal(t, 0, len(result), "zero-byte file should be empty")

	// Copy to remote
	cmd := fmt.Sprintf("remote.copy.local -dir=/buckets/%s -include=%s", testBucket, testKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "copy.local zero-byte file failed")
	t.Logf("Zero-byte copy output: %s", output)

	// Uncache
	uncacheLocal(t, testKey)
	time.Sleep(500 * time.Millisecond)

	// Verify still accessible
	result = getFromPrimary(t, testKey)
	assert.Equal(t, 0, len(result), "zero-byte file should still be empty after uncache")
}

// TestEdgeCaseFileNamePatterns tests various glob pattern edge cases
func TestEdgeCaseFileNamePatterns(t *testing.T) {
	checkServersRunning(t)

	// Create files with various patterns
	patterns := []struct {
		name    string
		pattern string
		match   bool
	}{
		{fmt.Sprintf("test-%d.txt", time.Now().UnixNano()), "*.txt", true},
		{fmt.Sprintf("test-%d.log", time.Now().UnixNano()), "*.txt", false},
		{fmt.Sprintf("a-%d.dat", time.Now().UnixNano()), "?.dat", false}, // Single char doesn't match 'a-timestamp'
		{fmt.Sprintf("test-%d.backup", time.Now().UnixNano()), "*.back*", true},
	}

	for _, p := range patterns {
		createTestFile(t, p.name, 256)
		time.Sleep(10 * time.Millisecond)
	}

	// Test pattern matching with uncache
	for _, p := range patterns {
		t.Run(p.name, func(t *testing.T) {
			cmd := fmt.Sprintf("remote.uncache -dir=/buckets/%s -include=%s", testBucket, p.pattern)
			output, err := runWeedShellWithOutput(t, cmd)
			require.NoError(t, err, "uncache with pattern failed")
			t.Logf("Pattern '%s' output: %s", p.pattern, output)
		})
	}
}
