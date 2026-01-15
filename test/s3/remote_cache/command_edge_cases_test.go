package remote_cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
