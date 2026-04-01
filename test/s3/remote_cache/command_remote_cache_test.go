package remote_cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRemoteCacheBasicCommand tests caching files from remote using command
func TestRemoteCacheBasicCommand(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("cache-basic-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, testKey, 1024)

	// Uncache first to push to remote
	t.Log("Uncaching file to remote...")
	uncacheLocal(t, testKey)

	// Now cache it back using remote.cache command
	t.Log("Caching file from remote using command...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache command failed")
	t.Logf("Cache output: %s", output)

	// Verify file is still readable
	verifyFileContent(t, testKey, testData)
}

// TestRemoteCacheWithInclude tests caching only matching files
func TestRemoteCacheWithInclude(t *testing.T) {
	checkServersRunning(t)

	// Create multiple files with different extensions
	pdfFile := fmt.Sprintf("doc-%d.pdf", time.Now().UnixNano())
	txtFile := fmt.Sprintf("doc-%d.txt", time.Now().UnixNano())

	pdfData := createTestFile(t, pdfFile, 512)
	txtData := createTestFile(t, txtFile, 512)

	// Uncache both
	uncacheLocal(t, "*.pdf")
	uncacheLocal(t, "*.txt")
	time.Sleep(500 * time.Millisecond)

	// Cache only PDF files
	t.Log("Caching only PDF files...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -include=*.pdf", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache with include failed")
	t.Logf("Cache output: %s", output)

	// Both files should still be readable
	verifyFileContent(t, pdfFile, pdfData)
	verifyFileContent(t, txtFile, txtData)
}

// TestRemoteCacheWithExclude tests caching excluding pattern
func TestRemoteCacheWithExclude(t *testing.T) {
	checkServersRunning(t)

	// Create test files
	keepFile := fmt.Sprintf("keep-%d.txt", time.Now().UnixNano())
	tmpFile := fmt.Sprintf("temp-%d.tmp", time.Now().UnixNano())

	keepData := createTestFile(t, keepFile, 512)
	tmpData := createTestFile(t, tmpFile, 512)

	// Uncache both
	uncacheLocal(t, "keep-*")
	uncacheLocal(t, "temp-*")
	time.Sleep(500 * time.Millisecond)

	// Cache excluding .tmp files
	t.Log("Caching excluding .tmp files...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -exclude=*.tmp", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache with exclude failed")
	t.Logf("Cache output: %s", output)

	// Both should still be readable
	verifyFileContent(t, keepFile, keepData)
	verifyFileContent(t, tmpFile, tmpData)
}

// TestRemoteCacheMinSize tests caching files larger than threshold
func TestRemoteCacheMinSize(t *testing.T) {
	checkServersRunning(t)

	// Create files of different sizes
	smallFile := fmt.Sprintf("small-%d.bin", time.Now().UnixNano())
	largeFile := fmt.Sprintf("large-%d.bin", time.Now().UnixNano())

	smallData := createTestFile(t, smallFile, 100)   // 100 bytes
	largeData := createTestFile(t, largeFile, 10000) // 10KB

	// Uncache both
	uncacheLocal(t, "small-*")
	uncacheLocal(t, "large-*")
	time.Sleep(500 * time.Millisecond)

	// Cache only files larger than 1KB
	t.Log("Caching files larger than 1KB...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -minSize=1024", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache with minSize failed")
	t.Logf("Cache output: %s", output)

	// Both should still be readable
	verifyFileContent(t, smallFile, smallData)
	verifyFileContent(t, largeFile, largeData)
}

// TestRemoteCacheMaxSize tests caching files smaller than threshold
func TestRemoteCacheMaxSize(t *testing.T) {
	checkServersRunning(t)

	// Create files of different sizes
	smallFile := fmt.Sprintf("tiny-%d.bin", time.Now().UnixNano())
	mediumFile := fmt.Sprintf("medium-%d.bin", time.Now().UnixNano())

	smallData := createTestFile(t, smallFile, 500)    // 500 bytes
	mediumData := createTestFile(t, mediumFile, 5000) // 5KB

	// Uncache both
	uncacheLocal(t, "tiny-*")
	uncacheLocal(t, "medium-*")
	time.Sleep(500 * time.Millisecond)

	// Cache only files smaller than 2KB
	t.Log("Caching files smaller than 2KB...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -maxSize=2048", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache with maxSize failed")
	t.Logf("Cache output: %s", output)

	// Both should still be readable
	verifyFileContent(t, smallFile, smallData)
	verifyFileContent(t, mediumFile, mediumData)
}

// TestRemoteCacheCombinedFilters tests multiple filters together
func TestRemoteCacheCombinedFilters(t *testing.T) {
	checkServersRunning(t)

	// Create test files
	matchFile := fmt.Sprintf("data-%d.dat", time.Now().UnixNano())
	noMatchFile := fmt.Sprintf("skip-%d.txt", time.Now().UnixNano())

	matchData := createTestFile(t, matchFile, 2000)    // 2KB .dat file
	noMatchData := createTestFile(t, noMatchFile, 100) // 100 byte .txt file

	// Uncache both
	uncacheLocal(t, "data-*")
	uncacheLocal(t, "skip-*")
	time.Sleep(500 * time.Millisecond)

	// Cache .dat files larger than 1KB
	t.Log("Caching .dat files larger than 1KB...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -include=*.dat -minSize=1024", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache with combined filters failed")
	t.Logf("Cache output: %s", output)

	// Both should still be readable
	verifyFileContent(t, matchFile, matchData)
	verifyFileContent(t, noMatchFile, noMatchData)
}

// TestRemoteCacheDryRun tests preview without actual caching
func TestRemoteCacheDryRun(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("dryrun-%d.txt", time.Now().UnixNano())
	createTestFile(t, testKey, 1024)

	// Uncache
	uncacheLocal(t, testKey)
	time.Sleep(500 * time.Millisecond)

	// Run cache in dry-run mode
	t.Log("Running cache in dry-run mode...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -dryRun=true", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache dry-run failed")
	t.Logf("Dry-run output: %s", output)

	// File should still be readable (caching happens on-demand anyway)
	getFromPrimary(t, testKey)
}

// TestRemoteUncacheBasic tests uncaching files (removing local chunks)
func TestRemoteUncacheBasic(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("uncache-basic-%d.txt", time.Now().UnixNano())
	testData := createTestFile(t, testKey, 2048)

	// Verify file exists
	verifyFileContent(t, testKey, testData)

	// Uncache it
	t.Log("Uncaching file...")
	cmd := fmt.Sprintf("remote.uncache -dir=/buckets/%s -include=%s", testBucket, testKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.uncache failed")
	t.Logf("Uncache output: %s", output)

	// File should still be readable (will be fetched from remote)
	verifyFileContent(t, testKey, testData)
}

// TestRemoteUncacheWithFilters tests uncaching with include/exclude patterns
func TestRemoteUncacheWithFilters(t *testing.T) {
	checkServersRunning(t)

	// Create multiple files
	file1 := fmt.Sprintf("uncache-filter1-%d.log", time.Now().UnixNano())
	file2 := fmt.Sprintf("uncache-filter2-%d.txt", time.Now().UnixNano())

	data1 := createTestFile(t, file1, 1024)
	data2 := createTestFile(t, file2, 1024)

	// Uncache only .log files
	t.Log("Uncaching only .log files...")
	cmd := fmt.Sprintf("remote.uncache -dir=/buckets/%s -include=*.log", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.uncache with filter failed")
	t.Logf("Uncache output: %s", output)

	// Both should still be readable
	verifyFileContent(t, file1, data1)
	verifyFileContent(t, file2, data2)
}

// TestRemoteUncacheMinSize tests uncaching files based on size
func TestRemoteUncacheMinSize(t *testing.T) {
	checkServersRunning(t)

	// Create files of different sizes
	smallFile := fmt.Sprintf("uncache-small-%d.bin", time.Now().UnixNano())
	largeFile := fmt.Sprintf("uncache-large-%d.bin", time.Now().UnixNano())

	smallData := createTestFile(t, smallFile, 500)
	largeData := createTestFile(t, largeFile, 5000)

	// Uncache only files larger than 2KB
	t.Log("Uncaching files larger than 2KB...")
	cmd := fmt.Sprintf("remote.uncache -dir=/buckets/%s -minSize=2048", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.uncache with minSize failed")
	t.Logf("Uncache output: %s", output)

	// Both should still be readable
	verifyFileContent(t, smallFile, smallData)
	verifyFileContent(t, largeFile, largeData)
}

// TestRemoteCacheConcurrency tests cache with different concurrency levels
func TestRemoteCacheConcurrency(t *testing.T) {
	checkServersRunning(t)

	// Create multiple files
	var files []string
	var dataMap = make(map[string][]byte)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("concurrent-%d-%d.bin", time.Now().UnixNano(), i)
		data := createTestFile(t, key, 1024)
		files = append(files, key)
		dataMap[key] = data
	}

	// Uncache all
	for _, file := range files {
		uncacheLocal(t, file)
	}
	time.Sleep(1 * time.Second)

	// Cache with high concurrency
	t.Log("Caching with concurrency=8...")
	cmd := fmt.Sprintf("remote.cache -dir=/buckets/%s -concurrent=8", testBucket)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "remote.cache with concurrency failed")
	t.Logf("Cache output: %s", output)

	// Verify all files are readable
	for _, file := range files {
		verifyFileContent(t, file, dataMap[file])
	}
}
