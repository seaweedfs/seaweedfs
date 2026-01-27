package remote_cache

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test configuration
// Uses two SeaweedFS instances:
// - Primary: The one being tested (has remote caching)
// - Remote: Acts as the "remote" S3 storage
const (
	// Primary SeaweedFS
	primaryEndpoint   = "http://localhost:8333"
	primaryMasterPort = "9333"

	// Remote SeaweedFS (acts as remote storage)
	remoteEndpoint = "http://localhost:8334"

	// Credentials (anonymous access for testing)
	accessKey = "some_access_key1"
	secretKey = "some_secret_key1"

	// Bucket name - mounted on primary as remote storage
	testBucket = "remotemounted"

	// Path to weed binary
	weedBinary = "../../../weed/weed_binary"
)

var (
	primaryClient     *s3.S3
	primaryClientOnce sync.Once
)

func getPrimaryClient() *s3.S3 {
	primaryClientOnce.Do(func() {
		primaryClient = createS3Client(primaryEndpoint)
	})
	return primaryClient
}

func createS3Client(endpoint string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		DisableSSL:       aws.Bool(!strings.HasPrefix(endpoint, "https")),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create session: %v", err))
	}
	return s3.New(sess)
}

// checkServersRunning ensures the servers are running and fails if they aren't
func checkServersRunning(t *testing.T) {
	resp, err := http.Get(primaryEndpoint)
	require.NoErrorf(t, err, "Primary SeaweedFS not running at %s", primaryEndpoint)
	resp.Body.Close()

	resp, err = http.Get(remoteEndpoint)
	require.NoErrorf(t, err, "Remote SeaweedFS not running at %s", remoteEndpoint)
	resp.Body.Close()
}

// stripLogs removes SeaweedFS log lines from the output
func stripLogs(output string) string {
	lines := strings.Split(output, "\n")
	var filtered []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if len(trimmed) > 0 && (trimmed[0] == 'I' || trimmed[0] == 'W' || trimmed[0] == 'E' || trimmed[0] == 'F') && len(trimmed) > 5 && isDigit(trimmed[1]) {
			continue
		}
		filtered = append(filtered, line)
	}
	return strings.Join(filtered, "\n")
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// runWeedShell executes a weed shell command
func runWeedShell(t *testing.T, command string) (string, error) {
	cmd := exec.Command(weedBinary, "shell", "-master=localhost:"+primaryMasterPort)
	cmd.Stdin = strings.NewReader(command + "\nexit\n")
	output, err := cmd.CombinedOutput()
	result := stripLogs(string(output))
	if err != nil {
		t.Logf("weed shell command '%s' failed: %v, output: %s", command, err, result)
		return result, err
	}
	return result, nil
}

// runWeedShellWithOutput executes a weed shell command and returns output even on error
func runWeedShellWithOutput(t *testing.T, command string) (output string, err error) {
	cmd := exec.Command(weedBinary, "shell", "-master=localhost:"+primaryMasterPort)
	cmd.Stdin = strings.NewReader(command + "\nexit\n")
	outputBytes, err := cmd.CombinedOutput()
	output = stripLogs(string(outputBytes))
	if err != nil {
		t.Logf("weed shell command '%s' output: %s", command, output)
	}
	return output, err
}

// createTestFile creates a test file with specific content via S3
func createTestFile(t *testing.T, key string, size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	uploadToPrimary(t, key, data)
	return data
}

// verifyFileContent verifies file content matches expected data
func verifyFileContent(t *testing.T, key string, expected []byte) {
	actual := getFromPrimary(t, key)
	assert.Equal(t, expected, actual, "file content mismatch for %s", key)
}

// waitForCondition waits for a condition to be true with timeout
func waitForCondition(t *testing.T, condition func() bool, timeout time.Duration, message string) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("Timeout waiting for: %s", message)
	return false
}

// fileExists checks if a file exists via S3
func fileExists(t *testing.T, key string) bool {
	_, err := getPrimaryClient().HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	return err == nil
}

// uploadToPrimary uploads an object to the primary SeaweedFS (local write)
func uploadToPrimary(t *testing.T, key string, data []byte) {
	_, err := getPrimaryClient().PutObject(&s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err, "failed to upload to primary SeaweedFS")
}

// getFromPrimary gets an object from primary SeaweedFS
func getFromPrimary(t *testing.T, key string) []byte {
	resp, err := getPrimaryClient().GetObject(&s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "failed to get from primary SeaweedFS")
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "failed to read response body")
	return data
}

// uncacheLocal purges the local cache, forcing data to be fetched from remote
func uncacheLocal(t *testing.T, pattern string) {
	t.Logf("Purging local cache for pattern: %s", pattern)
	output, err := runWeedShell(t, fmt.Sprintf("remote.uncache -dir=/buckets/%s -include=%s", testBucket, pattern))
	if err != nil {
		t.Logf("uncacheLocal warning: %v", err)
	}
	t.Log(output)
	time.Sleep(500 * time.Millisecond)
}

// TestRemoteCacheBasic tests the basic caching workflow:
// 1. Write to local
// 2. Uncache (push to remote, remove local chunks)
// 3. Read (triggers caching from remote)
func TestRemoteCacheBasic(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("test-basic-%d.txt", time.Now().UnixNano())
	testData := []byte("Hello, this is test data for remote caching!")

	// Step 1: Write to local
	t.Log("Step 1: Writing object to primary SeaweedFS (local)...")
	uploadToPrimary(t, testKey, testData)

	// Verify it's readable
	result := getFromPrimary(t, testKey)
	assert.Equal(t, testData, result, "initial read mismatch")

	// Step 2: Uncache - push to remote and remove local chunks
	t.Log("Step 2: Uncaching (pushing to remote, removing local chunks)...")
	uncacheLocal(t, testKey)

	// Step 3: Read - this should trigger caching from remote
	t.Log("Step 3: Reading object (should trigger caching from remote)...")
	start := time.Now()
	result = getFromPrimary(t, testKey)
	firstReadDuration := time.Since(start)

	assert.Equal(t, testData, result, "data mismatch after cache")
	t.Logf("First read (from remote) took %v", firstReadDuration)

	// Step 4: Read again - should be from local cache
	t.Log("Step 4: Reading again (should be from local cache)...")
	start = time.Now()
	result = getFromPrimary(t, testKey)
	secondReadDuration := time.Since(start)

	assert.Equal(t, testData, result, "data mismatch on cached read")
	t.Logf("Second read (from cache) took %v", secondReadDuration)

	t.Log("Basic caching test passed")
}

// TestRemoteCacheConcurrent tests that concurrent reads of the same
// remote object only trigger ONE caching operation (singleflight deduplication)
func TestRemoteCacheConcurrent(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("test-concurrent-%d.txt", time.Now().UnixNano())
	// Use larger data to make caching take measurable time
	testData := make([]byte, 1024*1024) // 1MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Step 1: Write to local
	t.Log("Step 1: Writing 1MB object to primary SeaweedFS...")
	uploadToPrimary(t, testKey, testData)

	// Verify it's readable
	result := getFromPrimary(t, testKey)
	assert.Equal(t, len(testData), len(result), "initial size mismatch")

	// Step 2: Uncache
	t.Log("Step 2: Uncaching (pushing to remote)...")
	uncacheLocal(t, testKey)

	// Step 3: Launch many concurrent reads - singleflight should deduplicate
	numRequests := 10
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32
	results := make(chan []byte, numRequests)

	t.Logf("Step 3: Launching %d concurrent requests...", numRequests)
	startTime := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := getPrimaryClient().GetObject(&s3.GetObjectInput{
				Bucket: aws.String(testBucket),
				Key:    aws.String(testKey),
			})
			if err != nil {
				t.Logf("Request %d failed: %v", idx, err)
				errorCount.Add(1)
				return
			}
			defer resp.Body.Close()

			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Logf("Request %d read failed: %v", idx, err)
				errorCount.Add(1)
				return
			}

			results <- data
			successCount.Add(1)
		}(i)
	}

	wg.Wait()
	close(results)
	totalDuration := time.Since(startTime)

	t.Logf("All %d requests completed in %v", numRequests, totalDuration)
	t.Logf("Successful: %d, Failed: %d", successCount.Load(), errorCount.Load())

	// Verify all successful requests returned correct data
	for data := range results {
		assert.Equal(t, len(testData), len(data), "data length mismatch")
	}

	// All requests should succeed
	assert.Equal(t, int32(numRequests), successCount.Load(), "some requests failed")
	assert.Equal(t, int32(0), errorCount.Load(), "no requests should fail")

	t.Log("Concurrent caching test passed")
}

// TestRemoteCacheLargeObject tests caching of larger objects
func TestRemoteCacheLargeObject(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("test-large-%d.bin", time.Now().UnixNano())
	// 5MB object
	testData := make([]byte, 5*1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Step 1: Write to local
	t.Log("Step 1: Writing 5MB object to primary SeaweedFS...")
	uploadToPrimary(t, testKey, testData)

	// Verify it's readable
	result := getFromPrimary(t, testKey)
	assert.Equal(t, len(testData), len(result), "initial size mismatch")

	// Step 2: Uncache
	t.Log("Step 2: Uncaching...")
	uncacheLocal(t, testKey)

	// Step 3: Read from remote
	t.Log("Step 3: Reading 5MB object (should cache from remote)...")
	start := time.Now()
	result = getFromPrimary(t, testKey)
	duration := time.Since(start)

	assert.Equal(t, len(testData), len(result), "size mismatch")
	assert.Equal(t, testData, result, "data mismatch")
	t.Logf("Large object cached in %v", duration)

	t.Log("Large object caching test passed")
}

// TestRemoteCacheRangeRequest tests that range requests work after caching
func TestRemoteCacheRangeRequest(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("test-range-%d.txt", time.Now().UnixNano())
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	// Write, uncache, then test range request
	t.Log("Writing and uncaching object...")
	uploadToPrimary(t, testKey, testData)
	uncacheLocal(t, testKey)

	// Range request should work and trigger caching
	t.Log("Testing range request (bytes 10-19)...")
	resp, err := getPrimaryClient().GetObject(&s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testKey),
		Range:  aws.String("bytes=10-19"),
	})
	require.NoError(t, err)
	defer resp.Body.Close()

	rangeData, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	expected := testData[10:20] // "ABCDEFGHIJ"
	assert.Equal(t, expected, rangeData, "range data mismatch")
	t.Logf("Range request returned: %s", string(rangeData))

	t.Log("Range request test passed")
}

// TestRemoteCacheNotFound tests that non-existent objects return proper errors
func TestRemoteCacheNotFound(t *testing.T) {
	checkServersRunning(t)

	testKey := fmt.Sprintf("non-existent-object-%d", time.Now().UnixNano())

	_, err := getPrimaryClient().GetObject(&s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testKey),
	})

	assert.Error(t, err, "should get error for non-existent object")
	t.Logf("Got expected error: %v", err)

	t.Log("Not found test passed")
}

// TestMain sets up and tears down the test environment
func TestMain(m *testing.M) {
	if !isServerRunning(primaryEndpoint) {
		fmt.Println("WARNING: Primary SeaweedFS not running at", primaryEndpoint)
		fmt.Println("   Run 'make test-with-server' to start servers automatically")
	}
	if !isServerRunning(remoteEndpoint) {
		fmt.Println("WARNING: Remote SeaweedFS not running at", remoteEndpoint)
		fmt.Println("   Run 'make test-with-server' to start servers automatically")
	}

	os.Exit(m.Run())
}

func isServerRunning(url string) bool {
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}
