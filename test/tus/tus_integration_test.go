package tus

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	TusVersion     = "1.0.0"
	testFilerPort  = "18888"
	testMasterPort = "19333"
	testVolumePort = "18080"
)

// TestCluster represents a running SeaweedFS cluster for testing
type TestCluster struct {
	masterCmd *exec.Cmd
	volumeCmd *exec.Cmd
	filerCmd  *exec.Cmd
	dataDir   string
}

func (c *TestCluster) Stop() {
	if c.filerCmd != nil && c.filerCmd.Process != nil {
		c.filerCmd.Process.Signal(os.Interrupt)
		c.filerCmd.Wait()
	}
	if c.volumeCmd != nil && c.volumeCmd.Process != nil {
		c.volumeCmd.Process.Signal(os.Interrupt)
		c.volumeCmd.Wait()
	}
	if c.masterCmd != nil && c.masterCmd.Process != nil {
		c.masterCmd.Process.Signal(os.Interrupt)
		c.masterCmd.Wait()
	}
}

func (c *TestCluster) FilerURL() string {
	return fmt.Sprintf("http://127.0.0.1:%s", testFilerPort)
}

func (c *TestCluster) TusURL() string {
	return fmt.Sprintf("%s/.tus", c.FilerURL())
}

// FullURL converts a relative path to a full URL
func (c *TestCluster) FullURL(path string) string {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	return fmt.Sprintf("http://127.0.0.1:%s%s", testFilerPort, path)
}

// startTestCluster starts a SeaweedFS cluster for testing
func startTestCluster(t *testing.T, ctx context.Context) (*TestCluster, error) {
	weedBinary := findWeedBinary()
	if weedBinary == "" {
		return nil, fmt.Errorf("weed binary not found - please build it first: cd weed && go build")
	}

	dataDir, err := os.MkdirTemp("", "seaweedfs_tus_test_")
	if err != nil {
		return nil, err
	}

	cluster := &TestCluster{dataDir: dataDir}

	// Create subdirectories
	masterDir := filepath.Join(dataDir, "master")
	volumeDir := filepath.Join(dataDir, "volume")
	filerDir := filepath.Join(dataDir, "filer")
	os.MkdirAll(masterDir, 0755)
	os.MkdirAll(volumeDir, 0755)
	os.MkdirAll(filerDir, 0755)

	// Start master
	masterCmd := exec.CommandContext(ctx, weedBinary, "master",
		"-port", testMasterPort,
		"-mdir", masterDir,
		"-ip", "127.0.0.1",
	)
	masterLogFile, err := os.Create(filepath.Join(masterDir, "master.log"))
	if err != nil {
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to create master log: %v", err)
	}
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile
	if err := masterCmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to start master: %v", err)
	}
	cluster.masterCmd = masterCmd

	// Wait for master to be ready
	if err := waitForHTTPServer("http://127.0.0.1:"+testMasterPort+"/dir/status", 30*time.Second); err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("master not ready: %v", err)
	}

	// Start volume server
	volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
		"-port", testVolumePort,
		"-dir", volumeDir,
		"-mserver", "127.0.0.1:"+testMasterPort,
		"-ip", "127.0.0.1",
	)
	volumeLogFile, err := os.Create(filepath.Join(volumeDir, "volume.log"))
	if err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to create volume log: %v", err)
	}
	volumeCmd.Stdout = volumeLogFile
	volumeCmd.Stderr = volumeLogFile
	if err := volumeCmd.Start(); err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to start volume server: %v", err)
	}
	cluster.volumeCmd = volumeCmd

	// Wait for volume server to register with master
	if err := waitForHTTPServer("http://127.0.0.1:"+testVolumePort+"/status", 30*time.Second); err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("volume server not ready: %v", err)
	}

	// Start filer with TUS enabled
	filerCmd := exec.CommandContext(ctx, weedBinary, "filer",
		"-port", testFilerPort,
		"-master", "127.0.0.1:"+testMasterPort,
		"-ip", "127.0.0.1",
		"-defaultStoreDir", filerDir,
		"-tusBasePath", "/.tus",
	)
	filerLogFile, err := os.Create(filepath.Join(filerDir, "filer.log"))
	if err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to create filer log: %v", err)
	}
	filerCmd.Stdout = filerLogFile
	filerCmd.Stderr = filerLogFile
	if err := filerCmd.Start(); err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to start filer: %v", err)
	}
	cluster.filerCmd = filerCmd

	// Wait for filer
	if err := waitForHTTPServer("http://127.0.0.1:"+testFilerPort+"/", 30*time.Second); err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("filer not ready: %v", err)
	}

	// Wait a bit more for the cluster to fully stabilize
	// Volumes are created lazily, and we need to ensure the master topology is ready
	time.Sleep(5 * time.Second)

	return cluster, nil
}

func findWeedBinary() string {
	candidates := []string{
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
	if path, err := exec.LookPath("weed"); err == nil {
		return path
	}
	return ""
}

func waitForHTTPServer(url string, timeout time.Duration) error {
	start := time.Now()
	client := &http.Client{Timeout: 1 * time.Second}
	for time.Since(start) < timeout {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

// encodeTusMetadata encodes key-value pairs for Upload-Metadata header
func encodeTusMetadata(metadata map[string]string) string {
	var parts []string
	for k, v := range metadata {
		encoded := base64.StdEncoding.EncodeToString([]byte(v))
		parts = append(parts, fmt.Sprintf("%s %s", k, encoded))
	}
	return strings.Join(parts, ",")
}

// TestTusOptionsHandler tests the OPTIONS endpoint for capability discovery
func TestTusOptionsHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	// Test OPTIONS request
	req, err := http.NewRequest(http.MethodOptions, cluster.TusURL()+"/", nil)
	require.NoError(t, err)
	req.Header.Set("Tus-Resumable", TusVersion)

	client := &http.Client{}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Verify TUS headers
	assert.Equal(t, http.StatusOK, resp.StatusCode, "OPTIONS should return 200 OK")
	assert.Equal(t, TusVersion, resp.Header.Get("Tus-Resumable"), "Should return Tus-Resumable header")
	assert.NotEmpty(t, resp.Header.Get("Tus-Version"), "Should return Tus-Version header")
	assert.NotEmpty(t, resp.Header.Get("Tus-Extension"), "Should return Tus-Extension header")
	assert.NotEmpty(t, resp.Header.Get("Tus-Max-Size"), "Should return Tus-Max-Size header")
}

// TestTusBasicUpload tests a simple complete upload
func TestTusBasicUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	testData := []byte("Hello, TUS Protocol! This is a test file.")
	targetPath := "/testdir/testfile.txt"

	// Step 1: Create upload (POST)
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))
	createReq.Header.Set("Upload-Metadata", encodeTusMetadata(map[string]string{
		"filename":     "testfile.txt",
		"content-type": "text/plain",
	}))

	client := &http.Client{}
	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()

	assert.Equal(t, http.StatusCreated, createResp.StatusCode, "POST should return 201 Created")
	uploadLocation := createResp.Header.Get("Location")
	assert.NotEmpty(t, uploadLocation, "Should return Location header with upload URL")
	t.Logf("Upload location: %s", uploadLocation)

	// Step 2: Upload data (PATCH)
	patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", "0")
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")
	patchReq.Header.Set("Content-Length", strconv.Itoa(len(testData)))

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	defer patchResp.Body.Close()

	assert.Equal(t, http.StatusNoContent, patchResp.StatusCode, "PATCH should return 204 No Content")
	newOffset := patchResp.Header.Get("Upload-Offset")
	assert.Equal(t, strconv.Itoa(len(testData)), newOffset, "Upload-Offset should equal total file size")

	// Step 3: Verify the file was created
	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode, "GET should return 200 OK")
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, testData, body, "File content should match uploaded data")
}

// TestTusChunkedUpload tests uploading a file in multiple chunks
func TestTusChunkedUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	// Create test data (100KB)
	testData := make([]byte, 100*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	chunkSize := 32 * 1024 // 32KB chunks
	targetPath := "/chunked/largefile.bin"

	client := &http.Client{}

	// Step 1: Create upload
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()

	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")
	require.NotEmpty(t, uploadLocation)
	t.Logf("Upload location: %s", uploadLocation)

	// Step 2: Upload in chunks
	offset := 0
	for offset < len(testData) {
		end := offset + chunkSize
		if end > len(testData) {
			end = len(testData)
		}
		chunk := testData[offset:end]

		patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(chunk))
		require.NoError(t, err)
		patchReq.Header.Set("Tus-Resumable", TusVersion)
		patchReq.Header.Set("Upload-Offset", strconv.Itoa(offset))
		patchReq.Header.Set("Content-Type", "application/offset+octet-stream")
		patchReq.Header.Set("Content-Length", strconv.Itoa(len(chunk)))

		patchResp, err := client.Do(patchReq)
		require.NoError(t, err)
		patchResp.Body.Close()

		require.Equal(t, http.StatusNoContent, patchResp.StatusCode,
			"PATCH chunk at offset %d should return 204", offset)
		newOffset, err := strconv.Atoi(patchResp.Header.Get("Upload-Offset"))
		require.NoError(t, err, "Upload-Offset header should be a valid integer")
		require.Equal(t, end, newOffset, "New offset should be %d", end)

		t.Logf("Uploaded chunk: offset=%d, size=%d, newOffset=%d", offset, len(chunk), newOffset)
		offset = end
	}

	// Step 3: Verify the complete file
	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, testData, body, "File content should match uploaded data")
}

// TestTusHeadRequest tests the HEAD endpoint to get upload offset
func TestTusHeadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	testData := []byte("Test data for HEAD request verification")
	targetPath := "/headtest/file.txt"
	client := &http.Client{}

	// Create upload
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")

	// HEAD before any data uploaded - offset should be 0
	headReq1, err := http.NewRequest(http.MethodHead, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	headReq1.Header.Set("Tus-Resumable", TusVersion)

	headResp1, err := client.Do(headReq1)
	require.NoError(t, err)
	defer headResp1.Body.Close()

	assert.Equal(t, http.StatusOK, headResp1.StatusCode)
	assert.Equal(t, "0", headResp1.Header.Get("Upload-Offset"), "Initial offset should be 0")
	assert.Equal(t, strconv.Itoa(len(testData)), headResp1.Header.Get("Upload-Length"))

	// Upload half the data
	halfLen := len(testData) / 2
	patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[:halfLen]))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", "0")
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	patchResp.Body.Close()
	require.Equal(t, http.StatusNoContent, patchResp.StatusCode)

	// HEAD after partial upload - offset should be halfLen
	headReq2, err := http.NewRequest(http.MethodHead, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	headReq2.Header.Set("Tus-Resumable", TusVersion)

	headResp2, err := client.Do(headReq2)
	require.NoError(t, err)
	defer headResp2.Body.Close()

	assert.Equal(t, http.StatusOK, headResp2.StatusCode)
	assert.Equal(t, strconv.Itoa(halfLen), headResp2.Header.Get("Upload-Offset"),
		"Offset should be %d after partial upload", halfLen)
}

// TestTusDeleteUpload tests canceling an in-progress upload
func TestTusDeleteUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	testData := []byte("Data to be deleted")
	targetPath := "/deletetest/file.txt"
	client := &http.Client{}

	// Create upload
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")

	// Upload some data
	patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[:10]))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", "0")
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	patchResp.Body.Close()

	// Delete the upload
	deleteReq, err := http.NewRequest(http.MethodDelete, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	deleteReq.Header.Set("Tus-Resumable", TusVersion)

	deleteResp, err := client.Do(deleteReq)
	require.NoError(t, err)
	defer deleteResp.Body.Close()

	assert.Equal(t, http.StatusNoContent, deleteResp.StatusCode, "DELETE should return 204")

	// Verify upload is gone - HEAD should return 404
	headReq, err := http.NewRequest(http.MethodHead, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	headReq.Header.Set("Tus-Resumable", TusVersion)

	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	defer headResp.Body.Close()

	assert.Equal(t, http.StatusNotFound, headResp.StatusCode, "HEAD after DELETE should return 404")
}

// TestTusInvalidOffset tests error handling for mismatched offsets
func TestTusInvalidOffset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	testData := []byte("Test data for offset validation")
	targetPath := "/offsettest/file.txt"
	client := &http.Client{}

	// Create upload
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")

	// Try to upload with wrong offset (should be 0, but we send 100)
	patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", "100") // Wrong offset!
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	defer patchResp.Body.Close()

	assert.Equal(t, http.StatusConflict, patchResp.StatusCode,
		"PATCH with wrong offset should return 409 Conflict")
}

// TestTusUploadNotFound tests accessing a non-existent upload
func TestTusUploadNotFound(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	client := &http.Client{}
	fakeUploadURL := cluster.TusURL() + "/.uploads/nonexistent-upload-id"

	// HEAD on non-existent upload
	headReq, err := http.NewRequest(http.MethodHead, fakeUploadURL, nil)
	require.NoError(t, err)
	headReq.Header.Set("Tus-Resumable", TusVersion)

	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	defer headResp.Body.Close()

	assert.Equal(t, http.StatusNotFound, headResp.StatusCode,
		"HEAD on non-existent upload should return 404")

	// PATCH on non-existent upload
	patchReq, err := http.NewRequest(http.MethodPatch, fakeUploadURL, bytes.NewReader([]byte("data")))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", "0")
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	defer patchResp.Body.Close()

	assert.Equal(t, http.StatusNotFound, patchResp.StatusCode,
		"PATCH on non-existent upload should return 404")
}

// TestTusCreationWithUpload tests the creation-with-upload extension
func TestTusCreationWithUpload(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	testData := []byte("Small file uploaded in creation request")
	targetPath := "/creationwithupload/smallfile.txt"
	client := &http.Client{}

	// Create upload with data in the same request
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, bytes.NewReader(testData))
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))
	createReq.Header.Set("Content-Type", "application/offset+octet-stream")

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()

	assert.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")
	assert.NotEmpty(t, uploadLocation)

	// Check Upload-Offset header - should indicate all data was received
	uploadOffset := createResp.Header.Get("Upload-Offset")
	assert.Equal(t, strconv.Itoa(len(testData)), uploadOffset,
		"Upload-Offset should equal file size for complete upload")

	// Verify the file
	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, testData, body)
}

// TestTusConcatenation tests the concatenation extension: partial uploads
// assembled into a final upload
func TestTusConcatenation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	client := &http.Client{}

	// The extension is announced
	optionsReq, err := http.NewRequest(http.MethodOptions, cluster.TusURL()+"/", nil)
	require.NoError(t, err)
	optionsReq.Header.Set("Tus-Resumable", TusVersion)
	optionsResp, err := client.Do(optionsReq)
	require.NoError(t, err)
	optionsResp.Body.Close()
	assert.Contains(t, optionsResp.Header.Get("Tus-Extension"), "concatenation")

	partA := []byte("Hello, ")
	partB := []byte("concatenated TUS world!")
	targetPath := "/concat/final.txt"

	createPartial := func(size int) string {
		req, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
		require.NoError(t, err)
		req.Header.Set("Tus-Resumable", TusVersion)
		req.Header.Set("Upload-Length", strconv.Itoa(size))
		req.Header.Set("Upload-Concat", "partial")
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		location := resp.Header.Get("Location")
		require.NotEmpty(t, location)
		return location
	}

	patchAll := func(location string, data []byte) {
		req, err := http.NewRequest(http.MethodPatch, cluster.FullURL(location), bytes.NewReader(data))
		require.NoError(t, err)
		req.Header.Set("Tus-Resumable", TusVersion)
		req.Header.Set("Upload-Offset", "0")
		req.Header.Set("Content-Type", "application/offset+octet-stream")
		resp, err := client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	}

	locationA := createPartial(len(partA))
	locationB := createPartial(len(partB))
	concatHeader := "final;" + locationA + " " + locationB

	patchAll(locationA, partA)

	// Concatenation is rejected while a listed partial is unfinished
	prematureReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	prematureReq.Header.Set("Tus-Resumable", TusVersion)
	prematureReq.Header.Set("Upload-Concat", concatHeader)
	prematureResp, err := client.Do(prematureReq)
	require.NoError(t, err)
	prematureResp.Body.Close()
	require.Equal(t, http.StatusBadRequest, prematureResp.StatusCode)

	patchAll(locationB, partB)

	// A completed partial reports its status and does not land at the target
	headReq, err := http.NewRequest(http.MethodHead, cluster.FullURL(locationA), nil)
	require.NoError(t, err)
	headReq.Header.Set("Tus-Resumable", TusVersion)
	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	headResp.Body.Close()
	require.Equal(t, http.StatusOK, headResp.StatusCode)
	assert.Equal(t, "partial", headResp.Header.Get("Upload-Concat"))

	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	getResp.Body.Close()
	require.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"completed partials should not land at the target path")

	// Concatenate into the final upload
	finalReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	finalReq.Header.Set("Tus-Resumable", TusVersion)
	finalReq.Header.Set("Upload-Concat", concatHeader)
	finalReq.Header.Set("Upload-Metadata", encodeTusMetadata(map[string]string{
		"content-type": "text/plain",
	}))
	finalResp, err := client.Do(finalReq)
	require.NoError(t, err)
	finalResp.Body.Close()
	require.Equal(t, http.StatusCreated, finalResp.StatusCode)
	assert.NotEmpty(t, finalResp.Header.Get("Location"))

	// The target file holds both parts in order
	expected := append(append([]byte{}, partA...), partB...)
	getResp2, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp2.Body.Close()
	require.Equal(t, http.StatusOK, getResp2.StatusCode)
	body, err := io.ReadAll(getResp2.Body)
	require.NoError(t, err)
	assert.Equal(t, expected, body, "Concatenated file should hold both parts in order")
	assert.Contains(t, getResp2.Header.Get("Content-Type"), "text/plain")

	// The consumed partials are gone
	headReq2, err := http.NewRequest(http.MethodHead, cluster.FullURL(locationA), nil)
	require.NoError(t, err)
	headReq2.Header.Set("Tus-Resumable", TusVersion)
	headResp2, err := client.Do(headReq2)
	require.NoError(t, err)
	headResp2.Body.Close()
	require.Equal(t, http.StatusNotFound, headResp2.StatusCode,
		"consumed partial should be removed after concatenation")
}

// TestTusResumeAfterInterruption simulates resuming an upload after failure
func TestTusResumeAfterInterruption(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	// 50KB test data
	testData := make([]byte, 50*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	targetPath := "/resume/interrupted.bin"
	client := &http.Client{}

	// Create upload
	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")

	// Upload first 20KB
	firstChunkSize := 20 * 1024
	patchReq1, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[:firstChunkSize]))
	require.NoError(t, err)
	patchReq1.Header.Set("Tus-Resumable", TusVersion)
	patchReq1.Header.Set("Upload-Offset", "0")
	patchReq1.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp1, err := client.Do(patchReq1)
	require.NoError(t, err)
	patchResp1.Body.Close()
	require.Equal(t, http.StatusNoContent, patchResp1.StatusCode)

	t.Log("Simulating network interruption...")

	// Simulate resumption: Query current offset with HEAD
	headReq, err := http.NewRequest(http.MethodHead, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	headReq.Header.Set("Tus-Resumable", TusVersion)

	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	defer headResp.Body.Close()

	require.Equal(t, http.StatusOK, headResp.StatusCode)
	currentOffset, err := strconv.Atoi(headResp.Header.Get("Upload-Offset"))
	require.NoError(t, err, "Upload-Offset header should be a valid integer")
	t.Logf("Resumed upload at offset: %d", currentOffset)
	require.Equal(t, firstChunkSize, currentOffset)

	// Resume upload from current offset
	patchReq2, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[currentOffset:]))
	require.NoError(t, err)
	patchReq2.Header.Set("Tus-Resumable", TusVersion)
	patchReq2.Header.Set("Upload-Offset", strconv.Itoa(currentOffset))
	patchReq2.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp2, err := client.Do(patchReq2)
	require.NoError(t, err)
	patchResp2.Body.Close()
	require.Equal(t, http.StatusNoContent, patchResp2.StatusCode)

	// Verify complete file
	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp.Body.Close()

	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, testData, body, "Resumed upload should produce complete file")
}

// TestTusAbortedPatchKeepsStoredChunks checks that a PATCH cut off mid-body
// leaves the sub-chunks it already stored in place. The filer splits a PATCH
// into 4MB sub-chunks and records each one as it lands; the offset a resuming
// client reads back covers them, so their data has to survive the failure.
func TestTusAbortedPatchKeepsStoredChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	const subChunkSize = 4 * 1024 * 1024
	testData := make([]byte, 3*subChunkSize)
	for i := range testData {
		testData[i] = byte(i % 251)
	}
	targetPath := "/aborted/interrupted.bin"
	client := &http.Client{}

	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")

	// Promise the whole body, then reset the connection while a later
	// sub-chunk is still being read.
	conn, err := net.Dial("tcp", "127.0.0.1:"+testFilerPort)
	require.NoError(t, err)
	_, err = fmt.Fprintf(conn, "PATCH %s HTTP/1.1\r\nHost: 127.0.0.1:%s\r\nTus-Resumable: %s\r\nContent-Type: application/offset+octet-stream\r\nUpload-Offset: 0\r\nContent-Length: %d\r\n\r\n",
		uploadLocation, testFilerPort, TusVersion, len(testData))
	require.NoError(t, err)
	_, err = conn.Write(testData[:subChunkSize+1024*1024])
	require.NoError(t, err)
	time.Sleep(3 * time.Second)
	require.NoError(t, conn.(*net.TCPConn).SetLinger(0))
	require.NoError(t, conn.Close())
	t.Log("PATCH connection reset mid-body")

	time.Sleep(5 * time.Second)

	headReq, err := http.NewRequest(http.MethodHead, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	headReq.Header.Set("Tus-Resumable", TusVersion)

	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	headResp.Body.Close()
	require.Equal(t, http.StatusOK, headResp.StatusCode)
	currentOffset, err := strconv.Atoi(headResp.Header.Get("Upload-Offset"))
	require.NoError(t, err)
	require.Equal(t, subChunkSize, currentOffset, "the sub-chunk stored before the reset should count towards the offset")

	patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[currentOffset:]))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", strconv.Itoa(currentOffset))
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	patchResp.Body.Close()
	require.Equal(t, http.StatusNoContent, patchResp.StatusCode)

	// A vacuum reclaims whatever the filer deleted, so the file survives this
	// only if the chunks the session kept are still stored.
	vacuumResp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%s/vol/vacuum?garbageThreshold=0.001", testMasterPort))
	require.NoError(t, err)
	vacuumResp.Body.Close()
	require.Equal(t, http.StatusOK, vacuumResp.StatusCode)

	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, testData, body, "the resumed upload should read back whole")
}

// TestTusConcurrentPatchRefused checks that a PATCH sent while another PATCH
// on the same session is still consuming its body is refused with 423 Locked.
// Before the per-session claim, both were accepted at the same offset, recorded
// the range twice, and completion failed on the duplicate while HEAD reported
// the upload fully received - the file was never created and every stored byte
// became garbage when the session expired.
func TestTusConcurrentPatchRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	cluster, err := startTestCluster(t, ctx)
	require.NoError(t, err)
	defer func() {
		cluster.Stop()
		os.RemoveAll(cluster.dataDir)
	}()

	const subChunkSize = 4 * 1024 * 1024
	testData := make([]byte, 3*subChunkSize)
	for i := range testData {
		testData[i] = byte(i % 251)
	}
	targetPath := "/raced/video.bin"
	client := &http.Client{}

	createReq, err := http.NewRequest(http.MethodPost, cluster.TusURL()+targetPath, nil)
	require.NoError(t, err)
	createReq.Header.Set("Tus-Resumable", TusVersion)
	createReq.Header.Set("Upload-Length", strconv.Itoa(len(testData)))

	createResp, err := client.Do(createReq)
	require.NoError(t, err)
	createResp.Body.Close()
	require.Equal(t, http.StatusCreated, createResp.StatusCode)
	uploadLocation := createResp.Header.Get("Location")

	// PATCH A promises one sub-chunk and stalls mid-body, holding the session.
	conn, err := net.Dial("tcp", "127.0.0.1:"+testFilerPort)
	require.NoError(t, err)
	defer conn.Close()
	// bound the raw reads below so a filer that never answers fails here
	require.NoError(t, conn.SetDeadline(time.Now().Add(60*time.Second)))
	_, err = fmt.Fprintf(conn, "PATCH %s HTTP/1.1\r\nHost: 127.0.0.1:%s\r\nTus-Resumable: %s\r\nContent-Type: application/offset+octet-stream\r\nUpload-Offset: 0\r\nContent-Length: %d\r\n\r\n",
		uploadLocation, testFilerPort, TusVersion, subChunkSize)
	require.NoError(t, err)
	_, err = conn.Write(testData[:1024*1024])
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// PATCH B is the client's retry of the same range while A is in flight.
	retryReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[:subChunkSize]))
	require.NoError(t, err)
	retryReq.Header.Set("Tus-Resumable", TusVersion)
	retryReq.Header.Set("Upload-Offset", "0")
	retryReq.Header.Set("Content-Type", "application/offset+octet-stream")

	retryResp, err := client.Do(retryReq)
	require.NoError(t, err)
	retryResp.Body.Close()
	require.Equal(t, http.StatusLocked, retryResp.StatusCode, "a concurrent PATCH must be refused, not recorded twice")

	// Finish PATCH A and read its response.
	_, err = conn.Write(testData[1024*1024 : subChunkSize])
	require.NoError(t, err)
	respReader := bufio.NewReader(conn)
	respA, err := http.ReadResponse(respReader, nil)
	require.NoError(t, err)
	respA.Body.Close()
	require.Equal(t, http.StatusNoContent, respA.StatusCode)

	headReq, err := http.NewRequest(http.MethodHead, cluster.FullURL(uploadLocation), nil)
	require.NoError(t, err)
	headReq.Header.Set("Tus-Resumable", TusVersion)

	headResp, err := client.Do(headReq)
	require.NoError(t, err)
	headResp.Body.Close()
	require.Equal(t, http.StatusOK, headResp.StatusCode)
	currentOffset, err := strconv.Atoi(headResp.Header.Get("Upload-Offset"))
	require.NoError(t, err)
	require.Equal(t, subChunkSize, currentOffset, "only PATCH A's sub-chunk should be recorded")

	patchReq, err := http.NewRequest(http.MethodPatch, cluster.FullURL(uploadLocation), bytes.NewReader(testData[currentOffset:]))
	require.NoError(t, err)
	patchReq.Header.Set("Tus-Resumable", TusVersion)
	patchReq.Header.Set("Upload-Offset", strconv.Itoa(currentOffset))
	patchReq.Header.Set("Content-Type", "application/offset+octet-stream")

	patchResp, err := client.Do(patchReq)
	require.NoError(t, err)
	patchResp.Body.Close()
	require.Equal(t, http.StatusNoContent, patchResp.StatusCode)

	getResp, err := client.Get(cluster.FilerURL() + targetPath)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, testData, body, "the raced upload should complete with intact content")
}
