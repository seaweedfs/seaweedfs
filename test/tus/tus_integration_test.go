package tus

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
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
	masterLogFile, _ := os.Create(filepath.Join(masterDir, "master.log"))
	masterCmd.Stdout = masterLogFile
	masterCmd.Stderr = masterLogFile
	if err := masterCmd.Start(); err != nil {
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to start master: %v", err)
	}
	cluster.masterCmd = masterCmd

	// Wait for master
	time.Sleep(2 * time.Second)

	// Start volume server
	volumeCmd := exec.CommandContext(ctx, weedBinary, "volume",
		"-port", testVolumePort,
		"-dir", volumeDir,
		"-mserver", "127.0.0.1:"+testMasterPort,
		"-ip", "127.0.0.1",
	)
	volumeLogFile, _ := os.Create(filepath.Join(volumeDir, "volume.log"))
	volumeCmd.Stdout = volumeLogFile
	volumeCmd.Stderr = volumeLogFile
	if err := volumeCmd.Start(); err != nil {
		cluster.Stop()
		os.RemoveAll(dataDir)
		return nil, fmt.Errorf("failed to start volume server: %v", err)
	}
	cluster.volumeCmd = volumeCmd

	// Wait for volume server
	time.Sleep(2 * time.Second)

	// Start filer with TUS enabled
	filerCmd := exec.CommandContext(ctx, weedBinary, "filer",
		"-port", testFilerPort,
		"-master", "127.0.0.1:"+testMasterPort,
		"-ip", "127.0.0.1",
		"-dataCenter", "dc1",
	)
	filerLogFile, _ := os.Create(filepath.Join(filerDir, "filer.log"))
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
		newOffset, _ := strconv.Atoi(patchResp.Header.Get("Upload-Offset"))
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
	currentOffset, _ := strconv.Atoi(headResp.Header.Get("Upload-Offset"))
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

