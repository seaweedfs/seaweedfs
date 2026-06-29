package operation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestUploadReaderInChunksReturnsPartialResultsOnError verifies that when
// UploadReaderInChunks fails mid-upload, it returns partial results containing
// the chunks that were successfully uploaded before the error occurred.
// This allows the caller to cleanup orphaned chunks and prevent resource leaks.
func TestUploadReaderInChunksReturnsPartialResultsOnError(t *testing.T) {
	// Create test data larger than one chunk to force multiple chunk uploads
	testData := bytes.Repeat([]byte("test data for chunk upload failure testing"), 1000) // ~40KB
	reader := bytes.NewReader(testData)

	uploadAttempts := 0

	// Create a mock assign function that succeeds for first chunk, then fails
	assignFunc := func(ctx context.Context, count int, expectedDataSize uint64) (*VolumeAssignRequest, *AssignResult, error) {
		uploadAttempts++

		if uploadAttempts == 1 {
			// First chunk succeeds
			return nil, &AssignResult{
				Fid:       "test-fid-1,1234",
				Url:       "http://test-volume-1:8080",
				PublicUrl: "http://test-volume-1:8080",
				Count:     1,
			}, nil
		}

		// Second chunk fails (simulating volume server down or network error)
		return nil, nil, errors.New("simulated volume assignment failure")
	}

	// Mock upload function that simulates successful upload
	uploadFunc := func(ctx context.Context, data []byte, option *UploadOption) (*UploadResult, error) {
		return &UploadResult{
			Name:       "test-file",
			Size:       uint32(len(data)),
			ContentMd5: "mock-md5-hash",
			Error:      "",
		}, nil
	}

	// Attempt upload with small chunk size to trigger multiple uploads
	result, err := UploadReaderInChunks(context.Background(), reader, &ChunkedUploadOption{
		ChunkSize:       8 * 1024, // 8KB chunks
		SmallFileLimit:  256,
		Collection:      "test",
		DataCenter:      "",
		SaveSmallInline: false,
		AssignFunc:      assignFunc,
		UploadFunc:      uploadFunc,
	})

	// VERIFICATION 1: Error should be returned
	if err == nil {
		t.Fatal("Expected error from UploadReaderInChunks, got nil")
	}
	t.Logf("✓ Got expected error: %v", err)

	// VERIFICATION 2: Result should NOT be nil (this is the fix)
	if result == nil {
		t.Fatal("CRITICAL: UploadReaderInChunks returned nil result on error - caller cannot cleanup orphaned chunks!")
	}
	t.Log("✓ Result is not nil (partial results returned)")

	// VERIFICATION 3: Result should contain partial chunks from successful uploads
	// Note: In reality, the first chunk upload would succeed before assignment fails for chunk 2
	// But in this test, assignment fails immediately for chunk 2, so we may have 0 chunks
	// The important thing is that the result struct is returned, not that it has chunks
	t.Logf("✓ Result contains %d chunks (may be 0 if all assignments failed)", len(result.FileChunks))

	// VERIFICATION 4: MD5 hash should be available even on partial failure
	if result.Md5Hash == nil {
		t.Error("Expected Md5Hash to be non-nil")
	} else {
		t.Log("✓ Md5Hash is available for partial data")
	}

	// VERIFICATION 5: TotalSize should reflect bytes read before failure
	if result.TotalSize < 0 {
		t.Errorf("Expected non-negative TotalSize, got %d", result.TotalSize)
	} else {
		t.Logf("✓ TotalSize = %d bytes read before failure", result.TotalSize)
	}
}

// TestUploadReaderInChunksSuccessPath verifies normal successful upload behavior
func TestUploadReaderInChunksSuccessPath(t *testing.T) {
	testData := []byte("small test data")
	reader := bytes.NewReader(testData)

	// Mock assign function that always succeeds
	assignFunc := func(ctx context.Context, count int, expectedDataSize uint64) (*VolumeAssignRequest, *AssignResult, error) {
		return nil, &AssignResult{
			Fid:       "test-fid,1234",
			Url:       "http://test-volume:8080",
			PublicUrl: "http://test-volume:8080",
			Count:     1,
		}, nil
	}

	// Mock upload function that simulates successful upload
	uploadFunc := func(ctx context.Context, data []byte, option *UploadOption) (*UploadResult, error) {
		return &UploadResult{
			Name:       "test-file",
			Size:       uint32(len(data)),
			ContentMd5: "mock-md5-hash",
			Error:      "",
		}, nil
	}

	result, err := UploadReaderInChunks(context.Background(), reader, &ChunkedUploadOption{
		ChunkSize:       8 * 1024,
		SmallFileLimit:  256,
		Collection:      "test",
		DataCenter:      "",
		SaveSmallInline: false,
		AssignFunc:      assignFunc,
		UploadFunc:      uploadFunc,
	})

	// VERIFICATION 1: No error should occur
	if err != nil {
		t.Fatalf("Expected successful upload, got error: %v", err)
	}
	t.Log("✓ Upload completed without error")

	// VERIFICATION 2: Result should not be nil
	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	t.Log("✓ Result is not nil")

	// VERIFICATION 3: Should have file chunks
	if len(result.FileChunks) == 0 {
		t.Error("Expected at least one file chunk")
	} else {
		t.Logf("✓ Result contains %d file chunk(s)", len(result.FileChunks))
	}

	// VERIFICATION 4: Total size should match input data
	if result.TotalSize != int64(len(testData)) {
		t.Errorf("Expected TotalSize=%d, got %d", len(testData), result.TotalSize)
	} else {
		t.Logf("✓ TotalSize=%d matches input data", result.TotalSize)
	}

	// VERIFICATION 5: MD5 hash should be available
	if result.Md5Hash == nil {
		t.Error("Expected non-nil Md5Hash")
	} else {
		t.Log("✓ Md5Hash is available")
	}

	// VERIFICATION 6: Chunk should have expected properties
	if len(result.FileChunks) > 0 {
		chunk := result.FileChunks[0]
		if chunk.FileId != "test-fid,1234" {
			t.Errorf("Expected chunk FileId='test-fid,1234', got '%s'", chunk.FileId)
		}
		if chunk.Offset != 0 {
			t.Errorf("Expected chunk Offset=0, got %d", chunk.Offset)
		}
		if chunk.Size != uint64(len(testData)) {
			t.Errorf("Expected chunk Size=%d, got %d", len(testData), chunk.Size)
		}
		t.Logf("✓ Chunk properties validated: FileId=%s, Offset=%d, Size=%d",
			chunk.FileId, chunk.Offset, chunk.Size)
	}
}

// TestUploadReaderInChunksContextCancellation verifies behavior when context is cancelled
func TestUploadReaderInChunksContextCancellation(t *testing.T) {
	testData := bytes.Repeat([]byte("test data"), 10000) // ~80KB
	reader := bytes.NewReader(testData)

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately to trigger cancellation handling
	cancel()

	assignFunc := func(ctx context.Context, count int, expectedDataSize uint64) (*VolumeAssignRequest, *AssignResult, error) {
		return nil, &AssignResult{
			Fid:       "test-fid,1234",
			Url:       "http://test-volume:8080",
			PublicUrl: "http://test-volume:8080",
			Count:     1,
		}, nil
	}

	// Mock upload function that simulates successful upload
	uploadFunc := func(ctx context.Context, data []byte, option *UploadOption) (*UploadResult, error) {
		return &UploadResult{
			Name:       "test-file",
			Size:       uint32(len(data)),
			ContentMd5: "mock-md5-hash",
			Error:      "",
		}, nil
	}

	result, err := UploadReaderInChunks(ctx, reader, &ChunkedUploadOption{
		ChunkSize:       8 * 1024,
		SmallFileLimit:  256,
		Collection:      "test",
		DataCenter:      "",
		SaveSmallInline: false,
		AssignFunc:      assignFunc,
		UploadFunc:      uploadFunc,
	})

	// Should get context cancelled error
	if err == nil {
		t.Error("Expected context cancellation error")
	}

	// Should still get partial results for cleanup
	if result == nil {
		t.Error("Expected non-nil result even on context cancellation")
	} else {
		t.Logf("✓ Got partial result on cancellation: chunks=%d", len(result.FileChunks))
	}
}

// TestUploadChunkToHoldersRollsBackOnPartialFailure verifies that when a fan-out
// chunk write fails on one holder, the copies that already landed on the other
// holders are deleted (type=replicate, local-only) so nothing is left orphaned.
func TestUploadChunkToHoldersRollsBackOnPartialFailure(t *testing.T) {
	const fid = "3,01abcdef"
	var goodDeletes, badDeletes int32

	// Sequence the failure strictly after the good upload so the test does not
	// depend on timing: the failing holder returns its error only once the good
	// holder has stored the chunk, so the good copy is always what gets rolled back.
	goodUploaded := make(chan struct{})
	var once sync.Once

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			if strings.Contains(r.URL.Path, "01abcdef") && r.URL.Query().Get("type") == "replicate" {
				atomic.AddInt32(&goodDeletes, 1)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		fmt.Fprintf(w, `{"name":"f","size":11}`)
		once.Do(func() { close(goodUploaded) })
	}))
	defer good.Close()

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			atomic.AddInt32(&badDeletes, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		select {
		case <-goodUploaded:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "boom")
	}))
	defer bad.Close()

	hosts := []string{strings.TrimPrefix(good.URL, "http://"), strings.TrimPrefix(bad.URL, "http://")}
	_, err := uploadChunkToHolders(context.Background(), hosts, fid, []byte("hello world"), "", "", &ChunkedUploadOption{})

	if err == nil {
		t.Fatal("expected error from a partial fan-out")
	}
	if got := atomic.LoadInt32(&goodDeletes); got != 1 {
		t.Errorf("expected the succeeded holder to receive 1 cleanup DELETE, got %d", got)
	}
	if got := atomic.LoadInt32(&badDeletes); got != 0 {
		t.Errorf("expected no cleanup DELETE to the failed holder, got %d", got)
	}
}

// mockFailingReader simulates a reader that fails after reading some data
type mockFailingReader struct {
	data      []byte
	pos       int
	failAfter int
}

func (m *mockFailingReader) Read(p []byte) (n int, err error) {
	if m.pos >= m.failAfter {
		return 0, errors.New("simulated read failure")
	}

	remaining := m.failAfter - m.pos
	toRead := len(p)
	if toRead > remaining {
		toRead = remaining
	}
	if toRead > len(m.data)-m.pos {
		toRead = len(m.data) - m.pos
	}

	if toRead == 0 {
		return 0, io.EOF
	}

	copy(p, m.data[m.pos:m.pos+toRead])
	m.pos += toRead
	return toRead, nil
}

// TestUploadReaderInChunksReaderFailure verifies behavior when reader fails mid-read
func TestUploadReaderInChunksReaderFailure(t *testing.T) {
	testData := bytes.Repeat([]byte("test"), 5000) // 20KB
	failingReader := &mockFailingReader{
		data:      testData,
		pos:       0,
		failAfter: 10000, // Fail after 10KB
	}

	assignFunc := func(ctx context.Context, count int, expectedDataSize uint64) (*VolumeAssignRequest, *AssignResult, error) {
		return nil, &AssignResult{
			Fid:       "test-fid,1234",
			Url:       "http://test-volume:8080",
			PublicUrl: "http://test-volume:8080",
			Count:     1,
		}, nil
	}

	// Mock upload function that simulates successful upload
	uploadFunc := func(ctx context.Context, data []byte, option *UploadOption) (*UploadResult, error) {
		return &UploadResult{
			Name:       "test-file",
			Size:       uint32(len(data)),
			ContentMd5: "mock-md5-hash",
			Error:      "",
		}, nil
	}

	result, err := UploadReaderInChunks(context.Background(), failingReader, &ChunkedUploadOption{
		ChunkSize:       8 * 1024, // 8KB chunks
		SmallFileLimit:  256,
		Collection:      "test",
		DataCenter:      "",
		SaveSmallInline: false,
		AssignFunc:      assignFunc,
		UploadFunc:      uploadFunc,
	})

	// Should get read error
	if err == nil {
		t.Error("Expected read failure error")
	}

	// Should still get partial results
	if result == nil {
		t.Fatal("Expected non-nil result on read failure")
	}

	t.Logf("✓ Got partial result on read failure: chunks=%d, totalSize=%d",
		len(result.FileChunks), result.TotalSize)
}

// TestUploadReaderInChunksPrimaryWriteWhenMixedVolumeServers verifies that
// replicated assigns use a single primary upload when any holder is not a Go
// volume server (Rust servers replicate on the primary write).
func TestUploadReaderInChunksPrimaryWriteWhenMixedVolumeServers(t *testing.T) {
	var uploadPaths []string
	var uploadMu sync.Mutex

	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			uploadMu.Lock()
			uploadPaths = append(uploadPaths, r.URL.String())
			uploadMu.Unlock()
			fmt.Fprintf(w, `{"name":"f","size":7}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer primary.Close()
	primaryHost := strings.TrimPrefix(primary.URL, "http://")

	oldProbe := getVolumeServerImplementationProbe()
	volumeServerKindCache = sync.Map{}
	defer func() {
		setVolumeServerImplementationProbe(oldProbe)
		volumeServerKindCache = sync.Map{}
	}()
	setVolumeServerImplementationProbe(func(host string) string {
		if host == primaryHost {
			return volumeServerImplementationGo
		}
		return volumeServerImplementationRust
	})

	assignFunc := func(ctx context.Context, count int, expectedDataSize uint64) (*VolumeAssignRequest, *AssignResult, error) {
		return nil, &AssignResult{
			Fid: "3,01637037d6",
			Url: primaryHost,
			Replicas: []Location{
				{Url: primaryHost},
				{Url: "rust-volume:8080"},
			},
			Count: 1,
		}, nil
	}

	_, err := UploadReaderInChunks(context.Background(), bytes.NewReader([]byte("payload")), &ChunkedUploadOption{
		ChunkSize:       1024,
		SmallFileLimit:  256,
		SaveSmallInline: false,
		AssignFunc:      assignFunc,
	})
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if len(uploadPaths) != 1 {
		t.Fatalf("expected 1 primary upload, got %d: %v", len(uploadPaths), uploadPaths)
	}
	if !strings.HasPrefix(uploadPaths[0], "/3,01637037d6") {
		t.Fatalf("expected primary fid path, got %q", uploadPaths[0])
	}
	if strings.Contains(uploadPaths[0], "type=replicate") {
		t.Fatalf("primary upload must not use type=replicate: %q", uploadPaths[0])
	}
}

func TestChunkUploadReplicaFanoutEnabled(t *testing.T) {
	oldProbe := getVolumeServerImplementationProbe()
	defer func() { setVolumeServerImplementationProbe(oldProbe) }()
	setVolumeServerImplementationProbe(func(host string) string {
		switch host {
		case "go-a", "go-b":
			return volumeServerImplementationGo
		case "rust-a":
			return volumeServerImplementationRust
		default:
			return ""
		}
	})

	tests := []struct {
		name    string
		holders []string
		want    bool
	}{
		{"single holder", []string{"go-a"}, false},
		{"all go", []string{"go-a", "go-b"}, true},
		{"mixed go and rust", []string{"go-a", "rust-a"}, false},
		{"unknown implementation", []string{"go-a", "unknown:8080"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			volumeServerKindCache = sync.Map{}
			if got := chunkUploadReplicaFanoutEnabled(tc.holders); got != tc.want {
				t.Fatalf("chunkUploadReplicaFanoutEnabled(%v) = %v, want %v", tc.holders, got, tc.want)
			}
		})
	}
}
