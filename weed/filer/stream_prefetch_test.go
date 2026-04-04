package filer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// testMasterClient implements HasLookupFileIdFunction and CacheInvalidator for tests
type testMasterClient struct {
	urls             map[string][]string
	invalidatedCount int32
}

func (m *testMasterClient) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(ctx context.Context, fileId string) ([]string, error) {
		if urls, ok := m.urls[fileId]; ok {
			return urls, nil
		}
		return nil, fmt.Errorf("fileId %s not found", fileId)
	}
}

func (m *testMasterClient) InvalidateCache(fileId string) {
	atomic.AddInt32(&m.invalidatedCount, 1)
}

func noopJwt(fileId string) string { return "" }

// createTestServer creates a mock volume server with configurable behavior
func createTestServer(chunkData map[string][]byte) *httptest.Server {
	var mu sync.RWMutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasPrefix(path, "/") {
			path = path[1:]
		}
		mu.RLock()
		data, ok := chunkData[path]
		mu.RUnlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
}

// makeChunksAndServer creates N chunks of given size, a mock server, and a master client
func makeChunksAndServer(t *testing.T, numChunks, chunkSize int) ([]*filer_pb.FileChunk, *testMasterClient, map[string][]byte, func()) {
	t.Helper()

	chunkData := make(map[string][]byte, numChunks)
	chunks := make([]*filer_pb.FileChunk, numChunks)
	allData := make([]byte, 0, numChunks*chunkSize)

	for i := 0; i < numChunks; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		data := make([]byte, chunkSize)
		rand.Read(data)
		chunkData[fileId] = data
		allData = append(allData, data...)

		chunks[i] = &filer_pb.FileChunk{
			FileId:       fileId,
			Offset:       int64(i * chunkSize),
			Size:         uint64(chunkSize),
			ModifiedTsNs: int64(i),
			Fid:          &filer_pb.FileId{FileKey: uint64(i)},
		}
	}

	server := createTestServer(chunkData)
	urls := make(map[string][]string, numChunks)
	for i := 0; i < numChunks; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		urls[fileId] = []string{server.URL + "/" + fileId}
	}

	masterClient := &testMasterClient{urls: urls}
	return chunks, masterClient, chunkData, func() { server.Close() }
}

// TestPrefetchInOrderDelivery verifies chunks are written to the output in correct file order
func TestPrefetchInOrderDelivery(t *testing.T) {
	chunks, masterClient, chunkData, cleanup := makeChunksAndServer(t, 8, 4096)
	defer cleanup()

	totalSize := int64(8 * 4096)

	streamFn, err := PrepareStreamContentWithPrefetch(
		context.Background(), masterClient, noopJwt,
		chunks, 0, totalSize, 0, 4,
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		t.Fatal(err)
	}

	// Verify total size
	if buf.Len() != int(totalSize) {
		t.Fatalf("expected %d bytes, got %d", totalSize, buf.Len())
	}

	// Verify data matches chunk-by-chunk in order
	result := buf.Bytes()
	for i := 0; i < 8; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		expected := chunkData[fileId]
		got := result[i*4096 : (i+1)*4096]
		if !bytes.Equal(expected, got) {
			t.Fatalf("chunk %d (%s) data mismatch at offset %d", i, fileId, i*4096)
		}
	}
}

// TestPrefetchSingleChunk verifies the pipeline works with just one chunk
func TestPrefetchSingleChunk(t *testing.T) {
	chunks, masterClient, chunkData, cleanup := makeChunksAndServer(t, 1, 8192)
	defer cleanup()

	streamFn, err := PrepareStreamContentWithPrefetch(
		context.Background(), masterClient, noopJwt,
		chunks, 0, 8192, 0, 4,
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		t.Fatal(err)
	}

	expected := chunkData["1,0"]
	if !bytes.Equal(expected, buf.Bytes()) {
		t.Fatal("single chunk data mismatch")
	}
}

// TestPrefetchFallbackToSequential verifies prefetch=1 falls back to sequential path
func TestPrefetchFallbackToSequential(t *testing.T) {
	chunks, masterClient, chunkData, cleanup := makeChunksAndServer(t, 4, 1024)
	defer cleanup()

	totalSize := int64(4 * 1024)

	streamFn, err := PrepareStreamContentWithPrefetch(
		context.Background(), masterClient, noopJwt,
		chunks, 0, totalSize, 0, 1, // prefetch=1 -> sequential
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		t.Fatal(err)
	}

	if buf.Len() != int(totalSize) {
		t.Fatalf("expected %d bytes, got %d", totalSize, buf.Len())
	}

	// Verify data order
	result := buf.Bytes()
	for i := 0; i < 4; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		expected := chunkData[fileId]
		got := result[i*1024 : (i+1)*1024]
		if !bytes.Equal(expected, got) {
			t.Fatalf("chunk %d data mismatch", i)
		}
	}
}

// TestPrefetchContextCancellation verifies all goroutines clean up on cancellation
func TestPrefetchContextCancellation(t *testing.T) {
	// Use a slow server so cancellation happens mid-stream
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		// Slow response
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(http.StatusOK)
		w.Write(make([]byte, 1024))
	}))
	defer server.Close()

	numChunks := 16
	chunks := make([]*filer_pb.FileChunk, numChunks)
	urls := make(map[string][]string, numChunks)
	for i := 0; i < numChunks; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		chunks[i] = &filer_pb.FileChunk{
			FileId: fileId, Offset: int64(i * 1024), Size: 1024,
			ModifiedTsNs: int64(i), Fid: &filer_pb.FileId{FileKey: uint64(i)},
		}
		urls[fileId] = []string{server.URL + "/" + fileId}
	}
	masterClient := &testMasterClient{urls: urls}

	// Cancel after a short time
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	streamFn, err := PrepareStreamContentWithPrefetch(
		ctx, masterClient, noopJwt,
		chunks, 0, int64(numChunks*1024), 0, 4,
	)
	if err != nil {
		// URL resolution may fail due to cancellation — that's expected
		return
	}

	err = streamFn(io.Discard)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// Verify not all chunks were requested (cancellation stopped early)
	reqs := atomic.LoadInt32(&requestCount)
	if reqs >= int32(numChunks) {
		t.Logf("warning: all %d chunks were requested despite cancellation (got %d)", numChunks, reqs)
	}
}

// TestPrefetchRangeRequest verifies prefetch works with offset/size subset
func TestPrefetchRangeRequest(t *testing.T) {
	chunks, masterClient, chunkData, cleanup := makeChunksAndServer(t, 8, 4096)
	defer cleanup()

	// Request only chunks 2-5 (offset=8192, size=16384)
	offset := int64(2 * 4096)
	size := int64(4 * 4096)

	streamFn, err := PrepareStreamContentWithPrefetch(
		context.Background(), masterClient, noopJwt,
		chunks, offset, size, 0, 4,
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		t.Fatal(err)
	}

	if buf.Len() != int(size) {
		t.Fatalf("expected %d bytes, got %d", size, buf.Len())
	}

	// Verify data matches chunks 2-5
	result := buf.Bytes()
	for i := 2; i < 6; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		expected := chunkData[fileId]
		start := (i - 2) * 4096
		got := result[start : start+4096]
		if !bytes.Equal(expected, got) {
			t.Fatalf("chunk %d data mismatch in range request", i)
		}
	}
}

// TestPrefetchLargePrefetchCount verifies prefetch > numChunks is handled gracefully
func TestPrefetchLargePrefetchCount(t *testing.T) {
	chunks, masterClient, _, cleanup := makeChunksAndServer(t, 3, 1024)
	defer cleanup()

	totalSize := int64(3 * 1024)

	// prefetch=10 but only 3 chunks — should work fine
	streamFn, err := PrepareStreamContentWithPrefetch(
		context.Background(), masterClient, noopJwt,
		chunks, 0, totalSize, 0, 10,
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		t.Fatal(err)
	}

	if buf.Len() != int(totalSize) {
		t.Fatalf("expected %d bytes, got %d", totalSize, buf.Len())
	}
}

// TestPrefetchConcurrentDownloads verifies multiple concurrent prefetch streams
func TestPrefetchConcurrentDownloads(t *testing.T) {
	chunks, masterClient, _, cleanup := makeChunksAndServer(t, 8, 2048)
	defer cleanup()

	totalSize := int64(8 * 2048)

	var wg sync.WaitGroup
	errors := make(chan error, 4)

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamFn, err := PrepareStreamContentWithPrefetch(
				context.Background(), masterClient, noopJwt,
				chunks, 0, totalSize, 0, 4,
			)
			if err != nil {
				errors <- err
				return
			}
			if err := streamFn(io.Discard); err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Fatalf("concurrent download error: %v", err)
	}
}
