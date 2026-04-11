package filer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func TestMain(m *testing.M) {
	// Initialize the global HTTP client required by ReadUrlAsStream
	util_http.InitGlobalHttpClient()
	os.Exit(m.Run())
}

// mockMasterClientForBenchmark implements HasLookupFileIdFunction and CacheInvalidator
type mockMasterClientForBenchmark struct {
	urls map[string][]string
}

func (m *mockMasterClientForBenchmark) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return func(ctx context.Context, fileId string) ([]string, error) {
		if urls, ok := m.urls[fileId]; ok {
			return urls, nil
		}
		return nil, fmt.Errorf("fileId %s not found", fileId)
	}
}

func (m *mockMasterClientForBenchmark) InvalidateCache(fileId string) {}

// noopJwtFunc returns empty JWT for testing
func noopJwtFunc(fileId string) string {
	return ""
}

// createMockVolumeServer creates an httptest server that serves chunk data
// with configurable per-request latency to simulate network conditions.
// The latency is applied once per request (simulating RTT), not per byte.
func createMockVolumeServer(chunkData map[string][]byte, latency time.Duration) *httptest.Server {
	var mu sync.RWMutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate network latency (connection setup + RTT)
		if latency > 0 {
			time.Sleep(latency)
		}

		// Extract fileId from path (e.g., "/1,abc123")
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

		// Handle Range header
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start, end int64
			fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
			if start >= 0 && end < int64(len(data)) && start <= end {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(data[start : end+1])
				return
			}
		}

		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
}

// benchmarkConfig holds parameters for a single benchmark scenario
type benchmarkConfig struct {
	numChunks int
	chunkSize int
	latency   time.Duration
	prefetch  int // 0 = sequential
}

func (c benchmarkConfig) name() string {
	name := fmt.Sprintf("chunks=%d/size=%dKB/latency=%dms",
		c.numChunks, c.chunkSize/1024, c.latency.Milliseconds())
	if c.prefetch > 0 {
		name += fmt.Sprintf("/prefetch=%d", c.prefetch)
	}
	return name
}

// setupBenchmark creates mock infrastructure and returns chunks, master client, and cleanup func
func setupBenchmark(b *testing.B, cfg benchmarkConfig) ([]*filer_pb.FileChunk, *mockMasterClientForBenchmark, func()) {
	b.Helper()

	// Generate random chunk data
	chunkData := make(map[string][]byte, cfg.numChunks)
	chunks := make([]*filer_pb.FileChunk, cfg.numChunks)

	for i := 0; i < cfg.numChunks; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		data := make([]byte, cfg.chunkSize)
		rand.Read(data)
		chunkData[fileId] = data

		chunks[i] = &filer_pb.FileChunk{
			FileId:       fileId,
			Offset:       int64(i * cfg.chunkSize),
			Size:         uint64(cfg.chunkSize),
			ModifiedTsNs: int64(i),
			Fid:          &filer_pb.FileId{FileKey: uint64(i)},
		}
	}

	// Start mock volume server
	server := createMockVolumeServer(chunkData, cfg.latency)

	// Build URL map
	urls := make(map[string][]string, cfg.numChunks)
	for i := 0; i < cfg.numChunks; i++ {
		fileId := fmt.Sprintf("1,%x", i)
		urls[fileId] = []string{server.URL + "/" + fileId}
	}

	masterClient := &mockMasterClientForBenchmark{urls: urls}
	cleanup := func() { server.Close() }

	return chunks, masterClient, cleanup
}

// runSequentialBenchmark runs the current sequential streaming path
func runSequentialBenchmark(b *testing.B, cfg benchmarkConfig) {
	chunks, masterClient, cleanup := setupBenchmark(b, cfg)
	defer cleanup()

	totalSize := int64(cfg.numChunks * cfg.chunkSize)

	b.ResetTimer()
	b.SetBytes(totalSize)

	for i := 0; i < b.N; i++ {
		streamFn, err := PrepareStreamContentWithThrottler(
			context.Background(),
			masterClient,
			noopJwtFunc,
			chunks,
			0,
			totalSize,
			0, // no throttle
		)
		if err != nil {
			b.Fatal(err)
		}
		if err := streamFn(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamSequential benchmarks the current sequential streaming path.
// This provides the BEFORE baseline for comparison.
func BenchmarkStreamSequential(b *testing.B) {
	configs := []benchmarkConfig{
		// Pure throughput (no latency)
		{numChunks: 16, chunkSize: 64 * 1024, latency: 0},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 0},
		// Moderate latency — shows RTT gap overhead
		{numChunks: 16, chunkSize: 64 * 1024, latency: 5 * time.Millisecond},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 5 * time.Millisecond},
		// High latency — significant RTT overhead
		{numChunks: 16, chunkSize: 64 * 1024, latency: 20 * time.Millisecond},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 10 * time.Millisecond},
	}

	for _, cfg := range configs {
		b.Run(cfg.name(), func(b *testing.B) {
			runSequentialBenchmark(b, cfg)
		})
	}
}

// BenchmarkStreamSequentialVerify is a quick functional test that the benchmark
// infrastructure works correctly — ensures data integrity through the pipeline.
func BenchmarkStreamSequentialVerify(b *testing.B) {
	cfg := benchmarkConfig{numChunks: 4, chunkSize: 1024, latency: 0}
	chunks, masterClient, cleanup := setupBenchmark(b, cfg)
	defer cleanup()

	totalSize := int64(cfg.numChunks * cfg.chunkSize)

	streamFn, err := PrepareStreamContentWithThrottler(
		context.Background(),
		masterClient,
		noopJwtFunc,
		chunks,
		0,
		totalSize,
		0,
	)
	if err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		b.Fatal(err)
	}

	if buf.Len() != int(totalSize) {
		b.Fatalf("expected %d bytes, got %d", totalSize, buf.Len())
	}
}

// runPrefetchBenchmark runs the new prefetch streaming path
func runPrefetchBenchmark(b *testing.B, cfg benchmarkConfig) {
	chunks, masterClient, cleanup := setupBenchmark(b, cfg)
	defer cleanup()

	totalSize := int64(cfg.numChunks * cfg.chunkSize)

	b.ResetTimer()
	b.SetBytes(totalSize)

	for i := 0; i < b.N; i++ {
		streamFn, err := PrepareStreamContentWithPrefetch(
			context.Background(),
			masterClient,
			noopJwtFunc,
			chunks,
			0,
			totalSize,
			0, // no throttle
			cfg.prefetch,
		)
		if err != nil {
			b.Fatal(err)
		}
		if err := streamFn(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamPrefetch benchmarks the new prefetch streaming path.
// Compare against BenchmarkStreamSequential for the AFTER measurement.
func BenchmarkStreamPrefetch(b *testing.B) {
	configs := []benchmarkConfig{
		// Pure throughput (no latency) — should be similar to sequential
		{numChunks: 16, chunkSize: 64 * 1024, latency: 0, prefetch: 4},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 0, prefetch: 4},
		// Moderate latency — prefetch should eliminate most RTT overhead
		{numChunks: 16, chunkSize: 64 * 1024, latency: 5 * time.Millisecond, prefetch: 4},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 5 * time.Millisecond, prefetch: 4},
		// High latency — most benefit from prefetch
		{numChunks: 16, chunkSize: 64 * 1024, latency: 20 * time.Millisecond, prefetch: 4},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 10 * time.Millisecond, prefetch: 4},
		// Vary prefetch count with moderate latency
		{numChunks: 64, chunkSize: 64 * 1024, latency: 5 * time.Millisecond, prefetch: 2},
		{numChunks: 64, chunkSize: 64 * 1024, latency: 5 * time.Millisecond, prefetch: 8},
	}

	for _, cfg := range configs {
		b.Run(cfg.name(), func(b *testing.B) {
			runPrefetchBenchmark(b, cfg)
		})
	}
}

// BenchmarkStreamPrefetchVerify verifies data integrity through the prefetch pipeline.
func BenchmarkStreamPrefetchVerify(b *testing.B) {
	cfg := benchmarkConfig{numChunks: 4, chunkSize: 1024, latency: 0, prefetch: 4}
	chunks, masterClient, cleanup := setupBenchmark(b, cfg)
	defer cleanup()

	totalSize := int64(cfg.numChunks * cfg.chunkSize)

	streamFn, err := PrepareStreamContentWithPrefetch(
		context.Background(),
		masterClient,
		noopJwtFunc,
		chunks,
		0,
		totalSize,
		0,
		cfg.prefetch,
	)
	if err != nil {
		b.Fatal(err)
	}

	var buf bytes.Buffer
	if err := streamFn(&buf); err != nil {
		b.Fatal(err)
	}

	if buf.Len() != int(totalSize) {
		b.Fatalf("expected %d bytes, got %d", totalSize, buf.Len())
	}
}
