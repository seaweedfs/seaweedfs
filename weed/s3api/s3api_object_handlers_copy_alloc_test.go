package s3api

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// TestDownloadChunkData_AllocationBound is a regression guard for
// https://github.com/seaweedfs/seaweedfs/issues/6541.
//
// downloadChunkData previously accumulated streamed bytes via
//
//	var chunkData []byte
//	... fn := func(data []byte) { chunkData = append(chunkData, data...) }
//
// The callback fires once per ReadUrlAsStream pump (256 KiB), so a 16 MiB
// chunk grew the slice geometrically (256K -> 512K -> 1M -> ... -> 32M),
// allocating ~2x the chunk size for every transferred byte. Combined with
// the 4-way per-request concurrency in copyChunks/copyChunksForRange and
// any number of in-flight UploadPartCopy calls (Harbor multipart assemble),
// this produced the runaway RSS reported in the issue.
//
// We assert that downloading a chunkSize-byte chunk allocates at most
// 1.5 * chunkSize bytes on the heap. The pre-fix nil-append pattern blows
// past that bound; the fixed (`make([]byte, 0, sizeInt)`) version stays
// well under it.
func TestDownloadChunkData_AllocationBound(t *testing.T) {
	// downloadChunkData drives the package-level global HTTP client. Tests
	// don't go through the normal `weed` startup that does this, so call it
	// here. Idempotent on subsequent test runs.
	util_http.InitGlobalHttpClient()

	const chunkSize = 16 << 20 // 16 MiB
	payload := make([]byte, chunkSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		// Write in 64 KiB increments so the body is delivered to the client
		// reader in many small reads, mirroring real volume-server streaming
		// behavior. The exact step does not matter as long as the body comes
		// through the client in multiple Read() calls.
		const step = 64 * 1024
		for i := 0; i < len(payload); i += step {
			end := i + step
			if end > len(payload) {
				end = len(payload)
			}
			if _, err := w.Write(payload[i:end]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}))
	defer srv.Close()

	s3a := &S3ApiServer{}

	// Warm up the global HTTP client and any package-level pools so they
	// don't show up in the measured allocation window.
	if _, err := s3a.downloadChunkData(srv.URL, "1,0", 0, int64(chunkSize), nil); err != nil {
		t.Fatalf("warm-up downloadChunkData: %v", err)
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	data, err := s3a.downloadChunkData(srv.URL, "1,0", 0, int64(chunkSize), nil)
	if err != nil {
		t.Fatalf("downloadChunkData: %v", err)
	}
	if len(data) != chunkSize {
		t.Fatalf("got %d bytes, want %d", len(data), chunkSize)
	}

	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc

	// The output slice itself is chunkSize bytes; the streaming pump uses a
	// pooled 256 KiB read buffer and doesn't otherwise accumulate. A 1.5x
	// bound is comfortably above the steady-state cost and well below the
	// pre-fix geometric-grow cost (~2x).
	maxAllowed := uint64(chunkSize) * 3 / 2
	if allocated > maxAllowed {
		t.Fatalf("downloadChunkData allocated %d bytes for a %d-byte chunk; bound is %d "+
			"(regression: unbounded append in callback?)",
			allocated, chunkSize, maxAllowed)
	}
	t.Logf("allocated=%d bytes, bound=%d, output=%d", allocated, maxAllowed, chunkSize)
}
