package needle

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

// TestParseUpload_AllocationBound is a regression guard for
// https://github.com/seaweedfs/seaweedfs/issues/6541 (volume-side amplifier).
//
// parseUpload reads the multipart part body via bytes.Buffer.ReadFrom. If the
// receive buffer has cap=0 (sync.Pool dropped the prior buffer, or the buffer
// was never grown), ReadFrom doubles capacity on each grow — for a 64 MiB
// chunk that's roughly 1+2+4+...+64 ≈ 128 MiB of allocations to receive one
// chunk. Combined with concurrent volume-server uploads (the destination side
// of every s3 chunk copy), this is one of the primary contributors to the
// runaway-RSS pattern in the issue.
//
// The fix: ParseUpload pre-sizes the buffer to r.ContentLength before
// parseUpload runs. This test asserts that downloading a 16 MiB multipart
// body allocates ≤ 1.5x the chunk size. Without the Grow call, parseUpload
// allocates ~2.5x and the bound trips.
func TestParseUpload_AllocationBound(t *testing.T) {
	const chunkSize = 16 << 20 // 16 MiB

	payload := make([]byte, chunkSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	var bodyBuf bytes.Buffer
	mw := multipart.NewWriter(&bodyBuf)
	fw, err := mw.CreateFormFile("file", "test.bin")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	bodyBytes := bodyBuf.Bytes()
	contentType := mw.FormDataContentType()
	contentLength := int64(len(bodyBytes))

	makeReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", contentType)
		req.ContentLength = contentLength
		return req
	}

	const sizeLimit = 256 << 20

	// Warm-up call: prime any package-level lazy allocations (mime tables,
	// http internals) so they don't pollute the measurement window.
	{
		bb := &bytes.Buffer{}
		if _, err := ParseUpload(makeReq(), sizeLimit, bb); err != nil {
			t.Fatalf("warm-up ParseUpload: %v", err)
		}
	}

	bb := &bytes.Buffer{}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	pu, err := ParseUpload(makeReq(), sizeLimit, bb)
	if err != nil {
		t.Fatalf("ParseUpload: %v", err)
	}
	if got := len(pu.Data); got != chunkSize {
		t.Fatalf("ParseUpload returned %d bytes, want %d", got, chunkSize)
	}

	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc

	// Receive buffer alone is chunkSize. Multipart framing + http internals
	// add a few KiB. A 1.5x bound is comfortably above the steady-state cost
	// and well below the geometric-grow cost (~2.5x in measurement).
	maxAllowed := uint64(chunkSize) * 3 / 2
	if allocated > maxAllowed {
		t.Fatalf("ParseUpload allocated %d bytes for a %d-byte chunk; bound is %d "+
			"(regression: ParseUpload not pre-sizing the receive buffer to r.ContentLength?)",
			allocated, chunkSize, maxAllowed)
	}
	t.Logf("allocated=%d bytes, bound=%d, output=%d", allocated, maxAllowed, chunkSize)
}
