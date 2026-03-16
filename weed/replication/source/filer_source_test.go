package source

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestMain(m *testing.M) {
	util_http.InitGlobalHttpClient()
	os.Exit(m.Run())
}

func TestDownloadFile_NoOffset(t *testing.T) {
	testData := []byte("0123456789abcdefghij")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			t.Error("Range header should not be set when offset is 0")
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(testData)
	}))
	defer server.Close()

	_, _, resp, err := util_http.DownloadFile(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, testData) {
		t.Fatalf("expected %q, got %q", testData, data)
	}
}

func TestDownloadFile_WithOffset(t *testing.T) {
	testData := []byte("0123456789abcdefghij")

	var receivedRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedRange = r.Header.Get("Range")
		var offset int
		fmt.Sscanf(receivedRange, "bytes=%d-", &offset)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(testData)-1, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[offset:])
	}))
	defer server.Close()

	_, _, resp, err := util_http.DownloadFile(server.URL, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if receivedRange != "bytes=10-" {
		t.Fatalf("expected Range header %q, got %q", "bytes=10-", receivedRange)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected status 206, got %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, testData[10:]) {
		t.Fatalf("expected %q, got %q", testData[10:], data)
	}
}

func TestDownloadFile_RejectsIgnoredRange(t *testing.T) {
	// Server ignores Range header and returns 200 OK with full body
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("full body"))
	}))
	defer server.Close()

	_, _, _, err := util_http.DownloadFile(server.URL, "", 100)
	if err == nil {
		t.Fatal("expected error when server ignores Range and returns 200")
	}
}

func TestDownloadFile_ContentDisposition(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="test.txt"`)
		w.Write([]byte("data"))
	}))
	defer server.Close()

	filename, _, resp, err := util_http.DownloadFile(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if filename != "test.txt" {
		t.Fatalf("expected filename %q, got %q", "test.txt", filename)
	}
}

// TestDownloadFile_PartialReadThenResume simulates a connection drop
// after partial data, then resumes from the offset. Verifies the combined
// data matches the original.
func TestDownloadFile_PartialReadThenResume(t *testing.T) {
	testData := bytes.Repeat([]byte("abcdefghij"), 100) // 1000 bytes
	dropAfter := 500

	var requestNum atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requestNum.Add(1)
		rangeHeader := r.Header.Get("Range")

		if n == 1 && rangeHeader == "" {
			// First request: write partial data then kill the connection
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server doesn't support hijacking")
				return
			}
			conn, buf, _ := hj.Hijack()
			fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\n")
			fmt.Fprintf(buf, "Content-Length: %d\r\n", len(testData))
			fmt.Fprintf(buf, "Content-Type: application/octet-stream\r\n")
			fmt.Fprintf(buf, "\r\n")
			buf.Write(testData[:dropAfter])
			buf.Flush()
			conn.Close()
			return
		}

		// Resume request with Range
		var offset int
		fmt.Sscanf(rangeHeader, "bytes=%d-", &offset)
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", offset, len(testData)-1, len(testData)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(testData[offset:])
	}))
	defer server.Close()

	// First read — should get partial data + error
	_, _, resp1, err := util_http.DownloadFile(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	partialData, readErr := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if readErr == nil {
		t.Fatal("expected error from truncated response")
	}
	if len(partialData) == 0 {
		t.Fatal("expected some partial data")
	}
	if !bytes.Equal(partialData, testData[:len(partialData)]) {
		t.Fatal("partial data doesn't match beginning of original")
	}

	// Resume from where we left off
	_, _, resp2, err := util_http.DownloadFile(server.URL, "", int64(len(partialData)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	remainingData, readErr := io.ReadAll(resp2.Body)
	if readErr != nil {
		t.Fatalf("unexpected error on resume: %v", readErr)
	}

	// Combined data must match original
	fullData := append(partialData, remainingData...)
	if !bytes.Equal(fullData, testData) {
		t.Fatalf("combined data mismatch: got %d bytes, want %d", len(fullData), len(testData))
	}
}

// TestDownloadFile_GzipPartialReadThenResume verifies the tricky case
// where the first response is gzip-encoded (and Go's HTTP client auto-
// decompresses it), but the resume request gets uncompressed data (because
// Go doesn't add Accept-Encoding when Range is set). The combined
// decompressed bytes must still match the original.
func TestDownloadFile_GzipPartialReadThenResume(t *testing.T) {
	testData := bytes.Repeat([]byte("hello world gzip test "), 100) // ~2200 bytes

	// Pre-compress
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	gz.Write(testData)
	gz.Close()
	compressedData := compressed.Bytes()
	dropAfter := len(compressedData) / 2

	var requestNum atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requestNum.Add(1)
		rangeHeader := r.Header.Get("Range")
		acceptsGzip := r.Header.Get("Accept-Encoding") != ""

		if n == 1 && acceptsGzip && rangeHeader == "" {
			// First request: serve gzip-encoded response, drop mid-stream.
			// Go's HTTP client auto-added Accept-Encoding: gzip (no Range),
			// so it will auto-decompress and strip the Content-Encoding header.
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("server doesn't support hijacking")
				return
			}
			conn, buf, _ := hj.Hijack()
			fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\n")
			fmt.Fprintf(buf, "Content-Encoding: gzip\r\n")
			fmt.Fprintf(buf, "Content-Length: %d\r\n", len(compressedData))
			fmt.Fprintf(buf, "Content-Type: application/octet-stream\r\n")
			fmt.Fprintf(buf, "\r\n")
			buf.Write(compressedData[:dropAfter])
			buf.Flush()
			conn.Close()
			return
		}

		// Resume request: Range is set, Go did NOT add Accept-Encoding,
		// so we serve decompressed data from the requested offset —
		// mimicking what the volume server does when the client doesn't
		// advertise gzip support.
		var offset int
		fmt.Sscanf(rangeHeader, "bytes=%d-", &offset)
		remaining := testData[offset:]
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", offset, len(testData)-1, len(testData)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusPartialContent)
		w.Write(remaining)
	}))
	defer server.Close()

	// First read: Go auto-decompresses; truncated stream → error + partial data
	_, _, resp1, err := util_http.DownloadFile(server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	partialData, readErr := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if readErr == nil {
		t.Fatal("expected error from truncated gzip response")
	}
	if len(partialData) == 0 {
		t.Fatal("expected some decompressed partial data")
	}
	// Partial decompressed data must match the beginning of the original
	if !bytes.Equal(partialData, testData[:len(partialData)]) {
		t.Fatalf("partial decompressed data doesn't match original (got %d bytes)", len(partialData))
	}

	// Resume: offset is in the *decompressed* domain.
	// The server (like the volume server) decompresses and serves from that offset.
	_, _, resp2, err := util_http.DownloadFile(server.URL, "", int64(len(partialData)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	remainingData, readErr := io.ReadAll(resp2.Body)
	if readErr != nil {
		t.Fatalf("unexpected error on resume: %v", readErr)
	}

	fullData := append(partialData, remainingData...)
	if !bytes.Equal(fullData, testData) {
		t.Fatalf("combined data mismatch: got %d bytes, want %d", len(fullData), len(testData))
	}
}
