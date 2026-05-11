package needle

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/base64"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"runtime"
	"testing"
)

// TestParseUpload_GzipStreamCount: gzipped uploads without Content-MD5
// must take the stream-count path and skip the uncompressed-slice
// allocation. See #6541.
func TestParseUpload_GzipStreamCount(t *testing.T) {
	const uncompressedSize = 4 * 1024 * 1024 // 4 MiB
	uncompressed := make([]byte, uncompressedSize)
	for i := range uncompressed {
		uncompressed[i] = byte(i*31 + 7)
	}
	gzipped := gzipBytes(t, uncompressed)
	uncompressedMD5 := base64.StdEncoding.EncodeToString(md5sum(uncompressed))

	cases := []struct {
		name       string
		md5OnPart  string
		md5OnReq   string
		wantStream bool
	}{
		{name: "no MD5: stream-count", wantStream: true},
		{name: "MD5 on part: materialize", md5OnPart: uncompressedMD5},
		{name: "MD5 on request: materialize", md5OnReq: uncompressedMD5},
	}

	// Warm the gzip.Reader sync.Pool so per-test alloc is steady-state.
	if _, err := ParseUpload(buildGzipReq(t, gzipped, "", ""), 256<<20, &bytes.Buffer{}); err != nil {
		t.Fatalf("warmup ParseUpload: %v", err)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := buildGzipReq(t, gzipped, c.md5OnPart, c.md5OnReq)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			pu, err := ParseUpload(req, 256<<20, &bytes.Buffer{})

			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc

			if err != nil {
				t.Fatalf("ParseUpload: %v", err)
			}
			if pu.OriginalDataSize != uncompressedSize {
				t.Errorf("OriginalDataSize=%d, want %d", pu.OriginalDataSize, uncompressedSize)
			}

			if c.wantStream {
				if allocated > uint64(uncompressedSize) {
					t.Errorf("stream path allocated %d, bound %d", allocated, uncompressedSize)
				}
			} else {
				if allocated < uint64(uncompressedSize) {
					t.Errorf("materialize path allocated %d, want >= %d", allocated, uncompressedSize)
				}
			}
			t.Logf("allocated=%d bytes (gzipped=%d, uncompressed=%d)",
				allocated, len(gzipped), uncompressedSize)
		})
	}
}

func gzipBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(in); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func md5sum(in []byte) []byte {
	h := md5.New()
	h.Write(in)
	return h.Sum(nil)
}

func buildGzipReq(t *testing.T, body []byte, md5OnPart, md5OnReq string) *http.Request {
	t.Helper()
	var bodyBuf bytes.Buffer
	mw := multipart.NewWriter(&bodyBuf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="file"; filename="test.bin"`)
	h.Set("Content-Encoding", "gzip")
	if md5OnPart != "" {
		h.Set("Content-MD5", md5OnPart)
	}
	fw, err := mw.CreatePart(h)
	if err != nil {
		t.Fatalf("CreatePart: %v", err)
	}
	if _, err := fw.Write(body); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close mw: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(bodyBuf.Bytes()))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.ContentLength = int64(bodyBuf.Len())
	if md5OnReq != "" {
		req.Header.Set("Content-MD5", md5OnReq)
	}
	return req
}
