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
	"strconv"
	"testing"
)

// TestParseUpload_OriginalSizeHint covers the gzipped-pass-through path
// added for https://github.com/seaweedfs/seaweedfs/issues/6541. When the
// upstream sets X-Seaweedfs-Original-Size on the multipart part, ParseUpload
// must skip the size-learning DecompressData pass, but only when no
// Content-MD5 verification is needed (MD5 must be computed against the
// uncompressed bytes to match what the s3 client sent).
func TestParseUpload_OriginalSizeHint(t *testing.T) {
	const uncompressedSize = 4 * 1024 * 1024 // 4 MiB
	uncompressed := make([]byte, uncompressedSize)
	for i := range uncompressed {
		uncompressed[i] = byte(i*31 + 7)
	}

	gzipped := gzipBytes(t, uncompressed)
	uncompressedMD5 := base64.StdEncoding.EncodeToString(md5sum(uncompressed))

	cases := []struct {
		name       string
		hintBytes  int64 // 0 means don't send the header
		md5OnPart  string
		md5OnReq   string
		wantSkip   bool   // ParseUpload should NOT decompress
		wantOrigSz int    // expected pu.OriginalDataSize
		wantErr    string // substring; "" means no error
	}{
		{
			name:       "hint present, no MD5 → skip decompress",
			hintBytes:  uncompressedSize,
			wantSkip:   true,
			wantOrigSz: uncompressedSize,
		},
		{
			name:       "no hint → existing decompress path",
			wantSkip:   false,
			wantOrigSz: uncompressedSize,
		},
		{
			name:       "hint present, MD5 on part → must decompress for MD5",
			hintBytes:  uncompressedSize,
			md5OnPart:  uncompressedMD5,
			wantSkip:   false,
			wantOrigSz: uncompressedSize,
		},
		{
			name:       "hint present, MD5 on request → must decompress for MD5",
			hintBytes:  uncompressedSize,
			md5OnReq:   uncompressedMD5,
			wantSkip:   false,
			wantOrigSz: uncompressedSize,
		},
		{
			name:       "garbage hint value falls back to decompress",
			hintBytes:  -1, // sentinel: write a non-numeric value below
			wantSkip:   false,
			wantOrigSz: uncompressedSize,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := buildReq(t, gzipped, c.hintBytes, c.md5OnPart, c.md5OnReq)

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)

			bb := &bytes.Buffer{}
			pu, err := ParseUpload(req, 256<<20, bb)

			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc

			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseUpload: want error containing %q, got nil", c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseUpload: %v", err)
			}
			if pu.OriginalDataSize != c.wantOrigSz {
				t.Errorf("OriginalDataSize=%d, want %d", pu.OriginalDataSize, c.wantOrigSz)
			}

			// Bound check: if we expected to skip decompress, the test should
			// allocate well under uncompressedSize. If we expected to
			// decompress, the test should allocate at least uncompressedSize.
			if c.wantSkip {
				bound := uint64(uncompressedSize) // generous
				if allocated > bound {
					t.Errorf("hint-present case allocated %d bytes for a %d-byte gzipped body; "+
						"bound %d (regression: did the decompress-skip break?)",
						allocated, len(gzipped), bound)
				}
			} else {
				// Decompress path must allocate at least the uncompressed
				// size (the unzipped slice). Sanity-check it actually ran.
				min := uint64(uncompressedSize)
				if allocated < min {
					t.Errorf("decompress-path case allocated only %d bytes; "+
						"expected at least %d (was the decompress wrongly skipped?)",
						allocated, min)
				}
			}
			t.Logf("allocated=%d bytes (gzipped=%d, uncompressed=%d)", allocated, len(gzipped), uncompressedSize)
		})
	}
}

// gzipBytes returns the gzip encoding of in.
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

// buildReq constructs an *http.Request whose body is a multipart/form-data
// body containing one file part. hintBytes>0 sets X-Seaweedfs-Original-Size
// to that value; hintBytes==-1 sets it to a non-numeric string (garbage
// case); hintBytes==0 omits the header.
func buildReq(t *testing.T, body []byte, hintBytes int64, md5OnPart, md5OnReq string) *http.Request {
	t.Helper()

	var bodyBuf bytes.Buffer
	mw := multipart.NewWriter(&bodyBuf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="file"; filename="test.bin"`)
	h.Set("Content-Encoding", "gzip")
	switch {
	case hintBytes > 0:
		h.Set(OriginalSizeHeader, strconv.FormatInt(hintBytes, 10))
	case hintBytes == -1:
		h.Set(OriginalSizeHeader, "not-a-number")
	}
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
