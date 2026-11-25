package needle

import (
	"bytes"
	"encoding/base64"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
)

func TestContentCrc32cEncoding(t *testing.T) {
	// The CRC32C check value for "123456789" is 0xE3069283.
	if got := ContentCrc32c([]byte("123456789")); got != "4waSgw==" {
		t.Fatalf("ContentCrc32c = %q, want 4waSgw==", got)
	}
	c, err := ParseContentCrc32c("4waSgw==")
	if err != nil || c != 0xE3069283 {
		t.Fatalf("ParseContentCrc32c = %x, %v", c, err)
	}
	for _, bad := range []string{"", "4waSgw", "4waSgx==", "not base64!", base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4, 5})} {
		if _, err := ParseContentCrc32c(bad); err == nil {
			t.Errorf("ParseContentCrc32c(%q) accepted", bad)
		}
	}
}

// crc32cUploadReq builds the multipart upload the Go uploader sends, with
// the checksum on the request header.
func crc32cUploadReq(t *testing.T, body []byte, gzipped bool, md5, crc string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="file"; filename="chunk.bin"`)
	h.Set("Content-Type", "application/octet-stream")
	if gzipped {
		h.Set("Content-Encoding", "gzip")
	}
	if md5 != "" {
		h.Set("Content-MD5", md5)
	}
	fw, err := mw.CreatePart(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/3,01637037d6", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if crc != "" {
		req.Header.Set(ContentCrc32cHeader, crc)
	}
	return req
}

func TestParseUploadContentCrc32c(t *testing.T) {
	data := bytes.Repeat([]byte("chunk bytes from the uploader "), 2000)
	gzipped := gzipBytes(t, data)
	right := ContentCrc32c(data)
	wrong := ContentCrc32c(append([]byte{'x'}, data[1:]...))
	md5 := base64.StdEncoding.EncodeToString(md5sum(data))

	cases := []struct {
		name    string
		body    []byte
		gzipped bool
		md5     string
		crc     string
		wantErr string
	}{
		{name: "absent", body: data},
		{name: "match", body: data, crc: right},
		{name: "mismatch", body: data, crc: wrong, wantErr: "did not match crc32c"},
		{name: "malformed", body: data, crc: "AAAA", wantErr: "not the base64"},
		{name: "gzipped match", body: gzipped, gzipped: true, crc: right},
		{name: "gzipped mismatch", body: gzipped, gzipped: true, crc: wrong, wantErr: "did not match crc32c"},
		{name: "gzipped with Content-MD5 match", body: gzipped, gzipped: true, md5: md5, crc: right},
		{name: "gzipped with Content-MD5 mismatch", body: gzipped, gzipped: true, md5: md5, crc: wrong, wantErr: "did not match crc32c"},
		{name: "corrupt gzip checks raw bytes", body: gzipped[:len(gzipped)/2], gzipped: true, crc: right, wantErr: "did not match crc32c"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pu, err := ParseUpload(crc32cUploadReq(t, c.body, c.gzipped, c.md5, c.crc), 1<<20, &bytes.Buffer{})
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseUpload: %v", err)
				}
				if pu.OriginalDataSize != len(data) {
					t.Errorf("OriginalDataSize = %d, want %d", pu.OriginalDataSize, len(data))
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("ParseUpload error = %v, want %q", err, c.wantErr)
			}
		})
	}
}
