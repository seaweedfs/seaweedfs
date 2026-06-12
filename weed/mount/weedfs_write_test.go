package mount

import (
	"crypto/md5"
	"encoding/base64"
	"testing"
)

// TestContentMD5Base64_MatchesVolumeServerExpectation pins the encoding of the
// Content-MD5 we attach to mount chunk uploads. The volume server computes the
// expected checksum as base64.StdEncoding(md5(plaintext)) (see
// weed/storage/needle/needle_parse_upload.go) and rejects the upload if the
// header differs, so a regression here — switching to hex or url-safe base64 —
// would silently break every mount write. Assert the helper reproduces the
// same value the server verifies against, and that it decodes back to the raw
// 16-byte digest the filer stores as the chunk ETag.
func TestContentMD5Base64_MatchesVolumeServerExpectation(t *testing.T) {
	cases := map[string][]byte{
		"empty":        {},
		"small":        []byte("the quick brown fox"),
		"binary-zeros": make([]byte, 4096),
		"16MB-chunk":   make([]byte, 16*1024*1024),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			got := contentMD5Base64(data)

			h := md5.New()
			h.Write(data)
			want := base64.StdEncoding.EncodeToString(h.Sum(nil))
			if got != want {
				t.Fatalf("contentMD5Base64 = %q, want %q", got, want)
			}

			decoded, err := base64.StdEncoding.DecodeString(got)
			if err != nil {
				t.Fatalf("output is not valid std-base64: %v", err)
			}
			if len(decoded) != md5.Size {
				t.Fatalf("decoded digest is %d bytes, want %d", len(decoded), md5.Size)
			}
		})
	}
}

// TestContentMD5Base64_KnownVector guards against encoding drift with a fixed,
// externally-verifiable vector: md5("abc") = 900150983cd24fb0d6963f7d28e17f72.
func TestContentMD5Base64_KnownVector(t *testing.T) {
	const want = "kAFQmDzST7DWlj99KOF/cg==" // std-base64 of md5("abc")
	if got := contentMD5Base64([]byte("abc")); got != want {
		t.Fatalf("contentMD5Base64(%q) = %q, want %q", "abc", got, want)
	}
}
