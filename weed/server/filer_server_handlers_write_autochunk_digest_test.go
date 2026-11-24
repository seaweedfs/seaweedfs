package weed_server

import (
	"crypto/fips140"
	"crypto/md5"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestContentMd5CheckedInBothDigests checks that the filer verifies a
// client's Content-MD5 against the body's real MD5 under digest md5 and
// digest sha256, reading the body once through teeContentMd5.
func TestContentMd5CheckedInBothDigests(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("GODEBUG=fips140=only refuses MD5")
	}
	t.Cleanup(func() { util.SetDigest("") })
	right := md5.Sum([]byte("hello"))
	wrong := md5.Sum([]byte("other"))
	for _, digest := range []string{util.DigestMd5, util.DigestSha256} {
		if err := util.SetDigest(digest); err != nil {
			t.Fatal(err)
		}
		for _, c := range []struct {
			header   string
			mismatch bool
		}{
			{util.Base64Encode(right[:]), false},
			{util.Base64Encode(wrong[:]), true},
		} {
			r := httptest.NewRequest("PUT", "/x/hello", nil)
			r.Header.Set("Content-Md5", c.header)
			body, contentMd5Hash := teeContentMd5(r, strings.NewReader("hello"))
			contentDigest := util.NewMd5()
			if _, err := io.Copy(contentDigest, body); err != nil {
				t.Fatal(err)
			}
			if got := contentMd5Mismatch(util.BodyMd5(contentDigest.Sum(nil), contentMd5Hash), c.header); got != c.mismatch {
				t.Errorf("digest %s, Content-MD5 %s: mismatch %v, want %v", digest, c.header, got, c.mismatch)
			}
		}
	}
}
