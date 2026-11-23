package weed_server

import (
	"crypto/fips140"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestContentMd5MismatchFollowsDigest(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("GODEBUG=fips140=only refuses digest md5")
	}
	t.Cleanup(func() { util.SetDigest("") })
	digest := []byte("0123456789abcdef")
	wrong := util.Base64Encode([]byte("fedcba9876543210"))

	if err := util.SetDigest(util.DigestMd5); err != nil {
		t.Fatal(err)
	}
	if !contentMd5Mismatch(digest, wrong) {
		t.Fatal("digest md5: a wrong Content-MD5 went unchecked")
	}
	if contentMd5Mismatch(digest, util.Base64Encode(digest)) {
		t.Fatal("digest md5: a matching Content-MD5 was refused")
	}
	if err := util.SetDigest(util.DigestSha256); err != nil {
		t.Fatal(err)
	}
	if contentMd5Mismatch(digest, wrong) {
		t.Fatal("digest sha256: Content-MD5 was checked against a SHA-256 digest")
	}
}
