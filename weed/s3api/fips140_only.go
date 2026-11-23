package s3api

import (
	"bytes"
	"crypto/fips140"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// fips140Enforced reports whether the process runs with GODEBUG=fips140=only.
// In that mode MD5, SHA-1 and HMAC-SHA1 panic, so SigV2, SSE-C and the SHA-1
// flexible checksum are refused with an S3 error before any hashing.
// It is a variable so tests can enable the refusals without running the whole
// test binary under fips140=only.
var fips140Enforced = fips140.Enforced

// contentMd5Matches compares a body digest from util.NewMd5 with the client's
// Content-MD5. Under digest sha256 the digest is not an MD5, so Content-MD5
// goes unchecked.
func contentMd5Matches(digest, contentMd5 []byte) bool {
	return util.DigestIsSha256() || bytes.Equal(digest, contentMd5)
}
