package s3api

import (
	"bytes"
	"crypto/fips140"
)

// fips140Enforced reports whether the process runs with GODEBUG=fips140=only.
// In that mode MD5, SHA-1 and HMAC-SHA1 panic, so SigV2, SSE-C and the SHA-1
// flexible checksum are refused with an S3 error before any hashing.
// It is a variable so tests can enable the refusals without running the whole
// test binary under fips140=only.
var fips140Enforced = fips140.Enforced

// contentMd5Matches compares the body's MD5, from util.BodyMd5, with the
// client's Content-MD5. bodyMd5 is nil only under fips140=only with digest
// sha256, where no MD5 can be computed, and Content-MD5 then goes unchecked.
func contentMd5Matches(bodyMd5, contentMd5 []byte) bool {
	return bodyMd5 == nil || bytes.Equal(bodyMd5, contentMd5)
}
