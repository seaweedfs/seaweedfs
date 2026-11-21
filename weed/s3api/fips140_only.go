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

// contentMd5Matches compares a body digest with the client's Content-MD5.
// Under fips140=only the digest is not an MD5, so Content-MD5 goes unchecked.
func contentMd5Matches(digest, contentMd5 []byte) bool {
	return fips140Enforced() || bytes.Equal(digest, contentMd5)
}
