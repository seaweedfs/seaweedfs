package s3api

import "crypto/fips140"

// fips140Enforced reports whether the process runs with GODEBUG=fips140=only.
// In that mode MD5, SHA-1 and HMAC-SHA1 panic, so SigV2, SSE-C and the SHA-1
// flexible checksum are refused with an S3 error before any hashing.
// It is a variable so tests can enable the refusals without running the whole
// test binary under fips140=only.
var fips140Enforced = fips140.Enforced
