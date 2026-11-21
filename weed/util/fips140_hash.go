package util

import (
	"crypto/fips140"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"hash"
)

var fips140Enforced = fips140.Enforced

// NewMd5 returns an MD5 hash. GODEBUG=fips140=only refuses MD5, so there it
// returns SHA-256 cut to MD5's size: digests keep their length but differ.
func NewMd5() hash.Hash {
	if fips140Enforced() {
		return NewTruncatedSha256(md5.Size)
	}
	return md5.New()
}

// NewSha1 is NewMd5 for SHA-1.
func NewSha1() hash.Hash {
	if fips140Enforced() {
		return NewTruncatedSha256(sha1.Size)
	}
	return sha1.New()
}

// NewTruncatedSha256 returns SHA-256 keeping the first size bytes of the digest.
func NewTruncatedSha256(size int) hash.Hash {
	return &truncatedHash{Hash: sha256.New(), size: size}
}

type truncatedHash struct {
	hash.Hash
	size int
}

func (h *truncatedHash) Size() int { return h.size }

func (h *truncatedHash) Sum(b []byte) []byte {
	return append(b, h.Hash.Sum(nil)[:h.size]...)
}
