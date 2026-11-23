package util

import (
	"crypto/fips140"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Digest modes. The mode decides what NewMd5 and NewSha1 hash with, and so
// the kind of every ETag, chunk digest and hashed store key a process writes.
const (
	DigestMd5    = "md5"
	DigestSha256 = "sha256"
)

var fips140Enforced = fips140.Enforced

const (
	digestFollowGodebug int32 = iota
	digestMd5
	digestSha256
)

var digestSetting atomic.Int32

// SetDigest sets this process's digest mode. An empty name follows GODEBUG:
// sha256 under fips140=only, md5 otherwise. fips140=only refuses md5, since
// MD5 panics there. Call it before serving: digests already written keep the
// mode they were written in.
func SetDigest(name string) error {
	switch name {
	case "":
		digestSetting.Store(digestFollowGodebug)
	case DigestMd5:
		if fips140Enforced() {
			return fmt.Errorf("digest %s needs MD5, which GODEBUG=fips140=only refuses", DigestMd5)
		}
		digestSetting.Store(digestMd5)
	case DigestSha256:
		digestSetting.Store(digestSha256)
	default:
		return fmt.Errorf("unknown digest %q, want %s or %s", name, DigestMd5, DigestSha256)
	}
	return nil
}

// DigestIsSha256 reports whether NewMd5 and NewSha1 return truncated SHA-256.
func DigestIsSha256() bool {
	switch digestSetting.Load() {
	case digestMd5:
		return false
	case digestSha256:
		return true
	}
	return fips140Enforced()
}

// Digest names the digest mode in effect.
func Digest() string {
	if DigestIsSha256() {
		return DigestSha256
	}
	return DigestMd5
}

// AdoptFilerDigest sets this process's digest mode to the one filer reports,
// so every process writing into a cluster writes one kind of digest. A filer
// that reports none predates the setting; the process then keeps its own mode.
func AdoptFilerDigest(filer, digest string) error {
	if digest == "" {
		glog.Warningf("filer %s does not report its digest; writing %s digests as this process's GODEBUG implies", filer, Digest())
		return nil
	}
	if err := SetDigest(digest); err != nil {
		return fmt.Errorf("filer %s uses digest %s: %w", filer, digest, err)
	}
	return nil
}

// NewMd5 returns an MD5 hash, or SHA-256 cut to MD5's size when the digest
// mode is sha256: digests keep their length but differ.
func NewMd5() hash.Hash {
	if DigestIsSha256() {
		return NewTruncatedSha256(md5.Size)
	}
	return md5.New()
}

// NewSha1 is NewMd5 for SHA-1.
func NewSha1() hash.Hash {
	if DigestIsSha256() {
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
