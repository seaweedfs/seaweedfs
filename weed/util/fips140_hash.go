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
	if err := CheckDigest(name); err != nil {
		return err
	}
	switch name {
	case "":
		digestSetting.Store(digestFollowGodebug)
	case DigestMd5:
		digestSetting.Store(digestMd5)
	case DigestSha256:
		digestSetting.Store(digestSha256)
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

// CheckDigest reports whether this process can hash with the named digest,
// "" meaning its own mode: fips140=only refuses md5, since MD5 panics there.
func CheckDigest(name string) error {
	switch name {
	case "", DigestSha256:
		return nil
	case DigestMd5:
		if fips140Enforced() {
			return fmt.Errorf("digest %s needs MD5, which GODEBUG=fips140=only refuses", DigestMd5)
		}
		return nil
	}
	return fmt.Errorf("unknown digest %q, want %s or %s", name, DigestMd5, DigestSha256)
}

// DigestIsSha256For is DigestIsSha256 for the named digest, "" meaning this
// process's mode. It serves writers that write into a filer other than the
// one whose digest this process adopted.
func DigestIsSha256For(name string) bool {
	switch name {
	case DigestMd5:
		return false
	case DigestSha256:
		return true
	}
	return DigestIsSha256()
}

// NewMd5 returns an MD5 hash, or SHA-256 cut to MD5's size when the digest
// mode is sha256: digests keep their length but differ.
func NewMd5() hash.Hash {
	return NewMd5For("")
}

// NewMd5For is NewMd5 for the named digest, "" meaning this process's mode.
// Check the name with CheckDigest first: md5 panics under fips140=only.
func NewMd5For(name string) hash.Hash {
	if DigestIsSha256For(name) {
		return NewTruncatedSha256(md5.Size)
	}
	return md5.New()
}

// Md5For is Md5 for the named digest, "" meaning this process's mode. Check
// the name with CheckDigest first: md5 panics under fips140=only.
func Md5For(name string, data []byte) []byte {
	if DigestIsSha256For(name) {
		sum := sha256.Sum256(data)
		return sum[:md5.Size]
	}
	sum := md5.Sum(data)
	return sum[:]
}

// BodyMd5Of is BodyMd5 for a body held in memory: the content digest when it
// is the MD5, else the body's MD5 when MD5 is available, else nil.
func BodyMd5Of(contentDigest, data []byte) []byte {
	if !DigestIsSha256() {
		return contentDigest
	}
	if fips140Enforced() {
		return nil
	}
	sum := md5.Sum(data)
	return sum[:]
}

// NewContentMd5 returns an MD5 hash for checking a client's Content-MD5 next
// to the content digest, when that digest is not itself an MD5. It returns
// nil when the content digest is the MD5 (digest md5) or MD5 is refused
// (fips140=only); BodyMd5 then gives the MD5 to check against, or none.
func NewContentMd5() hash.Hash {
	if !DigestIsSha256() || fips140Enforced() {
		return nil
	}
	return md5.New()
}

// BodyMd5 returns the body's MD5 for a Content-MD5 check: the hash from
// NewContentMd5 when there is one, else the content digest when that is an
// MD5. It returns nil when neither is an MD5 (fips140=only), and Content-MD5
// then goes unchecked.
func BodyMd5(contentDigest []byte, contentMd5Hash hash.Hash) []byte {
	if contentMd5Hash != nil {
		return contentMd5Hash.Sum(nil)
	}
	if !DigestIsSha256() {
		return contentDigest
	}
	return nil
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
