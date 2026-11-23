package util

import (
	"bytes"
	"crypto/fips140"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"testing"
)

func setFIPS140Enforced(t *testing.T, enforced bool) {
	t.Helper()
	saved := fips140Enforced
	fips140Enforced = func() bool { return enforced }
	t.Cleanup(func() { fips140Enforced = saved })
}

func TestNewMd5KeepsMd5WhenNotEnforced(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("GODEBUG=fips140=only refuses MD5 and SHA-1")
	}
	setFIPS140Enforced(t, false)
	data := []byte("hello")
	want := md5.Sum(data)
	if got := Md5(data); !bytes.Equal(got, want[:]) {
		t.Fatalf("Md5 = %x, want %x", got, want)
	}
	h := NewSha1()
	h.Write(data)
	wantSha1 := sha1.Sum(data)
	if got := h.Sum(nil); !bytes.Equal(got, wantSha1[:]) {
		t.Fatalf("NewSha1 = %x, want %x", got, wantSha1)
	}
}

func TestNewMd5UsesTruncatedSha256WhenEnforced(t *testing.T) {
	setFIPS140Enforced(t, true)
	data := []byte("hello")
	full := sha256.Sum256(data)

	got := Md5(data)
	if !bytes.Equal(got, full[:md5.Size]) {
		t.Fatalf("Md5 = %x, want %x", got, full[:md5.Size])
	}

	h := NewSha1()
	if h.Size() != sha1.Size {
		t.Fatalf("NewSha1 size = %d, want %d", h.Size(), sha1.Size)
	}
	h.Write(data)
	if got := h.Sum([]byte{0xff}); !bytes.Equal(got, append([]byte{0xff}, full[:sha1.Size]...)) {
		t.Fatalf("NewSha1 Sum = %x", got)
	}

	if HashStringToLong("/a/b") == 0 || HashToInt32([]byte("k")) == 0 {
		t.Fatalf("hashes should be non-zero")
	}
}

// setDigestForTest sets the digest mode and restores the GODEBUG default after the test.
func setDigestForTest(t *testing.T, name string) error {
	t.Helper()
	t.Cleanup(func() { digestSetting.Store(digestFollowGodebug) })
	return SetDigest(name)
}

func TestDigestDefaultFollowsFips140Enforced(t *testing.T) {
	if err := setDigestForTest(t, ""); err != nil {
		t.Fatal(err)
	}
	setFIPS140Enforced(t, false)
	if got := Digest(); got != DigestMd5 {
		t.Fatalf("Digest() = %s without fips140=only, want %s", got, DigestMd5)
	}
	setFIPS140Enforced(t, true)
	if got := Digest(); got != DigestSha256 {
		t.Fatalf("Digest() = %s under fips140=only, want %s", got, DigestSha256)
	}
}

func TestSetDigestRefusesMd5WhenEnforced(t *testing.T) {
	setFIPS140Enforced(t, true)
	if err := setDigestForTest(t, DigestMd5); err == nil {
		t.Fatal("SetDigest(md5) under fips140=only succeeded")
	}
	if got := Digest(); got != DigestSha256 {
		t.Fatalf("Digest() = %s after a refused md5, want %s", got, DigestSha256)
	}
	if err := SetDigest("sha1"); err == nil {
		t.Fatal("SetDigest(sha1) succeeded")
	}
}

func TestSetDigestSha256WithoutEnforced(t *testing.T) {
	setFIPS140Enforced(t, false)
	if err := setDigestForTest(t, DigestSha256); err != nil {
		t.Fatal(err)
	}
	data := []byte("hello")
	full := sha256.Sum256(data)
	if got := Md5(data); !bytes.Equal(got, full[:md5.Size]) {
		t.Fatalf("Md5 = %x, want %x", got, full[:md5.Size])
	}
	if !DigestIsSha256() {
		t.Fatal("DigestIsSha256() = false after SetDigest(sha256)")
	}
}

func TestAdoptFilerDigest(t *testing.T) {
	setFIPS140Enforced(t, false)
	if err := setDigestForTest(t, ""); err != nil {
		t.Fatal(err)
	}
	if err := AdoptFilerDigest("f:8888", ""); err != nil || Digest() != DigestMd5 {
		t.Fatalf("empty report: err %v, digest %s; want own mode %s", err, Digest(), DigestMd5)
	}
	if err := AdoptFilerDigest("f:8888", DigestSha256); err != nil || Digest() != DigestSha256 {
		t.Fatalf("sha256 report: err %v, digest %s", err, Digest())
	}
	if err := AdoptFilerDigest("f:8888", "crc32"); err == nil {
		t.Fatal("unknown digest adopted")
	}

	setFIPS140Enforced(t, true)
	if err := AdoptFilerDigest("f:8888", DigestMd5); err == nil {
		t.Fatal("md5 filer adopted under fips140=only")
	}
	if err := AdoptFilerDigest("f:8888", DigestSha256); err != nil || Digest() != DigestSha256 {
		t.Fatalf("sha256 report under fips140=only: err %v, digest %s", err, Digest())
	}
}
