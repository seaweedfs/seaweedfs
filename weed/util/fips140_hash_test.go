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
