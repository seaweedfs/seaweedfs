package filer

import (
	"archive/zip"
	"bytes"
	"io"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func zipFile(t *testing.T, name string, mode os.FileMode) *zip.File {
	t.Helper()
	h := &zip.FileHeader{Name: name, Method: zip.Store, Modified: time.Unix(1700000000, 0)}
	h.SetMode(mode)
	return &zip.File{FileHeader: *h}
}

func TestSanitizeZipMemberName(t *testing.T) {
	tests := []struct {
		name string
		mode os.FileMode
		want string
		ok   bool
	}{
		{"a.png", 0644, "a.png", true},
		{"dir/b.txt", 0644, "dir/b.txt", true},
		{"dir\\b.txt", 0644, "dir/b.txt", true},
		{"dir/", os.ModeDir | 0755, "dir", true},
		{"../escape.txt", 0644, "", false},
		{"a/../b.txt", 0644, "", false},
		{"./dot.txt", 0644, "", false},
		{"/abs.txt", 0644, "", false},
		{"a//b.txt", 0644, "", false},
		{"", 0644, "", false},
		{"link.txt", os.ModeSymlink | 0777, "", false},
		{"dev", os.ModeDevice | 0666, "", false},
	}
	for _, tt := range tests {
		got, ok := SanitizeZipMemberName(zipFile(t, tt.name, tt.mode))
		if ok != tt.ok || got != tt.want {
			t.Errorf("SanitizeZipMemberName(%q, %v) = (%q, %v), want (%q, %v)", tt.name, tt.mode, got, ok, tt.want, tt.ok)
		}
	}
}

// smallZipSpoolLimit drops the in-memory spool threshold to 1 KiB for the
// duration of a test. Crossing the real 32 MiB threshold needs buffers that
// are too large for the 32-bit test binary's address space.
func smallZipSpoolLimit(t *testing.T) {
	t.Helper()
	old := zipSpoolMemoryLimit
	zipSpoolMemoryLimit = 1024
	t.Cleanup(func() { zipSpoolMemoryLimit = old })
}

func TestSpoolZipReaderMemory(t *testing.T) {
	data := []byte(strings.Repeat("x", 1024))
	ra, n, cleanup, err := SpoolZipReader(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	defer cleanup()
	if n != int64(len(data)) {
		t.Fatalf("size = %d, want %d", n, len(data))
	}
	if _, isFile := ra.(*os.File); isFile {
		t.Fatal("small archive should spool to memory")
	}
	buf := make([]byte, 4)
	if _, err := ra.ReadAt(buf, 0); err != nil || string(buf) != "xxxx" {
		t.Fatalf("ReadAt = %q, %v", buf, err)
	}
}

func TestSpoolZipReaderTempFile(t *testing.T) {
	smallZipSpoolLimit(t)
	data := bytes.Repeat([]byte("y"), int(zipSpoolMemoryLimit)+1)
	ra, n, cleanup, err := SpoolZipReader(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	f, isFile := ra.(*os.File)
	if !isFile {
		cleanup()
		t.Fatal("large archive should spool to a temp file")
	}
	name := f.Name()
	cleanup()
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("temp file %s still exists after cleanup: %v", name, err)
	}
	if n != int64(len(data)) {
		t.Fatalf("size = %d, want %d", n, len(data))
	}
}

func TestSpoolZipReaderLimit(t *testing.T) {
	smallZipSpoolLimit(t)
	data := []byte(strings.Repeat("x", 2048))
	if _, _, _, err := SpoolZipReader(bytes.NewReader(data), 1024); err == nil {
		t.Fatal("expected limit error for in-memory archive")
	}
	if _, _, cleanup, err := SpoolZipReader(bytes.NewReader(bytes.Repeat([]byte("y"), int(zipSpoolMemoryLimit)+1024)), zipSpoolMemoryLimit+512); err == nil {
		t.Fatal("expected limit error for spooled archive")
	} else {
		cleanup()
	}
}

func TestCheckZipLimits(t *testing.T) {
	over := &zip.Reader{File: make([]*zip.File, ZipExtractMaxMembers+1)}
	for i := range over.File {
		over.File[i] = &zip.File{}
	}
	if err := CheckZipLimits(over); err == nil {
		t.Fatal("expected member count limit error")
	}

	big := &zip.Reader{File: []*zip.File{{}}}
	big.File[0].UncompressedSize64 = uint64(ZipExtractMaxBytes) + 1
	if err := CheckZipLimits(big); err == nil {
		t.Fatal("expected total size limit error")
	}

	overflow := &zip.Reader{File: []*zip.File{{}, {}}}
	overflow.File[0].UncompressedSize64 = math.MaxUint64
	overflow.File[1].UncompressedSize64 = math.MaxUint64
	if err := CheckZipLimits(overflow); err == nil {
		t.Fatal("expected total size overflow error")
	}

	ok := &zip.Reader{File: []*zip.File{{}, {}}}
	if err := CheckZipLimits(ok); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSpoolZipReaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("a/b.txt")
	w.Write([]byte("hello"))
	zw.Close()

	ra, n, cleanup, err := SpoolZipReader(bytes.NewReader(buf.Bytes()), 0)
	if err != nil {
		t.Fatalf("spool: %v", err)
	}
	defer cleanup()
	zr, err := zip.NewReader(ra, n)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	rc, err := zr.File[0].Open()
	if err != nil {
		t.Fatalf("open member: %v", err)
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	if string(data) != "hello" {
		t.Fatalf("member content = %q", data)
	}
}
