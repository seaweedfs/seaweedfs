package batchio

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func tempFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "batchio-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestStandard_PwriteBatch(t *testing.T) {
	f := tempFile(t)
	bio := NewStandard()
	defer bio.Close()

	ops := []Op{
		{Buf: []byte("AAAA"), Offset: 0},
		{Buf: []byte("BBBB"), Offset: 4096},
		{Buf: []byte("CCCC"), Offset: 8192},
	}
	if err := bio.PwriteBatch(f, ops); err != nil {
		t.Fatalf("PwriteBatch: %v", err)
	}

	// Verify each region.
	for _, op := range ops {
		got := make([]byte, len(op.Buf))
		if _, err := f.ReadAt(got, op.Offset); err != nil {
			t.Fatalf("ReadAt offset %d: %v", op.Offset, err)
		}
		if !bytes.Equal(got, op.Buf) {
			t.Errorf("offset %d: got %q, want %q", op.Offset, got, op.Buf)
		}
	}
}

func TestStandard_PreadBatch(t *testing.T) {
	f := tempFile(t)
	bio := NewStandard()
	defer bio.Close()

	// Write known data.
	data := []byte("Hello, World! This is batch I/O testing data.")
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	ops := []Op{
		{Buf: make([]byte, 5), Offset: 0},
		{Buf: make([]byte, 6), Offset: 7},
	}
	if err := bio.PreadBatch(f, ops); err != nil {
		t.Fatalf("PreadBatch: %v", err)
	}

	if string(ops[0].Buf) != "Hello" {
		t.Errorf("op[0]: got %q, want %q", ops[0].Buf, "Hello")
	}
	if string(ops[1].Buf) != "World!" {
		t.Errorf("op[1]: got %q, want %q", ops[1].Buf, "World!")
	}
}

func TestStandard_Fsync(t *testing.T) {
	f := tempFile(t)
	bio := NewStandard()
	defer bio.Close()

	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := bio.Fsync(f); err != nil {
		t.Fatalf("Fsync: %v", err)
	}
}

func TestStandard_LinkedWriteFsync(t *testing.T) {
	f := tempFile(t)
	bio := NewStandard()
	defer bio.Close()

	data := []byte("durable write")
	if err := bio.LinkedWriteFsync(f, data, 100); err != nil {
		t.Fatalf("LinkedWriteFsync: %v", err)
	}

	// Verify data persisted.
	got := make([]byte, len(data))
	if _, err := f.ReadAt(got, 100); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestStandard_EmptyBatch(t *testing.T) {
	f := tempFile(t)
	bio := NewStandard()
	defer bio.Close()

	// Empty ops should succeed.
	if err := bio.PreadBatch(f, nil); err != nil {
		t.Errorf("PreadBatch(nil): %v", err)
	}
	if err := bio.PwriteBatch(f, nil); err != nil {
		t.Errorf("PwriteBatch(nil): %v", err)
	}
	if err := bio.PreadBatch(f, []Op{}); err != nil {
		t.Errorf("PreadBatch([]): %v", err)
	}
	if err := bio.PwriteBatch(f, []Op{}); err != nil {
		t.Errorf("PwriteBatch([]): %v", err)
	}
}

func TestStandard_Close(t *testing.T) {
	bio := NewStandard()
	if err := bio.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Double close should be safe.
	if err := bio.Close(); err != nil {
		t.Fatalf("Close (2nd): %v", err)
	}
}

func TestStandard_LargeBatch(t *testing.T) {
	f := tempFile(t)
	bio := NewStandard()
	defer bio.Close()

	// Write 100 blocks of 4KB each.
	const blockSize = 4096
	const numBlocks = 100

	ops := make([]Op, numBlocks)
	for i := range ops {
		buf := make([]byte, blockSize)
		for j := range buf {
			buf[j] = byte(i)
		}
		ops[i] = Op{Buf: buf, Offset: int64(i) * blockSize}
	}

	if err := bio.PwriteBatch(f, ops); err != nil {
		t.Fatalf("PwriteBatch: %v", err)
	}
	if err := bio.Fsync(f); err != nil {
		t.Fatalf("Fsync: %v", err)
	}

	// Read back and verify.
	readOps := make([]Op, numBlocks)
	for i := range readOps {
		readOps[i] = Op{Buf: make([]byte, blockSize), Offset: int64(i) * blockSize}
	}
	if err := bio.PreadBatch(f, readOps); err != nil {
		t.Fatalf("PreadBatch: %v", err)
	}

	for i, op := range readOps {
		expected := byte(i)
		for j, b := range op.Buf {
			if b != expected {
				t.Fatalf("block %d byte %d: got %d, want %d", i, j, b, expected)
			}
		}
	}
}

func TestStandard_PreadBatch_ErrorOnInvalidFd(t *testing.T) {
	bio := NewStandard()
	defer bio.Close()

	// Create and close a file to get an invalid fd.
	f, err := os.CreateTemp(t.TempDir(), "batchio-bad-*")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	ops := []Op{{Buf: make([]byte, 10), Offset: 0}}
	if err := bio.PreadBatch(f, ops); err == nil {
		t.Error("expected error reading from closed file")
	}
}

func TestStandard_PwriteBatch_ErrorOnReadOnly(t *testing.T) {
	bio := NewStandard()
	defer bio.Close()

	// Create a file, write data, close, reopen read-only.
	dir := t.TempDir()
	path := filepath.Join(dir, "readonly")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path) // read-only
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ops := []Op{{Buf: []byte("new"), Offset: 0}}
	if err := bio.PwriteBatch(f, ops); err == nil {
		t.Error("expected error writing to read-only file")
	}
}

func TestNewIOUring_Unavailable(t *testing.T) {
	// On non-Linux (or without io_uring support), NewIOUring returns
	// ErrIOUringUnavailable instead of silently falling back.
	_, err := NewIOUring(256)
	if err == nil {
		// io_uring is actually available (Linux with kernel 5.6+).
		// This test validates the error path, so skip on supported systems.
		t.Skip("io_uring is available on this system")
	}
	if !errors.Is(err, ErrIOUringUnavailable) {
		t.Fatalf("expected ErrIOUringUnavailable, got: %v", err)
	}
}
