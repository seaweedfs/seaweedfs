//go:build linux

package util

import (
	"os"
	"testing"
)

func TestDropOSPageCache(t *testing.T) {
	f, err := os.CreateTemp("", "fadvise_test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	// Read the data back to populate page cache
	buf := make([]byte, 4096)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatalf("failed to read test data: %v", err)
	}

	// Call DropOSPageCache and verify no error
	fd := int(f.Fd())
	if err := DropOSPageCache(fd, 0, int64(len(data))); err != nil {
		t.Errorf("DropOSPageCache returned error: %v", err)
	}
}
