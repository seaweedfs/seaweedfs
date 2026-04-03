package localsink

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// TestCreateEntry_OverwriteReadOnlyFile reproduces a bug where
// filer.backup receives a create event (0-byte, mode 0400) followed by
// an update event (with chunks) for the same file. The update event
// calls CreateEntry again, which fails with "permission denied" because
// OpenFile with O_RDWR cannot open the 0400 file created by the first
// event.
func TestCreateEntry_OverwriteReadOnlyFile(t *testing.T) {
	tmpDir := t.TempDir()
	sink := &LocalSink{}
	sink.initialize(tmpDir, false)

	key := filepath.Join(tmpDir, "objects", "5c", "9fb207")

	// Create event: 0-byte file with mode 0400 (metadata only, no chunks)
	createEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileMode: uint32(0400),
		},
	}
	if err := os.MkdirAll(filepath.Dir(key), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := sink.CreateEntry(key, createEntry, nil); err != nil {
		t.Fatalf("CreateEntry (create event) failed: %v", err)
	}

	// Verify: 0-byte, 0400 file exists
	fi, err := os.Stat(key)
	if err != nil {
		t.Fatalf("stat after create event: %v", err)
	}
	if fi.Size() != 0 {
		t.Errorf("expected 0 bytes after create event, got %d", fi.Size())
	}
	if fi.Mode().Perm() != 0400 {
		t.Fatalf("expected 0400 after create event, got %o", fi.Mode().Perm())
	}

	// Update event: same file, now with content (simulated by entry.Content)
	updateEntry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			FileMode: uint32(0400),
			FileSize: 552,
		},
		Content: []byte("git object data placeholder"),
	}
	if err := sink.CreateEntry(key, updateEntry, nil); err != nil {
		t.Fatalf("CreateEntry (update event) failed on read-only file: %v", err)
	}

	// Verify: file has content and correct permissions
	fi, err = os.Stat(key)
	if err != nil {
		t.Fatalf("stat after update event: %v", err)
	}
	if fi.Size() == 0 {
		t.Errorf("expected non-zero size after update event, got 0")
	}
	if fi.Mode().Perm() != 0400 {
		t.Errorf("expected 0400 after update event, got %o", fi.Mode().Perm())
	}
}
