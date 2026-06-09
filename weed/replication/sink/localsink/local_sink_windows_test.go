//go:build windows

package localsink

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

func TestSanitizeFsKeyStripsColons(t *testing.T) {
	if got := sanitizeFsKey("/backup/a:b/c:d.txt"); got != "/backup/ab/cd.txt" {
		t.Errorf("sanitizeFsKey() = %q, want %q", got, "/backup/ab/cd.txt")
	}
	if got := sanitizeFsKey(`C:\a:b`); got != `C:\ab` {
		t.Errorf("sanitizeFsKey() = %q, want drive letter preserved %q", got, `C:\ab`)
	}
}

// TestCreateEntryWritesSanitizedPath confirms CreateEntry strips colons from
// the destination path before writing, so keys land at NTFS-legal paths.
func TestCreateEntryWritesSanitizedPath(t *testing.T) {
	tmpDir := t.TempDir()
	sink := &LocalSink{}
	sink.initialize(tmpDir, false)
	sink.SetSourceFiler(&source.FilerSource{})

	key := filepath.Join(tmpDir, "20:24", "fi:le.txt")
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{FileMode: uint32(0644)},
		Content:    []byte("data"),
	}
	if err := sink.CreateEntry(key, entry, nil); err != nil {
		t.Fatalf("CreateEntry failed: %v", err)
	}

	want := sanitizeFsKey(key)
	if _, err := os.Stat(want); err != nil {
		t.Errorf("expected file at sanitized path %q: %v", want, err)
	}
	if _, err := os.Stat(key); err == nil {
		t.Errorf("unexpected file at unsanitized path %q", key)
	}
}
