package localsink

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
)

func TestLocalSinkStaysInsideDir(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "backup")
	sink := &LocalSink{}
	sink.initialize(dir, false)
	sink.SetSourceFiler(&source.FilerSource{})

	entry := func() *filer_pb.Entry {
		return &filer_pb.Entry{
			Name:       "marker.txt",
			Content:    []byte("marker"),
			Attributes: &filer_pb.FuseAttributes{FileMode: 0644, FileSize: 6},
		}
	}

	inside := filepath.Join(dir, "safe", "marker.txt")
	if err := sink.CreateEntry(inside, entry(), nil); err != nil {
		t.Fatalf("create inside: %v", err)
	}
	if _, err := os.Stat(inside); err != nil {
		t.Fatalf("inside file missing: %v", err)
	}

	outside := filepath.Join(dir, "..", "escape", "marker.txt")
	if err := sink.CreateEntry(outside, entry(), nil); err != nil {
		t.Fatalf("create outside: %v", err)
	}
	if _, err := sink.UpdateEntry(outside, nil, "", entry(), false, nil); err != nil {
		t.Fatalf("update outside: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(err) {
		t.Fatalf("sink wrote outside %s: %v", dir, err)
	}

	victim := filepath.Join(root, "victim.txt")
	if err := os.WriteFile(victim, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := sink.DeleteEntry(filepath.Join(dir, "..", "victim.txt"), false, false, nil); err != nil {
		t.Fatalf("delete outside: %v", err)
	}
	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("sink deleted a file outside %s: %v", dir, err)
	}
}
