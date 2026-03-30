package command

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ sink.ReplicationSink = (*recordingSyncSink)(nil)

type recordingSyncSink struct {
	deleteKeys []string
	createKeys []string
	updateKeys []string
}

func (s *recordingSyncSink) GetName() string { return "recording" }
func (s *recordingSyncSink) Initialize(util.Configuration, string) error {
	return nil
}
func (s *recordingSyncSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	s.deleteKeys = append(s.deleteKeys, key)
	return nil
}
func (s *recordingSyncSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	s.createKeys = append(s.createKeys, key)
	return nil
}
func (s *recordingSyncSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (bool, error) {
	s.updateKeys = append(s.updateKeys, key)
	return true, nil
}
func (s *recordingSyncSink) GetSinkToDirectory() string         { return "/dest" }
func (s *recordingSyncSink) SetSourceFiler(*source.FilerSource) {}
func (s *recordingSyncSink) IsIncremental() bool                { return false }

func TestPathIsEqualOrUnderUsesDirectoryBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		candidate string
		other     string
		expected  bool
	}{
		{name: "equal", candidate: "/foo", other: "/foo", expected: true},
		{name: "descendant", candidate: "/foo/bar", other: "/foo", expected: true},
		{name: "sibling prefix", candidate: "/foobar/bar", other: "/foo", expected: false},
		{name: "root", candidate: "/foo/bar", other: "/", expected: true},
		{name: "empty", candidate: "", other: "/foo", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pathIsEqualOrUnder(tt.candidate, tt.other); got != tt.expected {
				t.Fatalf("pathIsEqualOrUnder(%q, %q) = %v, want %v", tt.candidate, tt.other, got, tt.expected)
			}
		})
	}
}

func TestMatchesExcludePathUsesDirectoryBoundaries(t *testing.T) {
	if !matchesExcludePath("/tmp", []string{"/tmp"}) {
		t.Fatal("expected exact directory match to be excluded")
	}
	if !matchesExcludePath("/tmp/sub", []string{"/tmp"}) {
		t.Fatal("expected descendant directory to be excluded")
	}
	if matchesExcludePath("/tmp2/sub", []string{"/tmp"}) {
		t.Fatal("did not expect sibling directory to be excluded")
	}
}

func TestGenProcessFunctionRenameToSiblingPrefixBecomesDelete(t *testing.T) {
	dataSink := &recordingSyncSink{}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, true, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewParentPath: "/foobar/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn rename to sibling prefix: %v", err)
	}

	if len(dataSink.deleteKeys) != 1 || dataSink.deleteKeys[0] != "/dest/dir/file.txt" {
		t.Fatalf("delete keys = %v, want [/dest/dir/file.txt]", dataSink.deleteKeys)
	}
	if len(dataSink.createKeys) != 0 || len(dataSink.updateKeys) != 0 {
		t.Fatalf("unexpected create/update calls: creates=%v updates=%v", dataSink.createKeys, dataSink.updateKeys)
	}
}

func TestGenProcessFunctionRenameFromExcludedDirBecomesCreate(t *testing.T) {
	dataSink := &recordingSyncSink{}
	processFn := genProcessFunction("/foo", "/dest", []string{"/foo/excluded"}, nil, nil, nil, dataSink, true, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/excluded",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewParentPath: "/foo/live",
		},
	})
	if err != nil {
		t.Fatalf("processFn rename from excluded dir: %v", err)
	}

	if len(dataSink.createKeys) != 1 || dataSink.createKeys[0] != "/dest/live/file.txt" {
		t.Fatalf("create keys = %v, want [/dest/live/file.txt]", dataSink.createKeys)
	}
	if len(dataSink.deleteKeys) != 0 || len(dataSink.updateKeys) != 0 {
		t.Fatalf("unexpected delete/update calls: deletes=%v updates=%v", dataSink.deleteKeys, dataSink.updateKeys)
	}
}
