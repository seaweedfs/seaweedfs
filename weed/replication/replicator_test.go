package replication

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ sink.ReplicationSink = (*recordingSink)(nil)

type deleteCall struct {
	key         string
	isDirectory bool
}

type createCall struct {
	key string
}

type updateCall struct {
	key           string
	newParentPath string
}

type recordingSink struct {
	name                string
	sinkToDirectory     string
	incremental         bool
	updateFoundExisting bool

	deleteCalls []deleteCall
	createCalls []createCall
	updateCalls []updateCall

	// ordered records the sink method names in call order so tests can assert
	// create-before-delete sequencing.
	ordered []string
}

func (s *recordingSink) GetName() string {
	return s.name
}

func (s *recordingSink) Initialize(util.Configuration, string) error {
	return nil
}

func (s *recordingSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	s.deleteCalls = append(s.deleteCalls, deleteCall{key: key, isDirectory: isDirectory})
	s.ordered = append(s.ordered, "delete")
	return nil
}

func (s *recordingSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	s.createCalls = append(s.createCalls, createCall{key: key})
	s.ordered = append(s.ordered, "create")
	return nil
}

func (s *recordingSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (bool, error) {
	s.updateCalls = append(s.updateCalls, updateCall{key: key, newParentPath: newParentPath})
	s.ordered = append(s.ordered, "update")
	return s.updateFoundExisting, nil
}

func (s *recordingSink) GetSinkToDirectory() string {
	return s.sinkToDirectory
}

func (s *recordingSink) SetSourceFiler(*source.FilerSource) {}

func (s *recordingSink) IsIncremental() bool {
	return s.incremental
}

func TestReplicateRenameUsesTargetKeyForNonFilerSink(t *testing.T) {
	s := &recordingSink{name: "local", sinkToDirectory: "/dest"}
	r := &Replicator{
		sink:   s,
		source: &source.FilerSource{Dir: "/source"},
	}

	err := r.Replicate(context.Background(), "/source/old/file.txt", &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: "renamed.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewParentPath: "/source/new",
	})
	if err != nil {
		t.Fatalf("Replicate rename: %v", err)
	}

	if len(s.updateCalls) != 0 {
		t.Fatalf("expected rename to bypass UpdateEntry, got %d calls", len(s.updateCalls))
	}
	if len(s.createCalls) != 1 || s.createCalls[0].key != "/dest/new/renamed.txt" {
		t.Fatalf("create calls = %+v, want target sink key", s.createCalls)
	}
	if len(s.deleteCalls) != 1 || s.deleteCalls[0].key != "/dest/old/file.txt" {
		t.Fatalf("delete calls = %+v, want old sink key", s.deleteCalls)
	}
	if got, want := s.ordered, []string{"create", "delete"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want create before delete %v", got, want)
	}
}

// A real move to a filer sink also goes through create-then-delete (the
// filer-sink special-case that previously routed renames to UpdateEntry is
// gone): UpdateEntry cannot move an entry across paths.
func TestReplicateRenameUsesCreateThenDeleteForFilerSink(t *testing.T) {
	s := &recordingSink{
		name:                "filer",
		sinkToDirectory:     "/dest",
		updateFoundExisting: true,
	}
	r := &Replicator{
		sink:   s,
		source: &source.FilerSource{Dir: "/source"},
	}

	err := r.Replicate(context.Background(), "/source/old/file.txt", &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: "renamed.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewParentPath: "/source/new",
	})
	if err != nil {
		t.Fatalf("Replicate rename: %v", err)
	}

	if len(s.updateCalls) != 0 {
		t.Fatalf("expected filer-sink rename to bypass UpdateEntry, got %d calls", len(s.updateCalls))
	}
	if len(s.createCalls) != 1 || s.createCalls[0].key != "/dest/new/renamed.txt" {
		t.Fatalf("create calls = %+v, want target sink key", s.createCalls)
	}
	if len(s.deleteCalls) != 1 || s.deleteCalls[0].key != "/dest/old/file.txt" {
		t.Fatalf("delete calls = %+v, want old sink key", s.deleteCalls)
	}
	if got, want := s.ordered, []string{"create", "delete"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want create before delete %v", got, want)
	}
}

// An in-place update (same parent + same name, so oldSinkKey == newSinkKey)
// still routes to UpdateEntry rather than create-then-delete.
func TestReplicateInPlaceUpdateUsesUpdateEntry(t *testing.T) {
	s := &recordingSink{
		name:                "filer",
		sinkToDirectory:     "/dest",
		updateFoundExisting: true,
	}
	r := &Replicator{
		sink:   s,
		source: &source.FilerSource{Dir: "/source"},
	}

	err := r.Replicate(context.Background(), "/source/dir/file.txt", &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 456,
			},
		},
		NewParentPath: "/source/dir",
	})
	if err != nil {
		t.Fatalf("Replicate in-place update: %v", err)
	}

	if len(s.updateCalls) != 1 {
		t.Fatalf("update calls = %d, want 1", len(s.updateCalls))
	}
	if s.updateCalls[0].key != "/dest/dir/file.txt" {
		t.Fatalf("update key = %q, want /dest/dir/file.txt", s.updateCalls[0].key)
	}
	if s.updateCalls[0].newParentPath != "/dest/dir" {
		t.Fatalf("update newParentPath = %q, want /dest/dir", s.updateCalls[0].newParentPath)
	}
	if len(s.deleteCalls) != 0 || len(s.createCalls) != 0 {
		t.Fatalf("unexpected delete/create calls: deletes=%+v creates=%+v", s.deleteCalls, s.createCalls)
	}
}

// When the in-place update finds no existing entry to update, fall back to
// delete-then-create at the same key.
func TestReplicateInPlaceUpdateFallbackCreates(t *testing.T) {
	s := &recordingSink{
		name:                "filer",
		sinkToDirectory:     "/dest",
		updateFoundExisting: false,
	}
	r := &Replicator{
		sink:   s,
		source: &source.FilerSource{Dir: "/source"},
	}

	err := r.Replicate(context.Background(), "/source/dir/file.txt", &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 456,
			},
		},
		NewParentPath: "/source/dir",
	})
	if err != nil {
		t.Fatalf("Replicate in-place update fallback: %v", err)
	}

	if len(s.updateCalls) != 1 {
		t.Fatalf("update calls = %d, want 1", len(s.updateCalls))
	}
	if len(s.deleteCalls) != 1 || s.deleteCalls[0].key != "/dest/dir/file.txt" {
		t.Fatalf("delete calls = %+v, want same sink key", s.deleteCalls)
	}
	if len(s.createCalls) != 1 || s.createCalls[0].key != "/dest/dir/file.txt" {
		t.Fatalf("create calls = %+v, want same sink key", s.createCalls)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := util.IsEqualOrUnder(tt.candidate, tt.other); got != tt.expected {
				t.Fatalf("IsEqualOrUnder(%q, %q) = %v, want %v", tt.candidate, tt.other, got, tt.expected)
			}
		})
	}
}

func TestReplicateRenameOutToSiblingPrefixBecomesDelete(t *testing.T) {
	s := &recordingSink{name: "local", sinkToDirectory: "/dest"}
	r := &Replicator{
		sink:   s,
		source: &source.FilerSource{Dir: "/foo"},
	}

	err := r.Replicate(context.Background(), "/foo/old/file.txt", &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewParentPath: "/foobar/new",
	})
	if err != nil {
		t.Fatalf("Replicate rename out to sibling prefix: %v", err)
	}

	if len(s.deleteCalls) != 1 || s.deleteCalls[0].key != "/dest/old/file.txt" {
		t.Fatalf("delete calls = %+v, want old sink key", s.deleteCalls)
	}
	if len(s.createCalls) != 0 || len(s.updateCalls) != 0 {
		t.Fatalf("unexpected create/update calls: creates=%+v updates=%+v", s.createCalls, s.updateCalls)
	}
}

func TestReplicateRenameFromExcludedDirBecomesCreate(t *testing.T) {
	s := &recordingSink{name: "local", sinkToDirectory: "/dest"}
	r := &Replicator{
		sink:        s,
		source:      &source.FilerSource{Dir: "/foo"},
		excludeDirs: []string{"/foo/excluded"},
	}

	err := r.Replicate(context.Background(), "/foo/excluded/file.txt", &filer_pb.EventNotification{
		OldEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewEntry: &filer_pb.Entry{
			Name: "file.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime: 123,
			},
		},
		NewParentPath: "/foo/live",
	})
	if err != nil {
		t.Fatalf("Replicate rename from excluded dir: %v", err)
	}

	if len(s.createCalls) != 1 || s.createCalls[0].key != "/dest/live/file.txt" {
		t.Fatalf("create calls = %+v, want target sink key", s.createCalls)
	}
	if len(s.deleteCalls) != 0 || len(s.updateCalls) != 0 {
		t.Fatalf("unexpected delete/update calls: deletes=%+v updates=%+v", s.deleteCalls, s.updateCalls)
	}
}
