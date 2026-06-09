package command

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ sink.ReplicationSink = (*recordingSyncSink)(nil)

type recordingSyncSink struct {
	deleteKeys  []string
	createKeys  []string
	updateKeys  []string
	incremental bool
	// updateFoundExisting is what UpdateEntry reports; false exercises the
	// delete-then-create fallback for an in-place update missing on the sink.
	updateFoundExisting bool
	// ordered records the sink method names in call order so tests can assert
	// create-before-delete sequencing.
	ordered []string
}

func (s *recordingSyncSink) GetName() string { return "recording" }
func (s *recordingSyncSink) Initialize(util.Configuration, string) error {
	return nil
}
func (s *recordingSyncSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	s.deleteKeys = append(s.deleteKeys, key)
	s.ordered = append(s.ordered, "delete")
	return nil
}
func (s *recordingSyncSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	s.createKeys = append(s.createKeys, key)
	s.ordered = append(s.ordered, "create")
	return nil
}
func (s *recordingSyncSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (bool, error) {
	s.updateKeys = append(s.updateKeys, key)
	s.ordered = append(s.ordered, "update")
	return s.updateFoundExisting, nil
}

func equalSyncStrings(a, b []string) bool {
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
func (s *recordingSyncSink) GetSinkToDirectory() string         { return "/dest" }
func (s *recordingSyncSink) SetSourceFiler(*source.FilerSource) {}
func (s *recordingSyncSink) IsIncremental() bool                { return s.incremental }

// TestDestKeyPreservesColonForNonLocalSink guards against the command layer
// stripping colons from keys destined for non-local sinks. Colon
// sanitization belongs to the local filesystem sink only.
func TestDestKeyPreservesColonForNonLocalSink(t *testing.T) {
	t.Run("non-incremental", func(t *testing.T) {
		got := destKey(&recordingSyncSink{}, "/backup", "/src", util.FullPath("/src/2024:01:02/file:name.txt"), 0)
		want := "/backup/2024:01:02/file:name.txt"
		if got != want {
			t.Errorf("destKey() = %q, want %q", got, want)
		}
	})
	t.Run("incremental", func(t *testing.T) {
		got := destKey(&recordingSyncSink{incremental: true}, "/backup", "/src", util.FullPath("/src/file:name.txt"), 0)
		// the date partition varies by timezone; assert the colon-bearing name survives.
		if !strings.HasPrefix(got, "/backup/") || !strings.HasSuffix(got, "/file:name.txt") {
			t.Errorf("destKey() = %q, want /backup/<date>/file:name.txt with colon preserved", got)
		}
	})
}

// movingSyncSink is a recordingSyncSink that also implements sink.EntryMover,
// modeling a sink (like the filer) with a native atomic move.
type movingSyncSink struct {
	*recordingSyncSink
	moveOldKeys []string
	moveNewKeys []string
}

var _ sink.EntryMover = (*movingSyncSink)(nil)

func (s *movingSyncSink) MoveEntry(oldKey, newKey string, newEntry *filer_pb.Entry, signatures []int32) error {
	s.moveOldKeys = append(s.moveOldKeys, oldKey)
	s.moveNewKeys = append(s.moveNewKeys, newKey)
	s.ordered = append(s.ordered, "move")
	return nil
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
		{name: "empty", candidate: "", other: "/foo", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := util.IsEqualOrUnder(tt.candidate, tt.other); got != tt.expected {
				t.Fatalf("IsEqualOrUnder(%q, %q) = %v, want %v", tt.candidate, tt.other, got, tt.expected)
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

// A combined rename event (old and new both under sourcePath, doDeleteFiles=true)
// creates at the new key then deletes the old key — never UpdateEntry.
func TestGenProcessFunctionRenameCreatesThenDeletes(t *testing.T) {
	dataSink := &recordingSyncSink{}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, true, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "old.txt"},
			NewEntry:      &filer_pb.Entry{Name: "new.txt"},
			NewParentPath: "/foo/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn rename: %v", err)
	}

	if len(dataSink.updateKeys) != 0 {
		t.Fatalf("expected rename to bypass UpdateEntry, got %v", dataSink.updateKeys)
	}
	if len(dataSink.createKeys) != 1 || dataSink.createKeys[0] != "/dest/dir/new.txt" {
		t.Fatalf("create keys = %v, want [/dest/dir/new.txt]", dataSink.createKeys)
	}
	if len(dataSink.deleteKeys) != 1 || dataSink.deleteKeys[0] != "/dest/dir/old.txt" {
		t.Fatalf("delete keys = %v, want [/dest/dir/old.txt]", dataSink.deleteKeys)
	}
	if got, want := dataSink.ordered, []string{"create", "delete"}; !equalSyncStrings(got, want) {
		t.Fatalf("call order = %v, want create before delete %v", got, want)
	}
}

// A sink with a native move relocates a rename via MoveEntry, not create-then-delete.
func TestGenProcessFunctionRenameUsesMoveEntryWhenSupported(t *testing.T) {
	dataSink := &movingSyncSink{recordingSyncSink: &recordingSyncSink{}}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, true, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "old.txt"},
			NewEntry:      &filer_pb.Entry{Name: "new.txt"},
			NewParentPath: "/foo/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn rename via mover: %v", err)
	}

	if len(dataSink.moveOldKeys) != 1 || dataSink.moveOldKeys[0] != "/dest/dir/old.txt" || dataSink.moveNewKeys[0] != "/dest/dir/new.txt" {
		t.Fatalf("move old=%v new=%v, want one move /dest/dir/old.txt => /dest/dir/new.txt", dataSink.moveOldKeys, dataSink.moveNewKeys)
	}
	if len(dataSink.createKeys) != 0 || len(dataSink.deleteKeys) != 0 || len(dataSink.updateKeys) != 0 {
		t.Fatalf("native move must not create/delete/update: creates=%v deletes=%v updates=%v", dataSink.createKeys, dataSink.deleteKeys, dataSink.updateKeys)
	}
}

// With deletes disabled (backup/incremental), even a mover keeps the old entry:
// it creates the new one and does not move or delete the old.
func TestGenProcessFunctionRenameMoverKeepsOldWhenDeletesDisabled(t *testing.T) {
	dataSink := &movingSyncSink{recordingSyncSink: &recordingSyncSink{}}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, false, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "old.txt"},
			NewEntry:      &filer_pb.Entry{Name: "new.txt"},
			NewParentPath: "/foo/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn rename (no delete) via mover: %v", err)
	}

	if len(dataSink.createKeys) != 1 || dataSink.createKeys[0] != "/dest/dir/new.txt" {
		t.Fatalf("create keys = %v, want [/dest/dir/new.txt]", dataSink.createKeys)
	}
	if len(dataSink.moveOldKeys) != 0 || len(dataSink.deleteKeys) != 0 {
		t.Fatalf("deletes-disabled rename must keep old: moves=%v deletes=%v", dataSink.moveOldKeys, dataSink.deleteKeys)
	}
}

// An in-place update (same dir + same name) must route to UpdateEntry, never the
// rename create-then-delete path — otherwise it would delete the key it just wrote.
func TestGenProcessFunctionInPlaceUpdateUsesUpdateEntry(t *testing.T) {
	dataSink := &recordingSyncSink{updateFoundExisting: true}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, true, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewParentPath: "/foo/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn in-place update: %v", err)
	}

	if len(dataSink.updateKeys) != 1 || dataSink.updateKeys[0] != "/dest/dir/file.txt" {
		t.Fatalf("update keys = %v, want [/dest/dir/file.txt]", dataSink.updateKeys)
	}
	if len(dataSink.createKeys) != 0 || len(dataSink.deleteKeys) != 0 {
		t.Fatalf("in-place update must not create or delete: creates=%v deletes=%v", dataSink.createKeys, dataSink.deleteKeys)
	}
}

// When an in-place update finds no existing entry on the sink, it falls back to
// delete-then-create on the same key — and must delete before create, or the
// recreated entry would be removed.
func TestGenProcessFunctionInPlaceUpdateFallbackDeletesBeforeCreate(t *testing.T) {
	dataSink := &recordingSyncSink{updateFoundExisting: false}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, true, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewEntry:      &filer_pb.Entry{Name: "file.txt"},
			NewParentPath: "/foo/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn in-place update fallback: %v", err)
	}

	if got, want := dataSink.ordered, []string{"update", "delete", "create"}; !equalSyncStrings(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
	if len(dataSink.deleteKeys) != 1 || dataSink.deleteKeys[0] != "/dest/dir/file.txt" ||
		len(dataSink.createKeys) != 1 || dataSink.createKeys[0] != "/dest/dir/file.txt" {
		t.Fatalf("fallback keys: deletes=%v creates=%v, want both [/dest/dir/file.txt]", dataSink.deleteKeys, dataSink.createKeys)
	}
}

// With deletes disabled (e.g. incremental backup), a rename creates the new
// entry only — the old key is left in place.
func TestGenProcessFunctionRenameCreateOnlyWhenDeletesDisabled(t *testing.T) {
	dataSink := &recordingSyncSink{}
	processFn := genProcessFunction("/foo", "/dest", nil, nil, nil, nil, dataSink, false, false)

	err := processFn(&filer_pb.SubscribeMetadataResponse{
		Directory: "/foo/dir",
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "old.txt"},
			NewEntry:      &filer_pb.Entry{Name: "new.txt"},
			NewParentPath: "/foo/dir",
		},
	})
	if err != nil {
		t.Fatalf("processFn rename (no delete): %v", err)
	}

	if len(dataSink.createKeys) != 1 || dataSink.createKeys[0] != "/dest/dir/new.txt" {
		t.Fatalf("create keys = %v, want [/dest/dir/new.txt]", dataSink.createKeys)
	}
	if len(dataSink.deleteKeys) != 0 || len(dataSink.updateKeys) != 0 {
		t.Fatalf("unexpected delete/update calls: deletes=%v updates=%v", dataSink.deleteKeys, dataSink.updateKeys)
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
