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
}

func (s *recordingSink) GetName() string {
	return s.name
}

func (s *recordingSink) Initialize(util.Configuration, string) error {
	return nil
}

func (s *recordingSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	s.deleteCalls = append(s.deleteCalls, deleteCall{key: key, isDirectory: isDirectory})
	return nil
}

func (s *recordingSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	s.createCalls = append(s.createCalls, createCall{key: key})
	return nil
}

func (s *recordingSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (bool, error) {
	s.updateCalls = append(s.updateCalls, updateCall{key: key, newParentPath: newParentPath})
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
		t.Fatalf("expected non-filer rename to bypass UpdateEntry, got %d calls", len(s.updateCalls))
	}
	if len(s.deleteCalls) != 1 || s.deleteCalls[0].key != "/dest/old/file.txt" {
		t.Fatalf("delete calls = %+v, want old sink key", s.deleteCalls)
	}
	if len(s.createCalls) != 1 || s.createCalls[0].key != "/dest/new/renamed.txt" {
		t.Fatalf("create calls = %+v, want target sink key", s.createCalls)
	}
}

func TestReplicateRenameUsesUpdateForFilerSink(t *testing.T) {
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

	if len(s.updateCalls) != 1 {
		t.Fatalf("update calls = %d, want 1", len(s.updateCalls))
	}
	if s.updateCalls[0].key != "/dest/old/file.txt" {
		t.Fatalf("update key = %q, want /dest/old/file.txt", s.updateCalls[0].key)
	}
	if s.updateCalls[0].newParentPath != "/dest/new" {
		t.Fatalf("update newParentPath = %q, want /dest/new", s.updateCalls[0].newParentPath)
	}
	if len(s.deleteCalls) != 0 || len(s.createCalls) != 0 {
		t.Fatalf("unexpected delete/create calls: deletes=%+v creates=%+v", s.deleteCalls, s.createCalls)
	}
}

func TestReplicateRenameFallbackCreatesTargetKey(t *testing.T) {
	s := &recordingSink{
		name:                "filer",
		sinkToDirectory:     "/dest",
		updateFoundExisting: false,
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
		t.Fatalf("Replicate rename fallback: %v", err)
	}

	if len(s.updateCalls) != 1 {
		t.Fatalf("update calls = %d, want 1", len(s.updateCalls))
	}
	if len(s.deleteCalls) != 1 || s.deleteCalls[0].key != "/dest/old/file.txt" {
		t.Fatalf("delete calls = %+v, want old sink key", s.deleteCalls)
	}
	if len(s.createCalls) != 1 || s.createCalls[0].key != "/dest/new/renamed.txt" {
		t.Fatalf("create calls = %+v, want target sink key", s.createCalls)
	}
}
