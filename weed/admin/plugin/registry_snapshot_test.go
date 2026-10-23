package plugin

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestCloneWorkerHeartbeatFiltersNilAndIsolatesState(t *testing.T) {
	t.Parallel()
	source := &plugin_pb.WorkerHeartbeat{
		RunningWork:      []*plugin_pb.RunningWork{nil, {WorkId: "first"}, nil, {WorkId: "second"}, nil},
		QueuedJobsByType: map[string]int32{"vacuum": 2},
		Metadata:         map[string]string{"rack": "rack-a"},
	}
	got := cloneWorkerHeartbeat(source)
	if len(got.RunningWork) != 2 || got.RunningWork[0].GetWorkId() != "first" || got.RunningWork[1].GetWorkId() != "second" {
		t.Fatalf("unexpected filtered work: %v", got.RunningWork)
	}
	got.RunningWork[0].WorkId = "changed"
	got.QueuedJobsByType["vacuum"] = 99
	got.Metadata["rack"] = "changed"
	if len(source.RunningWork) != 5 || source.RunningWork[0] != nil || source.RunningWork[1].WorkId != "first" {
		t.Fatalf("cloning mutated source work: %v", source.RunningWork)
	}
	if source.QueuedJobsByType["vacuum"] != 2 || source.Metadata["rack"] != "rack-a" {
		t.Fatal("cloned maps alias the source")
	}
}

func TestCloneWorkerHeartbeatPreservesEmptyCollections(t *testing.T) {
	t.Parallel()
	if cloneWorkerHeartbeat(nil) != nil {
		t.Fatal("nil heartbeat must stay nil")
	}
	for _, tc := range []struct {
		name        string
		source      *plugin_pb.WorkerHeartbeat
		wantNilWork bool
	}{
		{"nil", &plugin_pb.WorkerHeartbeat{}, true},
		{"empty", &plugin_pb.WorkerHeartbeat{
			RunningWork:      []*plugin_pb.RunningWork{},
			QueuedJobsByType: map[string]int32{},
			Metadata:         map[string]string{},
		}, false},
		{"all nil work", &plugin_pb.WorkerHeartbeat{RunningWork: []*plugin_pb.RunningWork{nil, nil}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := cloneWorkerHeartbeat(tc.source)
			if len(got.RunningWork) != 0 || (got.RunningWork == nil) != tc.wantNilWork {
				t.Fatalf("unexpected empty work slice: %#v", got.RunningWork)
			}
			if (got.QueuedJobsByType == nil) != (tc.source.QueuedJobsByType == nil) || (got.Metadata == nil) != (tc.source.Metadata == nil) {
				t.Fatal("clone changed whether empty maps are writable")
			}
			if got.Metadata != nil {
				got.Metadata["added"] = "value"
				if len(tc.source.Metadata) != 0 {
					t.Fatal("empty metadata map aliases source")
				}
			}
		})
	}
}
