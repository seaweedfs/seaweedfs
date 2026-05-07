package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
)

// TestPipelineIntegrationInMemory exercises router + dispatcher end-to-end
// without the Reader (which requires a live filer client). The Reader's
// behavior is covered separately in the reader package; this test pins the
// composition: an event flows through Route -> Schedule -> Dispatcher and
// the cursor advances on a successful RPC.
func TestPipelineIntegrationInMemory(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	hash := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	for _, k := range s3lifecycle.RuleActionKinds(rule) {
		prior[s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: k}] = engine.PriorState{
			BootstrapComplete: true,
			Mode:              engine.ModeEventDriven,
		}
	}
	e := engine.New()
	snap := e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		engine.CompileOptions{PriorStates: prior})

	now := time.Now()
	old := now.Add(-48 * time.Hour)

	ev := &reader.Event{
		TsNs:   old.UnixNano(),
		Bucket: "bk",
		Key:    "obj.txt",
		NewEntry: &filer_pb.Entry{
			Name: "obj.txt",
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    old.Unix(),
				FileSize: 1,
			},
		},
	}

	matches := router.Route(snap, ev, now)
	if len(matches) != 1 {
		t.Fatalf("expected 1 match, got %v", matches)
	}

	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
			}, nil
		},
	}
	d, sched := newDispatcher(client)
	d.ShardID = s3lifecycle.ShardID("bk", "obj.txt")
	for _, m := range matches {
		sched.Add(m)
	}

	processed := d.Tick(context.Background(), now.Add(time.Hour)) // past DueTime
	if processed != 1 {
		t.Fatalf("Tick processed=%d, want 1", processed)
	}
	if d.Cursor.Get(matches[0].Key) != ev.TsNs {
		t.Fatalf("cursor not at event TsNs: %d", d.Cursor.Get(matches[0].Key))
	}
}

func TestPipelineRunRequiresDependencies(t *testing.T) {
	p := &Pipeline{}
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error for empty Pipeline")
	}
}
