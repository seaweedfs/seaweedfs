package dispatcher

import (
	"context"
	"strings"
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

	matches := router.Route(context.Background(), snap, ev, now, nil)
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

// stubFilerClient satisfies filer_pb.SeaweedFilerClient just enough to
// pass the nil-check in Pipeline.Run; methods would panic if called,
// but the validation tests below all return before any RPC.
type stubFilerClient struct {
	filer_pb.SeaweedFilerClient
}

// fullPipeline assembles a Pipeline whose dependencies all pass the
// nil-check, so individual tests can knock out one piece at a time
// to exercise specific validation branches.
func fullPipeline() *Pipeline {
	return &Pipeline{
		Engine:      engine.New(),
		Persister:   reader.NewInMemoryPersister(),
		Client:      &fakeClient{},
		FilerClient: &stubFilerClient{},
		BucketsPath: "/buckets",
		ShardID:     0,
	}
}

func TestPipelineRunMissingEngine(t *testing.T) {
	p := fullPipeline()
	p.Engine = nil
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing required dependency") {
		t.Fatalf("expected dependency error, got %v", err)
	}
}

func TestPipelineRunMissingPersister(t *testing.T) {
	p := fullPipeline()
	p.Persister = nil
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing required dependency") {
		t.Fatalf("expected dependency error, got %v", err)
	}
}

func TestPipelineRunMissingClient(t *testing.T) {
	p := fullPipeline()
	p.Client = nil
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing required dependency") {
		t.Fatalf("expected dependency error, got %v", err)
	}
}

func TestPipelineRunMissingFilerClient(t *testing.T) {
	p := fullPipeline()
	p.FilerClient = nil
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "missing required dependency") {
		t.Fatalf("expected dependency error, got %v", err)
	}
}

func TestPipelineRunMissingBucketsPath(t *testing.T) {
	// BucketsPath has its own error message so operators can spot the
	// missing wiring (the dependency check runs first and would mask it).
	p := fullPipeline()
	p.BucketsPath = ""
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "BucketsPath required") {
		t.Fatalf("expected BucketsPath error, got %v", err)
	}
}

func TestPipelineRunRejectsNegativeShard(t *testing.T) {
	p := fullPipeline()
	p.ShardID = -1
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "out of") {
		t.Fatalf("expected shard-range error, got %v", err)
	}
}

func TestPipelineRunRejectsShardAtBoundary(t *testing.T) {
	// The range is half-open [0, ShardCount); ShardCount itself is
	// out-of-range. Catch this so a refactor that flips < to <= can't
	// introduce a one-past-the-end shard.
	p := fullPipeline()
	p.ShardID = s3lifecycle.ShardCount
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "out of") {
		t.Fatalf("expected shard-range error, got %v", err)
	}
}

func TestPipelineRunRejectsAnyShardOutOfRange(t *testing.T) {
	// Multi-shard configs use Shards rather than ShardID; if any one
	// of them is out of range the whole run must refuse, otherwise a
	// single bad entry could silently disable the rest.
	p := fullPipeline()
	p.ShardID = 0
	p.Shards = []int{0, 1, s3lifecycle.ShardCount + 1}
	err := p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "out of") {
		t.Fatalf("expected shard-range error for multi-shard config, got %v", err)
	}
}
