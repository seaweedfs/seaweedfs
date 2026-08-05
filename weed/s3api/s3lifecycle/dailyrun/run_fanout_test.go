package dailyrun

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// scriptedSubscribeStream replays a fixed list, then EOFs like a filer
// honoring UntilNs.
type scriptedSubscribeStream struct {
	grpc.ClientStream
	responses []*filer_pb.SubscribeMetadataResponse
	next      int
}

func (s *scriptedSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	if s.next >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.next]
	s.next++
	return resp, nil
}

type scriptedSubscribeClient struct {
	filer_pb.SeaweedFilerClient
	responses []*filer_pb.SubscribeMetadataResponse
}

func (c *scriptedSubscribeClient) SubscribeMetadata(_ context.Context, _ *filer_pb.SubscribeMetadataRequest, _ ...grpc.CallOption) (filer_pb.SeaweedFiler_SubscribeMetadataClient, error) {
	return &scriptedSubscribeStream{responses: c.responses}, nil
}

func subscribeResponse(bucket, key string, mtime time.Time, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		TsNs: tsNs,
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: "/buckets/" + bucket,
			NewEntry: &filer_pb.Entry{
				Name:       key,
				Attributes: &filer_pb.FuseAttributes{Mtime: mtime.Unix(), FileSize: 1},
			},
		},
	}
}

// keyForShard finds an object key in bucket that hashes to shardID.
func keyForShard(t *testing.T, bucket string, shardID int) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := "k" + strconv.Itoa(i)
		if s3lifecycle.ShardID(bucket, key) == shardID {
			return key
		}
	}
	t.Fatalf("no key hashes to shard %d", shardID)
	return ""
}

// The second wedge: a drain halted by a BLOCKED dispatch stops reading
// its channel, the fan-out blocks once the 256 buffer fills, and every
// other shard starves behind it.
func TestRun_HaltedShardDoesNotStarveOthers(t *testing.T) {
	const bucket = "bk"
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	ruleHash := s3lifecycle.RuleHash(rule)
	for _, k := range s3lifecycle.RuleActionKinds(rule) {
		prior[s3lifecycle.ActionKey{Bucket: bucket, RuleHash: ruleHash, ActionKind: k}] = engine.PriorState{
			BootstrapComplete: true,
			Mode:              engine.ModeEventDriven,
		}
	}
	e := engine.New()
	snap := e.Compile([]engine.CompileInput{{Bucket: bucket, Rules: []*s3lifecycle.Rule{rule}}},
		engine.CompileOptions{PriorStates: prior})

	haltedShard, starvedShard := 0, 1
	haltedKey := keyForShard(t, bucket, haltedShard)
	starvedKey := keyForShard(t, bucket, starvedShard)

	runNow := time.Now().UTC()
	evTime := runNow.Add(-48 * time.Hour) // past the 1-day expiration
	// Past the 256 buffer, so the fan-out blocks on the halted shard
	// before reaching the starved shard's event.
	var responses []*filer_pb.SubscribeMetadataResponse
	for i := 0; i < 300; i++ {
		responses = append(responses, subscribeResponse(bucket, haltedKey, evTime, evTime.UnixNano()+int64(i)))
	}
	responses = append(responses, subscribeResponse(bucket, starvedKey, evTime, runNow.Add(-time.Hour).UnixNano()))

	// Pinned per object, not call index: the shards dispatch concurrently.
	client := &recordingClient{outcomeByObject: map[string]s3_lifecycle_pb.LifecycleDeleteOutcome{
		haltedKey: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
	}}

	// Resume from before the events; the cold-start floor would skip them.
	// Hashes must match or runShard takes the recovery branch instead.
	persister := newMemPersister()
	seeded := Cursor{
		TsNs:         runNow.Add(-72 * time.Hour).UnixNano(),
		RuleSetHash:  engine.ReplayContentHash(snap),
		PromotedHash: engine.PromotedHash(snap, engine.MaxEffectiveTTL(snap)),
	}
	for _, sh := range []int{haltedShard, starvedShard} {
		require.NoError(t, persister.Save(context.Background(), sh, seeded))
	}

	cfg := Config{
		Shards:      []int{haltedShard, starvedShard},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: &scriptedSubscribeClient{responses: responses},
		Client:      client,
		Persister:   persister,
		Lister:      stubSiblingLister{},
		Now:         func() time.Time { return runNow },
	}

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run wedged: the fan-out blocked on a shard that had stopped draining")
	}

	// Returning isn't enough: dropping the starved shard's events would
	// return just as cleanly.
	assert.Contains(t, client.seenObjects(), starvedKey,
		"the shard behind the halted one must still get its events")
	starvedCursor, found, err := persister.Load(context.Background(), starvedShard)
	require.NoError(t, err)
	require.True(t, found)
	assert.Greater(t, starvedCursor.TsNs, seeded.TsNs, "starved shard's cursor must advance")
}
