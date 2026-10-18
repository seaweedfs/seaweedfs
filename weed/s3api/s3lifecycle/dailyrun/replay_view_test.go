package dailyrun

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunShard_WalkBoundRuleDoesNotPinCursor: a bucket with a 3-day and a
// 30-day expiration rule and an 8-day retention window. The 30-day rule is
// walk-bound, so its object's PUT must not hold the replay cursor back; the
// cursor advances past it and the due 3-day objects on both sides dispatch.
// The 3-day rule also carries a durable scan_only mode: the replay view
// must rehabilitate it to event_driven the way RulesForShard does.
func TestRunShard_WalkBoundRuleDoesNotPinCursor(t *testing.T) {
	shortRule := &s3lifecycle.Rule{ID: "short", Status: s3lifecycle.StatusEnabled, Prefix: "short/", ExpirationDays: 3}
	longRule := &s3lifecycle.Rule{ID: "long", Status: s3lifecycle.StatusEnabled, Prefix: "long/", ExpirationDays: 30}
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{shortRule, longRule}},
	}, engine.CompileOptions{PriorStates: map[s3lifecycle.ActionKey]engine.PriorState{
		{Bucket: "b1", RuleHash: s3lifecycle.RuleHash(shortRule), ActionKind: s3lifecycle.ActionKindExpirationDays}: {
			BootstrapComplete: true, Mode: engine.ModeScanOnly},
		{Bucket: "b1", RuleHash: s3lifecycle.RuleHash(longRule), ActionKind: s3lifecycle.ActionKindExpirationDays}: {
			BootstrapComplete: true},
	}})
	snap := e.Snapshot()
	for _, a := range snap.AllActions() {
		snap.MarkActive(a.Key)
	}
	runNow := time.Unix(1_700_000_000, 0).UTC()
	retention := s3lifecycle.DaysToDuration(8)
	startTsNs := runNow.Add(-retention).UnixNano()
	p := newMemPersister()
	require.NoError(t, p.Save(context.Background(), 0, Cursor{
		TsNs:         startTsNs,
		RuleSetHash:  engine.ReplayContentHash(snap),
		PromotedHash: engine.PromotedHash(snap, retention),
		LastWalkedNs: runNow.UnixNano(),
	}))
	put := func(key string, age time.Duration) *reader.Event {
		at := runNow.Add(-age)
		return &reader.Event{TsNs: at.UnixNano(), Bucket: "b1", Key: key, NewEntry: &filer_pb.Entry{
			Name:       key,
			Attributes: &filer_pb.FuseAttributes{Mtime: at.Unix(), FileSize: 1},
		}}
	}
	events := make(chan *reader.Event, 3)
	events <- put("short/a", s3lifecycle.DaysToDuration(7))
	events <- put("long/a", s3lifecycle.DaysToDuration(6))
	last := put("short/b", s3lifecycle.DaysToDuration(5))
	events <- last
	close(events)
	client := &recordingClient{}
	cfg := Config{Persister: p, Client: client, Lister: stubSiblingLister{}, RetentionWindow: retention}
	require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 0, events, nil))
	assert.ElementsMatch(t, []string{"short/a", "short/b"}, client.seenObjects())
	got, _, _ := p.Load(context.Background(), 0)
	assert.Equal(t, last.TsNs, got.TsNs, "cursor must advance past the walk-bound object's PUT")
}
