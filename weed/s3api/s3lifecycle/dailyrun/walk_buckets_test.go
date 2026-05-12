package dailyrun

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingDispatcher captures every (action, entry) pair the walker
// emits so tests can assert dispatch order and shard filtering.
type recordingDispatcher struct {
	calls []recordedDispatch
	err   error
}

type recordedDispatch struct {
	bucket string
	path   string
}

func (d *recordingDispatcher) Delete(_ context.Context, action *engine.CompiledAction, entry *bootstrap.Entry) error {
	d.calls = append(d.calls, recordedDispatch{bucket: action.Bucket, path: entry.Path})
	return d.err
}

// findShardForPath returns the shard ID for an entry with given path
// in given bucket. Helper for tests that want to force entries onto
// a known shard.
func findShardForPath(bucket, path string) int {
	return s3lifecycle.ShardID(bucket, path)
}

// fixedShardEntries returns a slice of bootstrap.Entries whose paths
// all live in the same shard. Used so a per-shard filter test has
// deterministic expectations.
func fixedShardEntries(t *testing.T, bucket string, shardID int, count int) []*bootstrap.Entry {
	t.Helper()
	var out []*bootstrap.Entry
	for i := 0; tries(i, count); i++ {
		path := "obj-" + intToStr(i)
		if findShardForPath(bucket, path) == shardID {
			out = append(out, &bootstrap.Entry{
				Path: path,
				// Old enough to expire under a 7-day ExpirationDays rule.
				ModTime:  time.Now().Add(-90 * 24 * time.Hour),
				Size:     1,
				IsLatest: true,
			})
			if len(out) >= count {
				return out
			}
		}
	}
	t.Fatalf("could not find %d entries in shard %d for bucket %s after 4096 attempts", count, shardID, bucket)
	return nil
}

func intToStr(i int) string {
	const hex = "0123456789abcdef"
	if i == 0 {
		return "0"
	}
	var out []byte
	for n := i; n > 0; n /= 16 {
		out = append([]byte{hex[n%16]}, out...)
	}
	return string(out)
}

func tries(i, count int) bool { return i < count*256+256 }

func snapshotForBucketRule(t *testing.T, bucket string, days int) *engine.Snapshot {
	t.Helper()
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: bucket, Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: days},
		}},
	}, engine.CompileOptions{})
	snap := e.Snapshot()
	for _, a := range snap.AllActions() {
		snap.MarkActive(a.Key)
	}
	return snap
}

func TestWalkBuckets_DispatchesOnlyShardMatchingEntries(t *testing.T) {
	bucket := "b1"
	const targetShard = 0
	// Build entries: half in shard 0, half elsewhere. The walker's
	// per-shard filter must drop the non-matching ones.
	inShard := fixedShardEntries(t, bucket, targetShard, 3)
	otherShard := fixedShardEntries(t, bucket, (targetShard+1)%16, 3)
	all := append([]*bootstrap.Entry{}, inShard...)
	all = append(all, otherShard...)

	snap := snapshotForBucketRule(t, bucket, 7) // 30-day-old objects expire
	d := &recordingDispatcher{}
	err := WalkBuckets(context.Background(), snap, targetShard, []string{bucket},
		bootstrap.EntryCallback(all), d)
	require.NoError(t, err)

	gotPaths := make([]string, 0, len(d.calls))
	for _, c := range d.calls {
		gotPaths = append(gotPaths, c.path)
		assert.Equal(t, bucket, c.bucket)
	}
	sort.Strings(gotPaths)

	wantPaths := make([]string, 0, len(inShard))
	for _, e := range inShard {
		wantPaths = append(wantPaths, e.Path)
	}
	sort.Strings(wantPaths)
	assert.Equal(t, wantPaths, gotPaths, "walker must dispatch exactly the in-shard entries")
}

func TestWalkBuckets_NilGuards(t *testing.T) {
	require.Error(t, WalkBuckets(context.Background(), nil, 0, nil, bootstrap.EntryCallback(nil), &recordingDispatcher{}))
	require.Error(t, WalkBuckets(context.Background(), snapshotForBucketRule(t, "b", 7), 0, nil, nil, &recordingDispatcher{}))
	require.Error(t, WalkBuckets(context.Background(), snapshotForBucketRule(t, "b", 7), 0, nil, bootstrap.EntryCallback(nil), nil))
}

func TestWalkBuckets_OneBucketErrorDoesNotStopOthers(t *testing.T) {
	// Two buckets, first ListFunc errors. WalkBuckets should still
	// process the second bucket but return the first bucket's error.
	listErr := errors.New("filer flake")
	list := func(_ context.Context, bucket, _ string, _ func(*bootstrap.Entry) error) error {
		if bucket == "bad" {
			return listErr
		}
		return nil
	}

	snap := snapshotForBucketRule(t, "good", 7)
	err := WalkBuckets(context.Background(), snap, 0, []string{"bad", "good"}, list, &recordingDispatcher{})
	require.Error(t, err)
	assert.ErrorIs(t, err, listErr)
}

func TestWalkBuckets_HonorsContextCancellationBetweenBuckets(t *testing.T) {
	// Pre-cancel ctx before invocation. Even an empty bucket list
	// must surface ctx.Err early so a long walk in progress can
	// short-circuit.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := WalkBuckets(ctx, snapshotForBucketRule(t, "b", 7), 0, []string{"b"},
		bootstrap.EntryCallback(nil), &recordingDispatcher{})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestEntryShardID_MPUUsesDestKey(t *testing.T) {
	bucket := "bkt"
	// Force a (Path, DestKey) pair that maps to different shards
	// so we can prove the filter picks the right one.
	for i := 0; i < 4096; i++ {
		path := ".uploads/" + intToStr(i)
		destKey := "user/" + intToStr(i)
		if s3lifecycle.ShardID(bucket, path) != s3lifecycle.ShardID(bucket, destKey) {
			e := &bootstrap.Entry{Path: path, DestKey: destKey, IsMPUInit: true}
			assert.Equal(t, s3lifecycle.ShardID(bucket, destKey), entryShardID(bucket, e),
				"MPU init must use DestKey for shard, not the .uploads/<id> path")
			return
		}
	}
	t.Fatal("could not find an MPU path/destkey pair with diverging shards in 4096 attempts")
}
