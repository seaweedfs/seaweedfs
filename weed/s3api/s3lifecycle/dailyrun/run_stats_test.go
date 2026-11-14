package dailyrun

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func allShards() []int {
	out := make([]int, s3lifecycle.ShardCount)
	for i := range out {
		out[i] = i
	}
	return out
}

func TestWalkBuckets_RecordsEveryBucketFailure(t *testing.T) {
	list := func(_ context.Context, bucket, _ string, _ func(*bootstrap.Entry) error) error {
		if strings.HasPrefix(bucket, "bad") {
			return errors.New("list " + bucket + " failed")
		}
		return nil
	}
	runStats := &RunStats{}
	err := WalkBuckets(context.Background(), snapshotForBucketRule(t, "good", 7), []int{0},
		[]string{"bad-a", "good", "bad-b"}, list, &recordingDispatcher{}, runStats)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad-a", "the returned error stays the first bucket's")

	sum := runStats.Summary()
	assert.Equal(t, 1, sum.BucketsWalked)
	assert.Equal(t, 2, sum.BucketsFailed)
	assert.Equal(t, 0, sum.BucketsSkipped)
	require.Len(t, sum.BucketFailures, 2)
	assert.Equal(t, "bad-a", sum.BucketFailures[0].Bucket)
	assert.Contains(t, sum.BucketFailures[0].Error, "list bad-a failed")
	assert.Equal(t, "bad-b", sum.BucketFailures[1].Bucket)
	assert.Equal(t, PhaseWalk, sum.Phase)
	assert.Equal(t, "bad-b", sum.Bucket)
}

func TestWalkBuckets_CancelMarksRemainingBucketsSkipped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	list := func(ctx context.Context, bucket, _ string, _ func(*bootstrap.Entry) error) error {
		if bucket == "second" {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	runStats := &RunStats{}
	err := WalkBuckets(ctx, snapshotForBucketRule(t, "first", 7), []int{0},
		[]string{"first", "second", "third"}, list, &recordingDispatcher{}, runStats)
	assert.ErrorIs(t, err, context.Canceled)

	sum := runStats.Summary()
	assert.Equal(t, 1, sum.BucketsWalked)
	assert.Equal(t, 0, sum.BucketsFailed, "a cancelled walk is not a bucket failure")
	assert.Equal(t, 2, sum.BucketsSkipped)
	assert.Empty(t, sum.BucketFailures)
}

func TestRunStats_FailedWalkWinsAcrossWalksOfOneBucket(t *testing.T) {
	runStats := &RunStats{}
	runStats.finishBucket("b", bucketSkipped, nil)
	runStats.finishBucket("b", bucketWalked, nil)
	runStats.finishBucket("b", bucketFailed, errors.New("first"))
	runStats.finishBucket("b", bucketFailed, errors.New("second"))
	runStats.finishBucket("b", bucketWalked, nil)

	sum := runStats.Summary()
	assert.Equal(t, 1, sum.BucketsFailed)
	assert.Equal(t, 0, sum.BucketsWalked)
	require.Len(t, sum.BucketFailures, 1)
	assert.Equal(t, "first", sum.BucketFailures[0].Error)
}

func TestRunStats_BoundsReportedFailures(t *testing.T) {
	runStats := &RunStats{}
	long := strings.Repeat("x", 2*maxReportedErrorLen)
	for i := 0; i < maxReportedBucketFailures+5; i++ {
		runStats.finishBucket("b-"+intToStr(i), bucketFailed, errors.New(long))
	}
	sum := runStats.Summary()
	assert.Equal(t, maxReportedBucketFailures+5, sum.BucketsFailed, "the count stays exact")
	require.Len(t, sum.BucketFailures, maxReportedBucketFailures)
	assert.LessOrEqual(t, len(sum.BucketFailures[0].Error), maxReportedErrorLen+len("..."))
}

func TestWalkBuckets_CountsEntriesAndObjects(t *testing.T) {
	old := time.Now().Add(-90 * 24 * time.Hour)
	entries := []*bootstrap.Entry{
		{Path: "a", VersionID: "v3", ModTime: old, IsLatest: true},
		{Path: "a", VersionID: "v2", ModTime: old},
		{Path: "a", VersionID: "v1", ModTime: old},
		{Path: "b", ModTime: old, IsLatest: true},
		{Path: "dir", IsDirectory: true},
		{Path: ".uploads/u1", DestKey: "c", IsDirectory: true, IsMPUInit: true, ModTime: old},
	}
	runStats := &RunStats{}
	require.NoError(t, WalkBuckets(context.Background(), snapshotForBucketRule(t, "bkt", 7), allShards(),
		[]string{"bkt"}, bootstrap.EntryCallback(entries), &recordingDispatcher{}, runStats))

	sum := runStats.Summary()
	assert.Equal(t, int64(5), sum.EntriesVisited, "versions and upload records count, plain directories do not")
	assert.Equal(t, int64(2), sum.ObjectsVisited, "versions of one key count as one object")
	assert.Equal(t, 1, sum.BucketsWalked)
}

func TestWalkerDispatcher_CountsOutcomes(t *testing.T) {
	runStats := &RunStats{}
	a := sampleAction(t, s3lifecycle.ActionKindExpirationDays)
	e := &bootstrap.Entry{Path: "obj"}

	d := &WalkerDispatcher{Client: &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, Stats: runStats}
	require.NoError(t, d.Delete(context.Background(), a, e))
	require.NoError(t, d.Delete(context.Background(), a, e))
	d.Client = &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK}
	require.NoError(t, d.Delete(context.Background(), a, e))
	d.Client = &walkerStubClient{err: errors.New("unavailable")}
	require.Error(t, d.Delete(context.Background(), a, e))
	d.Client = &walkerStubClient{nilResp: true}
	require.Error(t, d.Delete(context.Background(), a, e))

	assert.Equal(t, map[string]int64{"DONE": 2, "SKIPPED_OBJECT_LOCK": 1, "RPC_ERROR": 2}, runStats.Summary().Outcomes)
}

func TestProcessMatches_CountsOutcomes(t *testing.T) {
	runNow := time.Now()
	rule := s3lifecycle.ActionKey{Bucket: "b", RuleHash: [8]byte{1}, ActionKind: s3lifecycle.ActionKindExpirationDays}
	matches := []router.Match{
		{Key: rule, Bucket: "b", ObjectKey: "a", DueTime: runNow.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "b", DueTime: runNow.Add(-time.Hour)},
		{Key: rule, Bucket: "b", ObjectKey: "c", DueTime: runNow.Add(-time.Hour)},
	}
	client := &recordingClient{responses: []s3_lifecycle_pb.LifecycleDeleteOutcome{
		s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
	}}
	runStats := &RunStats{}
	_, halted, err := processMatches(context.Background(), Config{Client: client, Stats: runStats}, runNow, &reader.Event{}, matches)
	require.NoError(t, err)
	assert.True(t, halted)
	assert.Equal(t, map[string]int64{"DONE": 1, "NOOP_RESOLVED": 1, "RETRY_LATER": 1}, runStats.Summary().Outcomes)
}

func TestRunStats_NilIsANoOp(t *testing.T) {
	var runStats *RunStats
	runStats.startBucket("b")
	runStats.finishBucket("b", bucketFailed, errors.New("x"))
	runStats.startReplay()
	runStats.visitEntry(true)
	runStats.scanEvent()
	runStats.recordOutcome("DONE")
	runStats.setShardErrors(1)
	assert.Equal(t, RunSummary{}, runStats.Summary())
}
