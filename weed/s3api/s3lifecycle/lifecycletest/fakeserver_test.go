package lifecycletest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFake_DefaultIsDoneOutOfTheBox(t *testing.T) {
	// A test that doesn't queue anything should still get a non-error
	// response so it can exercise the worker's happy path.
	f := NewFakeLifecycleServer()
	resp, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:     "b",
		ObjectPath: "k",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, resp.Outcome)
	assert.Equal(t, "", resp.Reason)
}

func TestFake_QueuedOutcomesPopFIFO(t *testing.T) {
	// Per-key queue is FIFO and one-shot per entry; after the queue
	// drains, Default kicks in.
	f := NewFakeLifecycleServer()
	f.SetDefault(NoopResolved("nothing more queued"))
	f.Queue("b", "k", "", RetryLater("first"))
	f.Queue("b", "k", "", Blocked("second"))

	got := []s3_lifecycle_pb.LifecycleDeleteOutcome{}
	reasons := []string{}
	for i := 0; i < 3; i++ {
		resp, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{
			Bucket: "b", ObjectPath: "k",
		})
		require.NoError(t, err)
		got = append(got, resp.Outcome)
		reasons = append(reasons, resp.Reason)
	}
	assert.Equal(t, []s3_lifecycle_pb.LifecycleDeleteOutcome{
		s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
		s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
	}, got)
	assert.Equal(t, []string{"first", "second", "nothing more queued"}, reasons)
}

func TestFake_QueuesIsolatedByKey(t *testing.T) {
	// Queues are partitioned by (bucket, objectPath, versionId); a queued
	// outcome for one key must not bleed into another's lookup.
	f := NewFakeLifecycleServer()
	f.Queue("b", "a", "", Blocked("a-only"))
	f.Queue("b", "b", "", RetryLater("b-only"))

	respA, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "a"})
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED, respA.Outcome)

	respB, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "b"})
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER, respB.Outcome)
}

func TestFake_VersionIDPartOfKey(t *testing.T) {
	// Two requests for the same bucket/objectPath but different
	// versionIds must address different queues.
	f := NewFakeLifecycleServer()
	f.Queue("b", "k", "v1", SkippedObjectLock("v1-locked"))
	f.Queue("b", "k", "v2", Done())

	respV1, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k", VersionId: "v1"})
	require.NoError(t, err)
	respV2, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k", VersionId: "v2"})
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK, respV1.Outcome)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, respV2.Outcome)
}

func TestFake_KeyComponentsWithDelimitersDoNotCollide(t *testing.T) {
	// String-concatenation keys would have made these two requests
	// indistinguishable. The struct key keeps them separate.
	f := NewFakeLifecycleServer()
	f.Queue("b/k", "", "", Blocked("variant-a"))
	f.Queue("b", "k", "", Done())

	respA, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b/k", ObjectPath: ""})
	require.NoError(t, err)
	respB, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k"})
	require.NoError(t, err)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED, respA.Outcome)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_DONE, respB.Outcome)
}

func TestFake_ErrShortCircuitsBeforeRecording(t *testing.T) {
	// Err makes LifecycleDelete return (nil, err) without recording the
	// request — transport-error tests rely on the worker's own
	// bookkeeping, not the fake's.
	f := NewFakeLifecycleServer()
	transportErr := errors.New("connection refused")
	f.SetError(transportErr)

	resp, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k"})
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, transportErr)
	assert.Empty(t, f.Recorded(), "transport-error calls must not be recorded")

	// Clearing the error returns the server to normal behavior.
	f.SetError(nil)
	resp2, err2 := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k"})
	require.NoError(t, err2)
	require.NotNil(t, resp2)
	assert.Len(t, f.Recorded(), 1)
}

func TestFake_RecordsRequestsInOrder(t *testing.T) {
	// Recorded() preserves arrival order so tests can assert that
	// dispatch happened in the expected sequence.
	f := NewFakeLifecycleServer()
	for _, key := range []string{"k1", "k2", "k3"} {
		_, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{
			Bucket: "b", ObjectPath: key,
		})
		require.NoError(t, err)
	}
	rec := f.Recorded()
	require.Len(t, rec, 3)
	assert.Equal(t, "k1", rec[0].ObjectPath)
	assert.Equal(t, "k2", rec[1].ObjectPath)
	assert.Equal(t, "k3", rec[2].ObjectPath)
}

func TestFake_RecordedIsSnapshot(t *testing.T) {
	// Mutating the slice the caller got back must not bleed into the
	// fake's internal state — otherwise a flaky test could corrupt
	// bookkeeping for later assertions.
	f := NewFakeLifecycleServer()
	_, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k"})
	require.NoError(t, err)

	snap := f.Recorded()
	require.Len(t, snap, 1)
	snap[0] = nil
	again := f.Recorded()
	require.Len(t, again, 1)
	assert.NotNil(t, again[0], "internal record must survive caller-side mutation of the snapshot")
}

func TestFake_RecordedRequestsAreDeepCopies(t *testing.T) {
	// A caller mutating fields on a Recorded() entry must not bleed
	// into a later Recorded() snapshot. We also confirm the original
	// request the caller passed in stays decoupled from internal state
	// — proto.Clone runs at record time, so the caller's pointer is
	// no longer the one the fake holds.
	f := NewFakeLifecycleServer()
	orig := &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k", VersionId: "v"}
	_, err := f.LifecycleDelete(context.Background(), orig)
	require.NoError(t, err)

	snap := f.Recorded()
	require.Len(t, snap, 1)
	snap[0].Bucket = "mutated-by-caller"
	snap[0].ObjectPath = "also-mutated"

	again := f.Recorded()
	require.Len(t, again, 1)
	assert.Equal(t, "b", again[0].Bucket, "field mutations on a snapshot must not bleed back")
	assert.Equal(t, "k", again[0].ObjectPath)
	assert.Equal(t, "v", again[0].VersionId)

	// The caller's own pointer is independent of the fake's record.
	orig.Bucket = "caller-mutates-original"
	yetAgain := f.Recorded()
	assert.Equal(t, "b", yetAgain[0].Bucket, "caller mutating the input pointer must not bleed in either")
}

func TestFake_NilRequestUsesDefault(t *testing.T) {
	// gRPC won't deliver a nil request in practice, but defensive code
	// in the fake should still produce a deterministic response so a
	// regressing client doesn't panic the test process.
	f := NewFakeLifecycleServer()
	f.SetDefault(Blocked("no request"))
	resp, err := f.LifecycleDelete(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED, resp.Outcome)
}

func TestFake_ConcurrentCallsSerializeWithoutDeadlock(t *testing.T) {
	// The dispatcher fans dispatch across many goroutines; the fake
	// must not livelock or drop records under concurrent load. Capture
	// each goroutine's err so a regression in concurrent paths surfaces
	// instead of being masked by length-only assertions.
	f := NewFakeLifecycleServer()
	const N = 64
	var wg sync.WaitGroup
	errCh := make(chan error, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, err := f.LifecycleDelete(context.Background(), &s3_lifecycle_pb.LifecycleDeleteRequest{Bucket: "b", ObjectPath: "k"})
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	assert.Len(t, f.Recorded(), N)
}
