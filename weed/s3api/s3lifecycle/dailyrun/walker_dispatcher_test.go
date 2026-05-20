package dailyrun

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// walkerStubClient captures the last LifecycleDeleteRequest so tests
// can assert on the request shape produced by WalkerDispatcher.
type walkerStubClient struct {
	lastReq *s3_lifecycle_pb.LifecycleDeleteRequest
	outcome s3_lifecycle_pb.LifecycleDeleteOutcome
	err     error
	reason  string
	nilResp bool // return (nil, nil) — pin the dispatcher's defensive guard
}

func (c *walkerStubClient) LifecycleDelete(_ context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	c.lastReq = req
	if c.err != nil {
		return nil, c.err
	}
	if c.nilResp {
		return nil, nil
	}
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: c.outcome, Reason: c.reason}, nil
}

func sampleAction(t *testing.T, kind s3lifecycle.ActionKind) *engine.CompiledAction {
	t.Helper()
	var rh [8]byte
	for i := range rh {
		rh[i] = byte(0xa0 + i)
	}
	return &engine.CompiledAction{
		Bucket: "bkt",
		Key:    s3lifecycle.ActionKey{Bucket: "bkt", ActionKind: kind, RuleHash: rh},
		Mode:   engine.ModeEventDriven,
	}
}

func TestWalkerDispatcher_NonVersionedSendsExpectedRequest(t *testing.T) {
	c := &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
	d := &WalkerDispatcher{Client: c}
	a := sampleAction(t, s3lifecycle.ActionKindExpirationDays)
	err := d.Delete(context.Background(), a, &bootstrap.Entry{Path: "obj"})
	require.NoError(t, err)
	require.NotNil(t, c.lastReq)
	assert.Equal(t, "bkt", c.lastReq.Bucket)
	assert.Equal(t, "obj", c.lastReq.ObjectPath)
	assert.Equal(t, "", c.lastReq.VersionId)
	assert.Equal(t, a.Key.RuleHash[:], c.lastReq.RuleHash)
	assert.Equal(t, s3_lifecycle_pb.ActionKind_EXPIRATION_DAYS, c.lastReq.ActionKind)
	// Bootstrap-style call: server skips CAS witness when nil.
	assert.Nil(t, c.lastReq.ExpectedIdentity)
}

func TestWalkerDispatcher_VersionedPassesVersionID(t *testing.T) {
	c := &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
	d := &WalkerDispatcher{Client: c}
	a := sampleAction(t, s3lifecycle.ActionKindNoncurrentDays)
	err := d.Delete(context.Background(), a, &bootstrap.Entry{Path: "obj", VersionID: "v-abc"})
	require.NoError(t, err)
	assert.Equal(t, "v-abc", c.lastReq.VersionId)
}

func TestWalkerDispatcher_MPUInitUsesUploadsPath(t *testing.T) {
	// Rule-prefix matching uses DestKey to decide IF this MPU init
	// matches; dispatch uses Path (.uploads/<id>) because the server's
	// ABORT_MPU handler strips the .uploads/ prefix to get the upload
	// id. Sending DestKey here would BLOCK with FATAL_EVENT_ERROR.
	c := &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
	d := &WalkerDispatcher{Client: c}
	a := sampleAction(t, s3lifecycle.ActionKindAbortMPU)
	err := d.Delete(context.Background(), a, &bootstrap.Entry{
		Path:      ".uploads/abc123",
		DestKey:   "user/path/object",
		IsMPUInit: true,
	})
	require.NoError(t, err)
	assert.Equal(t, ".uploads/abc123", c.lastReq.ObjectPath)
}

func TestWalkerDispatcher_MPUInitEmptyDestKeyErrors(t *testing.T) {
	// An MPU init record with no DestKey is mid-write before metadata
	// landed; skipping silently in the walker is fine, but the
	// dispatcher must not invent a path.
	c := &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
	d := &WalkerDispatcher{Client: c}
	a := sampleAction(t, s3lifecycle.ActionKindAbortMPU)
	err := d.Delete(context.Background(), a, &bootstrap.Entry{
		Path:      ".uploads/abc123",
		IsMPUInit: true,
	})
	require.Error(t, err)
	assert.Nil(t, c.lastReq, "no RPC should be sent when DestKey is empty")
}

func TestWalkerDispatcher_AcceptsAllResolvedOutcomes(t *testing.T) {
	for _, oc := range []s3_lifecycle_pb.LifecycleDeleteOutcome{
		s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK,
	} {
		c := &walkerStubClient{outcome: oc}
		d := &WalkerDispatcher{Client: c}
		err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
		assert.NoError(t, err, "outcome %s must be treated as resolved", oc)
	}
}

func TestWalkerDispatcher_UnresolvedOutcomeReturnsError(t *testing.T) {
	// RETRY_LATER, BLOCKED, and UNSPECIFIED all halt the walk so it
	// resumes from Checkpoint.LastScannedPath on the next run.
	for _, oc := range []s3_lifecycle_pb.LifecycleDeleteOutcome{
		s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
		s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_LIFECYCLE_DELETE_OUTCOME_UNSPECIFIED,
	} {
		c := &walkerStubClient{outcome: oc, reason: "server said so"}
		d := &WalkerDispatcher{Client: c}
		err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
		require.Error(t, err, "outcome %s must halt the walk", oc)
		assert.Contains(t, err.Error(), oc.String())
	}
}

func TestWalkerDispatcher_TransportErrorReturnsWrappedError(t *testing.T) {
	c := &walkerStubClient{err: errors.New("transport boom")}
	d := &WalkerDispatcher{Client: c}
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transport boom")
}

func TestWalkerDispatcher_NilResponseReturnsError(t *testing.T) {
	// A server returning (nil, nil) would otherwise panic on the
	// outcome switch.
	c := &walkerStubClient{nilResp: true}
	d := &WalkerDispatcher{Client: c}
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil response")
}

func TestWalkerDispatcher_LimiterWaitsBeforeDispatch(t *testing.T) {
	// Build a tiny limiter (1 token, slow refill) and pre-drain it so
	// the next Wait blocks until the deadline. Dispatcher must respect
	// the limiter — without the wait the test passes trivially.
	c := &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
	lim := rate.NewLimiter(rate.Every(50*time.Millisecond), 1)
	_ = lim.AllowN(time.Now(), 1) // burn the burst token
	d := &WalkerDispatcher{Client: c, Limiter: lim}

	start := time.Now()
	err := d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, 30*time.Millisecond,
		"limiter must throttle the dispatch; elapsed=%v", elapsed)
}

func TestWalkerDispatcher_LimiterContextCancelHaltsWalker(t *testing.T) {
	// Pre-drained limiter + canceled ctx. Limiter.Wait returns the
	// cancel error; walker must surface it (not silently dispatch).
	c := &walkerStubClient{outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
	lim := rate.NewLimiter(rate.Every(time.Hour), 1)
	_ = lim.AllowN(time.Now(), 1)
	d := &WalkerDispatcher{Client: c, Limiter: lim}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.Delete(ctx, sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"})
	require.Error(t, err)
	assert.Nil(t, c.lastReq, "no RPC should be sent when ctx was cancelled in Wait")
}

func TestWalkerDispatcher_NilGuardsReturnError(t *testing.T) {
	d := &WalkerDispatcher{Client: &walkerStubClient{}}
	require.Error(t, d.Delete(context.Background(), nil, &bootstrap.Entry{Path: "obj"}))
	require.Error(t, d.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), nil))

	nilClient := &WalkerDispatcher{}
	require.Error(t, nilClient.Delete(context.Background(), sampleAction(t, s3lifecycle.ActionKindExpirationDays), &bootstrap.Entry{Path: "obj"}))
}
