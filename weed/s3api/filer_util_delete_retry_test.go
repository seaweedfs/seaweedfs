package s3api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteRetryClient is a filer whose DeleteEntry fails in a scripted way for
// the first attempts and then behaves. The two scripts are the two ways a
// delete can fail, and they are not interchangeable: callErrs fails the RPC
// itself, which is what a transport blip looks like and what issues #7204 and
// #7224 report, while respErrs returns a successful RPC carrying the filer's
// own message in resp.Error, which is how FilerServer.DeleteEntry reports every
// failure of its own.
type deleteRetryClient struct {
	filer_pb.SeaweedFilerClient

	callErrs []error
	respErrs []string

	attempts int
	requests []*filer_pb.DeleteEntryRequest
}

func (c *deleteRetryClient) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	attempt := c.attempts
	c.attempts++
	c.requests = append(c.requests, req)

	if attempt < len(c.callErrs) && c.callErrs[attempt] != nil {
		return nil, c.callErrs[attempt]
	}
	if attempt < len(c.respErrs) && c.respErrs[attempt] != "" {
		return &filer_pb.DeleteEntryResponse{Error: c.respErrs[attempt]}, nil
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

// alwaysTransientClient fails every DeleteEntry the same way, standing in for a
// filer whose channel is in TRANSIENT_FAILURE.
type alwaysTransientClient struct {
	filer_pb.SeaweedFilerClient

	attempts int
}

func (c *alwaysTransientClient) DeleteEntry(_ context.Context, _ *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	c.attempts++
	return nil, status.Error(codes.Unavailable, "transport is closing")
}

// ctxCapturingClient records each attempt's context deadline, and always fails
// so that every allowed attempt is made.
type ctxCapturingClient struct {
	filer_pb.SeaweedFilerClient

	deadlines   []time.Time
	hadDeadline []bool
}

func (c *ctxCapturingClient) DeleteEntry(ctx context.Context, _ *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	deadline, ok := ctx.Deadline()
	c.hadDeadline = append(c.hadDeadline, ok)
	c.deadlines = append(c.deadlines, deadline)
	return nil, status.Error(codes.Unavailable, "transport is closing")
}

// blockingClient stands in for a pick that finds no ready subconn: the call does
// not fail fast, it waits for the context, which is the case deleteReplayTimeout
// exists to bound.
type blockingClient struct {
	filer_pb.SeaweedFilerClient

	attempts int
}

func (c *blockingClient) DeleteEntry(ctx context.Context, _ *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	c.attempts++
	if c.attempts == 1 {
		// fail fast once so the loop reaches a replay
		return nil, status.Error(codes.Unavailable, "transport is closing")
	}
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

// transientKeys each embed a substring from util's transient-error list, and
// notFoundKey is named after the not-found sentinel. A client can create any of
// them, so none may influence whether a delete is replayed.
var (
	transientKeys = []string{"transport.log", "unavailable.txt", "slowdown.csv", "internalerror.bin", "throttling.json"}
	notFoundKey   = filer_pb.ErrNotFound.Error()
)

// Issue #7204: the bucket delete. A transport blip on the DeleteEntry call used
// to reach DeleteBucketHandler, which answered 500; boto3 then resent the
// DELETE, found the bucket already gone and surfaced NoSuchBucket. The replay
// keeps a blip inside the gateway, so the client sees the 204 it expects.
func TestDoDeleteEntryRecoversFromTransientCall(t *testing.T) {
	client := &deleteRetryClient{
		callErrs: []error{
			status.Error(codes.Unavailable, "transport is closing"),
			status.Error(codes.Unavailable, "transport is closing"),
			nil,
		},
	}

	err := doDeleteEntry(context.Background(), client, "/buckets", "test-bucket", false, true)

	require.NoError(t, err)
	assert.Equal(t, deleteRetryAttempts, client.attempts, "should have used every allowed attempt")
	// the replay must ask for exactly the same delete, recursion flags included
	for _, req := range client.requests {
		assert.Equal(t, "/buckets", req.Directory)
		assert.Equal(t, "test-bucket", req.Name)
		assert.True(t, req.IsRecursive)
		assert.True(t, req.IgnoreRecursiveError)
	}
}

// Issue #7224: the multi-object delete, driven through the wrapper
// DeleteMultipleObjectsHandler uses. The per-key failure used to be reported as
// an InternalError entry inside a 200, which no S3 SDK retries, so the object
// silently stayed.
func TestDeleteUnversionedObjectRecoversFromTransientCall(t *testing.T) {
	client := &deleteRetryClient{
		callErrs: []error{status.Error(codes.ResourceExhausted, "too many requests"), nil},
	}
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}

	err := s3a.deleteUnversionedObjectWithClient(client, "mybucket", "a/b/c.txt", false)

	require.NoError(t, err)
	assert.Equal(t, 2, client.attempts)
	require.Len(t, client.requests, 2)
	assert.Equal(t, "/buckets/mybucket/a/b", client.requests[1].Directory)
	assert.Equal(t, "c.txt", client.requests[1].Name)
}

// A failure the filer reports itself is its considered answer about the tree,
// and it is never replayed. The fixture is the shape production actually
// produces: weed/filer/filer_delete_entry.go formats the deleted path into the
// message, and weed/server/filer_grpc_server.go copies it into resp.Error
// verbatim, so the object's own key sits inside the text being judged.
func TestFilerReportedFailureIsNotReplayed(t *testing.T) {
	for _, key := range transientKeys {
		t.Run(key, func(t *testing.T) {
			client := &deleteRetryClient{
				respErrs: []string{"delete file /buckets/b/logs/" + key + ": permission denied"},
			}

			err := doDeleteEntry(context.Background(), client, "/buckets/b/logs", key, true, false)

			require.Error(t, err)
			assert.Equal(t, 1, client.attempts, "the key name must not make a denial look transient")
		})
	}
}

// The same holds for a recursive delete, where the filer names the child it
// stopped on. That child is a client-chosen key too, so stripping only the
// deleted entry's own path would not have been enough.
func TestFilerReportedRecursiveFailureIsNotReplayed(t *testing.T) {
	client := &deleteRetryClient{
		respErrs: []string{"delete directory /buckets/b: list folder /buckets/b/transport.log: permission denied"},
	}

	err := doDeleteEntry(context.Background(), client, "/buckets", "b", false, true)

	require.Error(t, err)
	assert.Equal(t, 1, client.attempts, "a child key must not make a denial look transient")
}

// The reverse spoof: an object named after the not-found sentinel must not make
// a genuine transport blip look authoritative and suppress the replay. On the
// RPC path this holds because the name is not in the status; it is pinned here
// so a future move back to text matching cannot reintroduce it. The resp.Error
// half of the same spoof is closed by not classifying that text at all.
func TestObjectNamedLikeNotFoundStillReplays(t *testing.T) {
	client := &deleteRetryClient{
		callErrs: []error{status.Error(codes.Unavailable, "connection reset by peer"), nil},
	}

	err := doDeleteEntry(context.Background(), client, "/buckets/b", notFoundKey, true, false)

	require.NoError(t, err)
	assert.Equal(t, 2, client.attempts, "the key name must not suppress a replay")
}

// The replay is immediate, so its cost is a property of the delete and not of
// the request. Both batching handlers delete far more than one entry per
// request, so any per-delete sleep would be paid once per key: the 100ms/200ms
// backoff this replaced would turn the loop below into 30 seconds. The ceiling
// is deliberately far above the real figure (single-digit milliseconds) so a
// loaded runner cannot flake it while a reintroduced backoff still cannot pass.
func TestDoDeleteEntryReplaysWithoutSleeping(t *testing.T) {
	const keys = 100

	client := &alwaysTransientClient{}

	start := time.Now()
	for i := 0; i < keys; i++ {
		require.Error(t, doDeleteEntry(context.Background(), client, "/buckets/b", fmt.Sprintf("k%d", i), true, false))
	}
	elapsed := time.Since(start)

	assert.Equal(t, deleteRetryAttempts*keys, client.attempts, "every key must still get its attempts")
	assert.Less(t, elapsed, 5*time.Second, "the replay must not sleep, so a batch cannot multiply a backoff")
}

// The first attempt keeps the caller's own context, so a recursive delete of a
// large bucket is not suddenly given a deadline it never had. Every replay
// shares one bounded context, because a pick with no ready subconn waits for
// the context rather than failing fast, and three unbounded waits would be
// three times the stall of the one attempt master made.
func TestDoDeleteEntryBoundsOnlyItsReplays(t *testing.T) {
	client := &ctxCapturingClient{}

	err := doDeleteEntry(context.Background(), client, "/buckets/b", "k", true, false)

	require.Error(t, err)
	require.Len(t, client.hadDeadline, deleteRetryAttempts)
	assert.False(t, client.hadDeadline[0], "the first attempt must keep the caller's context untouched")
	for i := 1; i < deleteRetryAttempts; i++ {
		assert.True(t, client.hadDeadline[i], "replay %d must be bounded", i)
	}
	// one budget shared by every replay, not one per replay
	assert.Equal(t, client.deadlines[1], client.deadlines[2], "the replays must share a single bound")
}

// A deadline the caller brought still wins: the replay context derives from it,
// so it cannot extend the caller's own budget.
func TestDoDeleteEntryReplayKeepsACallerDeadline(t *testing.T) {
	client := &ctxCapturingClient{}
	callerDeadline := time.Now().Add(20 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()

	_ = doDeleteEntry(ctx, client, "/buckets/b", "k", true, false)

	require.NotEmpty(t, client.hadDeadline)
	for i, had := range client.hadDeadline {
		assert.True(t, had, "attempt %d must carry the caller's deadline", i)
		assert.False(t, client.deadlines[i].After(callerDeadline),
			"attempt %d must not outlive the caller's own deadline", i)
	}
}

// The bound is what makes the replay escape a pick that never returns. Driven
// with a caller deadline far below deleteReplayTimeout so the test does not
// wait out the real bound.
func TestDoDeleteEntryReplayUnblocksOnTheContext(t *testing.T) {
	client := &blockingClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := doDeleteEntry(ctx, client, "/buckets/b", "k", true, false)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, deleteReplayTimeout, "a blocked replay must end on the context, not run to its own bound")
	assert.LessOrEqual(t, client.attempts, deleteRetryAttempts)
}

// A caller that has gone away stops the replay: the loop checks the context
// before every attempt, so a cancelled request cannot be held open by retries.
func TestDoDeleteEntryStopsWhenTheCallerIsGone(t *testing.T) {
	client := &alwaysTransientClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := doDeleteEntry(ctx, client, "/buckets/b", "k", true, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), context.Canceled.Error())
	assert.Zero(t, client.attempts, "a cancelled caller must not reach the filer at all")
}

// A cancellation that arrives as a gRPC status mid-flight is not replayed
// either: the caller is gone, so a further attempt has nobody to answer.
func TestDoDeleteEntryDoesNotReplayCancellation(t *testing.T) {
	for name, cancelErr := range map[string]error{
		"canceled":          status.Error(codes.Canceled, "context canceled"),
		"deadline exceeded": status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
	} {
		t.Run(name, func(t *testing.T) {
			client := &deleteRetryClient{callErrs: []error{cancelErr, nil}}

			err := doDeleteEntry(context.Background(), client, "/buckets/b", "k", true, false)

			require.Error(t, err)
			assert.Equal(t, 1, client.attempts)
		})
	}
}

// The replay is bounded: a filer whose channel stays down still fails the
// delete, and the error reaches the caller rather than being spun on.
func TestDoDeleteEntryGivesUpAfterBoundedAttempts(t *testing.T) {
	client := &alwaysTransientClient{}

	err := doDeleteEntry(context.Background(), client, "/buckets/b", "k", true, false)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unavailable")
	assert.Equal(t, deleteRetryAttempts, client.attempts)
}

// A code that is not a transport condition is a real failure and must propagate
// on the first attempt.
func TestDoDeleteEntryPropagatesNonTransientError(t *testing.T) {
	for name, deleteErr := range map[string]error{
		"permission denied": status.Error(codes.PermissionDenied, "not allowed"),
		"invalid argument":  status.Error(codes.InvalidArgument, "bad name"),
		"not found":         status.Error(codes.NotFound, filer_pb.ErrNotFound.Error()),
		// a conflict the filer decided on, not a channel that broke
		"aborted": status.Error(codes.Aborted, "conflicting update"),
	} {
		t.Run(name, func(t *testing.T) {
			client := &deleteRetryClient{callErrs: []error{deleteErr, nil}}

			err := doDeleteEntry(context.Background(), client, "/buckets/b", "k", true, false)

			require.Error(t, err)
			assert.Equal(t, 1, client.attempts, "only a transport condition is replayed")
		})
	}
}

// The wrapping the caller sees is unchanged, because deleteObjectEntry decides
// whether to demote a directory marker by matching on this text.
func TestDoDeleteEntryKeepsItsErrorWrapping(t *testing.T) {
	client := &deleteRetryClient{
		respErrs: []string{filer.MsgFailDelNonEmptyFolder + ": /buckets/test/photos"},
	}

	err := doDeleteEntry(context.Background(), client, "/buckets/test", "photos", true, false)

	require.Error(t, err)
	assert.Equal(t, "delete entry /buckets/test/photos: "+filer.MsgFailDelNonEmptyFolder+": /buckets/test/photos", err.Error())
	assert.Equal(t, 1, client.attempts, "a non-empty folder is an answer about the tree, not a hiccup")
}

func TestIsRetryableDeleteRPCError(t *testing.T) {
	assert.False(t, isRetryableDeleteRPCError(nil))
	assert.True(t, isRetryableDeleteRPCError(status.Error(codes.Unavailable, "transport is closing")))
	assert.True(t, isRetryableDeleteRPCError(status.Error(codes.ResourceExhausted, "too many requests")))
	// a server ending the stream without trailers, which is how a truncated
	// reply through an L7 gRPC proxy arrives
	assert.True(t, isRetryableDeleteRPCError(status.Error(codes.Internal, "server closed the stream without sending trailers")))
	// a conflict the filer decided on: replaying would only lose the race again
	assert.False(t, isRetryableDeleteRPCError(status.Error(codes.Aborted, "conflicting update")))
	assert.False(t, isRetryableDeleteRPCError(status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())))
	assert.False(t, isRetryableDeleteRPCError(status.Error(codes.PermissionDenied, "not allowed")))
	assert.False(t, isRetryableDeleteRPCError(status.Error(codes.Canceled, "context canceled")))
	assert.False(t, isRetryableDeleteRPCError(status.Error(codes.DeadlineExceeded, "context deadline exceeded")))
	assert.False(t, isRetryableDeleteRPCError(context.Canceled))
	// the text is never consulted, so a message that reads transient is not one
	assert.False(t, isRetryableDeleteRPCError(fmt.Errorf("connection reset by peer")))
	assert.False(t, isRetryableDeleteRPCError(status.Error(codes.PermissionDenied, "transport is closing")))
}
