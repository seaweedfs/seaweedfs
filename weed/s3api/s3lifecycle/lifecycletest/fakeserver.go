// Package lifecycletest provides reusable test doubles for the lifecycle
// worker pipeline. The pieces here let component-level tests stand up the
// gRPC boundary the worker dials at runtime without pulling in a real
// S3ApiServer or filer.
package lifecycletest

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
)

// Outcome is what the fake returns for a single LifecycleDelete call.
// Code is required; Reason is echoed verbatim into LifecycleDeleteResponse.
type Outcome struct {
	Code   s3_lifecycle_pb.LifecycleDeleteOutcome
	Reason string
}

// Done, NoopResolved, RetryLater, Blocked, SkippedObjectLock are
// constructors for the common outcomes; tests can also build Outcome
// values directly when they need a specific Reason.
func Done() Outcome {
	return Outcome{Code: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}
}
func NoopResolved(reason string) Outcome {
	return Outcome{Code: s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED, Reason: reason}
}
func RetryLater(reason string) Outcome {
	return Outcome{Code: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER, Reason: reason}
}
func Blocked(reason string) Outcome {
	return Outcome{Code: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED, Reason: reason}
}
func SkippedObjectLock(reason string) Outcome {
	return Outcome{Code: s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK, Reason: reason}
}

// FakeLifecycleServer implements s3_lifecycle_pb.SeaweedS3LifecycleInternalServer.
// It returns per-key queued outcomes (FIFO) and falls back to Default when a
// key has no queued entry. Every received request is recorded; tests assert
// against Recorded() in arrival order.
//
// A non-nil Err short-circuits everything — LifecycleDelete returns (nil, Err)
// immediately, before the per-key lookup or the request recording. Use it to
// simulate transport failures.
//
// All methods are safe for concurrent use. Outcomes/Default may be set at
// construction or via Queue/SetDefault between calls; mid-call mutation is
// supported but ordering across that boundary is undefined.
type FakeLifecycleServer struct {
	s3_lifecycle_pb.UnimplementedSeaweedS3LifecycleInternalServer

	mu       sync.Mutex
	queues   map[requestKey][]Outcome
	def      Outcome
	err      error
	received []*s3_lifecycle_pb.LifecycleDeleteRequest
}

// requestKey is the map key for queues. A struct rather than a delimited
// string so bucket/object/versionId values containing "/" or "@" can't
// collide.
type requestKey struct {
	bucket, objectPath, versionId string
}

// NewFakeLifecycleServer returns a server whose Default outcome is DONE.
// Most tests want a different default; call SetDefault to change it.
func NewFakeLifecycleServer() *FakeLifecycleServer {
	return &FakeLifecycleServer{
		queues: map[requestKey][]Outcome{},
		def:    Done(),
	}
}

// Queue appends an outcome to the FIFO for (bucket, objectPath, versionId).
// Subsequent calls matching the same key return outcomes in the order they
// were queued; once the queue is drained, Default applies.
func (f *FakeLifecycleServer) Queue(bucket, objectPath, versionId string, outcome Outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(bucket, objectPath, versionId)
	f.queues[k] = append(f.queues[k], outcome)
}

// SetDefault sets the outcome returned when no per-key queue entry remains.
func (f *FakeLifecycleServer) SetDefault(o Outcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.def = o
}

// SetError makes LifecycleDelete return (nil, err) on every call until
// SetError(nil) clears it. The request is not recorded while Err is set —
// transport-error tests should rely on the worker's own bookkeeping.
func (f *FakeLifecycleServer) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// Recorded returns a snapshot copy of every request the server has received
// (excluding calls that returned a transport error). Caller may mutate the
// returned slice freely.
func (f *FakeLifecycleServer) Recorded() []*s3_lifecycle_pb.LifecycleDeleteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*s3_lifecycle_pb.LifecycleDeleteRequest, len(f.received))
	copy(out, f.received)
	return out
}

// LifecycleDelete is the gRPC handler. It honors Err first, then dequeues
// the per-key outcome, falling back to Default.
func (f *FakeLifecycleServer) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	f.mu.Lock()
	if f.err != nil {
		err := f.err
		f.mu.Unlock()
		return nil, err
	}
	f.received = append(f.received, req)
	if req == nil {
		out := f.def
		f.mu.Unlock()
		return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: out.Code, Reason: out.Reason}, nil
	}
	k := key(req.Bucket, req.ObjectPath, req.VersionId)
	q := f.queues[k]
	var out Outcome
	if len(q) > 0 {
		out = q[0]
		f.queues[k] = q[1:]
	} else {
		out = f.def
	}
	f.mu.Unlock()
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: out.Code, Reason: out.Reason}, nil
}

func key(bucket, objectPath, versionId string) requestKey {
	return requestKey{bucket: bucket, objectPath: objectPath, versionId: versionId}
}
