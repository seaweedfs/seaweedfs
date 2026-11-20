package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fallbackRaceFiler holds the first create of key until the test releases it.
type fallbackRaceFiler struct {
	*ambiguousPutFiler
	key string

	mu            sync.Mutex
	creates       int
	firstAtCreate chan struct{}
	releaseFirst  chan struct{}
}

func (f *fallbackRaceFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if req.Directory+"/"+req.Entry.GetName() == f.key {
		f.mu.Lock()
		f.creates++
		first := f.creates == 1
		f.mu.Unlock()
		if first {
			close(f.firstAtCreate)
			select {
			case <-f.releaseFirst:
			case <-time.After(10 * time.Second):
			}
		}
	}
	return f.ambiguousPutFiler.CreateEntry(ctx, req)
}

func (f *fallbackRaceFiler) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates
}

// expiredLeaseLock grants every request, as the object write lock does once the
// previous holder's lease ran out.
type expiredLeaseLock struct{}

func (expiredLeaseLock) StopShortLivedLock() error { return nil }

// Two "If-None-Match: *" PUTs of one key on the lock fallback path (no ring):
// writer 1 passes its gateway check and is held before its create while
// writer 2 runs to completion under a lock whose lease has run out. Only the
// write condition on the create lets the filer refuse the second create.
func TestPutFallbackKeepsWriteCondition(t *testing.T) {
	const objectPath = "/buckets/b/k"

	volume := startFakeVolumeServer(t)
	inner := &ambiguousPutFiler{
		volume: volume,
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
		},
		apply: true,
	}
	filerImpl := &fallbackRaceFiler{
		ambiguousPutFiler: inner,
		key:               objectPath,
		firstAtCreate:     make(chan struct{}),
		releaseFirst:      make(chan struct{}),
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	var lockMu sync.Mutex
	lockGrants := 0
	s3a.newObjectWriteLock = func(bucket, object string) (objectWriteLock, error) {
		lockMu.Lock()
		lockGrants++
		lockMu.Unlock()
		return expiredLeaseLock{}, nil
	}
	if s3a.routableWriteOwner("b", "k") != "" {
		t.Fatal("the gateway has a ring: the PUTs would be routed, not take the fallback path")
	}

	put := func(body string) s3err.ErrorCode {
		r := httptest.NewRequest(http.MethodPut, "/b/k", nil)
		r.Header.Set(s3_constants.IfNoneMatch, "*")
		_, code, _ := s3a.putToFiler(r, objectPath, strings.NewReader(body), "b", "k", 1, 0, nil, false, "")
		return code
	}

	var code1 s3err.ErrorCode
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		code1 = put("writer one")
	}()
	select {
	case <-filerImpl.firstAtCreate:
	case <-done1:
		t.Fatalf("writer 1 finished (%v) without its create reaching the filer: the race was not exercised", code1)
	case <-time.After(10 * time.Second):
		close(filerImpl.releaseFirst)
		t.Fatal("writer 1 never reached its create: the race was not exercised")
	}

	code2 := put("writer two")
	if n := filerImpl.createCount(); n != 2 {
		close(filerImpl.releaseFirst)
		<-done1
		t.Fatalf("writer 2 made %d creates of the key while writer 1 was paused, want its own create (total 2): the race was not exercised", n)
	}
	close(filerImpl.releaseFirst)
	<-done1

	lockMu.Lock()
	grants := lockGrants
	lockMu.Unlock()
	if grants < 2 {
		t.Fatalf("the object write lock was taken %d times, want one per PUT at least: the PUTs did not run the lock fallback", grants)
	}

	inner.mu.Lock()
	conds := append([]*filer_pb.WriteCondition(nil), inner.conds...)
	inner.mu.Unlock()
	t.Logf("writer 1: %v, writer 2: %v; conditions on the creates the filer received: %v", code1, code2, conds)

	if code1 != s3err.ErrPreconditionFailed || code2 != s3err.ErrNone {
		t.Fatalf("writer 1 got %v and writer 2 got %v, want ErrPreconditionFailed and ErrNone", code1, code2)
	}
	inner.mu.Lock()
	winner := inner.entries[objectPath]
	inner.mu.Unlock()
	if len(winner.GetChunks()) != 1 {
		t.Fatalf("stored entry %+v, want writer 2's one chunk", winner)
	}
	deleted := volume.deleted()
	if len(deleted) != 1 || sameFileChunks([]*filer_pb.FileChunk{{FileId: deleted[0]}}, winner.GetChunks()) {
		t.Fatalf("deleted chunks %v, want only the refused writer's one chunk, not the stored %s", deleted, winner.GetChunks()[0].GetFileIdString())
	}
}

// Without a ring the filer forwards a conditional create to its owner, so a
// lost response can hide a commit the gateway's filers do not hold yet.
func TestPutFallbackLostConditionalCreateKeepsChunks(t *testing.T) {
	volume := startFakeVolumeServer(t)
	filerImpl := &ambiguousPutFiler{
		volume: volume,
		entries: map[string]*filer_pb.Entry{
			"/buckets/b": {Name: "b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
		},
		createErr: status.Error(codes.Unavailable, "forward to owner: connection reset"),
	}
	s3a := newPutTestServer(t, startFakeFiler(t, filerImpl))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)

	r := httptest.NewRequest(http.MethodPut, "/b/k", nil)
	r.Header.Set(s3_constants.IfNoneMatch, "*")
	if _, code, _ := s3a.putToFiler(r, "/buckets/b/k", strings.NewReader("hello"), "b", "k", 1, 0, nil, false, ""); code == s3err.ErrNone {
		t.Fatal("putToFiler succeeded although the create's outcome is unknown")
	}
	if deleted := volume.deleted(); len(deleted) != 0 {
		t.Fatalf("chunks of a create that may have committed were deleted: %v", deleted)
	}
}
