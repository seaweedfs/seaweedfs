package filer

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"

	"google.golang.org/protobuf/proto"
)

// staleTrackingInvalidator records invalidation calls and lets the test
// observe whether fetchWholeChunk triggered a cache drop after a fetch failure.
type staleTrackingInvalidator struct {
	invalidations atomic.Int32
}

func (inv *staleTrackingInvalidator) InvalidateCache(fileId string) {
	inv.invalidations.Add(1)
}

// twoStageLookupFn returns staleUrls on the first call and freshUrls on every
// subsequent call. Mirrors what a vidMapClient does after InvalidateCache: the
// next LookupVolumeIdsWithFallback queries master and returns current locations.
type twoStageLookupFn struct {
	staleUrls []string
	freshUrls []string
	calls     atomic.Int32
}

func (l *twoStageLookupFn) lookup(ctx context.Context, fileId string) ([]string, error) {
	n := l.calls.Add(1)
	if n == 1 {
		return l.staleUrls, nil
	}
	return l.freshUrls, nil
}

// TestFetchWholeChunkSelfHealOnStaleLocation verifies that when the first
// retriedStreamFetchChunkData attempt fails (stale volume location returning
// HTTP 500), fetchWholeChunk invalidates the cache, re-looks up, and retries
// against the fresh location. Without this, mount reads of large multipart
// files whose manifest chunk's volume has moved would permanently fail.
func TestFetchWholeChunkSelfHealOnStaleLocation(t *testing.T) {
	// Build a valid FileChunkManifest protobuf for the fresh response.
	manifest := &filer_pb.FileChunkManifest{
		Chunks: []*filer_pb.FileChunk{
			{FileId: "100,abc", Offset: 0, Size: 8},
			{FileId: "101,def", Offset: 8, Size: 8},
		},
	}
	manifestBytes, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	// Stale endpoint: 500 with a non-empty body so the streaming writer may
	// have already appended some bytes to bytesBuffer before the error status
	// was processed. This proves the bytesBuffer.Reset() before retry is needed.
	staleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("garbage-from-stale-server"))
	}))
	defer staleSrv.Close()

	// Fresh endpoint: 200 with valid gzipped (since isFullChunk=true triggers
	// gzip accept) or plain protobuf bytes. We use plain bytes because the
	// stream reader only applies gzip when the response says so.
	freshSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifestBytes)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(manifestBytes)
	}))
	defer freshSrv.Close()

	lookup := &twoStageLookupFn{
		staleUrls: []string{staleSrv.URL + "/5,stale"},
		freshUrls: []string{freshSrv.URL + "/5,stale"},
	}
	inv := &staleTrackingInvalidator{}

	bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
	bytesBuffer.Reset()
	defer bytesBufferPool.Put(bytesBuffer)

	err = fetchWholeChunk(context.Background(), bytesBuffer, lookup.lookup, "5,stale", nil, false, inv)
	if err != nil {
		t.Fatalf("fetchWholeChunk returned error after self-heal: %v", err)
	}

	if got := inv.invalidations.Load(); got != 1 {
		t.Errorf("expected exactly 1 InvalidateCache call, got %d", got)
	}
	if got := lookup.calls.Load(); got != 2 {
		t.Errorf("expected exactly 2 lookup calls (initial + re-lookup), got %d", got)
	}

	// Buffer must contain only the fresh manifest bytes, never the stale
	// "garbage-from-stale-server" prefix that the first attempt may have
	// streamed before returning 500. If bytesBuffer.Reset() were skipped, the
	// prefix would survive and proto.Unmarshal would fail.
	got := bytesBuffer.Bytes()
	if bytes.Contains(got, []byte("garbage-from-stale-server")) {
		t.Errorf("bytesBuffer still contains stale prefix; Reset() before retry is missing")
	}

	decoded := &filer_pb.FileChunkManifest{}
	if err := proto.Unmarshal(got, decoded); err != nil {
		t.Fatalf("proto.Unmarshal of fetched buffer: %v", err)
	}
	if len(decoded.Chunks) != 2 {
		t.Errorf("expected 2 manifest chunks, got %d", len(decoded.Chunks))
	}
	if decoded.Chunks[0].FileId != "100,abc" || decoded.Chunks[1].FileId != "101,def" {
		t.Errorf("unexpected manifest chunks: %+v", decoded.Chunks)
	}
}

// TestFetchWholeChunkNoInvalidatorSkipsRetry verifies that with a nil
// invalidator, fetchWholeChunk returns the original fetch error without
// retrying - preserving the existing semantics for non-mount callers that
// don't participate in cache invalidation.
func TestFetchWholeChunkNoInvalidatorSkipsRetry(t *testing.T) {
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	lookup := &twoStageLookupFn{
		staleUrls: []string{failSrv.URL + "/5,abc"},
		freshUrls: []string{"http://unused:8080/5,abc"},
	}

	bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
	bytesBuffer.Reset()
	defer bytesBufferPool.Put(bytesBuffer)

	err := fetchWholeChunk(context.Background(), bytesBuffer, lookup.lookup, "5,abc", nil, false, nil)
	if err == nil {
		t.Fatal("expected fetchWholeChunk to return the original error when invalidator is nil")
	}
	if got := lookup.calls.Load(); got != 1 {
		t.Errorf("expected only the initial lookup, got %d calls", got)
	}
}

// TestFetchWholeChunkSameUrlsSkipsRetry verifies that when invalidation leads
// to a re-lookup that returns the same stale URLs, fetchWholeChunk does not
// retry (avoiding infinite retry loops against the same broken servers).
func TestFetchWholeChunkSameUrlsSkipsRetry(t *testing.T) {
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	// Both stale and fresh lookups return the same URL (the stale endpoint).
	// This is what happens when the cache had the wrong info AND the master
	// still returns the same wrong info - we must not loop.
	lookup := &twoStageLookupFn{
		staleUrls: []string{failSrv.URL + "/5,abc"},
		freshUrls: []string{failSrv.URL + "/5,abc"},
	}
	inv := &staleTrackingInvalidator{}

	bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
	bytesBuffer.Reset()
	defer bytesBufferPool.Put(bytesBuffer)

	err := fetchWholeChunk(context.Background(), bytesBuffer, lookup.lookup, "5,abc", nil, false, inv)
	if err == nil {
		t.Fatal("expected fetchWholeChunk to return error when locations unchanged after re-lookup")
	}
	if got := lookup.calls.Load(); got != 2 {
		t.Errorf("expected initial + re-lookup (2 calls), got %d", got)
	}
	if got := inv.invalidations.Load(); got != 1 {
		t.Errorf("expected exactly 1 InvalidateCache call, got %d", got)
	}
}