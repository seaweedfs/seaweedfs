package s3api

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
)

// readOnlyAssignFiler answers AssignVolume the way a filer does for a path
// under a read-only rule (e.g. an over-quota bucket), and counts the calls.
type readOnlyAssignFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	calls int32
}

func (f *readOnlyAssignFiler) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	atomic.AddInt32(&f.calls, 1)
	return &filer_pb.AssignVolumeResponse{
		Error:     "assign volume: read only: /buckets/b (e.g. bucket over quota)",
		ErrorCode: filer_pb.FilerError_READ_ONLY,
	}, nil
}

// LookupDirectoryEntry finds nothing, so the bucket has no config to apply.
func (f *readOnlyAssignFiler) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{}, nil
}

// startReadOnlyFilers starts a read-only filer followed by a spare one, so a
// test can assert the verdict was not treated as a reason to fail over.
func startReadOnlyFilers(t *testing.T) (*S3ApiServer, *readOnlyAssignFiler, *readOnlyAssignFiler) {
	t.Helper()
	first, spare := &readOnlyAssignFiler{}, &readOnlyAssignFiler{}
	s3a := newPutTestServer(t, startFakeFiler(t, first), startFakeFiler(t, spare))
	return s3a, first, spare
}

func assertNoFailover(t *testing.T, first, spare *readOnlyAssignFiler) {
	t.Helper()
	if calls := atomic.LoadInt32(&first.calls); calls == 0 {
		t.Fatal("the first filer was never asked")
	}
	if calls := atomic.LoadInt32(&spare.calls); calls != 0 {
		t.Fatalf("read-only verdict failed over to the next filer %d time(s)", calls)
	}
}

// PutObject into an over-quota bucket is 403, not a retryable 500.
func TestPutToFilerReadOnlyBucketIsAccessDenied(t *testing.T) {
	s3a, first, spare := startReadOnlyFilers(t)

	if _, code := putTestObject(t, s3a); code != s3err.ErrAccessDenied {
		t.Fatalf("putToFiler = %v, want %v", code, s3err.ErrAccessDenied)
	}
	assertNoFailover(t, first, spare)
}

func TestAssignNewVolumeReadOnlyKeepsSentinel(t *testing.T) {
	s3a, first, spare := startReadOnlyFilers(t)

	_, err := s3a.assignNewVolume("/buckets/b/o", 1)
	if !errors.Is(err, weed_server.ErrReadOnly) {
		t.Fatalf("assignNewVolume err = %v, want ErrReadOnly", err)
	}
	assertNoFailover(t, first, spare)
}

// The copy paths fan chunks out to workers; the sentinel must survive the
// per-chunk wrapping so CopyObject and UploadPartCopy report 403 too.
func TestCopyChunksReadOnlyIsAccessDenied(t *testing.T) {
	s3a, _, _ := startReadOnlyFilers(t)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{FileSize: 8},
		Chunks: []*filer_pb.FileChunk{
			{FileId: "3,01637037d6", Offset: 0, Size: 4},
			{FileId: "3,02637037d6", Offset: 4, Size: 4},
		},
	}

	_, err := s3a.copyChunks(entry, "/buckets/b/dst")
	if code := s3a.mapCopyErrorToS3Error(err); code != s3err.ErrAccessDenied {
		t.Fatalf("copyChunks err %v maps to %v, want %v", err, code, s3err.ErrAccessDenied)
	}

	_, err = s3a.copyChunksForRange(entry, 0, 7, "/buckets/b/dst")
	if code := s3a.mapCopyErrorToS3Error(err); code != s3err.ErrAccessDenied {
		t.Fatalf("copyChunksForRange err %v maps to %v, want %v", err, code, s3err.ErrAccessDenied)
	}
}
