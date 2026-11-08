package s3api

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteLookupFiler answers the lookups and deletes deleteSpecificObjectVersion
// makes. lookup decides each lookup's outcome from the path and how many times
// that path has been looked up before.
type deleteLookupFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu        sync.Mutex
	lookup    func(path string, nth int) (*filer_pb.Entry, error)
	lookups   map[string]int
	deleteErr error
	// deleteResp is the error text a filer reports in the response, as it
	// does for an entry that is already gone.
	deleteResp string
	deletes    []string
}

func (f *deleteLookupFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := req.Directory + "/" + req.Name
	nth := f.lookups[path]
	f.lookups[path] = nth + 1
	entry, err := f.lookup(path, nth)
	if err != nil {
		return nil, err
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (f *deleteLookupFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, req.Directory+"/"+req.Name)
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &filer_pb.DeleteEntryResponse{Error: f.deleteResp}, nil
}

var (
	errLookupNotFound = status.Error(codes.NotFound, "no entry is found in filer store")
	// Not retried by retryFilerOp, so a failing delete returns at once.
	errDeleteTimedOut = status.Error(codes.DeadlineExceeded, "delete timed out")
	errStoreFailed    = status.Error(codes.Internal, "filer store read failed")
)

func TestDeleteSpecificObjectVersionRetriesTransientLookup(t *testing.T) {
	for _, versionId := range []string{"null", "672a75d526ef29c79fe6b5680cad4a0d"} {
		t.Run(versionId, func(t *testing.T) {
			fake := &deleteLookupFiler{
				lookups: map[string]int{},
				lookup: func(_ string, nth int) (*filer_pb.Entry, error) {
					if nth == 0 {
						return nil, errStoreFailed
					}
					return &filer_pb.Entry{Name: "o"}, nil
				},
			}
			s3a := newPutTestServer(t, startFakeFiler(t, fake))

			if err := s3a.deleteSpecificObjectVersion(context.Background(), "b", "o", versionId, false); err != nil {
				t.Fatalf("delete failed: %v", err)
			}
			if len(fake.deletes) != 1 {
				t.Fatalf("got %d delete calls, want 1", len(fake.deletes))
			}
			for path, n := range fake.lookups {
				if n != 2 {
					t.Errorf("%s looked up %d times, want 2", path, n)
				}
			}
		})
	}
}

// rm has already spent its retries by the time the recheck runs, so the
// recheck reads once instead of stacking a second retry loop on top.
func TestDeleteSpecificObjectVersionRecheckDoesNotRetry(t *testing.T) {
	fake := &deleteLookupFiler{
		lookups:   map[string]int{},
		deleteErr: errDeleteTimedOut,
		lookup: func(_ string, nth int) (*filer_pb.Entry, error) {
			if nth == 0 {
				return &filer_pb.Entry{Name: "o"}, nil
			}
			return nil, errStoreFailed
		},
	}
	s3a := newPutTestServer(t, startFakeFiler(t, fake))

	if err := s3a.deleteSpecificObjectVersion(context.Background(), "b", "o", "null", false); err == nil {
		t.Fatal("delete reported success")
	}
	if got := fake.lookups["/buckets/b/o"]; got != 2 {
		t.Fatalf("got %d lookups, want 2 (pre-check and one recheck)", got)
	}
}

func TestDeleteSpecificObjectVersionLookupErrors(t *testing.T) {
	const (
		versionId   = "672a75d526ef29c79fe6b5680cad4a0d"
		nullPath    = "/buckets/b/o"
		versionsDir = "/buckets/b/o" + s3_constants.VersionsFolder
	)
	versionPath := versionsDir + "/v_" + versionId
	object := &filer_pb.Entry{Name: "o"}

	cases := []struct {
		name        string
		versionId   string
		lookup      func(path string, nth int) (*filer_pb.Entry, error)
		deleteErr   error
		deleteResp  string
		wantErr     bool
		wantDeletes int
		// wantErrText lists messages the returned error has to carry.
		wantErrText []string
	}{
		{
			name:      "null version lookup fails",
			versionId: "null",
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return nil, errStoreFailed
			},
			wantErr: true,
		},
		{
			name:      "null version is absent",
			versionId: "null",
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return nil, errLookupNotFound
			},
		},
		{
			name:      "null version is deleted",
			versionId: "null",
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return object, nil
			},
			wantDeletes: 1,
		},
		{
			name:      "null version delete fails and the recheck fails",
			versionId: "null",
			lookup: func(_ string, nth int) (*filer_pb.Entry, error) {
				if nth == 0 {
					return object, nil
				}
				return nil, errStoreFailed
			},
			deleteErr:   errDeleteTimedOut,
			wantErr:     true,
			wantDeletes: 1,
			wantErrText: []string{"delete timed out", "filer store read failed"},
		},
		{
			name:      "null version delete fails and the recheck finds it gone",
			versionId: "null",
			lookup: func(_ string, nth int) (*filer_pb.Entry, error) {
				if nth == 0 {
					return object, nil
				}
				return nil, errLookupNotFound
			},
			deleteErr:   errDeleteTimedOut,
			wantDeletes: 1,
		},
		{
			name:      "null version delete fails and the recheck finds it still there",
			versionId: "null",
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return object, nil
			},
			deleteErr:   errDeleteTimedOut,
			wantErr:     true,
			wantDeletes: 1,
		},
		{
			name:      "versions directory lookup fails",
			versionId: versionId,
			lookup: func(path string, _ int) (*filer_pb.Entry, error) {
				if path == versionsDir {
					return nil, errStoreFailed
				}
				return &filer_pb.Entry{Name: "v_" + versionId}, nil
			},
			wantErr: true,
		},
		{
			name:      "versions directory is absent",
			versionId: versionId,
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return nil, errLookupNotFound
			},
			wantDeletes: 1,
		},
		{
			name:      "version delete fails and the recheck fails",
			versionId: versionId,
			lookup: func(path string, _ int) (*filer_pb.Entry, error) {
				if path == versionPath {
					return nil, errStoreFailed
				}
				return nil, errLookupNotFound
			},
			deleteErr:   errDeleteTimedOut,
			wantErr:     true,
			wantDeletes: 1,
			wantErrText: []string{"delete timed out", "filer store read failed"},
		},
		{
			name:      "version is already gone",
			versionId: versionId,
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return nil, errLookupNotFound
			},
			deleteResp:  "filer: no entry is found in filer store",
			wantDeletes: 1,
		},
		{
			name:      "version delete fails and the recheck finds it gone",
			versionId: versionId,
			lookup: func(string, int) (*filer_pb.Entry, error) {
				return nil, errLookupNotFound
			},
			deleteErr:   errDeleteTimedOut,
			wantDeletes: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &deleteLookupFiler{lookup: c.lookup, lookups: map[string]int{}, deleteErr: c.deleteErr, deleteResp: c.deleteResp}
			s3a := newPutTestServer(t, startFakeFiler(t, fake))

			// No retry allowance: a failing lookup gives up after one attempt.
			ctx := withFilerRetryBudget(context.Background(), 0)
			err := s3a.deleteSpecificObjectVersion(ctx, "b", "o", c.versionId, false)
			if c.wantErr && err == nil {
				t.Fatal("delete reported success")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("delete failed: %v", err)
			}
			for _, want := range c.wantErrText {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			if len(fake.deletes) != c.wantDeletes {
				t.Fatalf("got %d delete calls %v, want %d", len(fake.deletes), fake.deletes, c.wantDeletes)
			}
		})
	}
}
