package s3api

import (
	"context"
	"fmt"
	"math"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func ambiguousRouteServer(t *testing.T, addrs ...pb.ServerAddress) *S3ApiServer {
	t.Helper()
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	return &S3ApiServer{
		option:      &S3ApiServerOption{GrpcDialOption: dialOption, Filers: addrs},
		filerClient: wdclient.NewFilerClient(addrs, dialOption, ""),
	}
}

func TestCreateAfterAmbiguousRoute(t *testing.T) {
	uploaded := []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}
	filePath := "/buckets/b/o"
	chunksAlive := func(context.Context, []*filer_pb.FileChunk) bool { return true }
	chunksDead := func(context.Context, []*filer_pb.FileChunk) bool { return false }
	newS3a := func(t *testing.T, f *fakeLookupFiler) (*S3ApiServer, pb.ServerAddress) {
		addr := startFakeFiler(t, f)
		return ambiguousRouteServer(t, addr), addr
	}

	t.Run("stored entry is this PUT's — route landed", func(t *testing.T) {
		f := &fakeLookupFiler{entry: &filer_pb.Entry{Name: "o", Chunks: []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}}}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrNone || !entryCreated {
			t.Fatalf("code=%v entryCreated=%v", code, entryCreated)
		}
		if ran {
			t.Fatal("re-committed an entry the route already stored")
		}
	})

	t.Run("stored entry is a newer write — refuse stale re-commit", func(t *testing.T) {
		f := &fakeLookupFiler{entry: &filer_pb.Entry{Name: "o", Chunks: []*filer_pb.FileChunk{{FileId: "5,01637037d7", Size: 55}}}}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrServiceUnavailable {
			t.Fatalf("code = %v, want ServiceUnavailable", code)
		}
		if ran || entryCreated {
			t.Fatal("stale entry was committed over a newer write")
		}
	})

	t.Run("stored entry is a directory, chunks alive — lock path answers", func(t *testing.T) {
		f := &fakeLookupFiler{entry: &filer_pb.Entry{Name: "o", IsDirectory: true}}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrExistingObjectIsDirectory })
		if code != s3err.ErrExistingObjectIsDirectory || !ran {
			t.Fatalf("code=%v ran=%v — a directory conflict with live chunks must reach createUnderLock, which maps it", code, ran)
		}
	})

	// The routed PUT may have committed before a delete removed the entry and
	// its chunks and a nested write recreated the name as a directory; dead
	// chunks mean re-committing would store an entry pointing at them.
	t.Run("stored entry is a directory, chunks unverifiable — refuse", func(t *testing.T) {
		f := &fakeLookupFiler{entry: &filer_pb.Entry{Name: "o", IsDirectory: true}}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksDead, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrServiceUnavailable || ran {
			t.Fatalf("code=%v ran=%v — re-committed over a directory with possibly-deleted chunks", code, ran)
		}
	})

	t.Run("match behind an unanswered filer — refuse", func(t *testing.T) {
		deadAddr := startFakeFiler(t, &fakeLookupFiler{lookupErr: context.DeadlineExceeded})
		matchAddr := startFakeFiler(t, &fakeLookupFiler{entry: &filer_pb.Entry{Name: "o", Chunks: []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}}})
		s3a := ambiguousRouteServer(t, deadAddr, matchAddr)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", deadAddr, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrServiceUnavailable || entryCreated || ran {
			t.Fatalf("code=%v entryCreated=%v ran=%v — a match behind an unresolved lookup must not finalize", code, entryCreated, ran)
		}
	})

	t.Run("entry proven absent, chunks alive — normal create proceeds", func(t *testing.T) {
		f := &fakeLookupFiler{}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrNone || !ran {
			t.Fatalf("code=%v ran=%v — proven-absent entry did not reach createUnderLock", code, ran)
		}
	})

	t.Run("entry absent but chunks unverifiable — refuse", func(t *testing.T) {
		f := &fakeLookupFiler{}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksDead, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrServiceUnavailable || ran {
			t.Fatalf("code=%v ran=%v — re-committed an entry over possibly-deleted chunks", code, ran)
		}
	})

	t.Run("lookup cannot resolve state — refuse", func(t *testing.T) {
		f := &fakeLookupFiler{lookupErr: context.DeadlineExceeded}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, nil, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrServiceUnavailable || ran {
			t.Fatalf("code=%v ran=%v — uncertain state still committed", code, ran)
		}
	})

	t.Run("finalize fails — recovered entry rolls back", func(t *testing.T) {
		f := &fakeLookupFiler{entry: &filer_pb.Entry{Name: "o", Chunks: []*filer_pb.FileChunk{{FileId: "5,01637037d6", Size: 55}}}}
		s3a, owner := newS3a(t, f)
		entryCreated := false
		ran := false
		finalize := &putFinalize{afterCreate: func(*filer_pb.Entry) s3err.ErrorCode { return s3err.ErrInternalError }}
		code := s3a.createAfterAmbiguousRoute(filePath, "b", "o", owner, &filer_pb.Entry{Name: "o"}, uploaded, finalize, &entryCreated, chunksAlive, func() s3err.ErrorCode { ran = true; return s3err.ErrNone })
		if code != s3err.ErrInternalError || ran {
			t.Fatalf("code=%v ran=%v", code, ran)
		}
		if len(f.deleted) != 1 || f.deleted[0] != filePath {
			t.Fatalf("recovered entry was not rolled back: deleted=%v", f.deleted)
		}
	})
}

type fakeVersionedDirFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries map[string]*filer_pb.Entry
	updates map[string]*filer_pb.Entry
}

func (f *fakeVersionedDirFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if e, ok := f.entries[path.Join(req.Directory, req.Name)]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (f *fakeVersionedDirFiler) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	f.updates[path.Join(req.Directory, req.Entry.Name)] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

// A late versioned write keeps the newer latest pointer, but the version it
// just stored is born noncurrent — without a NoncurrentSinceNs stamp the
// lifecycle engine never ages it out.
func TestUpdateLatestVersionInDirectoryBornNoncurrent(t *testing.T) {
	now := time.Now().UnixNano()
	newerId := fmt.Sprintf("%016x%s", math.MaxInt64-now, "1111111111111111")
	olderId := fmt.Sprintf("%016x%s", math.MaxInt64-(now-int64(time.Hour)), "2222222222222222")

	bucketDir := "/buckets/b"
	versionsPath := path.Join(bucketDir, "o"+s3_constants.VersionsFolder)
	f := &fakeVersionedDirFiler{
		entries: map[string]*filer_pb.Entry{
			versionsPath: {
				Name:        "o" + s3_constants.VersionsFolder,
				IsDirectory: true,
				Extended: map[string][]byte{
					s3_constants.ExtLatestVersionIdKey:       []byte(newerId),
					s3_constants.ExtLatestVersionFileNameKey: []byte("newer.v"),
				},
			},
			path.Join(versionsPath, "late.v"): {Name: "late.v"},
		},
		updates: map[string]*filer_pb.Entry{},
	}
	s3a := ambiguousRouteServer(t, startFakeFiler(t, f))
	s3a.option.BucketsPath = "/buckets"

	err := s3a.updateLatestVersionInDirectory("b", "o", olderId, "late.v", &filer_pb.Entry{})
	if err != nil {
		t.Fatalf("updateLatestVersionInDirectory: %v", err)
	}
	stamped := f.updates[path.Join(versionsPath, "late.v")]
	if stamped == nil {
		t.Fatal("late version never got its noncurrent stamp")
	}
	if stamped.Extended[s3_constants.ExtNoncurrentSinceNsKey] == nil {
		t.Fatal("stamp did not set ExtNoncurrentSinceNsKey")
	}
	if _, pointerTouched := f.updates[versionsPath]; pointerTouched {
		t.Fatal("the newer latest pointer must not move")
	}
}
