package s3api

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
