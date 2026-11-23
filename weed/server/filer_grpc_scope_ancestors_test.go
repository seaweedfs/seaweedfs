package weed_server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func scopedCreate(t *testing.T, fs *FilerServer, scope *filerGrpcScope, grant string, req *filer_pb.CreateEntryRequest) error {
	t.Helper()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+scopedToken("ancestors", time.Hour, []string{grant}, nil)))
	_, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{
		FullMethod: scopeFilerMethodPrefix + "CreateEntry",
	}, func(handlerCtx context.Context, handlerReq any) (any, error) {
		return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
	})
	return err
}

func insertScopeTestEntry(t *testing.T, fs *FilerServer, p string, mode os.FileMode, ttlSec int32) {
	t.Helper()
	now := time.Now()
	e := &filer.Entry{FullPath: util.FullPath(p), Attr: filer.Attr{Mode: mode, TtlSec: ttlSec, Crtime: now, Mtime: now}}
	if err := fs.filer.Store.InsertEntry(context.Background(), e); err != nil {
		t.Fatalf("insert %s: %v", p, err)
	}
}

func TestScopeCreateCannotMakeAncestorsOutsideGrant(t *testing.T) {
	t.Run("file ancestor is not promoted", func(t *testing.T) {
		fs, scope := newScopeRetentionServer(t, newRenameTestStore())
		insertScopeTestEntry(t, fs, "/data", os.ModeDir|0755, 0)
		insertScopeTestEntry(t, fs, "/data/proj", 0644, 3600)
		err := scopedCreate(t, fs, scope, "/data/proj/ws", &filer_pb.CreateEntryRequest{Directory: "/data/proj/ws", Entry: &filer_pb.Entry{Name: "x"}})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("create under a file ancestor err=%v, want PermissionDenied", err)
		}
		got, findErr := fs.filer.FindEntry(context.Background(), "/data/proj")
		if findErr != nil || got.IsDirectory() || got.TtlSec != 3600 {
			t.Fatalf("/data/proj = %+v err=%v, want the file with ttl 3600", got, findErr)
		}
	})
	t.Run("missing ancestors are not created", func(t *testing.T) {
		fs, scope := newScopeRetentionServer(t, newRenameTestStore())
		err := scopedCreate(t, fs, scope, "/q/r/ws", &filer_pb.CreateEntryRequest{Directory: "/q/r/ws", Entry: &filer_pb.Entry{Name: "x"}})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("create under missing ancestors err=%v, want PermissionDenied", err)
		}
		if _, findErr := fs.filer.FindEntry(context.Background(), "/q"); findErr != filer_pb.ErrNotFound {
			t.Fatalf("/q lookup err=%v, want not found", findErr)
		}
	})
	t.Run("missing ancestors inside the grant are created", func(t *testing.T) {
		fs, scope := newScopeRetentionServer(t, newRenameTestStore())
		insertScopeTestEntry(t, fs, "/data", os.ModeDir|0755, 0)
		insertScopeTestEntry(t, fs, "/data/proj", os.ModeDir|0755, 0)
		err := scopedCreate(t, fs, scope, "/data/proj/ws", &filer_pb.CreateEntryRequest{Directory: "/data/proj/ws/a", Entry: &filer_pb.Entry{Name: "x"}})
		if err != nil {
			t.Fatalf("create with in-grant ancestors missing: %v", err)
		}
		if got, findErr := fs.filer.FindEntry(context.Background(), "/data/proj/ws/a"); findErr != nil || !got.IsDirectory() {
			t.Fatalf("/data/proj/ws/a = %+v err=%v, want a directory", got, findErr)
		}
	})
}

func TestScopeRenameCannotMakeAncestorsOutsideGrant(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS, "/q/r/ws"}, nil))
	h.fake.putDir(aliceWS)
	_, err := c.AtomicRenameEntry(scopeCtx(t), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: aliceWS, OldName: "old", NewDirectory: "/q/r/ws", NewName: "new",
	})
	expectScopeCode(t, "rename under missing ancestors", err, codes.PermissionDenied)
	if got := len(h.fake.received("AtomicRenameEntry")); got != 0 {
		t.Fatalf("refused rename reached the handler %d times", got)
	}
}
