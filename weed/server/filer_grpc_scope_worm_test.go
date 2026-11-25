package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func newScopedWormServer(t *testing.T) (*FilerServer, *filerGrpcScope, *renameTestStore) {
	t.Helper()
	store := newRenameTestStore()
	store.entries["/"] = newDirectoryEntry("/", 1)
	store.entries[aliceWS] = newDirectoryEntry(aliceWS, 2)
	fs, scope := newScopeRetentionServer(t, store)
	if err := fs.filer.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix:           aliceWS,
		Worm:                     proto.Bool(true),
		WormRetentionTimeSeconds: 0,
	}); err != nil {
		t.Fatalf("set WORM path rule: %v", err)
	}
	return fs, scope, store
}

func insertScopedWormEntry(t *testing.T, store *renameTestStore, name string, enforcedAt time.Time) util.FullPath {
	t.Helper()
	path := util.NewFullPath(aliceWS, name)
	entry := newFileEntry(string(path), 3)
	if !enforcedAt.IsZero() {
		entry.WORMEnforcedAtTsNs = enforcedAt.UnixNano()
	}
	entry.Content = []byte("stored")
	store.entries[string(path)] = entry
	return path
}

func scopedWormUnary(t *testing.T, scope *filerGrpcScope, method string, req any, handler grpc.UnaryHandler) error {
	t.Helper()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+scopedToken("worm", time.Hour, []string{aliceWS}, nil),
	))
	_, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, handler)
	return err
}

func TestScopedWormRefusesLocalMutations(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		fs, scope, store := newScopedWormServer(t)
		path := insertScopedWormEntry(t, store, "create", time.Now())
		called := false
		err := scopedWormUnary(t, scope, "CreateEntry", &filer_pb.CreateEntryRequest{
			Directory:                aliceWS,
			SkipCheckParentDirectory: true,
			Entry:                    &filer_pb.Entry{Name: "create"},
		}, func(ctx context.Context, req any) (any, error) {
			called = true
			return fs.CreateEntry(ctx, req.(*filer_pb.CreateEntryRequest))
		})
		if status.Code(err) != codes.PermissionDenied || !called {
			t.Fatalf("scoped create WORM error = %v called=%v, want handler and PermissionDenied", err, called)
		}
		got, findErr := store.FindEntry(context.Background(), path)
		if findErr != nil || string(got.Content) != "stored" {
			t.Fatalf("scoped create changed WORM entry: got=%v err=%v", got, findErr)
		}
	})

	t.Run("update", func(t *testing.T) {
		fs, scope, store := newScopedWormServer(t)
		path := insertScopedWormEntry(t, store, "update", time.Now())
		called := false
		err := scopedWormUnary(t, scope, "UpdateEntry", &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     &filer_pb.Entry{Name: "update", Attributes: &filer_pb.FuseAttributes{Mtime: 1700000001}},
		}, func(ctx context.Context, req any) (any, error) {
			called = true
			return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
		})
		if status.Code(err) != codes.PermissionDenied || !called {
			t.Fatalf("scoped update WORM error = %v called=%v, want handler and PermissionDenied", err, called)
		}
		got, findErr := store.FindEntry(context.Background(), path)
		if findErr != nil || string(got.Content) != "stored" {
			t.Fatalf("scoped update changed WORM entry: got=%v err=%v", got, findErr)
		}
	})

	t.Run("delete", func(t *testing.T) {
		fs, scope, store := newScopedWormServer(t)
		path := insertScopedWormEntry(t, store, "delete", time.Now())
		called := false
		err := scopedWormUnary(t, scope, "DeleteEntry", &filer_pb.DeleteEntryRequest{
			Directory: aliceWS,
			Name:      "delete",
		}, func(ctx context.Context, req any) (any, error) {
			called = true
			return fs.DeleteEntry(ctx, req.(*filer_pb.DeleteEntryRequest))
		})
		if status.Code(err) != codes.PermissionDenied || !called {
			t.Fatalf("scoped delete WORM error = %v called=%v, want handler and PermissionDenied", err, called)
		}
		if _, findErr := store.FindEntry(context.Background(), path); findErr != nil {
			t.Fatalf("scoped delete removed WORM entry: %v", findErr)
		}
	})

	t.Run("atomic rename", func(t *testing.T) {
		fs, scope, store := newScopedWormServer(t)
		path := insertScopedWormEntry(t, store, "rename", time.Now())
		called := false
		err := scopedWormUnary(t, scope, "AtomicRenameEntry", &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: aliceWS,
			OldName:      "rename",
			NewDirectory: aliceWS,
			NewName:      "renamed",
		}, func(ctx context.Context, req any) (any, error) {
			called = true
			return fs.AtomicRenameEntry(ctx, req.(*filer_pb.AtomicRenameEntryRequest))
		})
		if status.Code(err) != codes.PermissionDenied || !called {
			t.Fatalf("scoped atomic rename WORM error = %v called=%v, want handler and PermissionDenied", err, called)
		}
		if _, findErr := store.FindEntry(context.Background(), path); findErr != nil {
			t.Fatalf("scoped atomic rename removed WORM source: %v", findErr)
		}
		if _, findErr := store.FindEntry(context.Background(), util.NewFullPath(aliceWS, "renamed")); findErr == nil {
			t.Fatal("scoped atomic rename created the destination")
		}
	})
}

func TestScopedWormRefusesRenameOntoWormDestination(t *testing.T) {
	fs, scope, store := newScopedWormServer(t)
	insertScopedWormEntry(t, store, "destination", time.Now())
	source := newFileEntry(string(util.NewFullPath(aliceWS, "source")), 4)
	source.Content = []byte("source")
	store.entries[string(source.FullPath)] = source

	err := scopedWormUnary(t, scope, "AtomicRenameEntry", &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: aliceWS,
		OldName:      "source",
		NewDirectory: aliceWS,
		NewName:      "destination",
	}, func(ctx context.Context, req any) (any, error) {
		return fs.AtomicRenameEntry(ctx, req.(*filer_pb.AtomicRenameEntryRequest))
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped rename onto WORM destination error = %v, want PermissionDenied", err)
	}
	if _, findErr := store.FindEntry(context.Background(), source.FullPath); findErr != nil {
		t.Fatalf("scoped rename removed source while refusing destination: %v", findErr)
	}
	if got, findErr := store.FindEntry(context.Background(), util.NewFullPath(aliceWS, "destination")); findErr != nil || string(got.Content) != "stored" {
		t.Fatalf("scoped rename changed WORM destination: got=%v err=%v", got, findErr)
	}
}

type scopedWormRenameStream struct {
	ctx context.Context
	req *filer_pb.StreamRenameEntryRequest
}

func (s *scopedWormRenameStream) SetHeader(metadata.MD) error  { return nil }
func (s *scopedWormRenameStream) SendHeader(metadata.MD) error { return nil }
func (s *scopedWormRenameStream) SetTrailer(metadata.MD)       {}
func (s *scopedWormRenameStream) Context() context.Context     { return s.ctx }
func (s *scopedWormRenameStream) SendMsg(any) error            { return nil }
func (s *scopedWormRenameStream) RecvMsg(message any) error {
	request, ok := message.(*filer_pb.StreamRenameEntryRequest)
	if !ok {
		return status.Errorf(codes.Internal, "unexpected stream request %T", message)
	}
	proto.Reset(request)
	proto.Merge(request, s.req)
	return nil
}

func TestScopedWormRefusesStreamRenameWithMarkerPreserved(t *testing.T) {
	fs, scope, store := newScopedWormServer(t)
	path := insertScopedWormEntry(t, store, "stream", time.Now())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+scopedToken("worm-stream", time.Hour, []string{aliceWS}, nil),
	))
	raw := &scopedWormRenameStream{ctx: ctx, req: &filer_pb.StreamRenameEntryRequest{
		OldDirectory: aliceWS,
		OldName:      "stream",
		NewDirectory: aliceWS,
		NewName:      "stream-renamed",
	}}
	err := scope.stream(nil, raw, &grpc.StreamServerInfo{
		FullMethod: filer_pb.SeaweedFiler_StreamRenameEntry_FullMethodName,
	}, func(_ any, stream grpc.ServerStream) error {
		request := &filer_pb.StreamRenameEntryRequest{}
		if err := stream.RecvMsg(request); err != nil {
			return err
		}
		return fs.StreamRenameEntry(request, &grpc.GenericServerStream[filer_pb.StreamRenameEntryRequest, filer_pb.StreamRenameEntryResponse]{ServerStream: stream})
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("scoped stream rename WORM error = %v, want PermissionDenied", err)
	}
	if _, findErr := store.FindEntry(context.Background(), path); findErr != nil {
		t.Fatalf("scoped stream rename removed WORM source: %v", findErr)
	}
}

func TestScopeWormAllowsUnenforcedEntry(t *testing.T) {
	fs, scope, store := newScopedWormServer(t)
	path := insertScopedWormEntry(t, store, "unenforced", time.Time{})
	err := scopedWormUnary(t, scope, "UpdateEntry", &filer_pb.UpdateEntryRequest{
		Directory: aliceWS,
		Entry:     &filer_pb.Entry{Name: "unenforced", Attributes: &filer_pb.FuseAttributes{Mtime: 1700000001}},
	}, func(ctx context.Context, req any) (any, error) {
		return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
	})
	if err != nil {
		t.Fatalf("scoped update of unenforced entry: %v", err)
	}
	got, findErr := store.FindEntry(context.Background(), path)
	if findErr != nil || got.Content != nil || got.Mtime.Unix() != 1700000001 {
		t.Fatalf("unenforced update got=%v err=%v, want changed metadata and cleared content", got, findErr)
	}
}

func TestScopeWormAllowsInactiveRetention(t *testing.T) {
	for _, tc := range []struct {
		name     string
		worm     bool
		enforced time.Time
		duration uint64
	}{
		{name: "not enforced", worm: true},
		{name: "rule disabled", enforced: time.Now()},
		{name: "retention expired", worm: true, enforced: time.Now().Add(-time.Hour), duration: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, scope, store := newScopedWormServer(t)
			if err := fs.filer.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{
				LocationPrefix: aliceWS, Worm: proto.Bool(tc.worm), WormRetentionTimeSeconds: tc.duration,
			}); err != nil {
				t.Fatal(err)
			}
			entry := newFileEntry(aliceWS+"/mutable", 1)
			if !tc.enforced.IsZero() {
				entry.WORMEnforcedAtTsNs = tc.enforced.UnixNano()
			}
			entry.Content = []byte("stored")
			store.entries[string(entry.FullPath)] = entry
			err := scopedWormUnary(t, scope, "UpdateEntry", &filer_pb.UpdateEntryRequest{
				Directory: aliceWS, Entry: &filer_pb.Entry{Name: "mutable"},
			}, func(ctx context.Context, req any) (any, error) {
				return fs.UpdateEntry(ctx, req.(*filer_pb.UpdateEntryRequest))
			})
			if err != nil {
				t.Fatal(err)
			}
			got, err := fs.filer.FindEntry(context.Background(), entry.FullPath)
			if err != nil || len(got.Content) != 0 {
				t.Fatalf("update entry=%v error=%v", got, err)
			}
			err = scopedWormUnary(t, scope, "AtomicRenameEntry", &filer_pb.AtomicRenameEntryRequest{
				OldDirectory: aliceWS, OldName: "mutable", NewDirectory: aliceWS, NewName: "renamed",
			}, func(ctx context.Context, req any) (any, error) {
				return fs.AtomicRenameEntry(ctx, req.(*filer_pb.AtomicRenameEntryRequest))
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fs.filer.FindEntry(context.Background(), util.FullPath(aliceWS+"/renamed")); err != nil {
				t.Fatal(err)
			}
			err = scopedWormUnary(t, scope, "DeleteEntry", &filer_pb.DeleteEntryRequest{
				Directory: aliceWS, Name: "renamed",
			}, func(ctx context.Context, req any) (any, error) {
				resp, err := fs.DeleteEntry(ctx, req.(*filer_pb.DeleteEntryRequest))
				if resp.GetError() != "" {
					t.Fatal(resp.GetError())
				}
				return resp, err
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fs.filer.FindEntry(context.Background(), util.FullPath(aliceWS+"/renamed")); err != filer_pb.ErrNotFound {
				t.Fatalf("entry after delete: %v", err)
			}
		})
	}
}

func TestScopeWormRefusesAppend(t *testing.T) {
	fs, store := newAppendLockServer(t)
	close(store.release)
	if err := fs.filer.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{
		LocationPrefix: "/data", Worm: proto.Bool(true),
	}); err != nil {
		t.Fatal(err)
	}
	entry := newFileEntry(appendLockTestPath, 1)
	entry.WORMEnforcedAtTsNs = time.Now().UnixNano()
	entry.Chunks = []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}}
	store.entries[appendLockTestPath] = entry
	_, err := fs.AppendToEntry(withScopeKeepOnly(context.Background()), &filer_pb.AppendToEntryRequest{
		Directory: "/data", EntryName: "app.log", Chunks: []*filer_pb.FileChunk{{FileId: "5,0c00000055", Size: 10}},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("append error=%v, want PermissionDenied", err)
	}
	got, err := fs.filer.FindEntry(context.Background(), appendLockTestPath)
	if err != nil || len(got.GetChunks()) != 1 {
		t.Fatalf("append changed WORM data: entry=%v error=%v", got, err)
	}
}

func TestScopedWormUpdateRechecksAfterPreparation(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain store", true: "transactional store"}[transactional], func(t *testing.T) {
			fs, scope, store := newScopedWormServer(t)
			if transactional {
				conf := fs.filer.FilerConf
				fs.filer = newRenameTestFiler(t, conflictRefusingStore{&txnRenameStore{renameTestStore: store}})
				fs.filer.FilerConf = conf
			}
			queue := &captureQueue{}
			swapNotificationQueue(t, queue)
			path := insertScopedWormEntry(t, store, "update", time.Now())
			stored := store.entries[string(path)]
			stored.Content = nil
			stored.Chunks = []*filer_pb.FileChunk{written(storedChunk, 1)}
			before := proto.Clone(stored.ToProtoEntry()).(*filer_pb.Entry)
			replacement := proto.Clone(before).(*filer_pb.Entry)
			replacement.Attributes.Mtime++
			setRule := func(enabled bool) {
				t.Helper()
				if err := fs.filer.FilerConf.SetLocationConf(&filer_pb.FilerConf_PathConf{
					LocationPrefix: aliceWS,
					Worm:           proto.Bool(enabled),
				}); err != nil {
					t.Fatalf("set WORM rule: %v", err)
				}
			}
			setRule(false)
			const manifestID = "4,0d04"
			preparations := 0
			err := scopedWormUnary(t, scope, "UpdateEntry", &filer_pb.UpdateEntryRequest{
				Directory: aliceWS, Entry: replacement,
			}, func(ctx context.Context, request any) (any, error) {
				return fs.updateEntry(ctx, request.(*filer_pb.UpdateEntryRequest), string(path),
					func(context.Context, string, *filer.Entry, *filer_pb.Entry) ([]*filer_pb.FileChunk, []*filer_pb.FileChunk, error) {
						preparations++
						setRule(true)
						manifest := written(manifestID, 2)
						manifest.IsChunkManifest = true
						return []*filer_pb.FileChunk{manifest}, nil, nil
					})
			})
			if status.Code(err) != codes.PermissionDenied || preparations != 1 {
				t.Fatalf("update = %v, preparations = %d, want PermissionDenied after one preparation", err, preparations)
			}
			got, err := store.FindEntry(context.Background(), path)
			if err != nil || !proto.Equal(got.ToProtoEntry(), before) {
				t.Fatalf("refused update changed stored entry: got=%v err=%v", got, err)
			}
			if events := queue.snapshot(); len(events) != 0 {
				t.Fatalf("refused update emitted %d events", len(events))
			}
			checkDeletions(t, fs.filer, manifestID)
		})
	}
}
