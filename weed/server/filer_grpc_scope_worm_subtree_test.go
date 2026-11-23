package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
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

func TestScopeWormDirectoryMutationsRespectConfiguredSubtrees(t *testing.T) {
	rule := func(prefix string, enabled bool) *filer_pb.FilerConf_PathConf {
		return &filer_pb.FilerConf_PathConf{LocationPrefix: prefix, Worm: proto.Bool(enabled)}
	}
	for _, tc := range []struct {
		name              string
		rules             []*filer_pb.FilerConf_PathConf
		denied            bool
		destinationOnly   bool
		unmarked          bool
		tokenless         bool
		emptyNonrecursive bool
	}{
		{name: "inherited rule protects descendants", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS, true)}, denied: true},
		{name: "directory rule protects descendants", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS+"/dir", true)}, denied: true},
		{name: "trailing slash rule protects descendants", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS+"/dir/", true)}, denied: true},
		{name: "nested rule protects descendants", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS+"/dir/locked", true)}, denied: true},
		{name: "configured subtree refused without retained entries", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS+"/dir/locked", true)}, denied: true, unmarked: true},
		{name: "destination subtree protected before rename", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS+"/moved/locked", true)}, denied: true, destinationOnly: true},
		{name: "no WORM rules allows directory mutation"},
		{name: "sibling prefix does not block directory mutation", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS+"/dir-other", true)}},
		{name: "disabled inherited WORM allows directory mutation", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS, true), rule(aliceWS+"/dir/", false), rule(aliceWS+"/moved/", false)}},
		{name: "nested enable overrides disabled inherited rule", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS, true), rule(aliceWS+"/dir/", false), rule(aliceWS+"/dir/locked", true)}, denied: true},
		{name: "tokenless directory mutation unchanged", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS, true)}, tokenless: true},
		{name: "empty directory nonrecursive delete allowed under WORM", rules: []*filer_pb.FilerConf_PathConf{rule(aliceWS, true)}, emptyNonrecursive: true, unmarked: true},
	} {
		for _, operation := range []string{"recursive delete", "atomic rename", "stream rename"} {
			if tc.emptyNonrecursive && operation != "recursive delete" {
				continue
			}
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				fs, scope, store := newScopedWormServer(t)
				fs.filer.FilerConf = filer.NewFilerConf()
				for _, r := range tc.rules {
					if err := fs.filer.FilerConf.SetLocationConf(r); err != nil {
						t.Fatal(err)
					}
				}
				dir := util.FullPath(aliceWS + "/dir")
				store.entries[string(dir)] = newDirectoryEntry(string(dir), 1)
				original := map[util.FullPath]*filer_pb.Entry{dir: store.entries[string(dir)].ToProtoEntry()}
				if !tc.emptyNonrecursive {
					for _, name := range []string{"aaa", "locked"} {
						entry := newFileEntry(string(dir.Child(name)), 1)
						entry.Content = []byte(name)
						if name == "locked" && !tc.unmarked {
							entry.WORMEnforcedAtTsNs = time.Now().UnixNano()
						}
						store.entries[string(entry.FullPath)] = entry
						original[entry.FullPath] = entry.ToProtoEntry()
					}
				}
				err := runWormDirectoryMutation(t, fs, scope, operation, !tc.tokenless, !tc.emptyNonrecursive)
				denied := tc.denied && !(tc.destinationOnly && operation == "recursive delete")
				if denied {
					if status.Code(err) != codes.PermissionDenied {
						t.Errorf("error=%v, want PermissionDenied", err)
					}
					for path, before := range original {
						after, findErr := store.FindEntry(context.Background(), path)
						if findErr != nil || !proto.Equal(before, after.ToProtoEntry()) {
							t.Errorf("source changed at %s: %v", path, findErr)
						}
					}
					if _, findErr := store.FindEntry(context.Background(), util.FullPath(aliceWS+"/moved")); findErr != filer_pb.ErrNotFound {
						t.Errorf("destination created: %v", findErr)
					}
					return
				}
				if err != nil {
					t.Fatalf("allowed operation failed: %v", err)
				}
				for path, before := range original {
					if _, findErr := store.FindEntry(context.Background(), path); findErr != filer_pb.ErrNotFound {
						t.Errorf("source still exists at %s: %v", path, findErr)
					}
					if operation != "recursive delete" {
						movedPath := util.FullPath(aliceWS + "/moved" + string(path)[len(dir):])
						after, findErr := store.FindEntry(context.Background(), movedPath)
						expected := proto.Clone(before).(*filer_pb.Entry)
						if path == dir {
							expected.Name = "moved"
						}
						if findErr != nil || !bytes.Equal(expected.Content, after.Content) || expected.IsDirectory != after.IsDirectory() || expected.Name != after.Name() {
							t.Errorf("destination data differs at %s: %v", movedPath, findErr)
						}
					}
				}
			})
		}
	}
}

func runWormDirectoryMutation(t *testing.T, fs *FilerServer, scope *filerGrpcScope, operation string, scoped, recursive bool) error {
	t.Helper()
	ctx := context.Background()
	if scoped {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+scopedToken("worm-parent", time.Hour, []string{aliceWS}, nil)))
	}
	unary := func(method string, req any, handler grpc.UnaryHandler) error {
		_, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: "/filer_pb.SeaweedFiler/" + method}, handler)
		return err
	}
	var err error
	switch operation {
	case "recursive delete":
		err = unary("DeleteEntry", &filer_pb.DeleteEntryRequest{
			Directory: aliceWS, Name: "dir", IsRecursive: recursive,
		}, func(ctx context.Context, req any) (any, error) {
			resp, err := fs.DeleteEntry(ctx, req.(*filer_pb.DeleteEntryRequest))
			if err == nil && resp.GetError() != "" {
				err = fmt.Errorf("delete response: %s", resp.GetError())
			}
			return resp, err
		})
	case "atomic rename":
		err = unary("AtomicRenameEntry", &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: aliceWS, OldName: "dir", NewDirectory: aliceWS, NewName: "moved",
		}, func(ctx context.Context, req any) (any, error) {
			return fs.AtomicRenameEntry(ctx, req.(*filer_pb.AtomicRenameEntryRequest))
		})
	case "stream rename":
		raw := &scopedWormRenameStream{ctx: ctx, req: &filer_pb.StreamRenameEntryRequest{
			OldDirectory: aliceWS, OldName: "dir", NewDirectory: aliceWS, NewName: "moved",
		}}
		err = scope.stream(nil, raw, &grpc.StreamServerInfo{
			FullMethod: filer_pb.SeaweedFiler_StreamRenameEntry_FullMethodName,
		}, func(_ any, stream grpc.ServerStream) error {
			req := &filer_pb.StreamRenameEntryRequest{}
			if err := stream.RecvMsg(req); err != nil {
				return err
			}
			return fs.StreamRenameEntry(req, &grpc.GenericServerStream[filer_pb.StreamRenameEntryRequest, filer_pb.StreamRenameEntryResponse]{ServerStream: stream})
		})
	}
	return err
}

func TestScopeWormRecursiveFlagStillAllowsDeletingUnretainedFile(t *testing.T) {
	fs, scope, store := newScopedWormServer(t)
	path := util.FullPath(aliceWS + "/file")
	store.entries[string(path)] = newFileEntry(string(path), 1)
	err := scopedWormUnary(t, scope, "DeleteEntry", &filer_pb.DeleteEntryRequest{
		Directory: aliceWS, Name: "file", IsRecursive: true,
	}, func(ctx context.Context, req any) (any, error) {
		resp, err := fs.DeleteEntry(ctx, req.(*filer_pb.DeleteEntryRequest))
		if err == nil && resp.GetError() != "" {
			t.Errorf("delete response: %s", resp.GetError())
		}
		return resp, err
	})
	if err != nil {
		t.Fatalf("delete unretained file: %v", err)
	}
	if _, findErr := store.FindEntry(context.Background(), path); findErr != filer_pb.ErrNotFound {
		t.Errorf("unretained file still exists: %v", findErr)
	}
}

func TestScopeWormStreamMutateRefusesSubtreeBeforeChangingEntries(t *testing.T) {
	for _, rename := range []bool{false, true} {
		t.Run(fmt.Sprintf("rename=%t", rename), func(t *testing.T) {
			fs, scope, store := newScopedWormServer(t)
			dir := util.FullPath(aliceWS + "/dir")
			store.entries[string(dir)] = newDirectoryEntry(string(dir), 1)
			child := newFileEntry(string(dir.Child("locked")), 1)
			child.Content = []byte("retained")
			child.WORMEnforcedAtTsNs = time.Now().UnixNano()
			store.entries[string(child.FullPath)] = child
			before := child.ToProtoEntry()
			req := &filer_pb.StreamMutateEntryRequest{RequestId: 1}
			if rename {
				req.Request = &filer_pb.StreamMutateEntryRequest_RenameRequest{RenameRequest: &filer_pb.StreamRenameEntryRequest{
					OldDirectory: aliceWS, OldName: "dir", NewDirectory: aliceWS, NewName: "moved",
				}}
			} else {
				req.Request = &filer_pb.StreamMutateEntryRequest_DeleteRequest{DeleteRequest: &filer_pb.DeleteEntryRequest{
					Directory: aliceWS, Name: "dir", IsRecursive: true,
				}}
			}
			raw := &wormMutateStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
				"authorization", "Bearer "+scopedToken("worm-mutate", time.Hour, []string{aliceWS}, nil),
			)), req: req}
			err := scope.stream(nil, raw, &grpc.StreamServerInfo{FullMethod: filer_pb.SeaweedFiler_StreamMutateEntry_FullMethodName}, func(_ any, stream grpc.ServerStream) error {
				return fs.StreamMutateEntry(&grpc.GenericServerStream[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]{ServerStream: stream})
			})
			if err != nil {
				t.Fatalf("stream failed instead of returning a per-request refusal: %v", err)
			}
			if len(raw.responses) != 1 {
				t.Fatalf("responses=%v, want one refusal and no mutation events", raw.responses)
			}
			response := raw.responses[0]
			if response.RequestId != 1 || !response.IsLast || response.Errno == 0 || !strings.Contains(response.Error, "PermissionDenied") {
				t.Errorf("response=%v, want terminal refusal", response)
			}
			after, findErr := store.FindEntry(context.Background(), child.FullPath)
			if findErr != nil || !proto.Equal(before, after.ToProtoEntry()) {
				t.Errorf("protected child changed: %v", findErr)
			}
			if _, findErr := store.FindEntry(context.Background(), dir); findErr != nil {
				t.Errorf("source directory removed: %v", findErr)
			}
			if _, findErr := store.FindEntry(context.Background(), util.FullPath(aliceWS+"/moved")); findErr != filer_pb.ErrNotFound {
				t.Errorf("destination created: %v", findErr)
			}
		})
	}
}

type wormMutateStream struct {
	grpc.ServerStream
	ctx       context.Context
	req       *filer_pb.StreamMutateEntryRequest
	responses []*filer_pb.StreamMutateEntryResponse
}

func (s *wormMutateStream) Context() context.Context { return s.ctx }
func (s *wormMutateStream) RecvMsg(message any) error {
	if s.req == nil {
		return io.EOF
	}
	proto.Merge(message.(*filer_pb.StreamMutateEntryRequest), s.req)
	s.req = nil
	return nil
}
func (s *wormMutateStream) SendMsg(message any) error {
	s.responses = append(s.responses, proto.Clone(message.(*filer_pb.StreamMutateEntryResponse)).(*filer_pb.StreamMutateEntryResponse))
	return nil
}
