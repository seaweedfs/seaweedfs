package weed_server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func scopedEntryEqualCondition(expected *filer_pb.Entry, equalFirst bool) *filer_pb.WriteCondition {
	equal := &filer_pb.WriteCondition_Clause{
		Kind:          filer_pb.WriteCondition_IF_ENTRY_EQUAL,
		ExpectedEntry: proto.Clone(expected).(*filer_pb.Entry),
	}
	exists := &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_EXISTS}
	clauses := []*filer_pb.WriteCondition_Clause{exists, equal}
	if equalFirst {
		clauses[0], clauses[1] = clauses[1], clauses[0]
	}
	return &filer_pb.WriteCondition{Clauses: clauses}
}

func runScopedEntryEqualCondition(t *testing.T, method string, ctx context.Context, initial *filer.Entry, request *filer_pb.Entry, condition *filer_pb.WriteCondition, beforeHandler ...func(*filer.Entry)) (error, bool, *filer.Entry, int) {
	t.Helper()
	store := newRenameTestStore()
	path := string(initial.FullPath)
	store.entries[path] = initial
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	fs, scope := newScopeRetentionServer(t, store)

	var req any
	switch method {
	case "CreateEntry":
		req = &filer_pb.CreateEntryRequest{
			Directory:                aliceWS,
			Entry:                    request,
			Condition:                condition,
			SkipCheckParentDirectory: true,
		}
	case "UpdateEntry":
		req = &filer_pb.UpdateEntryRequest{
			Directory: aliceWS,
			Entry:     request,
			Condition: condition,
		}
	default:
		t.Fatalf("unexpected method %q", method)
	}

	called := false
	response, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, func(handlerCtx context.Context, handlerReq any) (any, error) {
		called = true
		for _, update := range beforeHandler {
			update(store.entries[path])
		}
		switch method {
		case "CreateEntry":
			return fs.CreateEntry(handlerCtx, handlerReq.(*filer_pb.CreateEntryRequest))
		case "UpdateEntry":
			return fs.UpdateEntry(handlerCtx, handlerReq.(*filer_pb.UpdateEntryRequest))
		default:
			t.Fatalf("unexpected method %q", method)
			return nil, nil
		}
	})

	if err == nil && method == "CreateEntry" {
		cr := response.(*filer_pb.CreateEntryResponse)
		if cr.GetErrorCode() != filer_pb.FilerError_OK {
			if cr.GetErrorCode() != filer_pb.FilerError_PRECONDITION_FAILED {
				t.Fatalf("unexpected create refusal: %v", cr)
			}
			err = fmt.Errorf("%s: %w", cr.GetError(), filer_pb.ErrPreconditionFailed)
		}
	}

	got, findErr := store.FindEntry(context.Background(), util.FullPath(path))
	if findErr != nil {
		t.Fatalf("find entry after %s: %v", method, findErr)
	}
	fs.filer.FileIdDeletionQueue.Consume(func(ids []string) {
		if len(ids) != 0 {
			t.Errorf("%s queued unexpected chunk deletions: %v", method, ids)
		}
	})
	return err, called, got, len(queue.snapshot())
}

func pendingRemoteRenameEntry(name string) *filer.Entry {
	entry := newFileEntry(scopeJoin(aliceWS, name), 1)
	entry.Remote = &filer_pb.RemoteEntry{
		StorageName:       "s3",
		LastLocalSyncTsNs: 123,
		RemoteETag:        "etag",
		RemoteMtime:       456,
		RemoteSize:        789,
	}
	entry.Extended = map[string][]byte{
		"visible-tag":                  []byte("kept"),
		filer.ExtRemoteRenameSourceKey: []byte("/buckets/t1/source"),
	}
	return entry
}

func TestScopedEntryEqualRefusesHiddenRemoteRenameMarker(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		for _, equalFirst := range []bool{true, false} {
			label := "second"
			if equalFirst {
				label = "first"
			}
			t.Run(method+"/equal-"+label, func(t *testing.T) {
				initial := pendingRemoteRenameEntry("conditional-" + method)
				before := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
				request := proto.Clone(before).(*filer_pb.Entry)
				delete(request.Extended, filer.ExtRemoteRenameSourceKey)
				request.RemoteEntry = nil
				err, called, got, events := runScopedEntryEqualCondition(t, method, scopedRetentionContext(), initial, request, scopedEntryEqualCondition(request, equalFirst))
				if (method == "CreateEntry" && !errors.Is(err, filer_pb.ErrPreconditionFailed)) || (method == "UpdateEntry" && status.Code(err) != codes.FailedPrecondition) {
					t.Fatalf("%s error = %v, want FailedPrecondition", method, err)
				}
				if !strings.Contains(err.Error(), "scoped IF_ENTRY_EQUAL") {
					t.Fatalf("%s error = %v, want a clear scoped IF_ENTRY_EQUAL refusal", method, err)
				}
				if strings.Contains(err.Error(), "/buckets/t1/source") {
					t.Fatalf("%s error exposed the hidden source path: %v", method, err)
				}
				if called != (method == "CreateEntry") {
					t.Fatalf("%s handler called=%v, want Create refusal under lock and Update admission refusal", method, called)
				}
				if !proto.Equal(got.ToProtoEntry(), before) {
					t.Fatalf("%s changed the stored entry: got=%v want=%v", method, got.ToProtoEntry(), before)
				}
				if events != 0 {
					t.Fatalf("%s emitted %d metadata events on refusal", method, events)
				}
			})
		}
	}
}

func TestScopedEntryEqualWithoutHiddenMarkerIsAllowed(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		t.Run(method, func(t *testing.T) {
			initial := newFileEntry(scopeJoin(aliceWS, "conditional-"+method), 1)
			request := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			err, called, _, _ := runScopedEntryEqualCondition(t, method, scopedRetentionContext(), initial, request, scopedEntryEqualCondition(request, true))
			if err != nil || !called {
				t.Fatalf("%s error=%v called=%v, want allowed scoped conditional write", method, err, called)
			}
		})
	}
}

func TestUnscopedEntryEqualWithHiddenMarkerIsUnchanged(t *testing.T) {
	contexts := []struct {
		name string
		ctx  context.Context
	}{
		{name: "tokenless", ctx: context.Background()},
		{name: "unscoped filer token", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(
			"authorization", "Bearer "+unscopedToken(),
		))},
	}
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		for _, tc := range contexts {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				initial := pendingRemoteRenameEntry(tc.name + "-" + method)
				expected := initial.ToProtoEntry()
				err, called, _, _ := runScopedEntryEqualCondition(t, method, tc.ctx, initial, expected, scopedEntryEqualCondition(expected, true))
				if err != nil || !called {
					t.Fatalf("%s error=%v called=%v, want unscoped conditional write unchanged", method, err, called)
				}
			})
		}
	}
}

func TestScopedEntryEqualRechecksPendingRenameUnderLock(t *testing.T) {
	for _, method := range []string{"CreateEntry", "UpdateEntry"} {
		t.Run(method, func(t *testing.T) {
			initial := pendingRemoteRenameEntry("late-" + method)
			expected := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			delete(initial.Extended, filer.ExtRemoteRenameSourceKey)
			request := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			request.RemoteEntry = nil
			request.Content = []byte("replacement")
			err, called, got, events := runScopedEntryEqualCondition(t, method, scopedRetentionContext(), initial, request, scopedEntryEqualCondition(expected, true), func(current *filer.Entry) {
				current.Extended[filer.ExtRemoteRenameSourceKey] = []byte("/buckets/t1/source")
			})
			if !called || (method == "CreateEntry" && !errors.Is(err, filer_pb.ErrPreconditionFailed)) || (method == "UpdateEntry" && status.Code(err) != codes.FailedPrecondition) || !strings.Contains(err.Error(), "scoped IF_ENTRY_EQUAL") {
				t.Fatalf("%s error=%v called=%v, want locked FailedPrecondition refusal", method, err, called)
			}
			if !proto.Equal(got.ToProtoEntry(), expected) {
				t.Fatalf("%s changed stored entry on locked refusal: got=%v want=%v", method, got.ToProtoEntry(), expected)
			}
			if events != 0 {
				t.Fatalf("%s emitted %d metadata events on locked refusal", method, events)
			}
		})
	}
}

type pendingRenameCreateServer struct {
	*FilerServer
	beforeCreate func()
}

func (s *pendingRenameCreateServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if s.beforeCreate != nil {
		s.beforeCreate()
	}
	return s.FilerServer.CreateEntry(ctx, req)
}

func TestScopedEntryEqualCreateClientRecognizesDefiniteRefusal(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "stored marker"
		if late {
			name = "marker after admission"
		}
		t.Run(name, func(t *testing.T) {
			initial := pendingRemoteRenameEntry("client-refusal")
			expected := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			if late {
				delete(initial.Extended, filer.ExtRemoteRenameSourceKey)
			}
			request := proto.Clone(initial.ToProtoEntry()).(*filer_pb.Entry)
			delete(request.Extended, filer.ExtRemoteRenameSourceKey)
			request.RemoteEntry = nil
			store := newRenameTestStore()
			store.entries[string(initial.FullPath)] = initial
			queue := &captureQueue{}
			swapNotificationQueue(t, queue)
			fs, scope := newScopeRetentionServer(t, store)
			impl := &pendingRenameCreateServer{FilerServer: fs}
			if late {
				impl.beforeCreate = func() { initial.Extended[filer.ExtRemoteRenameSourceKey] = []byte("/buckets/t1/source") }
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer(scope.ServerOptions()...)
			filer_pb.RegisterSeaweedFilerServer(server, impl)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); _ = listener.Close() })
			conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			auth, _ := metadata.FromIncomingContext(scopedRetentionContext())
			ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), auth.Copy()), 5*time.Second)
			defer cancel()
			client := filer_pb.NewSeaweedFilerClient(conn)
			req := &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: request, Condition: scopedEntryEqualCondition(expected, true), SkipCheckParentDirectory: true}
			_, err = filer_pb.CreateEntryWithResponse(ctx, client, req)
			if !errors.Is(err, filer_pb.ErrPreconditionFailed) || !filer_pb.IsFilerVerdict(err) {
				t.Fatalf("create client error = %v, want definite precondition refusal", err)
			}
			raw, err := client.CreateEntry(ctx, req)
			if err != nil || raw.GetErrorCode() != filer_pb.FilerError_PRECONDITION_FAILED || !strings.Contains(raw.GetError(), "scoped IF_ENTRY_EQUAL") {
				t.Fatalf("raw create response=%v error=%v, want clear coded precondition refusal", raw, err)
			}
			stream, err := client.StreamMutateEntry(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&filer_pb.StreamMutateEntryRequest{RequestId: 9, Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{CreateRequest: req}}); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			streamed, err := stream.Recv()
			if err != nil || streamed.GetRequestId() != 9 || streamed.GetCreateResponse().GetErrorCode() != filer_pb.FilerError_PRECONDITION_FAILED || !strings.Contains(streamed.GetCreateResponse().GetError(), "scoped IF_ENTRY_EQUAL") {
				t.Fatalf("streamed create response=%v error=%v, want nested coded refusal", streamed, err)
			}

			got, err := store.FindEntry(context.Background(), initial.FullPath)
			if err != nil || !proto.Equal(got.ToProtoEntry(), expected) {
				t.Fatalf("stored entry changed: got=%v error=%v", got, err)
			}
			if events := len(queue.snapshot()); events != 0 {
				t.Fatalf("refusal emitted %d events", events)
			}
		})
	}
}
