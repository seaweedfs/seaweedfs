package weed_server

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	_, err := scope.unary(ctx, req, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + method}, func(handlerCtx context.Context, handlerReq any) (any, error) {
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
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("%s error = %v, want FailedPrecondition", method, err)
				}
				if !strings.Contains(err.Error(), "scoped IF_ENTRY_EQUAL") {
					t.Fatalf("%s error = %v, want a clear scoped IF_ENTRY_EQUAL refusal", method, err)
				}
				if strings.Contains(err.Error(), "/buckets/t1/source") {
					t.Fatalf("%s error exposed the hidden source path: %v", method, err)
				}
				if called {
					t.Fatalf("%s handler was called after admission refusal", method)
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
			if !called || status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "scoped IF_ENTRY_EQUAL") {
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
