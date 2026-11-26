package weed_server

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	scopeSubscriptionScopeKey     = "scope-subscription-scope-signing-key"
	scopeSubscriptionFilerKey     = "scope-subscription-handler-filer-key"
	scopeSubscriptionSubscribeKey = "scope-subscription-signing-key"
)

type scopeSubscriptionGRPCHarness struct {
	*subscribeHarness
	client  filer_pb.SeaweedFilerClient
	address string
}

func newScopeSubscriptionGRPCHarness(t *testing.T, token string) *scopeSubscriptionGRPCHarness {
	t.Helper()
	h := newScopeSubscriptionGRPCServer(t)
	h.client = h.clientForToken(t, token)
	return h
}

func newScopeSubscriptionGRPCServer(t *testing.T) *scopeSubscriptionGRPCHarness {
	t.Helper()
	h := newSubscribeHarness(t)
	h.fs.filerGuard = security.NewGuard(nil, scopeSubscriptionFilerKey, 0, scopeSubscriptionFilerKey, 0, "", "")
	h.fs.grpcScope = newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(scopeSubscriptionScopeKey), security.SigningKey("")
		},
		subscriptionScope: h.fs.metadataSubscriptionScope,
	}, h.fs)

	v := util.GetViper()
	previousSubscribeKey := v.GetString("jwt.filer_signing.subscribe.key")
	v.Set("jwt.filer_signing.subscribe.key", scopeSubscriptionSubscribeKey)
	t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", previousSubscribeKey) })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(h.fs.grpcScope.ServerOptions()...)
	filer_pb.RegisterSeaweedFilerServer(server, h.fs)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	return &scopeSubscriptionGRPCHarness{subscribeHarness: h, address: listener.Addr().String()}
}

func (h *scopeSubscriptionGRPCHarness) clientForToken(t *testing.T, token string) filer_pb.SeaweedFilerClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+h.address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(scopeToken(token)),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return filer_pb.NewSeaweedFilerClient(conn)
}

func scopeSubscriptionToken(t *testing.T, lifetime time.Duration, allowed, readOnly []string) string {
	t.Helper()
	token := security.GenJwtForFilerScope(security.SigningKey(scopeSubscriptionScopeKey), lifetime, "scope-subscription", allowed, readOnly)
	if token == "" {
		t.Fatal("failed to mint general filer scope token")
	}
	return string(token)
}

func scopeSubscriptionMetadataLogEntry(t *testing.T, event *filer_pb.SubscribeMetadataResponse) *filer_pb.LogEntry {
	t.Helper()
	data, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal metadata event: %v", err)
	}
	return &filer_pb.LogEntry{
		TsNs: event.TsNs,
		Data: data,
		Key:  []byte(filer_pb.MetadataEventSourceFullPath(event)),
	}
}

func appendScopeSubscriptionMetadataEvent(t *subscribeHarness, event *filer_pb.SubscribeMetadataResponse) {
	t.t.Helper()
	if err := t.f.LocalMetaLogBuffer.AddLogEntryToBuffer(scopeSubscriptionMetadataLogEntry(t.t, event)); err != nil {
		t.t.Fatalf("append metadata event: %v", err)
	}
}

func scopeSubscriptionResponse(tsNs int64, dir, name, newParent string, oldEntry bool) *filer_pb.SubscribeMetadataResponse {
	n := &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: name}, NewParentPath: newParent}
	if oldEntry {
		n.OldEntry = &filer_pb.Entry{Name: name}
	}
	return &filer_pb.SubscribeMetadataResponse{Directory: dir, TsNs: tsNs, EventNotification: n}
}

func nextScopeSubscriptionEvent(t *testing.T, stream grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse]) *filer_pb.SubscribeMetadataResponse {
	t.Helper()
	for {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("receive metadata event: %v", err)
		}
		if len(resp.GetLogFileRefs()) != 0 {
			t.Fatalf("scoped subscription sent log file references: %+v", resp.GetLogFileRefs())
		}
		for _, event := range append([]*filer_pb.SubscribeMetadataResponse{resp}, resp.GetEvents()...) {
			if notification := event.GetEventNotification(); notification != nil &&
				(notification.GetNewEntry() != nil || notification.GetOldEntry() != nil) {
				return event
			}
		}
	}
}

func TestGeneralScopedSubscriptionBoundsPrefixesThroughFilerHandlers(t *testing.T) {
	for _, method := range []string{"SubscribeMetadata", "SubscribeLocalMetadata"} {
		t.Run(method, func(t *testing.T) {
			token := scopeSubscriptionToken(t, time.Minute, []string{"/t/allowed"}, []string{"/t/read"})
			h := newScopeSubscriptionGRPCHarness(t, token)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			request := &filer_pb.SubscribeMetadataRequest{
				ClientName:                   "scope-subscription",
				ClientId:                     73,
				ClientEpoch:                  1,
				PathPrefix:                   "/t/allowed/",
				PathPrefixes:                 []string{"/t/read"},
				ClientSupportsMetadataChunks: true,
			}
			if replacing, _, _ := h.fs.addClient("local", "mount", "127.0.0.1:9", "/", -request.ClientId, 1); !replacing {
				t.Fatal("existing listener was not registered")
			}
			var stream grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse]
			var err error
			if method == "SubscribeMetadata" {
				stream, err = h.client.SubscribeMetadata(ctx, request)
			} else {
				stream, err = h.client.SubscribeLocalMetadata(ctx, request)
			}
			if err != nil {
				t.Fatalf("start subscription: %v", err)
			}

			base := time.Now().UnixNano()
			appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+1, "/t/allowed", "write-visible", "", false))
			appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+2, "/t/read", "read-visible", "", false))
			appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+3, "/t/allowed-private", "sibling-hidden", "", false))
			appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+4, "/t/outside", "outside-hidden", "", false))
			appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+5, "/t/allowed", "visible-sentinel", "", false))

			got := []string{nextScopeSubscriptionEvent(t, stream).GetEventNotification().GetNewEntry().GetName()}
			got = append(got, nextScopeSubscriptionEvent(t, stream).GetEventNotification().GetNewEntry().GetName())
			got = append(got, nextScopeSubscriptionEvent(t, stream).GetEventNotification().GetNewEntry().GetName())
			if !reflect.DeepEqual(got, []string{"write-visible", "read-visible", "visible-sentinel"}) {
				t.Fatalf("delivered events %v, want allowed and read-only events followed by the sentinel", got)
			}
			if !h.fs.hasClient(-request.ClientId, 1) {
				t.Fatal("scoped caller replaced the existing listener with its ClientId")
			}
		})
	}
}

func TestGeneralScopedSubscriptionRejectsOutOfScopeStartPrefix(t *testing.T) {
	for _, method := range []string{"SubscribeMetadata", "SubscribeLocalMetadata"} {
		t.Run(method, func(t *testing.T) {
			token := scopeSubscriptionToken(t, time.Minute, []string{"/t/allowed"}, nil)
			h := newScopeSubscriptionGRPCHarness(t, token)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request := &filer_pb.SubscribeMetadataRequest{
				ClientName: "scope-subscription-invalid-prefix",
				ClientId:   74,
				PathPrefix: "/t/outside",
			}

			var stream grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse]
			var err error
			if method == "SubscribeMetadata" {
				stream, err = h.client.SubscribeMetadata(ctx, request)
			} else {
				stream, err = h.client.SubscribeLocalMetadata(ctx, request)
			}
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("out-of-scope start prefix error=%v, want PermissionDenied", err)
			}
		})
	}
}

func TestGeneralScopedSubscriptionFiltersFilerConfThroughFilerHandler(t *testing.T) {
	token := scopeSubscriptionToken(t, time.Minute, []string{scopeFilerConfDir}, nil)
	h := newScopeSubscriptionGRPCHarness(t, token)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := h.client.SubscribeLocalMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
		ClientName: "scope-subscription-filer-conf",
		ClientId:   75,
		PathPrefix: scopeFilerConfDir,
	})
	if err != nil {
		t.Fatalf("start subscription: %v", err)
	}

	base := time.Now().UnixNano()
	appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+1, scopeFilerConfDir, "visible.json", "", false))
	appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+2, scopeFilerConfDir, scopeFilerConfName, "", false))
	delete := &filer_pb.SubscribeMetadataResponse{
		Directory:         scopeFilerConfDir,
		TsNs:              base + 3,
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: scopeFilerConfName}},
	}
	appendScopeSubscriptionMetadataEvent(h.subscribeHarness, delete)
	renameInto := &filer_pb.SubscribeMetadataResponse{
		Directory: scopeFilerConfDir,
		TsNs:      base + 4,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "visible-old"},
			NewEntry:      &filer_pb.Entry{Name: scopeFilerConfName},
			NewParentPath: scopeFilerConfDir,
		},
	}
	appendScopeSubscriptionMetadataEvent(h.subscribeHarness, renameInto)
	appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(base+5, scopeFilerConfDir, "visible-tail", "", false))

	visible := nextScopeSubscriptionEvent(t, stream)
	if visible.GetEventNotification().GetNewEntry().GetName() != "visible.json" {
		t.Fatalf("first event=%+v, want visible sibling", visible)
	}
	oldSide := nextScopeSubscriptionEvent(t, stream)
	if oldSide.GetEventNotification().GetOldEntry().GetName() != "visible-old" || oldSide.GetEventNotification().GetNewEntry() != nil {
		t.Fatalf("rename into filer.conf exposed the wrong event: %+v", oldSide)
	}
	tail := nextScopeSubscriptionEvent(t, stream)
	if tail.GetEventNotification().GetNewEntry().GetName() != "visible-tail" {
		t.Fatalf("trailing visible event=%+v, want visible-tail", tail)
	}
}

func TestGeneralScopedSubscriptionExpiresIdleAndActive(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "active"}[active], func(t *testing.T) {
			h := newScopeSubscriptionGRPCServer(t)
			token := scopeSubscriptionToken(t, 2*time.Second, []string{"/t"}, nil)
			h.client = h.clientForToken(t, token)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := h.client.SubscribeLocalMetadata(ctx, &filer_pb.SubscribeMetadataRequest{
				ClientName: "scope-subscription-expiry",
				ClientId:   76,
				PathPrefix: "/t",
			})
			if err != nil {
				t.Fatalf("start subscription: %v", err)
			}
			if active {
				appendScopeSubscriptionMetadataEvent(h.subscribeHarness, scopeSubscriptionResponse(time.Now().UnixNano(), "/t", "before-expiry", "", false))
				if got := nextScopeSubscriptionEvent(t, stream).GetEventNotification().GetNewEntry().GetName(); got != "before-expiry" {
					t.Fatalf("active event=%q, want before-expiry", got)
				}
			}
			_, err = stream.Recv()
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("%s subscription ended with %v, want Unauthenticated", map[bool]string{false: "idle", true: "active"}[active], err)
			}
		})
	}
}

type scopeSubscriptionRecordingServerStream struct {
	ctx       context.Context
	request   proto.Message
	mu        sync.Mutex
	responses []*filer_pb.SubscribeMetadataResponse
}

func (s *scopeSubscriptionRecordingServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *scopeSubscriptionRecordingServerStream) SendHeader(metadata.MD) error { return nil }
func (s *scopeSubscriptionRecordingServerStream) SetTrailer(metadata.MD)       {}
func (s *scopeSubscriptionRecordingServerStream) Context() context.Context     { return s.ctx }

func (s *scopeSubscriptionRecordingServerStream) SendMsg(m any) error {
	response, ok := m.(*filer_pb.SubscribeMetadataResponse)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, proto.Clone(response).(*filer_pb.SubscribeMetadataResponse))
	return nil
}

func (s *scopeSubscriptionRecordingServerStream) RecvMsg(m any) error {
	if s.request == nil {
		return context.Canceled
	}
	proto.Merge(m.(proto.Message), s.request)
	s.request = nil
	return nil
}

func (s *scopeSubscriptionRecordingServerStream) snapshot() []*filer_pb.SubscribeMetadataResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*filer_pb.SubscribeMetadataResponse(nil), s.responses...)
}

func TestGeneralScopedSubscriptionBoundsVerifiedMTLSHandler(t *testing.T) {
	h := newSubscribeHarness(t)
	h.fs.filerGuard = security.NewGuard(nil, scopeSubscriptionFilerKey, 0, scopeSubscriptionFilerKey, 0, "", "")
	h.fs.grpcScope = newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(scopeSubscriptionScopeKey), security.SigningKey("")
		},
		subscriptionScope: h.fs.metadataSubscriptionScope,
	}, h.fs)
	token := scopeSubscriptionToken(t, 2*time.Second, []string{"/t/allowed"}, nil)
	request := &filer_pb.SubscribeMetadataRequest{ClientName: "scope-subscription-mtls", ClientId: 77, PathPrefix: "/t/allowed"}
	underlying := &scopeSubscriptionRecordingServerStream{ctx: verifiedSubscriptionContext(token), request: request}
	done := make(chan error, 1)
	go func() {
		done <- h.fs.grpcScope.stream(nil, underlying, &grpc.StreamServerInfo{
			FullMethod: filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName,
		}, func(_ any, stream grpc.ServerStream) error {
			decoded := &filer_pb.SubscribeMetadataRequest{}
			if err := stream.RecvMsg(decoded); err != nil {
				return err
			}
			return h.fs.SubscribeLocalMetadata(decoded, &grpc.GenericServerStream[filer_pb.SubscribeMetadataRequest, filer_pb.SubscribeMetadataResponse]{ServerStream: stream})
		})
	}()

	base := time.Now().UnixNano()
	appendScopeSubscriptionMetadataEvent(h, scopeSubscriptionResponse(base+1, "/t/allowed", "mtls-visible", "", false))
	appendScopeSubscriptionMetadataEvent(h, scopeSubscriptionResponse(base+2, "/t/allowed-private", "mtls-prefix-hidden", "", false))
	appendScopeSubscriptionMetadataEvent(h, scopeSubscriptionResponse(base+3, "/t/outside", "mtls-hidden", "", false))
	deadline := time.Now().Add(3 * time.Second)
	observedVisible := false
	for time.Now().Before(deadline) {
		var names []string
		for _, response := range underlying.snapshot() {
			for _, event := range append([]*filer_pb.SubscribeMetadataResponse{response}, response.GetEvents()...) {
				if entry := event.GetEventNotification().GetNewEntry(); entry != nil {
					names = append(names, entry.GetName())
				}
			}
		}
		if len(names) > 0 {
			if len(names) != 1 || names[0] != "mtls-visible" {
				t.Fatalf("verified mTLS scope delivered %v", names)
			}
			observedVisible = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !observedVisible {
		t.Fatal("verified mTLS scope did not deliver the visible event before expiry")
	}
	select {
	case err := <-done:
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("verified mTLS stream ended with %v, want Unauthenticated at expiry", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("verified mTLS scoped stream did not expire")
	}
	var finalNames []string
	for _, response := range underlying.snapshot() {
		for _, event := range append([]*filer_pb.SubscribeMetadataResponse{response}, response.GetEvents()...) {
			if entry := event.GetEventNotification().GetNewEntry(); entry != nil {
				finalNames = append(finalNames, entry.GetName())
			}
		}
	}
	if !reflect.DeepEqual(finalNames, []string{"mtls-visible"}) {
		t.Fatalf("verified mTLS scope delivered hidden events after filtering: %v", finalNames)
	}
}

func TestGeneralScopedSubscriptionRejectsExpiredHandoffWithoutMTLS(t *testing.T) {
	h := newSubscribeHarness(t)
	h.fs.filerGuard = security.NewGuard(nil, scopeSubscriptionFilerKey, 0, scopeSubscriptionFilerKey, 0, "", "")
	h.fs.grpcScope = newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(scopeSubscriptionScopeKey), security.SigningKey("")
		},
		subscriptionScope: h.fs.metadataSubscriptionScope,
	}, h.fs)
	token := scopeSubscriptionToken(t, time.Minute, []string{"/t"}, nil)
	underlying := &scopeSubscriptionRecordingServerStream{
		ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token)),
		request: &filer_pb.SubscribeMetadataRequest{
			ClientName: "scope-subscription-expired-handoff",
			ClientId:   78,
			PathPrefix: "/t",
		},
	}

	err := h.fs.grpcScope.stream(nil, underlying, &grpc.StreamServerInfo{
		FullMethod: filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName,
	}, func(_ any, stream grpc.ServerStream) error {
		sc, ok := stream.Context().Value(scopeSubscriptionContextKey{}).(*scope)
		if !ok {
			t.Fatal("scoped subscription marker was not handed to the handler")
		}
		decoded := &filer_pb.SubscribeMetadataRequest{}
		if err := stream.RecvMsg(decoded); err != nil {
			return err
		}
		sc.exp = time.Now().Add(-time.Second)
		return h.fs.SubscribeLocalMetadata(decoded, &grpc.GenericServerStream[filer_pb.SubscribeMetadataRequest, filer_pb.SubscribeMetadataResponse]{ServerStream: stream})
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expired interceptor-to-handler handoff error=%v, want Unauthenticated", err)
	}
}
