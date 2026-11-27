package weed_server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestScopeListEntriesBoundsDirectoriesAndFiltersFilerConf(t *testing.T) {
	h := newScopeHarness(t)
	h.fake.put(scopeFilerConfDir+"/"+scopeFilerConfName, entryWith(scopeFilerConfName))
	visible := entryWith("visible.json")
	visible.Extended = map[string][]byte{
		filer.ExtRemoteRenameSourceKey: []byte("/buckets/t1/source"),
		"client-tag":                   []byte("keep"),
	}
	h.fake.put(scopeFilerConfDir+"/visible.json", visible)
	c := h.client(scopedToken("alice", time.Hour, []string{scopeFilerConfDir}, nil))

	stream, err := c.ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: scopeFilerConfDir})
	if err != nil {
		t.Fatalf("list in scope: %v", err)
	}
	names := map[string]bool{}
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatalf("list response: %v", recvErr)
		}
		if response.GetEntry().GetName() == "visible.json" {
			if got := response.GetEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) != 0 {
				t.Fatalf("scoped list exposed remote rename source %q", got)
			}
			if got := string(response.GetEntry().GetExtended()["client-tag"]); got != "keep" {
				t.Fatalf("scoped list lost client metadata %q", got)
			}
		}
		names[response.GetEntry().GetName()] = true
	}
	if names[scopeFilerConfName] {
		t.Fatal("scoped list exposed filer.conf")
	}
	if !names["visible.json"] {
		t.Fatalf("scoped list dropped visible entry: %v", names)
	}
	stored, err := h.fake.scopeFindEntry(context.Background(), scopeFilerConfDir+"/visible.json")
	if err != nil {
		t.Fatalf("read stored list entry: %v", err)
	}
	if got := stored.GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) == 0 {
		t.Fatal("scoped list redaction mutated the stored entry")
	}
	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "tokenless"},
		{name: "unscoped", token: unscopedToken()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			list, err := h.client(tc.token).ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: scopeFilerConfDir})
			if err != nil {
				t.Fatalf("%s list: %v", tc.name, err)
			}
			found := false
			for {
				response, recvErr := list.Recv()
				if errors.Is(recvErr, io.EOF) {
					break
				}
				if recvErr != nil {
					t.Fatalf("%s list response: %v", tc.name, recvErr)
				}
				if response.GetEntry().GetName() == "visible.json" {
					found = true
					if got := response.GetEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) == 0 {
						t.Fatalf("%s list lost remote rename source", tc.name)
					}
				}
			}
			if !found {
				t.Fatalf("%s list did not return visible entry", tc.name)
			}
		})
	}

	before := len(h.fake.received("ListEntries"))
	out, err := c.ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: "/buckets/t10"})
	if err == nil {
		_, err = out.Recv()
	}
	expectScopeCode(t, "list outside scope", err, codes.PermissionDenied)
	if got := len(h.fake.received("ListEntries")); got != before {
		t.Fatalf("out-of-scope list reached handler: %d calls before=%d", got, before)
	}
}

func TestScopeStreamRenameBoundsBothPaths(t *testing.T) {
	h := newScopeHarness(t)
	h.fake.putDir(aliceWS)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	allowed, err := c.StreamRenameEntry(scopeCtx(t), &filer_pb.StreamRenameEntryRequest{
		OldDirectory: aliceWS,
		OldName:      "old",
		NewDirectory: aliceWS + "/sub",
		NewName:      "new",
	})
	if err != nil {
		t.Fatalf("in-scope stream rename: %v", err)
	}
	if _, err := allowed.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("in-scope stream rename ended with %v, want EOF", err)
	}

	before := len(h.fake.received("StreamRenameEntry"))
	refused, err := c.StreamRenameEntry(scopeCtx(t), &filer_pb.StreamRenameEntryRequest{
		OldDirectory: aliceWS,
		OldName:      "old",
		NewDirectory: bobWS,
		NewName:      "new",
	})
	if err == nil {
		_, err = refused.Recv()
	}
	expectScopeCode(t, "cross-prefix stream rename", err, codes.PermissionDenied)
	if got := len(h.fake.received("StreamRenameEntry")); got != before {
		t.Fatalf("refused stream rename reached handler: %d calls before=%d", got, before)
	}
}

func TestScopeStreamMutateRefusesMessageAndContinues(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))
	stream, err := c.StreamMutateEntry(scopeCtx(t))
	if err != nil {
		t.Fatalf("open mutate stream: %v", err)
	}

	sendDelete := func(id uint64, directory string) {
		t.Helper()
		if err := stream.Send(&filer_pb.StreamMutateEntryRequest{
			RequestId: id,
			Request:   &filer_pb.StreamMutateEntryRequest_DeleteRequest{DeleteRequest: &filer_pb.DeleteEntryRequest{Directory: directory, Name: "file"}},
		}); err != nil {
			t.Fatalf("send %d: %v", id, err)
		}
	}
	receive := func(id uint64) *filer_pb.StreamMutateEntryResponse {
		t.Helper()
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("receive %d: %v", id, err)
		}
		if response.GetRequestId() != id {
			t.Fatalf("response id=%d, want %d", response.GetRequestId(), id)
		}
		return response
	}

	sendDelete(1, aliceWS)
	if response := receive(1); response.GetError() != "" {
		t.Fatalf("in-scope delete returned %q", response.GetError())
	}
	sendDelete(2, bobWS)
	response := receive(2)
	if response.GetErrno() != scopeErrnoEACCES || response.GetError() == "" || !response.GetIsLast() {
		t.Fatalf("out-of-scope delete response=%v, want EACCES refusal", response)
	}
	sendDelete(3, aliceWS)
	if response := receive(3); response.GetError() != "" {
		t.Fatalf("stream did not continue after refusal: %q", response.GetError())
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close mutate stream: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("mutate stream after close = %v, want EOF", err)
	}
	if got := len(h.fake.received("StreamMutateEntry")); got != 2 {
		t.Fatalf("handler received %d messages, want the two allowed messages", got)
	}
}

func TestScopeStreamExpiryStopsFurtherMessages(t *testing.T) {
	underlying := &scopeExpiryServerStream{ctx: context.Background()}
	wrapped := &scopedStream{
		ServerStream: underlying,
		sc:           &scope{exp: time.Now().Add(time.Minute)},
	}
	if err := wrapped.RecvMsg(&filer_pb.StreamMutateEntryRequest{}); err != nil {
		t.Fatalf("receive before expiry: %v", err)
	}
	if underlying.recvCalls != 1 {
		t.Fatalf("underlying receive calls=%d, want 1", underlying.recvCalls)
	}

	wrapped.sc.exp = time.Now().Add(-time.Second)
	expectScopeCode(t, "receive after token expiry", wrapped.RecvMsg(&filer_pb.StreamMutateEntryRequest{}), codes.Unauthenticated)
	expectScopeCode(t, "send after token expiry", wrapped.SendMsg(&filer_pb.StreamMutateEntryResponse{}), codes.Unauthenticated)
	if underlying.recvCalls != 1 {
		t.Fatalf("expired stream attempted another receive: calls=%d, want 1", underlying.recvCalls)
	}
	if underlying.sendCalls != 0 {
		t.Fatalf("expired response reached underlying stream: %d sends", underlying.sendCalls)
	}
}

func TestScopedStreamMutationResponsesHideMetadataMarker(t *testing.T) {
	marker := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, Extended: map[string][]byte{
			filer.ExtRemoteRenameSourceKey: []byte("/buckets/t1/source"),
		}}
	}
	rename := &filer_pb.StreamRenameEntryResponse{EventNotification: &filer_pb.EventNotification{
		OldEntry: marker("old"), NewEntry: marker("new"),
	}}
	mutate := &filer_pb.StreamMutateEntryResponse{Response: &filer_pb.StreamMutateEntryResponse_RenameResponse{
		RenameResponse: rename,
	}}
	underlying := &scopeExpiryServerStream{ctx: context.Background()}
	wrapped := &scopedStream{ServerStream: underlying, sc: &scope{exp: time.Now().Add(time.Minute)}}
	if err := wrapped.SendMsg(mutate); err != nil {
		t.Fatalf("send scoped mutation response: %v", err)
	}
	got, ok := underlying.sent.(*filer_pb.StreamMutateEntryResponse)
	if !ok {
		t.Fatalf("sent response type = %T, want StreamMutateEntryResponse", underlying.sent)
	}
	for _, entry := range []*filer_pb.Entry{
		got.GetRenameResponse().GetEventNotification().GetOldEntry(),
		got.GetRenameResponse().GetEventNotification().GetNewEntry(),
	} {
		if marker := entry.GetExtended()[filer.ExtRemoteRenameSourceKey]; len(marker) != 0 {
			t.Fatalf("scoped mutation response exposed remote rename source %q", marker)
		}
	}
	for _, entry := range []*filer_pb.Entry{rename.GetEventNotification().GetOldEntry(), rename.GetEventNotification().GetNewEntry()} {
		if marker := entry.GetExtended()[filer.ExtRemoteRenameSourceKey]; len(marker) == 0 {
			t.Fatal("scoped mutation redaction mutated handler response")
		}
	}
}

type scopeExpiryServerStream struct {
	ctx       context.Context
	recvCalls int
	sendCalls int
	sent      any
}

func (s *scopeExpiryServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *scopeExpiryServerStream) SendHeader(metadata.MD) error { return nil }
func (s *scopeExpiryServerStream) SetTrailer(metadata.MD)       {}
func (s *scopeExpiryServerStream) Context() context.Context     { return s.ctx }

func (s *scopeExpiryServerStream) SendMsg(m any) error {
	s.sendCalls++
	s.sent = m
	return nil
}

func (s *scopeExpiryServerStream) RecvMsg(any) error {
	s.recvCalls++
	return nil
}
