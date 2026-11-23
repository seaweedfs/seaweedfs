package weed_server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestScopeListEntriesBoundsDirectoriesAndFiltersFilerConf(t *testing.T) {
	h := newScopeHarness(t)
	h.fake.put(scopeFilerConfDir+"/"+scopeFilerConfName, entryWith(scopeFilerConfName))
	h.fake.put(scopeFilerConfDir+"/visible.json", entryWith("visible.json"))
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
		names[response.GetEntry().GetName()] = true
	}
	if names[scopeFilerConfName] {
		t.Fatal("scoped list exposed filer.conf")
	}
	if !names["visible.json"] {
		t.Fatalf("scoped list dropped visible entry: %v", names)
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

type scopeExpiryServerStream struct {
	ctx       context.Context
	recvCalls int
	sendCalls int
}

func (s *scopeExpiryServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *scopeExpiryServerStream) SendHeader(metadata.MD) error { return nil }
func (s *scopeExpiryServerStream) SetTrailer(metadata.MD)       {}
func (s *scopeExpiryServerStream) Context() context.Context     { return s.ctx }

func (s *scopeExpiryServerStream) SendMsg(any) error {
	s.sendCalls++
	return nil
}

func (s *scopeExpiryServerStream) RecvMsg(any) error {
	s.recvCalls++
	return nil
}
