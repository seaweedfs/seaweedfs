package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func scopeTestEvent(dir, name string, tsNs int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory:         dir,
		TsNs:              tsNs,
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: name}},
	}
}

func TestScopeMetadataResponse(t *testing.T) {
	prefixes := []string{"/buckets/uploads"}

	if got := scopeMetadataResponse(scopeTestEvent("/buckets/uploads/2026", "a.txt", 1), prefixes); got == nil {
		t.Error("an event under the prefix must pass")
	}
	if got := scopeMetadataResponse(scopeTestEvent("/buckets", "uploads", 1), prefixes); got == nil {
		t.Error("an event on the prefix itself must pass")
	}
	for _, dir := range []string{"/buckets/other", "/buckets/uploads-private", "/buckets/uploads/../other"} {
		if got := scopeMetadataResponse(scopeTestEvent(dir, "a.txt", 1), prefixes); got != nil {
			t.Errorf("an event in %s must be dropped", dir)
		}
	}

	out := scopeTestEvent("/buckets/uploads", "a.txt", 1)
	out.EventNotification.OldEntry = &filer_pb.Entry{Name: "a.txt"}
	out.EventNotification.NewParentPath = "/buckets/other"
	if got := scopeMetadataResponse(out, prefixes); got != nil {
		t.Error("a rename out of the scope must be dropped")
	}
	in := scopeTestEvent("/buckets/other", "a.txt", 1)
	in.EventNotification.OldEntry = &filer_pb.Entry{Name: "a.txt"}
	in.EventNotification.NewParentPath = "/buckets/uploads"
	if got := scopeMetadataResponse(in, prefixes); got != nil {
		t.Error("a rename into the scope must be dropped")
	}
	del := &filer_pb.SubscribeMetadataResponse{Directory: "/etc", EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "x"}}}
	if got := scopeMetadataResponse(del, prefixes); got != nil {
		t.Error("a delete outside the scope must be dropped")
	}

	for name, marker := range map[string]*filer_pb.SubscribeMetadataResponse{
		"heartbeat":    {TsNs: 5},
		"flush report": {FlushedTsNs: 5},
		"empty marker": {TsNs: 5, EventNotification: &filer_pb.EventNotification{}},
	} {
		if got := scopeMetadataResponse(marker, prefixes); got != marker {
			t.Errorf("%s must pass unchanged", name)
		}
	}
	if got := scopeMetadataResponse(&filer_pb.SubscribeMetadataResponse{LogFileRefs: []*filer_pb.LogFileChunkRef{{}}}, prefixes); got != nil {
		t.Error("log file refs must never be sent")
	}

	batch := scopeTestEvent("/buckets/other", "x", 10)
	batch.MutationTsNs = 9
	batch.FlushedTsNs = 7
	batch.Events = []*filer_pb.SubscribeMetadataResponse{scopeTestEvent("/buckets/uploads", "ok", 11), scopeTestEvent("/etc", "secret", 12)}
	got := scopeMetadataResponse(batch, prefixes)
	if got == nil || len(got.Events) != 1 || got.Events[0].Directory != "/buckets/uploads" {
		t.Fatalf("batch not filtered: %+v", got)
	}
	if got.Directory != "" || !filer_pb.IsEmpty(got) || got.MutationTsNs != 0 || got.TsNs != 10 || got.FlushedTsNs != 7 {
		t.Errorf("an out-of-scope batch head must become a marker at its position: %+v", got)
	}
	if len(batch.Events) != 2 {
		t.Error("the original response must not be modified")
	}

	inScopeHead := scopeTestEvent("/buckets/uploads", "head", 10)
	inScopeHead.Events = []*filer_pb.SubscribeMetadataResponse{scopeTestEvent("/etc", "secret", 11)}
	got = scopeMetadataResponse(inScopeHead, prefixes)
	if got == nil || got.EventNotification != inScopeHead.EventNotification || len(got.Events) != 0 {
		t.Errorf("an in-scope head keeps its event and drops the out-of-scope tail: %+v", got)
	}
}

func TestScopedSubscriptionEndsAtTokenExpiry(t *testing.T) {
	h := newSubscribeHarness(t)
	v := util.GetViper()
	v.Set("jwt.filer_signing.subscribe.key", "sub-key")
	t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", "") })
	token := security.GenJwtForMetadataSubscription(security.SigningKey("sub-key"), 2, []string{"/t"})
	ctx, cancel := context.WithCancel(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(token))))
	defer cancel()
	stream := &fakeSubscribeStream{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		done <- h.fs.SubscribeLocalMetadata(&filer_pb.SubscribeMetadataRequest{ClientName: "scoped", ClientId: 9, ClientEpoch: 1, SinceNs: 1}, stream)
	}()

	now := time.Now().UnixNano()
	h.append(now)
	outside, _ := proto.Marshal(&filer_pb.SubscribeMetadataResponse{Directory: "/other", TsNs: now + 1, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "x"}}})
	if err := h.f.LocalMetaLogBuffer.AddLogEntryToBuffer(&filer_pb.LogEntry{TsNs: now + 1, Data: outside, Key: []byte("/other/x")}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("stream ended with %v, want Unauthenticated at token expiry", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("scoped subscription outlived its token")
	}
	for _, m := range stream.snapshot() {
		if m.Directory == "/other" {
			t.Fatalf("out-of-scope event delivered: %+v", m)
		}
	}
	if got := eventTimestamps(stream.snapshot()); len(got) != 1 || got[0] != now {
		t.Fatalf("delivered %v, want only the in-scope event", got)
	}
}
