package weed_server

import (
	"context"
	"net/http/httptest"
	"sort"
	"strings"
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

func TestScopedSubscriptionKeepsOtherRegistrations(t *testing.T) {
	h := newSubscribeHarness(t)
	v := util.GetViper()
	v.Set("jwt.filer_signing.subscribe.key", "sub-key")
	t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", "") })
	if replacing, _, _ := h.fs.addClient("local", "mount", "10.0.0.1:1", "/", -9, 1); !replacing {
		t.Fatal("unrestricted registration was not added")
	}

	token := security.GenJwtForMetadataSubscription(security.SigningKey("sub-key"), 60, []string{"/t"})
	ctx, cancel := context.WithCancel(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(token))))
	stream := &fakeSubscribeStream{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		done <- h.fs.SubscribeLocalMetadata(&filer_pb.SubscribeMetadataRequest{ClientName: "scoped", ClientId: 9, ClientEpoch: 100, SinceNs: 1}, stream)
	}()

	now := time.Now().UnixNano()
	h.append(now)
	deadline := time.Now().Add(5 * time.Second)
	for len(stream.snapshot()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("scoped subscription delivered nothing")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !h.fs.hasClient(-9, 1) {
		t.Fatal("scoped caller replaced the unrestricted registration")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("scoped subscription did not end")
	}
	if !h.fs.hasClient(-9, 1) {
		t.Fatal("scoped caller removed the unrestricted registration on exit")
	}
}

func TestScopedRegistrationNeverReplacesLaterRegistration(t *testing.T) {
	h := newSubscribeHarness(t)
	id := h.fs.unusedClientId()
	if replacing, _, _ := h.fs.addClient("", "mount", "10.0.0.1:1", "/", id, 1); !replacing {
		t.Fatal("unrestricted registration was not added")
	}
	if replacing, alreadyKnown, _ := h.fs.addClient("", "scoped", "10.0.0.2:1", "/t", id, scopedClientEpoch); replacing || !alreadyKnown {
		t.Fatalf("scoped registration replacing=%v alreadyKnown=%v, want refused", replacing, alreadyKnown)
	}
	h.fs.deleteClient("", "scoped", id, scopedClientEpoch)
	if !h.fs.hasClient(id, 1) {
		t.Fatal("unrestricted registration lost to a scoped caller")
	}
}

func TestScopedStreamMarksWithheldProgress(t *testing.T) {
	var sent []*filer_pb.SubscribeMetadataResponse
	send := func(m *filer_pb.SubscribeMetadataResponse) error {
		sent = append(sent, m)
		return nil
	}
	s, cancel := newScopedMetadataStream(context.Background(), &security.MetadataSubscriptionScope{Prefixes: []string{"/a"}, ExpiresAt: time.Now().Add(time.Hour)})
	defer cancel()
	var last int64
	for i := int64(1); i <= MaxUnsyncedEvents+1; i++ {
		last = i
		if err := s.send(scopeTestEvent("/other", "x", i), send); err != nil {
			t.Fatal(err)
		}
	}
	if len(sent) != 1 {
		t.Fatalf("sent %d messages, want one position marker", len(sent))
	}
	m := sent[0]
	if m.TsNs != last || !filer_pb.IsEmpty(m) || m.Directory != "" || len(m.Events) > 0 {
		t.Fatalf("marker %+v, want an empty event at %d", m, last)
	}
}

func TestScopedStreamMarksWithheldBatch(t *testing.T) {
	var sent []*filer_pb.SubscribeMetadataResponse
	send := func(m *filer_pb.SubscribeMetadataResponse) error {
		sent = append(sent, m)
		return nil
	}
	s, cancel := newScopedMetadataStream(context.Background(), &security.MetadataSubscriptionScope{Prefixes: []string{"/a"}, ExpiresAt: time.Now().Add(time.Hour)})
	defer cancel()
	batch := scopeTestEvent("/other", "head", 1)
	for i := int64(2); i <= MaxUnsyncedEvents+1; i++ {
		batch.Events = append(batch.Events, scopeTestEvent("/other", "x", i))
	}
	if err := s.send(batch, send); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 {
		t.Fatalf("sent %d messages, want one position marker", len(sent))
	}
	m, want := sent[0], int64(MaxUnsyncedEvents+1)
	if m.TsNs != want || !filer_pb.IsEmpty(m) || m.Directory != "" || len(m.Events) > 0 {
		t.Fatalf("marker %+v, want an empty event at %d", m, want)
	}
}

func TestScopedStreamSendMsgIsFiltered(t *testing.T) {
	scoped, cancel := newScopedMetadataStream(context.Background(), &security.MetadataSubscriptionScope{Prefixes: []string{"/a"}, ExpiresAt: time.Now().Add(time.Hour)})
	defer cancel()
	for _, s := range []interface{ SendMsg(any) error }{
		&scopedSubscribeStream{SeaweedFiler_SubscribeMetadataServer: &fakeSubscribeStream{ctx: context.Background()}, scopedMetadataStream: scoped},
		&scopedSubscribeLocalStream{SeaweedFiler_SubscribeLocalMetadataServer: &fakeSubscribeStream{ctx: context.Background()}, scopedMetadataStream: scoped},
	} {
		if err := s.SendMsg(scopeTestEvent("/b", "x", 1)); err != nil {
			t.Fatal(err)
		}
		if err := s.SendMsg(scopeTestEvent("/a", "x", 2)); err != nil {
			t.Fatal(err)
		}
		if err := s.SendMsg(&filer_pb.Entry{}); err == nil {
			t.Error("an unexpected message type must be refused")
		}
		var inner *fakeSubscribeStream
		switch w := s.(type) {
		case *scopedSubscribeStream:
			inner = w.SeaweedFiler_SubscribeMetadataServer.(*fakeSubscribeStream)
		case *scopedSubscribeLocalStream:
			inner = w.SeaweedFiler_SubscribeLocalMetadataServer.(*fakeSubscribeStream)
		}
		if msgs := inner.snapshot(); len(msgs) != 1 || msgs[0].Directory != "/a" {
			t.Errorf("%T sent %v", s, msgs)
		}
	}
}

func scopedLogEntry(tsNs int64, dir, name, newParent string, oldEntry bool) *filer_pb.LogEntry {
	n := &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: name}, NewParentPath: newParent}
	if oldEntry {
		n.OldEntry = &filer_pb.Entry{Name: name}
	}
	data, err := proto.Marshal(&filer_pb.SubscribeMetadataResponse{Directory: dir, EventNotification: n, TsNs: tsNs})
	if err != nil {
		panic(err)
	}
	return &filer_pb.LogEntry{TsNs: tsNs, Data: data, Key: []byte(dir + "/" + name)}
}

// The real subscribe loop with a scoped token: flushed backlog, the memory
// tail and live events are all filtered, and log file references are not sent
// although the client asks for them.
func TestSubscribeLocalMetadataScopedEndToEnd(t *testing.T) {
	for _, enforce := range []bool{true, false} {
		t.Run(map[bool]string{true: "scoped", false: "key unset"}[enforce], func(t *testing.T) {
			h := newSubscribeHarness(t)
			if enforce {
				v := util.GetViper()
				v.Set("jwt.filer_signing.subscribe.key", "sub-key")
				t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", "") })
			}

			ts := func(i int) int64 { return h.base + int64(i)*int64(time.Millisecond) }
			entries := []*filer_pb.LogEntry{
				scopedLogEntry(ts(0), "/buckets/uploads", "in-flushed", "", false),
				scopedLogEntry(ts(1), "/buckets/uploads-private", "sibling", "", false),
				scopedLogEntry(ts(2), "/etc/iam", "identity.json", "", false),
				scopedLogEntry(ts(3), "/buckets/uploads/..", "climb", "", false),
				scopedLogEntry(ts(4), "/buckets/uploads", "renamed-out", "/buckets/other", true),
				scopedLogEntry(ts(5), "/buckets/uploads/2026", "in-memory", "", false),
				scopedLogEntry(ts(6), "/buckets/uploads%2F..%2Fother", "encoded", "", false),
			}
			for i, e := range entries {
				if err := h.f.LocalMetaLogBuffer.AddLogEntryToBuffer(e); err != nil {
					t.Fatal(err)
				}
				if i == 4 {
					h.f.LocalMetaLogBuffer.ForceFlush()
				}
			}

			token := security.GenJwtForMetadataSubscription(security.SigningKey("sub-key"), 60, []string{"/buckets/uploads"})
			ctx, cancel := context.WithCancel(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+string(token))))
			defer cancel()
			stream := &fakeSubscribeStream{ctx: ctx}
			done := make(chan error, 1)
			go func() {
				done <- h.fs.SubscribeLocalMetadata(&filer_pb.SubscribeMetadataRequest{
					ClientName: "scope-test", ClientId: 9, ClientEpoch: 1, PathPrefix: "/",
					ClientSupportsMetadataChunks: true,
				}, stream)
			}()

			time.Sleep(100 * time.Millisecond)
			for _, e := range []*filer_pb.LogEntry{
				scopedLogEntry(ts(8), "/", "outside-live", "", false),
				scopedLogEntry(ts(7), "/buckets/uploads", "in-live", "", false),
			} {
				if err := h.f.LocalMetaLogBuffer.AddLogEntryToBuffer(e); err != nil {
					t.Fatal(err)
				}
			}

			want := "in-flushed,in-live,in-memory"
			var got []string
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				got = got[:0]
				for _, m := range stream.snapshot() {
					if enforce && len(m.LogFileRefs) > 0 {
						t.Fatal("a scoped subscriber was sent log file references")
					}
					for _, e := range append([]*filer_pb.SubscribeMetadataResponse{m}, m.Events...) {
						if n := e.GetEventNotification(); n.GetNewEntry() != nil {
							got = append(got, n.NewEntry.Name)
						}
					}
				}
				if (enforce && len(got) >= 3) || (!enforce && strings.Contains(strings.Join(got, ","), "outside-live")) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			sort.Strings(got)
			if enforce && strings.Join(got, ",") != want {
				t.Fatalf("delivered %v, want %s", got, want)
			}
			if !enforce && !strings.Contains(strings.Join(got, ","), "outside-live") {
				t.Fatalf("without the subscribe key every event must still arrive; got %v", got)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("subscription did not end on cancel")
			}
		})
	}
}

// A subscription token opens no filer HTTP read or write, also when an
// operator reuses the read or write key as the subscribe key.
func TestMetadataSubscriptionTokenRefusedOverHTTP(t *testing.T) {
	for _, keys := range []struct{ write, read, subscribe string }{
		{"write-key", "read-key", "sub-key"},
		{"write-key", "same", "same"},
		{"same", "read-key", "same"},
		{"same", "same", "same"},
	} {
		fs := &FilerServer{filerGuard: security.NewGuard(nil, keys.write, 0, keys.read, 0, "", "")}
		token := string(security.GenJwtForMetadataSubscription(security.SigningKey(keys.subscribe), 60, []string{"/buckets/uploads"}))
		for _, c := range []struct {
			method  string
			isWrite bool
		}{{"GET", false}, {"HEAD", false}, {"PUT", true}, {"POST", true}, {"DELETE", true}} {
			for _, target := range []string{"/buckets/uploads/a.txt", "/buckets/uploads/", "/etc/iam/identity.json"} {
				r := httptest.NewRequest(c.method, target, nil)
				r.Header.Set("Authorization", "Bearer "+token)
				if fs.maybeCheckJwtAuthorization(r, c.isWrite) {
					t.Errorf("keys %+v: %s %s accepted a subscription token", keys, c.method, target)
				}
			}
		}
	}
}
