package weed_server

import (
	"context"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

const (
	scopeTestReadKey  = "filer-read-key"
	scopeTestWriteKey = "filer-write-key"
)

func ev(dir, name string) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory:         dir,
		TsNs:              1,
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: name}},
	}
}

func TestScopeMetadataResponse(t *testing.T) {
	prefixes := []string{"/buckets/uploads"}

	if got := scopeMetadataResponse(ev("/buckets/uploads/2026", "a.txt"), prefixes); got == nil {
		t.Error("an event under the prefix must pass")
	}
	if got := scopeMetadataResponse(ev("/buckets/other", "a.txt"), prefixes); got != nil {
		t.Error("an event outside the prefix must be dropped")
	}
	if got := scopeMetadataResponse(ev("/buckets/uploads-private", "a.txt"), prefixes); got != nil {
		t.Error("a sibling sharing the prefix's characters must be dropped")
	}
	if got := scopeMetadataResponse(ev("/buckets/uploads/..", "etc"), prefixes); got != nil {
		t.Error("a directory that climbs out of the prefix must be dropped")
	}
	if got := scopeMetadataResponse(ev("/buckets", "uploads"), prefixes); got == nil {
		t.Error("the prefix directory itself is in scope")
	}

	// A delete names only the old entry.
	del := &filer_pb.SubscribeMetadataResponse{Directory: "/etc", EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "passwd"}}}
	if got := scopeMetadataResponse(del, prefixes); got != nil {
		t.Error("a delete outside the prefix must be dropped")
	}

	// A rename from inside to outside, or the reverse, names a path outside.
	out := ev("/buckets/uploads", "a.txt")
	out.EventNotification.OldEntry = &filer_pb.Entry{Name: "a.txt"}
	out.EventNotification.NewParentPath = "/buckets/other"
	if got := scopeMetadataResponse(out, prefixes); got != nil {
		t.Error("a rename out of the scope must be dropped")
	}
	in := ev("/buckets/other", "a.txt")
	in.EventNotification.OldEntry = &filer_pb.Entry{Name: "a.txt"}
	in.EventNotification.NewParentPath = "/buckets/uploads"
	if got := scopeMetadataResponse(in, prefixes); got != nil {
		t.Error("a rename into the scope must be dropped")
	}

	// A heartbeat or position marker names no path and passes.
	if got := scopeMetadataResponse(&filer_pb.SubscribeMetadataResponse{TsNs: 5}, prefixes); got == nil {
		t.Error("a marker must pass")
	}

	// Log file references are never passed on, and a batch is filtered.
	batch := ev("/buckets/other", "x")
	batch.LogFileRefs = []*filer_pb.LogFileChunkRef{{}}
	batch.Events = []*filer_pb.SubscribeMetadataResponse{ev("/buckets/uploads", "ok"), ev("/etc", "secret")}
	got := scopeMetadataResponse(batch, prefixes)
	if got == nil || len(got.Events) != 1 || got.Events[0].Directory != "/buckets/uploads" || got.LogFileRefs != nil {
		t.Fatalf("batch not filtered: %+v", got)
	}
	if got.EventNotification.GetNewEntry() != nil || got.EventNotification.GetOldEntry() != nil {
		t.Error("the out-of-scope head of a batch must be replaced by a marker")
	}
	refsOnly := &filer_pb.SubscribeMetadataResponse{TsNs: 9, LogFileRefs: []*filer_pb.LogFileChunkRef{{}}}
	if got := scopeMetadataResponse(refsOnly, prefixes); got != nil && len(got.LogFileRefs) > 0 {
		t.Error("log file references must never be sent")
	}
}

// SendMsg on the wrapper must not reach the client unfiltered.
func TestScopedStreamSendMsgIsFiltered(t *testing.T) {
	inner := &fakeSubscribeStream{ctx: context.Background()}
	s := &scopedSubscribeStream{SeaweedFiler_SubscribeMetadataServer: inner, prefixes: []string{"/a"}}
	if err := s.SendMsg(ev("/b", "x")); err != nil {
		t.Fatal(err)
	}
	if err := s.SendMsg(ev("/a", "x")); err != nil {
		t.Fatal(err)
	}
	if err := s.SendMsg(&filer_pb.Entry{}); err == nil {
		t.Error("an unexpected message type must be refused")
	}
	if msgs := inner.snapshot(); len(msgs) != 1 || msgs[0].Directory != "/a" {
		t.Errorf("sent %v", msgs)
	}
}

func scopeTestToken(t *testing.T, key string, prefixes, methods []string, expiresIn time.Duration) string {
	t.Helper()
	claims := security.SeaweedFilerClaims{AllowedPrefixes: prefixes, AllowedMethods: methods,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiresIn))}}
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func bearerContext(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}

// A subscription token is refused by the filer's HTTP API for reads and
// writes, also when the read and write keys are the same.
func TestMetadataSubscriptionTokenRefusedOverHTTP(t *testing.T) {
	for _, keys := range [][2]string{{scopeTestWriteKey, scopeTestReadKey}, {"same", "same"}} {
		fs := &FilerServer{filerGuard: security.NewGuard(nil, keys[0], 0, keys[1], 0)}
		tok := string(security.GenJwtForMetadataSubscription(security.SigningKey(keys[1]), 60, []string{"/buckets/uploads"}))
		for _, c := range []struct {
			method  string
			isWrite bool
		}{{"GET", false}, {"HEAD", false}, {"PUT", true}, {"POST", true}, {"DELETE", true}} {
			for _, target := range []string{"/buckets/uploads/a.txt", "/buckets/uploads/", "/etc/iam/identity.json"} {
				r := httptest.NewRequest(c.method, target, nil)
				r.Header.Set("Authorization", "Bearer "+tok)
				if fs.maybeCheckJwtAuthorization(r, c.isWrite) {
					t.Errorf("keys %v: %s %s accepted a subscription token", keys, c.method, target)
				}
			}
		}
	}
}

func TestMetadataSubscriptionScopeOffByDefault(t *testing.T) {
	fs := &FilerServer{filerGuard: security.NewGuard(nil, scopeTestWriteKey, 0, scopeTestReadKey, 0)}
	fs.loadMetadataSubscribeAuth(testConfig{"jwt.filer_signing.read.key": scopeTestReadKey})
	scope, err := fs.metadataSubscriptionScope(context.Background(), security.MethodSubscribeMetadata)
	if err != nil || scope != nil {
		t.Errorf("unset option: scope %v, err %v; want unchanged, unrestricted behaviour", scope, err)
	}
}

// Both handlers refuse before reading the log; fs has no filer, so a call
// that got past the check would panic.
func TestSubscribeHandlersRefuseWithoutValidToken(t *testing.T) {
	fs := &FilerServer{filerGuard: security.NewGuard(nil, scopeTestWriteKey, 0, scopeTestReadKey, 0)}
	fs.metadataSubscribeAuth.Store(true)

	onlyMeta := []string{security.MethodSubscribeMetadata}
	cases := map[string]string{
		"no token":                 "",
		"expired":                  scopeTestToken(t, scopeTestReadKey, []string{"/a"}, onlyMeta, -time.Second),
		"signed with write key":    scopeTestToken(t, scopeTestWriteKey, []string{"/a"}, onlyMeta, time.Minute),
		"wrong signature":          scopeTestToken(t, "guess", []string{"/a"}, onlyMeta, time.Minute),
		"write methods":            scopeTestToken(t, scopeTestReadKey, []string{"/a"}, []string{"PUT", "CreateEntry"}, time.Minute),
		"plain read token":         string(security.GenJwtForFilerServer(security.SigningKey(scopeTestReadKey), 60)),
		"dot-dot prefix":           scopeTestToken(t, scopeTestReadKey, []string{"/a/../.."}, onlyMeta, time.Minute),
		"url-encoded dot-dot path": scopeTestToken(t, scopeTestReadKey, []string{"/a/%2e%2e/"}, onlyMeta, time.Minute),
	}
	for name, tok := range cases {
		ctx := bearerContext(context.Background(), tok)
		req := &filer_pb.SubscribeMetadataRequest{PathPrefix: "/"}
		if err := fs.SubscribeMetadata(req, &fakeSubscribeStream{ctx: ctx}); status.Code(err) != codes.Unauthenticated {
			t.Errorf("SubscribeMetadata %s: err %v", name, err)
		}
		if err := fs.SubscribeLocalMetadata(req, &fakeSubscribeStream{ctx: ctx}); status.Code(err) != codes.Unauthenticated {
			t.Errorf("SubscribeLocalMetadata %s: err %v", name, err)
		}
	}
	// A token for SubscribeMetadata only is refused on SubscribeLocalMetadata.
	tok := scopeTestToken(t, scopeTestReadKey, []string{"/a"}, onlyMeta, time.Minute)
	err := fs.SubscribeLocalMetadata(&filer_pb.SubscribeMetadataRequest{}, &fakeSubscribeStream{ctx: bearerContext(context.Background(), tok)})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("token for the other call: err %v", err)
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

// The real subscribe loop, with a scoped token: flushed backlog, the memory
// tail and live events are all filtered, and log file references are not sent
// although the client asks for them.
func TestSubscribeLocalMetadataScopedEndToEnd(t *testing.T) {
	for _, enforce := range []bool{true, false} {
		t.Run(map[bool]string{true: "scoped", false: "option unset"}[enforce], func(t *testing.T) {
			h := newSubscribeHarness(t)
			h.fs.filerGuard = security.NewGuard(nil, scopeTestWriteKey, 0, scopeTestReadKey, 0)
			h.fs.metadataSubscribeAuth.Store(enforce)

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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = bearerContext(ctx, string(security.GenJwtForMetadataSubscription(security.SigningKey(scopeTestReadKey), 60, []string{"/buckets/uploads"})))
			stream := &fakeSubscribeStream{ctx: ctx}
			done := make(chan error, 1)
			go func() {
				done <- h.fs.SubscribeLocalMetadata(&filer_pb.SubscribeMetadataRequest{
					ClientName: "scope-test", ClientId: 9, ClientEpoch: 1, PathPrefix: "/",
					ClientSupportsMetadataChunks: true,
				}, stream)
			}()

			live := scopedLogEntry(ts(7), "/buckets/uploads", "in-live", "", false)
			liveOut := scopedLogEntry(ts(8), "/", "outside-live", "", false)
			time.Sleep(100 * time.Millisecond)
			for _, e := range []*filer_pb.LogEntry{liveOut, live} {
				if err := h.f.LocalMetaLogBuffer.AddLogEntryToBuffer(e); err != nil {
					t.Fatal(err)
				}
			}

			want := []string{"in-flushed", "in-live", "in-memory"}
			if !enforce {
				want = nil // everything, refs or inline: the stream is unchanged
			}
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
				if (enforce && len(got) >= len(want)) || (!enforce && strings.Contains(strings.Join(got, ","), "outside-live")) {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			sort.Strings(got)
			if enforce && strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("delivered %v, want %v", got, want)
			}
			if !enforce && !strings.Contains(strings.Join(got, ","), "outside-live") {
				t.Fatalf("with the option unset every event must still arrive; got %v", got)
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
