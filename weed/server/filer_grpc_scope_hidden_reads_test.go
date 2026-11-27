package weed_server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestScopeLookupHidesRemoteRenameSourceKey(t *testing.T) {
	h := newScopeHarness(t)
	entry := entryWith("visible", "1,abc")
	entry.Extended = map[string][]byte{
		filer.ExtRemoteRenameSourceKey: []byte("/buckets/t1/source"),
		"client-tag":                   []byte("keep"),
	}
	h.fake.put(aliceWS+"/visible", entry)
	h.fake.put(aliceWS+"/other", entryWith("other"))
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	single, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "visible"})
	if err != nil {
		t.Fatalf("single lookup: %v", err)
	}
	if got := single.GetEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) != 0 {
		t.Fatalf("single scoped lookup exposed remote rename source %q", got)
	}
	if got := string(single.GetEntry().GetExtended()["client-tag"]); got != "keep" {
		t.Fatalf("single scoped lookup lost client metadata %q", got)
	}

	batch, err := c.LookupDirectoryEntries(scopeCtx(t), &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{
		{Directory: aliceWS, Name: "visible"},
		{Directory: aliceWS, Name: "other"},
	}})
	if err != nil {
		t.Fatalf("batched lookup: %v", err)
	}
	if got := batch.GetResults()[0].GetEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) != 0 {
		t.Fatalf("batched scoped lookup exposed remote rename source %q", got)
	}
	stored, err := h.fake.scopeFindEntry(context.Background(), aliceWS+"/visible")
	if err != nil {
		t.Fatalf("read stored entry: %v", err)
	}
	if got := stored.GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) == 0 {
		t.Fatal("scoped lookup redaction mutated the stored entry")
	}
	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "tokenless"},
		{name: "unscoped", token: unscopedToken()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := h.client(tc.token).LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "visible"})
			if err != nil {
				t.Fatalf("%s lookup: %v", tc.name, err)
			}
			if got := resp.GetEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]; len(got) == 0 {
				t.Fatalf("%s lookup lost remote rename source", tc.name)
			}
		})
	}
}

func TestScopeResponseRedactionCoversMutationCarriers(t *testing.T) {
	marker := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, Extended: map[string][]byte{
			filer.ExtRemoteRenameSourceKey: []byte("/buckets/t1/source"),
		}}
	}
	metadata := func() *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{EventNotification: &filer_pb.EventNotification{
			OldEntry: marker("old"),
			NewEntry: marker("new"),
		}}
	}
	tests := []struct {
		name    string
		resp    any
		entries func(any) []*filer_pb.Entry
	}{
		{
			name: "create metadata",
			resp: &filer_pb.CreateEntryResponse{MetadataEvent: metadata()},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.CreateEntryResponse).GetMetadataEvent().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "update metadata",
			resp: &filer_pb.UpdateEntryResponse{MetadataEvent: metadata()},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.UpdateEntryResponse).GetMetadataEvent().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "delete metadata",
			resp: &filer_pb.DeleteEntryResponse{MetadataEvent: metadata()},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.DeleteEntryResponse).GetMetadataEvent().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "stream rename",
			resp: &filer_pb.StreamRenameEntryResponse{EventNotification: metadata().GetEventNotification()},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.StreamRenameEntryResponse).GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "stream mutate create",
			resp: &filer_pb.StreamMutateEntryResponse{Response: &filer_pb.StreamMutateEntryResponse_CreateResponse{
				CreateResponse: &filer_pb.CreateEntryResponse{MetadataEvent: metadata()},
			}},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.StreamMutateEntryResponse).GetCreateResponse().GetMetadataEvent().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "stream mutate update",
			resp: &filer_pb.StreamMutateEntryResponse{Response: &filer_pb.StreamMutateEntryResponse_UpdateResponse{
				UpdateResponse: &filer_pb.UpdateEntryResponse{MetadataEvent: metadata()},
			}},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.StreamMutateEntryResponse).GetUpdateResponse().GetMetadataEvent().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "stream mutate delete",
			resp: &filer_pb.StreamMutateEntryResponse{Response: &filer_pb.StreamMutateEntryResponse_DeleteResponse{
				DeleteResponse: &filer_pb.DeleteEntryResponse{MetadataEvent: metadata()},
			}},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.StreamMutateEntryResponse).GetDeleteResponse().GetMetadataEvent().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
		{
			name: "stream mutate rename",
			resp: &filer_pb.StreamMutateEntryResponse{Response: &filer_pb.StreamMutateEntryResponse_RenameResponse{
				RenameResponse: &filer_pb.StreamRenameEntryResponse{EventNotification: metadata().GetEventNotification()},
			}},
			entries: func(resp any) []*filer_pb.Entry {
				event := resp.(*filer_pb.StreamMutateEntryResponse).GetRenameResponse().GetEventNotification()
				return []*filer_pb.Entry{event.GetOldEntry(), event.GetNewEntry()}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			redacted := scopeRedactResponse(tc.resp)
			if redacted == tc.resp {
				t.Fatal("response was not cloned before redaction")
			}
			for _, entry := range tc.entries(redacted) {
				if marker := entry.GetExtended()[filer.ExtRemoteRenameSourceKey]; len(marker) != 0 {
					t.Fatalf("redacted response exposed remote rename source %q", marker)
				}
			}
			for _, entry := range tc.entries(tc.resp) {
				if marker := entry.GetExtended()[filer.ExtRemoteRenameSourceKey]; len(marker) == 0 {
					t.Fatal("redaction mutated the source response")
				}
			}
		})
	}
}

func TestScopedUnaryMutationResponseHidesMetadataMarker(t *testing.T) {
	h := newScopeHarness(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+scopedToken("alice", time.Hour, []string{aliceWS}, nil),
	))
	metadataEvent := func() *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Extended: map[string][]byte{
				filer.ExtRemoteRenameSourceKey: []byte("/buckets/t1/source"),
			}},
		}}
	}
	cases := []struct {
		name     string
		method   string
		request  any
		response any
		marker   func(any) []byte
	}{
		{
			name:   "create",
			method: "CreateEntry",
			request: &filer_pb.CreateEntryRequest{
				Directory: aliceWS, Entry: entryWith("created"), SkipCheckParentDirectory: true,
			},
			response: &filer_pb.CreateEntryResponse{MetadataEvent: metadataEvent()},
			marker: func(resp any) []byte {
				return resp.(*filer_pb.CreateEntryResponse).GetMetadataEvent().GetEventNotification().GetNewEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]
			},
		},
		{
			name:     "update",
			method:   "UpdateEntry",
			request:  &filer_pb.UpdateEntryRequest{Directory: aliceWS, Entry: entryWith("updated")},
			response: &filer_pb.UpdateEntryResponse{MetadataEvent: metadataEvent()},
			marker: func(resp any) []byte {
				return resp.(*filer_pb.UpdateEntryResponse).GetMetadataEvent().GetEventNotification().GetNewEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]
			},
		},
		{
			name:     "delete",
			method:   "DeleteEntry",
			request:  &filer_pb.DeleteEntryRequest{Directory: aliceWS, Name: "deleted"},
			response: &filer_pb.DeleteEntryResponse{MetadataEvent: metadataEvent()},
			marker: func(resp any) []byte {
				return resp.(*filer_pb.DeleteEntryResponse).GetMetadataEvent().GetEventNotification().GetNewEntry().GetExtended()[filer.ExtRemoteRenameSourceKey]
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.response
			resp, err := h.scope.unary(ctx, tc.request, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + tc.method}, func(context.Context, any) (any, error) {
				return original, nil
			})
			if err != nil {
				t.Fatalf("scoped %s: %v", tc.method, err)
			}
			if got := tc.marker(resp); len(got) != 0 {
				t.Fatalf("scoped %s exposed remote rename source %q", tc.method, got)
			}
			if got := tc.marker(original); len(got) == 0 {
				t.Fatalf("scoped %s redaction mutated handler response", tc.method)
			}
			for _, caller := range []struct {
				name string
				ctx  context.Context
			}{
				{name: "tokenless", ctx: context.Background()},
				{name: "unscoped", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+unscopedToken()))},
			} {
				t.Run(caller.name, func(t *testing.T) {
					resp, err := h.scope.unary(caller.ctx, tc.request, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + tc.method}, func(context.Context, any) (any, error) {
						return original, nil
					})
					if err != nil {
						t.Fatalf("%s %s: %v", caller.name, tc.method, err)
					}
					if got := tc.marker(resp); len(got) == 0 {
						t.Fatalf("%s %s lost remote rename source", caller.name, tc.method)
					}
				})
			}
		})
	}
}

func TestScopeHidesFilerConfFromSingleAndBatchLookups(t *testing.T) {
	h := newScopeHarness(t)
	h.fake.put(scopeFilerConfDir+"/visible-one", entryWith("visible-one"))
	h.fake.put(scopeFilerConfDir+"/visible-two", entryWith("visible-two"))
	c := h.client(scopedToken("alice", time.Hour, []string{scopeFilerConfDir}, nil))

	before := len(h.fake.received("LookupDirectoryEntry"))
	_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: scopeFilerConfDir, Name: scopeFilerConfName})
	if err == nil || !strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
		t.Fatalf("single filer.conf lookup = %v, want not found", err)
	}
	if got := len(h.fake.received("LookupDirectoryEntry")); got != before {
		t.Fatalf("hidden single lookup reached handler: %d calls before=%d", got, before)
	}

	before = len(h.fake.received("LookupDirectoryEntries"))
	batch, err := c.LookupDirectoryEntries(scopeCtx(t), &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{
		{Directory: scopeFilerConfDir, Name: "visible-one"},
		{Directory: scopeFilerConfDir, Name: scopeFilerConfName},
		{Directory: scopeFilerConfDir, Name: "visible-two"},
	}})
	if err != nil {
		t.Fatalf("mixed lookup: %v", err)
	}
	if len(batch.GetResults()) != 3 {
		t.Fatalf("mixed lookup returned %d results, want 3", len(batch.GetResults()))
	}
	if !batch.GetResults()[0].GetFound() || batch.GetResults()[1].GetFound() || !batch.GetResults()[2].GetFound() {
		t.Fatalf("mixed lookup results = %v, want visible/miss/visible", batch.GetResults())
	}
	if got := len(h.fake.received("LookupDirectoryEntries")); got != before+1 {
		t.Fatalf("mixed lookup handler calls=%d, want %d", got, before+1)
	}
	forwarded := h.fake.received("LookupDirectoryEntries")
	if len(forwarded) != 1 || len(forwarded[0].(*filer_pb.LookupDirectoryEntriesRequest).GetRequests()) != 2 {
		t.Fatalf("mixed lookup forwarded request=%v, want two visible requests", forwarded)
	}

	before = len(h.fake.received("LookupDirectoryEntries"))
	hidden, err := c.LookupDirectoryEntries(scopeCtx(t), &filer_pb.LookupDirectoryEntriesRequest{Requests: []*filer_pb.LookupDirectoryEntryRequest{
		{Directory: scopeFilerConfDir, Name: scopeFilerConfName},
		{Directory: scopeFilerConfDir, Name: scopeFilerConfName},
	}})
	if err != nil {
		t.Fatalf("hidden-only lookup: %v", err)
	}
	if len(hidden.GetResults()) != 2 || hidden.GetResults()[0].GetFound() || hidden.GetResults()[1].GetFound() {
		t.Fatalf("hidden-only results = %v, want two misses", hidden.GetResults())
	}
	if got := len(h.fake.received("LookupDirectoryEntries")); got != before {
		t.Fatalf("hidden-only lookup reached handler: %d calls before=%d", got, before)
	}
}

func TestScopeHidesFilerConfWhenDirectoryNamesTheFile(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{scopeFilerConfDir}, nil))
	before := len(h.fake.received("LookupDirectoryEntry"))
	_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{
		Directory: scopeFilerConfDir + "/" + scopeFilerConfName,
	})
	if err == nil || !strings.Contains(err.Error(), filer_pb.ErrNotFound.Error()) {
		t.Fatalf("directory-form filer.conf lookup = %v, want not found", err)
	}
	if got := len(h.fake.received("LookupDirectoryEntry")); got != before {
		t.Fatalf("directory-form hidden lookup reached handler: %d calls before=%d", got, before)
	}
}

func TestScopeRejectsOversizedBatchBeforeFiltering(t *testing.T) {
	h := newScopeHarness(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+scopedToken("alice", time.Hour, []string{scopeFilerConfDir}, nil)))
	requests := make([]*filer_pb.LookupDirectoryEntryRequest, maxFilerBatchLookupRequests+1)
	requests[0] = &filer_pb.LookupDirectoryEntryRequest{Directory: scopeFilerConfDir, Name: scopeFilerConfName}
	for i := 1; i < len(requests); i++ {
		requests[i] = &filer_pb.LookupDirectoryEntryRequest{Directory: scopeFilerConfDir, Name: "visible-" + string(rune('a'+i%26))}
	}
	called := false
	_, err := h.scope.unary(ctx, &filer_pb.LookupDirectoryEntriesRequest{Requests: requests}, &grpc.UnaryServerInfo{
		FullMethod: scopeFilerMethodPrefix + "LookupDirectoryEntries",
	}, func(context.Context, any) (any, error) {
		called = true
		return &filer_pb.LookupDirectoryEntriesResponse{}, nil
	})
	expectScopeCode(t, "oversized filtered batch", err, codes.ResourceExhausted)
	if called {
		t.Fatal("oversized batch reached the handler after hidden-entry filtering")
	}
}
