package weed_server

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

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
