package meta_cache

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestSectionOf(t *testing.T) {
	sl := newSectionTable([]string{"g", "p"})
	for name, want := range map[string]int{
		"a": 0, "f": 0,
		"g": 1, "h": 1, "o": 1,
		"p": 2, "z": 2,
	} {
		if got := sl.sectionOf(name); got != want {
			t.Errorf("sectionOf(%q) = %d, want %d", name, got, want)
		}
	}
	if lo, hi := sl.sectionRange(0); lo != "" || hi != "g" {
		t.Errorf("sectionRange(0) = %q..%q, want ..g", lo, hi)
	}
	if lo, hi := sl.sectionRange(1); lo != "g" || hi != "p" {
		t.Errorf("sectionRange(1) = %q..%q, want g..p", lo, hi)
	}
	if lo, hi := sl.sectionRange(2); lo != "p" || hi != "" {
		t.Errorf("sectionRange(2) = %q..%q, want p..", lo, hi)
	}
}

func buildSectionedDir(t *testing.T, mc *MetaCache, dir util.FullPath, snapshotTsNs int64, bounds []string) {
	t.Helper()
	if err := mc.BeginDirectoryBuild(context.Background(), dir); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.CompleteDirectoryBuild(context.Background(), dir, snapshotTsNs, bounds); err != nil {
		t.Fatalf("complete build: %v", err)
	}
}

func applyChurn(t *testing.T, mc *MetaCache, dir, prefix string, n int, baseTsNs int64, options MetadataResponseApplyOptions) {
	t.Helper()
	for i := 0; i < n; i++ {
		resp := &filer_pb.SubscribeMetadataResponse{
			Directory: dir,
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name: fmt.Sprintf("%s%03d", prefix, i),
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   1,
						Mtime:    1,
						FileMode: 0100644,
						FileSize: 1,
					},
				},
			},
			TsNs: baseTsNs + int64(i),
		}
		if err := mc.ApplyMetadataResponse(context.Background(), resp, options); err != nil {
			t.Fatalf("apply churn %d: %v", i, err)
		}
	}
}

func TestSectionBurstInvalidatesOnlyItsSection(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})

	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("burst section should be invalidated")
	}
	if !mc.IsNameFresh(util.FullPath("/dir/zzz")) {
		t.Fatal("the other section must stay fresh")
	}
}

func TestLocalChangesDoNotInvalidateSections(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)

	applyChurn(t, mc, "/dir", "a-", 2*sectionHotThreshold, 2000, LocalMetadataResponseApplyOptions)

	if !mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("this mount's own writes must not invalidate its cache")
	}
}

func TestSectionRefreshReconciles(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(func(entry *filer.Entry) bool {
		return entry.Name() == "b-pinned"
	})

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})

	for _, name := range []string{"b-keep", "b-vanish", "b-pinned"} {
		if err := mc.InsertEntry(context.Background(), &filer.Entry{
			FullPath: util.FullPath("/dir/" + name),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		}, 0); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	// applied ahead of the refresh snapshot; the listing must not roll it back
	if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "b-newer",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 9, FileMode: 0100644, FileSize: 9},
			},
		},
		TsNs: 9000,
	}, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply newer event: %v", err)
	}

	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)
	if mc.IsNameFresh(util.FullPath("/dir/b-keep")) {
		t.Fatal("section should be stale before the refresh")
	}

	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh: &sectionRefresh{
			hi: "m",
			entries: []*filer.Entry{
				{
					FullPath: util.FullPath("/dir/b-keep"),
					Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 7},
				},
				{
					FullPath: util.FullPath("/dir/b-new"),
					Attr:     filer.Attr{Crtime: time.Unix(2, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 3},
				},
			},
			snapshotTsNs: 5000,
		},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if !mc.IsNameFresh(util.FullPath("/dir/b-keep")) {
		t.Fatal("section should be fresh after the refresh")
	}
	entry, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-keep"))
	if err != nil || entry.FileSize != 7 {
		t.Fatalf("b-keep = %+v, %v; want size 7", entry, err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-new")); err != nil || entry.FileSize != 3 {
		t.Fatalf("b-new = %+v, %v; want size 3", entry, err)
	}
	if _, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-vanish")); err != filer_pb.ErrNotFound {
		t.Fatalf("b-vanish error = %v, want not found", err)
	}
	if _, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/a-000")); err != filer_pb.ErrNotFound {
		t.Fatalf("churned a-000 error = %v, want not found", err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-newer")); err != nil || entry.FileSize != 9 {
		t.Fatalf("b-newer = %+v, %v; want size 9 kept", entry, err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-pinned")); err != nil || entry == nil {
		t.Fatalf("pinned local-only entry must survive a refresh: %v", err)
	}

	// a redelivered create older than the snapshot must not resurrect b-vanish
	if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "b-vanish",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 1},
			},
		},
		TsNs: 4000,
	}, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply stale event: %v", err)
	}
	if _, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-vanish")); err != filer_pb.ErrNotFound {
		t.Fatalf("b-vanish resurrected by a pre-snapshot event: %v", err)
	}
}

func TestSectionRefreshSplitsOvergrownSection(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)

	count := 2*dirSectionSize + 1
	entries := make([]*filer.Entry, 0, count)
	for i := 0; i < count; i++ {
		entries = append(entries, &filer.Entry{
			FullPath: util.FullPath(fmt.Sprintf("/dir/f-%05d", i)),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		})
	}
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh:   &sectionRefresh{entries: entries, snapshotTsNs: 5000},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	mc.RLock()
	sl := mc.dirSections[util.FullPath("/dir")]
	bounds := append([]string(nil), sl.bounds...)
	sections := len(sl.sections)
	mc.RUnlock()
	want := []string{fmt.Sprintf("f-%05d", dirSectionSize), fmt.Sprintf("f-%05d", 2*dirSectionSize)}
	if len(bounds) != len(want) || bounds[0] != want[0] || bounds[1] != want[1] {
		t.Fatalf("bounds = %v, want %v", bounds, want)
	}
	if sections != len(bounds)+1 {
		t.Fatalf("sections = %d, want %d", sections, len(bounds)+1)
	}
}

type sectionFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu       sync.Mutex
	names    []string // sorted; all under one directory
	snapshot int64
	requests []*filer_pb.ListEntriesRequest
}

func (s *sectionFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	names := s.names
	snapshot := s.snapshot
	s.mu.Unlock()

	sent := uint32(0)
	first := true
	for _, name := range names {
		if name < req.StartFromFileName || (name == req.StartFromFileName && !req.InclusiveStartFrom) {
			continue
		}
		resp := &filer_pb.ListEntriesResponse{Entry: &filer_pb.Entry{
			Name:       name,
			Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 1},
		}}
		if first {
			resp.SnapshotTsNs = snapshot
			first = false
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		sent++
		if req.Limit > 0 && sent >= req.Limit {
			break
		}
	}
	return nil
}

func (s *sectionFilerServer) listRequests() []*filer_pb.ListEntriesRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*filer_pb.ListEntriesRequest(nil), s.requests...)
}

type sectionTestFilerClient struct {
	addr string
}

func (c *sectionTestFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(filer_pb.NewSeaweedFilerClient(conn))
}

func (c *sectionTestFilerClient) AdjustedUrl(location *filer_pb.Location) string { return location.Url }

func (c *sectionTestFilerClient) GetDataCenter() string { return "" }

func startSectionFilerServer(t *testing.T, s *sectionFilerServer) filer_pb.FilerClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, s)
	go srv.Serve(listener)
	t.Cleanup(srv.Stop)
	return &sectionTestFilerClient{addr: listener.Addr().String()}
}

func TestEnsureListingFreshRefreshesFromFiler(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	server := &sectionFilerServer{snapshot: 5000}
	for i := 0; i < 1500; i++ {
		server.names = append(server.names, fmt.Sprintf("b-%04d", i))
	}
	// at and beyond the section's end, must not be applied
	server.names = append(server.names, "m", "z-1")
	client := startSectionFilerServer(t, server)

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)
	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("section should be stale before the refresh")
	}

	if err := EnsureListingFresh(context.Background(), mc, client, util.FullPath("/dir"), ""); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if !mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("section should be fresh after the refresh")
	}
	for _, name := range []string{"b-0000", "b-1499"} {
		if entry, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/"+name)); err != nil || entry == nil {
			t.Fatalf("%s missing after refresh: %v", name, err)
		}
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/a-000")); err != filer_pb.ErrNotFound {
		t.Fatalf("churned a-000 error = %v, want not found", err)
	}
	for _, name := range []string{"m", "z-1"} {
		if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/"+name)); err != filer_pb.ErrNotFound {
			t.Fatalf("%s at or beyond the section's end was applied: %v", name, err)
		}
	}

	requests := server.listRequests()
	if len(requests) < 2 {
		t.Fatalf("multi-page refresh made %d requests, want at least 2", len(requests))
	}
	if requests[0].SnapshotTsNs != 0 || requests[1].SnapshotTsNs != 5000 {
		t.Fatalf("snapshot not pinned across pages: %d then %d", requests[0].SnapshotTsNs, requests[1].SnapshotTsNs)
	}

	// a fresh section costs no filer calls
	if err := EnsureListingFresh(context.Background(), mc, client, util.FullPath("/dir"), ""); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if got := len(server.listRequests()); got != len(requests) {
		t.Fatalf("fresh section still listed the filer: %d requests, was %d", got, len(requests))
	}
}

func TestEnsureListingFreshGivesUpOnOvergrownRange(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	server := &sectionFilerServer{snapshot: 5000}
	for i := 0; i <= sectionRefreshMaxEntries; i++ {
		server.names = append(server.names, fmt.Sprintf("c-%05d", i))
	}
	client := startSectionFilerServer(t, server)

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)
	applyChurn(t, mc, "/dir", "x-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	err := EnsureListingFresh(context.Background(), mc, client, util.FullPath("/dir"), "")
	if !errors.Is(err, ErrRefreshRangeTooLarge) {
		t.Fatalf("refresh error = %v, want ErrRefreshRangeTooLarge", err)
	}
	if mc.IsNameFresh(util.FullPath("/dir/x-000")) {
		t.Fatal("an aborted refresh must leave the section stale")
	}
}

func TestSectionNoteChangeWindowExpiry(t *testing.T) {
	sl := newSectionTable(nil)
	t0 := time.Unix(100, 0)
	for i := 0; i < sectionHotThreshold-1; i++ {
		sl.noteChange(fmt.Sprintf("f-%03d", i), t0)
	}
	if !sl.isFresh("f-000") {
		t.Fatal("below the threshold the section must stay fresh")
	}

	// the burst never completed inside one window, so the count restarts
	t1 := t0.Add(sectionHotWindow + time.Second)
	for i := 0; i < sectionHotThreshold-1; i++ {
		sl.noteChange(fmt.Sprintf("f-%03d", i), t1)
	}
	if !sl.isFresh("f-000") {
		t.Fatal("an expired window must not carry its count forward")
	}

	sl.noteChange("f-999", t1)
	if sl.isFresh("f-000") {
		t.Fatal("a full burst within one window must invalidate the section")
	}
}

func TestSectionCompleteRefreshGuardsChangedTable(t *testing.T) {
	sl := newSectionTable([]string{"g", "p"})
	sl.sections[1].stale = true

	if sl.completeRefresh("g", "q", []string{"h"}, 5000) {
		t.Fatal("a range the table no longer has must be ignored")
	}
	if sl.isFresh("h") {
		t.Fatal("an ignored refresh must not mark anything fresh")
	}
	if !sl.completeRefresh("g", "p", []string{"h"}, 5000) {
		t.Fatal("the matching range must be accepted")
	}
	if !sl.isFresh("h") {
		t.Fatal("section should be fresh after the refresh")
	}
}

func TestSectionCompleteRefreshSplitsMiddleSection(t *testing.T) {
	sl := newSectionTable([]string{"g", "p"})
	sl.sections[1].stale = true

	names := make([]string, 0, 2*dirSectionSize+1)
	for i := 0; i <= 2*dirSectionSize; i++ {
		names = append(names, fmt.Sprintf("g-%05d", i))
	}
	if !sl.completeRefresh("g", "p", names, 5000) {
		t.Fatal("refresh of the middle section must be accepted")
	}

	wantBounds := []string{"g", names[dirSectionSize], names[2*dirSectionSize], "p"}
	if len(sl.bounds) != len(wantBounds) {
		t.Fatalf("bounds = %v, want %v", sl.bounds, wantBounds)
	}
	for i, b := range wantBounds {
		if sl.bounds[i] != b {
			t.Fatalf("bounds = %v, want %v", sl.bounds, wantBounds)
		}
	}
	if len(sl.sections) != len(sl.bounds)+1 {
		t.Fatalf("sections = %d, want %d", len(sl.sections), len(sl.bounds)+1)
	}
	for _, name := range []string{"a", "g-00000", names[dirSectionSize], "z"} {
		if !sl.isFresh(name) {
			t.Fatalf("%q should be fresh after the split", name)
		}
	}
	if got := sl.sectionOf(names[dirSectionSize+1]); got != 2 {
		t.Fatalf("sectionOf(%q) = %d, want 2", names[dirSectionSize+1], got)
	}
}

func TestSectionFloorFencesAbsentNames(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh:   &sectionRefresh{hi: "m", snapshotTsNs: 5000},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// a delayed create between the directory floor and the refresh snapshot,
	// for a name neither the cache nor the listing ever held
	ghost := func(tsNs int64) {
		t.Helper()
		if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name:       "b-ghost",
					Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 1},
				},
			},
			TsNs: tsNs,
		}, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply ghost event: %v", err)
		}
	}
	ghost(4500)
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-ghost")); err != filer_pb.ErrNotFound {
		t.Fatalf("pre-snapshot event resurrected an absent name: %v", err)
	}
	ghost(6000)
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-ghost")); err != nil {
		t.Fatalf("post-snapshot event must apply: %v", err)
	}
}

func TestUnversionedRefreshStaysStale(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh: &sectionRefresh{entries: []*filer.Entry{{
			FullPath: util.FullPath("/dir/b-gap"),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		}}},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("an unversioned refresh vouches for nothing and must stay stale")
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-gap")); err != nil {
		t.Fatalf("gap fill should still land: %v", err)
	}
	if ranges := mc.staleRangesAhead(util.FullPath("/dir"), ""); len(ranges) != 0 {
		t.Fatalf("an unverifiable section must not be re-listed, got %d ranges", len(ranges))
	}
}

func TestEnsureListingFreshCoversAllStaleSectionsAhead(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	server := &sectionFilerServer{snapshot: 5000, names: []string{"b-1", "n-1"}}
	client := startSectionFilerServer(t, server)

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)
	applyChurn(t, mc, "/dir", "n-", sectionHotThreshold, 3000, SubscriberMetadataResponseApplyOptions)

	if err := EnsureListingFresh(context.Background(), mc, client, util.FullPath("/dir"), ""); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if !mc.IsNameFresh(util.FullPath("/dir/a-000")) || !mc.IsNameFresh(util.FullPath("/dir/n-000")) {
		t.Fatal("one call must re-validate every stale section ahead of the start")
	}
}

func TestSectionCompleteRefreshSetsFloors(t *testing.T) {
	sl := newSectionTable([]string{"g", "p"})
	sl.sections[1].stale = true

	names := make([]string, 0, 2*dirSectionSize+1)
	for i := 0; i <= 2*dirSectionSize; i++ {
		names = append(names, fmt.Sprintf("g-%05d", i))
	}
	if !sl.completeRefresh("g", "p", names, 7000) {
		t.Fatal("refresh must be accepted")
	}
	for _, name := range []string{"g", "g-99999", names[dirSectionSize+1]} {
		if got := sl.floorOf(name); got != 7000 {
			t.Fatalf("floorOf(%q) = %d, want 7000", name, got)
		}
	}
	if got := sl.floorOf("a"); got != 0 {
		t.Fatalf("floorOf(a) = %d, want 0 for a never-refreshed section", got)
	}
}

func TestSectionFloorOutranksOlderTombstone(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)

	event := func(tsNs int64, oldEntry, newEntry *filer_pb.Entry) {
		t.Helper()
		if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
			Directory:         "/dir",
			EventNotification: &filer_pb.EventNotification{OldEntry: oldEntry, NewEntry: newEntry},
			TsNs:              tsNs,
		}, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply event at %d: %v", tsNs, err)
		}
	}
	pbFile := &filer_pb.Entry{
		Name:       "b-x",
		Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 1},
	}
	event(1500, nil, pbFile)                       // create
	event(2000, &filer_pb.Entry{Name: "b-x"}, nil) // delete, leaves a tombstone at 2000

	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2500, SubscriberMetadataResponseApplyOptions)
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh:   &sectionRefresh{snapshotTsNs: 5000},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// the floor at 5000 outranks the tombstone at 2000
	event(4000, nil, pbFile)
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-x")); err != filer_pb.ErrNotFound {
		t.Fatalf("pre-floor event applied over an older tombstone: %v", err)
	}
	event(6000, nil, pbFile)
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-x")); err != nil {
		t.Fatalf("post-floor event must apply: %v", err)
	}
}

func TestMismatchedRefreshLeavesStoreAlone(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: util.FullPath("/dir/b-keep"),
		Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
	}, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	// a range the table does not have: the reconcile must not touch the store
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh: &sectionRefresh{hi: "q", snapshotTsNs: 5000, entries: []*filer.Entry{{
			FullPath: util.FullPath("/dir/b-new"),
			Attr:     filer.Attr{Crtime: time.Unix(2, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 3},
		}}},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-new")); err != filer_pb.ErrNotFound {
		t.Fatalf("mismatched refresh inserted an entry: %v", err)
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-keep")); err != nil {
		t.Fatalf("mismatched refresh swept an entry: %v", err)
	}
	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("mismatched refresh must not mark anything fresh")
	}
}

func TestRefreshClearsUnversionedMarker(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()
	mc.SetPinnedChildFn(func(entry *filer.Entry) bool {
		return entry.Name() == "b-pin"
	})

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)
	for _, name := range []string{"b-local", "b-pin"} {
		if err := mc.InsertEntry(context.Background(), &filer.Entry{
			FullPath: util.FullPath("/dir/" + name),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		}, 0); err != nil { // versionTsNs 0 leaves the unversioned marker
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	refreshed := func(name string) *filer.Entry {
		return &filer.Entry{
			FullPath: util.FullPath("/dir/" + name),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 7},
		}
	}
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh:   &sectionRefresh{snapshotTsNs: 5000, entries: []*filer.Entry{refreshed("b-local"), refreshed("b-pin")}},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	entry, versionTsNs, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-local"))
	if err != nil || entry.FileSize != 7 {
		t.Fatalf("b-local = %+v, %v; want the snapshot's size 7", entry, err)
	}
	if versionTsNs != 5000 {
		t.Fatalf("b-local version = %d, want the floor 5000 once the marker is cleared", versionTsNs)
	}

	// a delayed event below the floor must not roll the snapshot write back
	if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "b-local",
				Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 3, FileMode: 0100644, FileSize: 9},
			},
		},
		TsNs: 4000,
	}, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delayed event: %v", err)
	}
	if entry, _, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-local")); err != nil || entry.FileSize != 7 {
		t.Fatalf("b-local rolled back to %+v, %v", entry, err)
	}

	// pinned local-only state is neither replaced nor unmarked
	entry, versionTsNs, err = mc.FindEntry(context.Background(), util.FullPath("/dir/b-pin"))
	if err != nil || entry.FileSize != 1 {
		t.Fatalf("b-pin = %+v, %v; want local size 1 kept", entry, err)
	}
	if versionTsNs != 0 {
		t.Fatalf("b-pin version = %d, want 0 while pinned", versionTsNs)
	}
}

func TestRefreshSkipsBuildingDirectory(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, nil)
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	// a rebuild is streaming: its inserts must survive a refresh's sweep
	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: util.FullPath("/dir/b-built"),
		Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
	}, 0); err != nil {
		t.Fatalf("insert mid-build: %v", err)
	}

	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh:   &sectionRefresh{snapshotTsNs: 5000},
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/b-built")); err != nil {
		t.Fatalf("refresh swept a mid-build insert: %v", err)
	}
	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("a skipped refresh must not mark the section fresh")
	}
	if err := mc.AbortDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("abort build: %v", err)
	}
}

// TestSectionBorderEntries pins the border semantics: a bound-named entry
// belongs to the section starting at the bound, a refresh sweeps [lo, hi)
// inclusive of lo and exclusive of hi, and the neighboring sections' entries
// come through untouched.
func TestSectionBorderEntries(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"g", "p"})
	for _, name := range []string{"g", "h", "p"} {
		if err := mc.InsertEntry(context.Background(), &filer.Entry{
			FullPath: util.FullPath("/dir/" + name),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0100644, FileSize: 1},
		}, 0); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	applyChurn(t, mc, "/dir", "a-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)
	applyChurn(t, mc, "/dir", "h-", sectionHotThreshold, 3000, SubscriberMetadataResponseApplyOptions)

	// the bound name is part of the section starting at it
	if mc.IsNameFresh(util.FullPath("/dir/a-000")) || mc.IsNameFresh(util.FullPath("/dir/g")) {
		t.Fatal("both churned sections should be stale")
	}

	// refreshing [ , g) sweeps its churn but must not reach the bound entry
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh:   &sectionRefresh{hi: "g", snapshotTsNs: 5000},
	}); err != nil {
		t.Fatalf("refresh section 0: %v", err)
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/a-000")); err != filer_pb.ErrNotFound {
		t.Fatalf("churned a-000 should be swept: %v", err)
	}
	for _, name := range []string{"g", "h", "p"} {
		if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/"+name)); err != nil {
			t.Fatalf("%s swept by the neighboring refresh: %v", name, err)
		}
	}
	if !mc.IsNameFresh(util.FullPath("/dir/a-000")) || mc.IsNameFresh(util.FullPath("/dir/g")) {
		t.Fatal("only the refreshed section should be fresh")
	}

	// refreshing [g, p) covers the bound entry itself and stops before p
	if err := mc.enqueueAndWait(context.Background(), metadataApplyRequest{
		kind:      metadataSectionRefresh,
		buildPath: util.FullPath("/dir"),
		refresh: &sectionRefresh{lo: "g", hi: "p", snapshotTsNs: 5001, entries: []*filer.Entry{{
			FullPath: util.FullPath("/dir/h"),
			Attr:     filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(2, 0), Mode: 0100644, FileSize: 7},
		}}},
	}); err != nil {
		t.Fatalf("refresh section 1: %v", err)
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/g")); err != filer_pb.ErrNotFound {
		t.Fatalf("vanished bound entry should be swept by its own section: %v", err)
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/h-000")); err != filer_pb.ErrNotFound {
		t.Fatalf("churned h-000 should be swept: %v", err)
	}
	if entry, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/h")); err != nil || entry.FileSize != 7 {
		t.Fatalf("h = %+v, %v; want size 7", entry, err)
	}
	if _, _, err := mc.FindEntry(context.Background(), util.FullPath("/dir/p")); err != nil {
		t.Fatalf("the next bound's entry is outside the range and must survive: %v", err)
	}
}

func TestChurnBeyondBuiltEntriesLandsInEdgeSection(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})

	// names sorting past everything the build saw land in the last section
	applyChurn(t, mc, "/dir", "z-", sectionHotThreshold, 2000, SubscriberMetadataResponseApplyOptions)

	if mc.IsNameFresh(util.FullPath("/dir/z-000")) {
		t.Fatal("the tail section should be invalidated")
	}
	if !mc.IsNameFresh(util.FullPath("/dir/a")) {
		t.Fatal("the head section must stay fresh")
	}
}

func TestRenameAcrossSectionsCountsBoth(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/dir": true,
	})
	defer mc.Shutdown()

	buildSectionedDir(t, mc, util.FullPath("/dir"), 1000, []string{"m"})

	for i := 0; i < sectionHotThreshold; i++ {
		resp := &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			EventNotification: &filer_pb.EventNotification{
				OldEntry: &filer_pb.Entry{Name: fmt.Sprintf("a-%03d", i)},
				NewEntry: &filer_pb.Entry{
					Name:       fmt.Sprintf("n-%03d", i),
					Attributes: &filer_pb.FuseAttributes{Crtime: 1, Mtime: 1, FileMode: 0100644, FileSize: 1},
				},
				NewParentPath: "/dir",
			},
			TsNs: 2000 + int64(i),
		}
		if err := mc.ApplyMetadataResponse(context.Background(), resp, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply rename %d: %v", i, err)
		}
	}

	if mc.IsNameFresh(util.FullPath("/dir/a-000")) {
		t.Fatal("the vacated side's section should be invalidated")
	}
	if mc.IsNameFresh(util.FullPath("/dir/n-000")) {
		t.Fatal("the landing side's section should be invalidated")
	}
}
