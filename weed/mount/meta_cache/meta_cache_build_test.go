package meta_cache

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type buildListStream struct {
	responses   []*filer_pb.ListEntriesResponse
	onFirstRecv func()
	once        sync.Once
	index       int
}

func (s *buildListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	s.once.Do(func() {
		if s.onFirstRecv != nil {
			s.onFirstRecv()
		}
	})
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *buildListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *buildListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *buildListStream) CloseSend() error             { return nil }
func (s *buildListStream) Context() context.Context     { return context.Background() }
func (s *buildListStream) SendMsg(any) error            { return nil }
func (s *buildListStream) RecvMsg(any) error            { return nil }

type buildListClient struct {
	filer_pb.SeaweedFilerClient
	responses   []*filer_pb.ListEntriesResponse
	onFirstRecv func()
}

func (c *buildListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return &buildListStream{
		responses:   c.responses,
		onFirstRecv: c.onFirstRecv,
	}, nil
}

type buildFilerAccessor struct {
	client filer_pb.SeaweedFilerClient
}

func (a *buildFilerAccessor) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(a.client)
}

func (a *buildFilerAccessor) AdjustedUrl(*filer_pb.Location) string { return "" }
func (a *buildFilerAccessor) GetDataCenter() string                 { return "" }

func TestEnsureVisitedReplaysBufferedEventsAfterSnapshot(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/": true,
	})
	defer mc.Shutdown()

	var applyErr error
	accessor := &buildFilerAccessor{
		client: &buildListClient{
			responses: []*filer_pb.ListEntriesResponse{
				{
					Entry: &filer_pb.Entry{
						Name: "base.txt",
						Attributes: &filer_pb.FuseAttributes{
							Crtime:   1,
							Mtime:    1,
							FileMode: 0100644,
							FileSize: 3,
						},
					},
					SnapshotTsNs: 100,
				},
			},
			onFirstRecv: func() {
				applyErr = mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
					Directory: "/dir",
					EventNotification: &filer_pb.EventNotification{
						NewEntry: &filer_pb.Entry{
							Name: "after.txt",
							Attributes: &filer_pb.FuseAttributes{
								Crtime:   2,
								Mtime:    2,
								FileMode: 0100644,
								FileSize: 9,
							},
						},
					},
					TsNs: 101,
				}, SubscriberMetadataResponseApplyOptions)
			},
		},
	}

	if err := EnsureVisited(mc, accessor, util.FullPath("/dir")); err != nil {
		t.Fatalf("ensure visited: %v", err)
	}
	if applyErr != nil {
		t.Fatalf("apply buffered event: %v", applyErr)
	}
	if !mc.IsDirectoryCached(util.FullPath("/dir")) {
		t.Fatal("directory /dir should be cached after build completes")
	}

	baseEntry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/base.txt"))
	if err != nil {
		t.Fatalf("find base entry: %v", err)
	}
	if baseEntry.FileSize != 3 {
		t.Fatalf("base entry size = %d, want 3", baseEntry.FileSize)
	}

	afterEntry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/after.txt"))
	if err != nil {
		t.Fatalf("find replayed entry: %v", err)
	}
	if afterEntry.FileSize != 9 {
		t.Fatalf("replayed entry size = %d, want 9", afterEntry.FileSize)
	}
}

// TestDirectoryNotificationsSuppressedDuringBuild verifies that metadata events
// targeting a directory under active build do NOT fire onDirectoryUpdate for
// that directory. In production, onDirectoryUpdate can trigger
// markDirectoryReadThrough → DeleteFolderChildren, which would wipe entries
// that EnsureVisited already inserted mid-build.
func TestDirectoryNotificationsSuppressedDuringBuild(t *testing.T) {
	mc, _, notifications, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/": true,
	})
	defer mc.Shutdown()

	// Start building /dir (simulates the beginning of EnsureVisited)
	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}

	// Insert an entry as EnsureVisited would during the filer listing
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/existing.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 100,
		},
	}); err != nil {
		t.Fatalf("insert entry during build: %v", err)
	}

	// Simulate multiple metadata events arriving for /dir while the build
	// is in progress. Each event would normally call noteDirectoryUpdate,
	// which in production can trigger markDirectoryReadThrough and wipe entries.
	for i := 0; i < 5; i++ {
		resp := &filer_pb.SubscribeMetadataResponse{
			Directory: "/dir",
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name: fmt.Sprintf("new-%d.txt", i),
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   int64(10 + i),
						Mtime:    int64(10 + i),
						FileMode: 0100644,
						FileSize: uint64(i + 1),
					},
				},
			},
			TsNs: int64(200 + i),
		}
		if err := mc.ApplyMetadataResponse(context.Background(), resp, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply event %d: %v", i, err)
		}
	}

	// The building directory /dir must NOT have received any notifications.
	// If it did, markDirectoryReadThrough would wipe the cache mid-build.
	for _, p := range notifications.paths() {
		if p == util.FullPath("/dir") {
			t.Fatal("onDirectoryUpdate was called for /dir during build; this would cause markDirectoryReadThrough to wipe entries mid-build")
		}
	}

	// The entry inserted during the build must still be present
	entry, err := mc.FindEntry(context.Background(), util.FullPath("/dir/existing.txt"))
	if err != nil {
		t.Fatalf("entry wiped during build: %v", err)
	}
	if entry.FileSize != 100 {
		t.Fatalf("entry size = %d, want 100", entry.FileSize)
	}

	// Complete the build — buffered events should be replayed
	if err := mc.CompleteDirectoryBuild(context.Background(), util.FullPath("/dir"), 150); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	// After build completes, the entry from the listing should still exist
	entry, err = mc.FindEntry(context.Background(), util.FullPath("/dir/existing.txt"))
	if err != nil {
		t.Fatalf("entry lost after build completion: %v", err)
	}
	if entry.FileSize != 100 {
		t.Fatalf("entry size after build = %d, want 100", entry.FileSize)
	}

	// Buffered events with TsNs > snapshotTsNs (150) should have been replayed
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("new-%d.txt", i)
		e, err := mc.FindEntry(context.Background(), util.FullPath("/dir/"+name))
		if err != nil {
			t.Fatalf("replayed entry %s not found: %v", name, err)
		}
		if e.FileSize != uint64(i+1) {
			t.Fatalf("replayed entry %s size = %d, want %d", name, e.FileSize, i+1)
		}
	}
}

// TestEmptyDirectoryBuildReplaysAllBufferedEvents verifies that when a
// directory build completes with snapshotTsNs=0 (empty directory — server
// returned no entries and no snapshot), ALL buffered events are replayed
// without any TsNs filtering. This prevents clock-skew between client and
// filer from dropping legitimate mutations.
func TestEmptyDirectoryBuildReplaysAllBufferedEvents(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/": true,
	})
	defer mc.Shutdown()

	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/empty")); err != nil {
		t.Fatalf("begin build: %v", err)
	}

	// Buffer events with a range of TsNs values — some very old, some recent.
	// With a client-synthesized snapshot, old events could be incorrectly filtered.
	tsValues := []int64{1, 50, 500, 5000, 50000}
	for i, ts := range tsValues {
		resp := &filer_pb.SubscribeMetadataResponse{
			Directory: "/empty",
			EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{
					Name: fmt.Sprintf("file-%d.txt", i),
					Attributes: &filer_pb.FuseAttributes{
						Crtime:   ts,
						Mtime:    ts,
						FileMode: 0100644,
						FileSize: uint64(i + 10),
					},
				},
			},
			TsNs: ts,
		}
		if err := mc.ApplyMetadataResponse(context.Background(), resp, SubscriberMetadataResponseApplyOptions); err != nil {
			t.Fatalf("apply event %d: %v", i, err)
		}
	}

	// Complete with snapshotTsNs=0 — simulates empty directory listing
	if err := mc.CompleteDirectoryBuild(context.Background(), util.FullPath("/empty"), 0); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	// Every buffered event must have been replayed, regardless of TsNs
	for i := range tsValues {
		name := fmt.Sprintf("file-%d.txt", i)
		e, err := mc.FindEntry(context.Background(), util.FullPath("/empty/"+name))
		if err != nil {
			t.Fatalf("replayed entry %s not found: %v", name, err)
		}
		if e.FileSize != uint64(i+10) {
			t.Fatalf("replayed entry %s size = %d, want %d", name, e.FileSize, i+10)
		}
	}

	if !mc.IsDirectoryCached(util.FullPath("/empty")) {
		t.Fatal("/empty should be marked cached after build completes")
	}
}

// TestBuildCompletionSurvivesCallerCancellation verifies that once
// CompleteDirectoryBuild is enqueued, a cancelled caller context does not
// prevent the build from completing. The apply loop uses context.Background()
// internally, so the operation finishes even if the caller gives up waiting.
func TestBuildCompletionSurvivesCallerCancellation(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/": true,
	})
	defer mc.Shutdown()

	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dir")); err != nil {
		t.Fatalf("begin build: %v", err)
	}

	// Insert an entry during the build (as EnsureVisited would)
	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/dir/kept.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 42,
		},
	}); err != nil {
		t.Fatalf("insert entry: %v", err)
	}

	// Buffer an event that should be replayed
	if err := mc.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name: "buffered.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   5,
					Mtime:    5,
					FileMode: 0100644,
					FileSize: 77,
				},
			},
		},
		TsNs: 200,
	}, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	// Complete with an already-cancelled context. The operation should still
	// succeed because enqueueAndWait sets req.ctx = context.Background().
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// CompleteDirectoryBuild may return ctx.Err() if the select picks
	// ctx.Done() first, but the operation itself still completes in the
	// apply loop. We retry with a fresh context to confirm.
	err := mc.CompleteDirectoryBuild(cancelledCtx, util.FullPath("/dir"), 100)
	if err != nil {
		// The cancelled context may cause enqueueAndWait's select to
		// return early. Wait briefly for the apply loop to process, then
		// verify the build completed via its observable side effects.
		time.Sleep(50 * time.Millisecond)
	}

	// The directory must be cached — proving CompleteDirectoryBuild ran
	if !mc.IsDirectoryCached(util.FullPath("/dir")) {
		t.Fatal("/dir should be cached — CompleteDirectoryBuild must have executed despite cancelled context")
	}

	// The pre-existing entry must survive
	entry, findErr := mc.FindEntry(context.Background(), util.FullPath("/dir/kept.txt"))
	if findErr != nil {
		t.Fatalf("find kept entry: %v", findErr)
	}
	if entry.FileSize != 42 {
		t.Fatalf("kept entry size = %d, want 42", entry.FileSize)
	}

	// The buffered event (TsNs 200 > snapshot 100) must have been replayed
	buffered, findErr := mc.FindEntry(context.Background(), util.FullPath("/dir/buffered.txt"))
	if findErr != nil {
		t.Fatalf("find buffered entry: %v", findErr)
	}
	if buffered.FileSize != 77 {
		t.Fatalf("buffered entry size = %d, want 77", buffered.FileSize)
	}
}

func TestBufferedRenameUpdatesOtherDirectoryBeforeBuildCompletes(t *testing.T) {
	mc, _, _, _ := newTestMetaCache(t, map[util.FullPath]bool{
		"/":    true,
		"/src": true,
	})
	defer mc.Shutdown()

	if err := mc.InsertEntry(context.Background(), &filer.Entry{
		FullPath: "/src/from.txt",
		Attr: filer.Attr{
			Crtime:   time.Unix(1, 0),
			Mtime:    time.Unix(1, 0),
			Mode:     0100644,
			FileSize: 7,
		},
	}); err != nil {
		t.Fatalf("insert source entry: %v", err)
	}

	if err := mc.BeginDirectoryBuild(context.Background(), util.FullPath("/dst")); err != nil {
		t.Fatalf("begin build: %v", err)
	}

	renameResp := &filer_pb.SubscribeMetadataResponse{
		Directory: "/src",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "from.txt",
			},
			NewEntry: &filer_pb.Entry{
				Name: "to.txt",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   2,
					Mtime:    2,
					FileMode: 0100644,
					FileSize: 12,
				},
			},
			NewParentPath: "/dst",
		},
		TsNs: 101,
	}

	if err := mc.ApplyMetadataResponse(context.Background(), renameResp, SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	oldEntry, err := mc.FindEntry(context.Background(), util.FullPath("/src/from.txt"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("find old path error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	if oldEntry != nil {
		t.Fatalf("old path should be removed before build completes: %+v", oldEntry)
	}

	newEntry, err := mc.FindEntry(context.Background(), util.FullPath("/dst/to.txt"))
	if err != filer_pb.ErrNotFound {
		t.Fatalf("find buffered new path error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	if newEntry != nil {
		t.Fatalf("new path should stay hidden until build completes: %+v", newEntry)
	}

	if err := mc.CompleteDirectoryBuild(context.Background(), util.FullPath("/dst"), 100); err != nil {
		t.Fatalf("complete build: %v", err)
	}

	newEntry, err = mc.FindEntry(context.Background(), util.FullPath("/dst/to.txt"))
	if err != nil {
		t.Fatalf("find replayed new path: %v", err)
	}
	if newEntry.FileSize != 12 {
		t.Fatalf("replayed new path size = %d, want 12", newEntry.FileSize)
	}
}
