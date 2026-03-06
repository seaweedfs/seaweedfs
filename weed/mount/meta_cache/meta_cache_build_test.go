package meta_cache

import (
	"context"
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
