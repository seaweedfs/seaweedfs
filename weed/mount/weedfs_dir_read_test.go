package mount

import (
	"context"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type directoryListStream struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
}

func (s *directoryListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *directoryListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *directoryListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *directoryListStream) CloseSend() error             { return nil }
func (s *directoryListStream) Context() context.Context     { return context.Background() }
func (s *directoryListStream) SendMsg(any) error            { return nil }
func (s *directoryListStream) RecvMsg(any) error            { return nil }

type directoryListClient struct {
	filer_pb.SeaweedFilerClient
	responses []*filer_pb.ListEntriesResponse
}

func (c *directoryListClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	return &directoryListStream{responses: c.responses}, nil
}

type directoryFilerAccessor struct {
	client filer_pb.SeaweedFilerClient
}

func (a *directoryFilerAccessor) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(a.client)
}

func (a *directoryFilerAccessor) AdjustedUrl(*filer_pb.Location) string { return "" }
func (a *directoryFilerAccessor) GetDataCenter() string                 { return "" }

func TestLoadDirectoryEntriesDirectFiltersHiddenEntriesAndMapsIds(t *testing.T) {
	mapper, err := meta_cache.NewUidGidMapper("10:1000", "20:2000")
	if err != nil {
		t.Fatalf("uid/gid mapper: %v", err)
	}

	client := &directoryFilerAccessor{
		client: &directoryListClient{
			responses: []*filer_pb.ListEntriesResponse{
				{
					Entry: &filer_pb.Entry{
						Name: "topics",
						Attributes: &filer_pb.FuseAttributes{
							Uid: 1000,
							Gid: 2000,
						},
					},
				},
				{
					Entry: &filer_pb.Entry{
						Name: "visible",
						Attributes: &filer_pb.FuseAttributes{
							Uid: 1000,
							Gid: 2000,
						},
					},
				},
			},
		},
	}

	entries, _, err := loadDirectoryEntriesDirect(context.Background(), client, mapper, util.FullPath("/"), "", false, 10, 0)
	if err != nil {
		t.Fatalf("loadDirectoryEntriesDirect: %v", err)
	}
	if got := len(entries); got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
	if entries[0].Name() != "visible" {
		t.Fatalf("entry name = %q, want visible", entries[0].Name())
	}
	if entries[0].Attr.Uid != 10 || entries[0].Attr.Gid != 20 {
		t.Fatalf("mapped uid/gid = %d/%d, want 10/20", entries[0].Attr.Uid, entries[0].Attr.Gid)
	}
}
