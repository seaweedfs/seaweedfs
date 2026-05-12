package scheduler

import (
	"context"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeListStream implements grpc.ServerStreamingClient[filer_pb.ListEntriesResponse]
// for configload tests.
type fakeListStream struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
	ctx       context.Context
}

func (s *fakeListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.ctx != nil {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	r := s.responses[s.index]
	s.index++
	return r, nil
}

func (s *fakeListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fakeListStream) CloseSend() error             { return nil }
func (s *fakeListStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *fakeListStream) SendMsg(any) error { return nil }
func (s *fakeListStream) RecvMsg(any) error { return nil }

// fakeFilerClient is the in-memory filer used by configload tests.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient

	mu      sync.Mutex
	tree    map[string][]*filer_pb.Entry
	listed  []string
	listedN int32
}

func (c *fakeFilerClient) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.tree[in.Directory] {
		if e != nil && e.Name == in.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, filer_pb.ErrNotFound
}

func (c *fakeFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	c.listed = append(c.listed, in.Directory)
	src := c.tree[in.Directory]
	c.mu.Unlock()
	atomic.AddInt32(&c.listedN, 1)

	filtered := make([]*filer_pb.Entry, 0, len(src))
	for _, e := range src {
		if e == nil {
			continue
		}
		if in.StartFromFileName != "" {
			if in.InclusiveStartFrom {
				if e.Name < in.StartFromFileName {
					continue
				}
			} else if e.Name <= in.StartFromFileName {
				continue
			}
		}
		filtered = append(filtered, e)
	}
	sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].Name < filtered[j].Name })
	if in.Limit > 0 && uint32(len(filtered)) > in.Limit {
		filtered = filtered[:in.Limit]
	}
	resps := make([]*filer_pb.ListEntriesResponse, 0, len(filtered))
	for _, e := range filtered {
		resps = append(resps, &filer_pb.ListEntriesResponse{Entry: e})
	}
	return &fakeListStream{responses: resps, ctx: ctx}, nil
}

func dirEntry(name string, extended map[string][]byte) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{},
		Extended:    extended,
	}
}

func fileEntry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: false,
		Attributes:  &filer_pb.FuseAttributes{},
	}
}
