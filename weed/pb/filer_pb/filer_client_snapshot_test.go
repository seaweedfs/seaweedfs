package filer_pb

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type snapshotListStream struct {
	responses []*ListEntriesResponse
	index     int
}

func (s *snapshotListStream) Recv() (*ListEntriesResponse, error) {
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *snapshotListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *snapshotListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *snapshotListStream) CloseSend() error             { return nil }
func (s *snapshotListStream) Context() context.Context     { return context.Background() }
func (s *snapshotListStream) SendMsg(any) error            { return nil }
func (s *snapshotListStream) RecvMsg(any) error            { return nil }

type snapshotListClient struct {
	SeaweedFilerClient
	entries    []*Entry
	requests   []*ListEntriesRequest
	snapshotTs int64
}

func (c *snapshotListClient) ListEntries(ctx context.Context, in *ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ListEntriesResponse], error) {
	c.requests = append(c.requests, proto.Clone(in).(*ListEntriesRequest))

	start := 0
	if in.StartFromFileName != "" {
		start = len(c.entries)
		for i, entry := range c.entries {
			if entry.Name == in.StartFromFileName {
				start = i
				if !in.InclusiveStartFrom {
					start++
				}
				break
			}
		}
	}

	end := len(c.entries)
	if in.Limit > 0 && start+int(in.Limit) < end {
		end = start + int(in.Limit)
	}

	snapshotTs := in.SnapshotTsNs
	if snapshotTs == 0 {
		snapshotTs = c.snapshotTs
	}

	responses := make([]*ListEntriesResponse, 0, end-start)
	for i, entry := range c.entries[start:end] {
		resp := &ListEntriesResponse{
			Entry: entry,
		}
		if i == 0 {
			resp.SnapshotTsNs = snapshotTs
		}
		responses = append(responses, resp)
	}

	return &snapshotListStream{responses: responses}, nil
}

type snapshotFilerAccessor struct {
	client SeaweedFilerClient
}

func (a *snapshotFilerAccessor) WithFilerClient(_ bool, fn func(SeaweedFilerClient) error) error {
	return fn(a.client)
}

func (a *snapshotFilerAccessor) AdjustedUrl(*Location) string { return "" }
func (a *snapshotFilerAccessor) GetDataCenter() string        { return "" }

func TestReadDirAllEntriesWithSnapshotCarriesSnapshotAcrossPages(t *testing.T) {
	entries := make([]*Entry, 0, 10001)
	for i := 0; i < 10001; i++ {
		entries = append(entries, &Entry{Name: fmt.Sprintf("entry-%05d", i), Attributes: &FuseAttributes{}})
	}

	client := &snapshotListClient{
		entries:    entries,
		snapshotTs: 123456789,
	}
	accessor := &snapshotFilerAccessor{client: client}

	var listed []string
	snapshotTs, err := ReadDirAllEntriesWithSnapshot(context.Background(), accessor, util.FullPath("/dir"), "", func(entry *Entry, isLast bool) error {
		listed = append(listed, entry.Name)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadDirAllEntriesWithSnapshot: %v", err)
	}

	if got := len(listed); got != len(entries) {
		t.Fatalf("listed %d entries, want %d", got, len(entries))
	}
	if snapshotTs != client.snapshotTs {
		t.Fatalf("snapshotTs = %d, want %d", snapshotTs, client.snapshotTs)
	}
	if got := len(client.requests); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
	if client.requests[0].SnapshotTsNs != 0 {
		t.Fatalf("first request snapshot = %d, want 0", client.requests[0].SnapshotTsNs)
	}
	if client.requests[1].SnapshotTsNs != client.snapshotTs {
		t.Fatalf("second request snapshot = %d, want %d", client.requests[1].SnapshotTsNs, client.snapshotTs)
	}
	if client.requests[1].StartFromFileName != entries[9999].Name {
		t.Fatalf("second request marker = %q, want %q", client.requests[1].StartFromFileName, entries[9999].Name)
	}
}
