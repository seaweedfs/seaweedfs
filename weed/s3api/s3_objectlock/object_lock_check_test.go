package s3_objectlock

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type fakeListStream struct {
	entries []*filer_pb.Entry
	index   int
}

func (s *fakeListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.index >= len(s.entries) {
		return nil, io.EOF
	}
	e := s.entries[s.index]
	s.index++
	return &filer_pb.ListEntriesResponse{Entry: e}, nil
}

func (s *fakeListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fakeListStream) CloseSend() error             { return nil }
func (s *fakeListStream) Context() context.Context     { return context.Background() }
func (s *fakeListStream) SendMsg(any) error            { return nil }
func (s *fakeListStream) RecvMsg(any) error            { return nil }

// fakeFilerClient serves a fixed directory -> entries map.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient
	dirs map[string][]*filer_pb.Entry
}

func (c *fakeFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	entries := c.dirs[in.Directory]
	if in.StartFromFileName != "" {
		// Already returned everything on the first page; the second call gets nothing.
		return &fakeListStream{}, nil
	}
	return &fakeListStream{entries: entries}, nil
}

func lockedDirMarker(name string) *filer_pb.Entry {
	until := strconv.FormatInt(time.Now().Add(24*time.Hour).Unix(), 10)
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.ExtObjectLockModeKey:     []byte(s3_constants.RetentionModeCompliance),
			s3_constants.ExtRetentionUntilDateKey: []byte(until),
		},
	}
}

func TestHasObjectsWithActiveLocksDirectoryMarker(t *testing.T) {
	client := &fakeFilerClient{dirs: map[string][]*filer_pb.Entry{
		"/buckets/lockb": {lockedDirMarker("records")},
		// The marker's own subtree is empty; only the marker entry carries the lock.
		"/buckets/lockb/records": {},
	}}

	has, err := HasObjectsWithActiveLocks(context.Background(), client, "/buckets/lockb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has {
		t.Fatalf("expected the locked directory marker to be reported as an active lock")
	}
}

func TestHasObjectsWithActiveLocksUnlockedDirectory(t *testing.T) {
	client := &fakeFilerClient{dirs: map[string][]*filer_pb.Entry{
		"/buckets/plainb":        {{Name: "prefix", IsDirectory: true}},
		"/buckets/plainb/prefix": {{Name: "obj.txt"}},
	}}

	has, err := HasObjectsWithActiveLocks(context.Background(), client, "/buckets/plainb")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if has {
		t.Fatalf("expected no active lock for an ordinary prefix and object")
	}
}
