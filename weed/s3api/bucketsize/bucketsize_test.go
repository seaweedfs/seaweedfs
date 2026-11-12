package bucketsize

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// treeFiler serves a directory tree from a map and records listed directories.
type treeFiler struct {
	filer_pb.SeaweedFilerClient
	tree    map[string][]*filer_pb.Entry
	errDirs map[string]error
	mu      sync.Mutex
	listed  []string
}

func (f *treeFiler) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(f)
}
func (f *treeFiler) AdjustedUrl(*filer_pb.Location) string { return "" }
func (f *treeFiler) GetDataCenter() string                 { return "" }

func (f *treeFiler) ListEntries(ctx context.Context, req *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	f.mu.Lock()
	f.listed = append(f.listed, req.Directory)
	f.mu.Unlock()
	if err := f.errDirs[req.Directory]; err != nil {
		return nil, err
	}
	if req.StartFromFileName != "" {
		return &listStream{}, nil
	}
	return &listStream{entries: f.tree[req.Directory]}, nil
}

type listStream struct {
	grpc.ClientStream
	entries []*filer_pb.Entry
	idx     int
}

func (s *listStream) Trailer() metadata.MD { return nil }
func (s *listStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.idx >= len(s.entries) {
		return nil, io.EOF
	}
	s.idx++
	return &filer_pb.ListEntriesResponse{Entry: s.entries[s.idx-1]}, nil
}

func dir(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
}

func file(name string, size uint64) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{FileSize: size}}
}

func versionsDir(name string, ext map[string]string) *filer_pb.Entry {
	e := dir(name)
	e.Extended = map[string][]byte{}
	for k, v := range ext {
		e.Extended[k] = []byte(v)
	}
	return e
}

func size(t *testing.T, f *treeFiler, bucketDir string) uint64 {
	t.Helper()
	got, err := Size(context.Background(), f, util.FullPath(bucketDir))
	if err != nil {
		t.Fatalf("Size(%s): %v", bucketDir, err)
	}
	return got
}

// Every stored byte counts, whatever a listing shows: in-progress multipart
// parts, noncurrent versions, versions behind a delete marker, and a
// suspended-versioning layout whose .versions directory has no current-version
// pointer (a suspended PUT clears it) next to the null object.
func TestSizeCountsEveryStoredByte(t *testing.T) {
	f := &treeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/b": {
			dir(".uploads"),
			file("a", 7), // null object written while versioning was suspended
			versionsDir("a.versions", nil),
			versionsDir("b.versions", map[string]string{s3_constants.ExtLatestVersionIdKey: "v9", s3_constants.ExtLatestVersionIsDeleteMarker: "true"}),
			dir("ns"),
		},
		"/buckets/b/.uploads":    {dir("u1")},
		"/buckets/b/.uploads/u1": {file("0001.part", 1000)},
		"/buckets/b/a.versions":  {file("v_v1", 100), file("v_v2", 300)},
		"/buckets/b/b.versions":  {file("v_v8", 50), file("v_v9", 0)},
		"/buckets/b/ns":          {file("x", 5)},
	}}
	if got := size(t, f, "/buckets/b"); got != 1000+7+100+300+50+5 {
		t.Fatalf("size = %d, want %d", got, 1000+7+100+300+50+5)
	}
}

func TestSizeListingErrorIsNotASize(t *testing.T) {
	f := &treeFiler{
		tree:    map[string][]*filer_pb.Entry{"/buckets/b": {file("a", 7), dir("ns")}, "/buckets/b/ns": {file("x", 5)}},
		errDirs: map[string]error{"/buckets/b/ns": errors.New("filer unavailable")},
	}
	if got, err := Size(context.Background(), f, "/buckets/b"); err == nil {
		t.Fatalf("partial size %d returned without an error", got)
	}
}

// A "<key>/" directory object stores its inline body; a plain directory nothing.
func TestSizeCountsDirectoryKeyObjects(t *testing.T) {
	marker := dir("folder")
	marker.Content = []byte("hello")
	marker.Attributes.Mime = "application/octet-stream"
	f := &treeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/d":        {marker, dir("plain")},
		"/buckets/d/folder": {file("x", 10)},
		"/buckets/d/plain":  {file("y", 20)},
	}}
	if got := size(t, f, "/buckets/d"); got != 5+10+20 {
		t.Fatalf("size = %d, want %d", got, 5+10+20)
	}
}

func TestIsMounted(t *testing.T) {
	m := &remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/a":        {Name: "r"},
		"/buckets/c/inside": {Name: "r"},
		"/mnt":              {Name: "r"},
	}}
	for dir, want := range map[string]bool{
		"/buckets/a":  true,  // the bucket is the mount
		"/buckets/ab": false, // a sibling sharing a prefix
		"/buckets/c":  true,  // a mount inside the bucket
		"/buckets/d":  false,
	} {
		if got := IsMounted(m, dir); got != want {
			t.Errorf("IsMounted(%s) = %v, want %v", dir, got, want)
		}
	}
	if !IsMounted(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{"/buckets": {}}}, "/buckets/x") {
		t.Error("a mount above the bucket does not count")
	}
}
