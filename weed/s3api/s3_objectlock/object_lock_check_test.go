package s3_objectlock

import (
	"context"
	"fmt"
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
	done    func()
}

func (s *fakeListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.index >= len(s.entries) {
		if s.done != nil {
			s.done()
			s.done = nil
		}
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
	// oneListing refuses a listing while another is still open, like a store
	// with a single connection.
	oneListing bool
	open       int
}

func (c *fakeFilerClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	if c.oneListing && c.open > 0 {
		return nil, fmt.Errorf("listing %s while another listing is open", in.Directory)
	}
	c.open++
	done := func() { c.open-- }
	entries := c.dirs[in.Directory]
	if in.StartFromFileName != "" {
		// Already returned everything on the first page; the second call gets nothing.
		return &fakeListStream{done: done}, nil
	}
	return &fakeListStream{entries: entries, done: done}, nil
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

func TestHasObjectsWithActiveLocksBelowLiteralVersionsDirectory(t *testing.T) {
	client := &fakeFilerClient{dirs: map[string][]*filer_pb.Entry{
		"/buckets/b":                  {{Name: s3_constants.VersionsFolder, IsDirectory: true}},
		"/buckets/b/.versions":        {{Name: "nested", IsDirectory: true}},
		"/buckets/b/.versions/nested": {{Name: "held", Extended: map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}}},
	}}

	has, err := HasObjectsWithActiveLocks(context.Background(), client, "/buckets/b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has {
		t.Fatalf("expected the held object below a literal .versions directory to be reported")
	}
}

func TestHasObjectsWithActiveLocksReadsEachPageBeforeSubdirectories(t *testing.T) {
	client := &fakeFilerClient{oneListing: true, dirs: map[string][]*filer_pb.Entry{
		"/buckets/b":   {{Name: "a", IsDirectory: true}, {Name: "z"}},
		"/buckets/b/a": {{Name: "held", Extended: map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}}},
	}}

	has, err := HasObjectsWithActiveLocks(context.Background(), client, "/buckets/b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has {
		t.Fatalf("expected the held object in the subdirectory to be reported")
	}
}

func TestHasObjectsWithActiveLocksNamesWithBackslashes(t *testing.T) {
	held := map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}
	for name, dirs := range map[string]map[string][]*filer_pb.Entry{
		"object": {"/buckets/b": {{Name: `a\b`, Extended: held}}},
		"prefix": {"/buckets/b": {{Name: `a\b`, IsDirectory: true}}, `/buckets/b/a\b`: {{Name: "c", Extended: held}}},
	} {
		has, err := HasObjectsWithActiveLocks(context.Background(), &fakeFilerClient{dirs: dirs}, "/buckets/b")
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", name, err)
		}
		if !has {
			t.Fatalf("%s: expected the held object with a backslash in its key to be reported", name)
		}
	}
}

func TestHasObjectsWithActiveLocksUploadsDirectories(t *testing.T) {
	held := map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}
	upload := lockedDirMarker("upload-id")
	upload.Extended[s3_constants.ExtMultipartObjectKey] = []byte("k")
	for _, tt := range []struct {
		name string
		dirs map[string][]*filer_pb.Entry
		want bool
	}{
		{"pending upload", map[string][]*filer_pb.Entry{
			"/buckets/b":          {{Name: s3_constants.MultipartUploadsFolder, IsDirectory: true}},
			"/buckets/b/.uploads": {upload},
		}, false},
		{"object below a pending upload", map[string][]*filer_pb.Entry{
			"/buckets/b":                    {{Name: s3_constants.MultipartUploadsFolder, IsDirectory: true}},
			"/buckets/b/.uploads":           {upload},
			"/buckets/b/.uploads/upload-id": {{Name: "0001_x.part"}, {Name: "c", Extended: held}},
		}, true},
		{"upload directory renamed out of .uploads", map[string][]*filer_pb.Entry{
			"/buckets/b": {upload},
		}, true},
		{"object below a .uploads prefix", map[string][]*filer_pb.Entry{
			"/buckets/b":            {{Name: "a", IsDirectory: true}},
			"/buckets/b/a":          {{Name: s3_constants.MultipartUploadsFolder, IsDirectory: true}},
			"/buckets/b/a/.uploads": {{Name: "c", Extended: held}},
		}, true},
	} {
		has, err := HasObjectsWithActiveLocks(context.Background(), &fakeFilerClient{dirs: tt.dirs}, "/buckets/b")
		if err != nil || has != tt.want {
			t.Fatalf("%s: got %v, %v, want %v", tt.name, has, err, tt.want)
		}
	}
}

func TestListEntriesPageDropsInlineContent(t *testing.T) {
	held := map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}
	client := &fakeFilerClient{dirs: map[string][]*filer_pb.Entry{
		"/buckets/b": {{Name: "a", Content: make([]byte, 1<<20), Extended: held}},
	}}
	page, err := listEntriesPage(context.Background(), client, "/buckets/b", "")
	if err != nil || len(page) != 1 {
		t.Fatalf("got %v, %v", page, err)
	}
	if page[0].Content != nil || page[0].Name != "a" || !EntryHasActiveLock(page[0], time.Now()) {
		t.Fatalf("page entry %+v", page[0])
	}
}
