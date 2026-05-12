package dailyrun

import (
	"context"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeFilerStream implements the ListEntries server-streaming client.
type fakeFilerStream struct {
	responses []*filer_pb.ListEntriesResponse
	idx       int
	ctx       context.Context
}

func (s *fakeFilerStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.ctx != nil {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s.idx >= len(s.responses) {
		return nil, io.EOF
	}
	r := s.responses[s.idx]
	s.idx++
	return r, nil
}
func (s *fakeFilerStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fakeFilerStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fakeFilerStream) CloseSend() error             { return nil }
func (s *fakeFilerStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *fakeFilerStream) SendMsg(any) error { return nil }
func (s *fakeFilerStream) RecvMsg(any) error { return nil }

// fakeFiler maps directory paths to their immediate children. Only
// ListEntries is implemented; other methods of SeaweedFilerClient are
// inherited from the embedded interface and panic if called.
type fakeFiler struct {
	filer_pb.SeaweedFilerClient

	mu   sync.Mutex
	tree map[string][]*filer_pb.Entry
}

func (c *fakeFiler) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	src := c.tree[in.Directory]
	// Mirror the filer: sort by name, honor StartFromFileName exclusive,
	// cap at Limit. listAll's pagination loop depends on these.
	filtered := make([]*filer_pb.Entry, 0, len(src))
	for _, e := range src {
		if e == nil {
			continue
		}
		if in.StartFromFileName != "" && !in.InclusiveStartFrom && e.Name <= in.StartFromFileName {
			continue
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
	return &fakeFilerStream{responses: resps, ctx: ctx}, nil
}

func file(name string, mtime time.Time, size int64) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name: name,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    mtime.Unix(),
			MtimeNs:  int32(mtime.Nanosecond()),
			FileSize: uint64(size),
		},
	}
}

func dir(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}
}

func TestFilerListFunc_EmitsFlatFiles(t *testing.T) {
	mtime := time.Now().Add(-7 * 24 * time.Hour)
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {file("a.txt", mtime, 10), file("b.txt", mtime, 20)},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got []string
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		got = append(got, e.Path)
		return nil
	}))
	assert.Equal(t, []string{"a.txt", "b.txt"}, got)
}

func TestFilerListFunc_RecursesIntoSubdirs(t *testing.T) {
	mtime := time.Now()
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt":         {dir("logs"), file("root.txt", mtime, 1)},
		"/buckets/bkt/logs":    {dir("2026"), file("a.log", mtime, 5)},
		"/buckets/bkt/logs/2026": {file("b.log", mtime, 7)},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var paths []string
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		paths = append(paths, e.Path)
		return nil
	}))
	sort.Strings(paths)
	assert.Equal(t, []string{"logs/2026/b.log", "logs/a.log", "root.txt"}, paths)
}

func TestFilerListFunc_SkipsVersionsAndUploadsDirsForNow(t *testing.T) {
	// Phase 4b-pre: `.versions/<key>/` and `.uploads/<id>/` are not yet
	// expanded. Pin that they don't leak raw children into the dispatch
	// path; the follow-up commit adds the proper sibling/MPU expansion.
	mtime := time.Now()
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {
			file("regular.txt", mtime, 1),
			dir("foo" + s3_constants.VersionsFolder),
			dir(s3_constants.MultipartUploadsFolder),
		},
		"/buckets/bkt/foo" + s3_constants.VersionsFolder: {
			file("v_001", mtime, 1),
		},
		"/buckets/bkt/" + s3_constants.MultipartUploadsFolder: {
			dir("upload-id-1"),
		},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var paths []string
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		paths = append(paths, e.Path)
		return nil
	}))
	assert.Equal(t, []string{"regular.txt"}, paths, ".versions/ and .uploads/ must not surface raw children")
}

func TestFilerListFunc_HonorsStart(t *testing.T) {
	// The walker's kill-resume contract: skip entries whose Path <= start.
	mtime := time.Now()
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {file("a", mtime, 1), file("b", mtime, 1), file("c", mtime, 1)},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got []string
	require.NoError(t, listFn(context.Background(), "bkt", "a", func(e *bootstrap.Entry) error {
		got = append(got, e.Path)
		return nil
	}))
	assert.Equal(t, []string{"b", "c"}, got)
}

func TestFilerListFunc_NilClient(t *testing.T) {
	listFn := FilerListFunc(nil, "/buckets")
	require.Error(t, listFn(context.Background(), "bkt", "", func(*bootstrap.Entry) error { return nil }))
}

func TestFilerListFunc_AttributesPropagate(t *testing.T) {
	mtime := time.Date(2026, 5, 11, 12, 0, 0, 1234, time.UTC)
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {file("obj", mtime, 4096)},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got *bootstrap.Entry
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		got = e
		return nil
	}))
	require.NotNil(t, got)
	assert.Equal(t, "obj", got.Path)
	assert.Equal(t, mtime.Unix(), got.ModTime.Unix())
	assert.Equal(t, int64(1234), got.ModTime.UnixNano()-mtime.Unix()*int64(time.Second))
	assert.Equal(t, int64(4096), got.Size)
	assert.True(t, got.IsLatest)
}
