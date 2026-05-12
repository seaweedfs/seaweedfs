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

func (c *fakeFiler) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.tree[in.Directory] {
		if e != nil && e.Name == in.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, filer_pb.ErrNotFound
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

func TestFilerListFunc_SkipsUploadsDirsForNow(t *testing.T) {
	// Phase 4b-pre: `.uploads/<id>/` MPU init records are skipped
	// here. The follow-up commit emits one IsMPUInit Entry per init.
	mtime := time.Now()
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {
			file("regular.txt", mtime, 1),
			dir(s3_constants.MultipartUploadsFolder),
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
	assert.Equal(t, []string{"regular.txt"}, paths, ".uploads/ must not surface raw children")
}

// fileWithExt is a versioned-file entry helper.
func fileWithExt(name string, mtime time.Time, size int64, ext map[string][]byte) *filer_pb.Entry {
	e := file(name, mtime, size)
	e.Extended = ext
	return e
}

func versionsDir(name string, latestID string) *filer_pb.Entry {
	d := dir(name)
	d.Extended = map[string][]byte{}
	if latestID != "" {
		d.Extended[s3_constants.ExtLatestVersionIdKey] = []byte(latestID)
	}
	return d
}

func TestFilerListFunc_VersionedExpansionMarksLatestByPointer(t *testing.T) {
	// .versions/<key>/ with three real versions; parent's
	// ExtLatestVersionIdKey points to v2 → IsLatest set on v2; the
	// other two get NoncurrentIndex computed against the latest's
	// position in the sorted (newest-first) list.
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	versions := []*filer_pb.Entry{
		fileWithExt("v1", t1, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v1")}),
		fileWithExt("v2", t2, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v2")}),
		fileWithExt("v3", t3, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v3")}),
	}
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {versionsDir("foo"+s3_constants.VersionsFolder, "v2")},
		"/buckets/bkt/foo" + s3_constants.VersionsFolder: versions,
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got []*bootstrap.Entry
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 3)

	byID := map[string]*bootstrap.Entry{}
	for _, e := range got {
		byID[e.VersionID] = e
		assert.Equal(t, "foo", e.Path, "every sibling's Path is the logical key")
		assert.Equal(t, 3, e.NumVersions)
	}
	assert.True(t, byID["v2"].IsLatest, "pointer wins regardless of mtime order")
	assert.False(t, byID["v1"].IsLatest)
	assert.False(t, byID["v3"].IsLatest)
	require.NotNil(t, byID["v3"].NoncurrentIndex, "noncurrent siblings get a rank")
	require.NotNil(t, byID["v1"].NoncurrentIndex, "noncurrent siblings get a rank")
}

func TestFilerListFunc_VersionedExpansionNoPointerNewestSiblingWins(t *testing.T) {
	// Parent has no ExtLatestVersionIdKey. With no explicit-null bare
	// version, the newest-by-mtime sibling becomes latest.
	tNew := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	tOld := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	versions := []*filer_pb.Entry{
		fileWithExt("v_old", tOld, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("vold")}),
		fileWithExt("v_new", tNew, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("vnew")}),
	}
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {versionsDir("foo"+s3_constants.VersionsFolder, "")},
		"/buckets/bkt/foo" + s3_constants.VersionsFolder: versions,
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got []*bootstrap.Entry
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		got = append(got, e)
		return nil
	}))
	require.Len(t, got, 2)
	byID := map[string]*bootstrap.Entry{}
	for _, e := range got {
		byID[e.VersionID] = e
	}
	assert.True(t, byID["vnew"].IsLatest, "newest sibling wins when pointer is absent")
	assert.False(t, byID["vold"].IsLatest)
}

func TestFilerListFunc_VersionedExpansionExplicitNullIsLatestWhenPointerMissing(t *testing.T) {
	// Suspended-versioning shape: bare object marked with
	// ExtVersionIdKey="null", parent has no pointer. null is latest.
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tNull := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) // newest
	bareNull := fileWithExt("foo", tNull, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("null")})
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {
			bareNull,
			versionsDir("foo"+s3_constants.VersionsFolder, ""),
		},
		"/buckets/bkt/foo" + s3_constants.VersionsFolder: {
			fileWithExt("v1", t1, 1, map[string][]byte{s3_constants.ExtVersionIdKey: []byte("v1")}),
		},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got []*bootstrap.Entry
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		got = append(got, e)
		return nil
	}))
	// 2 sibling entries from expansion (null + v1). The bare "foo"
	// MUST be suppressed in pass 2 via skipBare.
	require.Len(t, got, 2)
	byID := map[string]*bootstrap.Entry{}
	for _, e := range got {
		byID[e.VersionID] = e
	}
	require.NotNil(t, byID["null"])
	require.NotNil(t, byID["v1"])
	assert.True(t, byID["null"].IsLatest)
	assert.False(t, byID["v1"].IsLatest)

	// Walk again, verify no duplicate emission of the bare "foo".
	count := 0
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		if e.Path == "foo" && e.VersionID == "" {
			t.Errorf("bare foo should be suppressed by skipBare, got %+v", e)
		}
		count++
		return nil
	}))
	assert.Equal(t, 2, count)
}

func TestFilerListFunc_VersionsDirWithoutMarkersRecursesAsRegular(t *testing.T) {
	// A `.versions`-named folder whose children have no
	// ExtVersionIdKey is a coincidence (user folder). Recurse into
	// it; the file inside should surface as a regular entry.
	mtime := time.Now()
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {versionsDir("looksLikeUserFolder"+s3_constants.VersionsFolder, "")},
		"/buckets/bkt/looksLikeUserFolder" + s3_constants.VersionsFolder: {
			file("inner.txt", mtime, 1),
		},
	}}
	listFn := FilerListFunc(client, "/buckets")
	var paths []string
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		paths = append(paths, e.Path)
		return nil
	}))
	assert.Equal(t, []string{"looksLikeUserFolder" + s3_constants.VersionsFolder + "/inner.txt"}, paths)
}

func TestFilerListFunc_VersionedDeleteMarkerPropagates(t *testing.T) {
	mtime := time.Now()
	versions := []*filer_pb.Entry{
		fileWithExt("v1", mtime, 0, map[string][]byte{
			s3_constants.ExtVersionIdKey:    []byte("v1"),
			s3_constants.ExtDeleteMarkerKey: []byte("true"),
		}),
	}
	client := &fakeFiler{tree: map[string][]*filer_pb.Entry{
		"/buckets/bkt": {versionsDir("foo"+s3_constants.VersionsFolder, "v1")},
		"/buckets/bkt/foo" + s3_constants.VersionsFolder: versions,
	}}
	listFn := FilerListFunc(client, "/buckets")
	var got *bootstrap.Entry
	require.NoError(t, listFn(context.Background(), "bkt", "", func(e *bootstrap.Entry) error {
		got = e
		return nil
	}))
	require.NotNil(t, got)
	assert.True(t, got.IsDeleteMarker, "ExtDeleteMarkerKey='true' must surface as IsDeleteMarker")
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
