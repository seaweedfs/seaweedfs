package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Tests for filerSiblingLister cover the four routing-critical surfaces
// the dispatcher pipeline depends on: Survivors (count cap + null
// detection), ListVersions (pagination + filter), LookupNullVersion
// (regular vs directory-key marker, explicit-null flag), and
// LookupVersion (v_ prefix + NotFound collapse).

type fsListStream struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
	// recvErr is returned from the FIRST Recv() call before any
	// responses are delivered; lets tests inject the gRPC NotFound the
	// real filer surfaces when the parent directory doesn't exist.
	recvErr error
	ctx     context.Context
}

func (s *fsListStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	if s.ctx != nil {
		if err := s.ctx.Err(); err != nil {
			return nil, err
		}
	}
	if s.recvErr != nil {
		err := s.recvErr
		s.recvErr = nil
		return nil, err
	}
	if s.index >= len(s.responses) {
		return nil, io.EOF
	}
	resp := s.responses[s.index]
	s.index++
	return resp, nil
}

func (s *fsListStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *fsListStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *fsListStream) CloseSend() error             { return nil }
func (s *fsListStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}
func (s *fsListStream) SendMsg(any) error { return nil }
func (s *fsListStream) RecvMsg(any) error { return nil }

// fsFakeFiler implements just the SeaweedFilerClient methods that
// filerSiblingLister calls (ListEntries + LookupDirectoryEntry). Other
// methods on the embedded interface remain nil; calling them panics,
// which the tests rely on to catch regressions that try to reach beyond
// the documented surface.
type fsFakeFiler struct {
	filer_pb.SeaweedFilerClient

	mu sync.Mutex
	// tree maps a directory path to its immediate children. A directory
	// is "missing" when its key is absent from tree (we return ErrNotFound
	// from ListEntries / LookupDirectoryEntry to mimic the real filer).
	tree map[string][]*filer_pb.Entry
	// listErr / lookupErr force the next call to fail with this error.
	// Reset to nil after use so tests can mix success + failure runs.
	listErr   error
	lookupErr error
	// missing controls per-directory NotFound for ListEntries: when a path
	// is in this set, ListEntries returns filer_pb.ErrNotFound regardless
	// of whether tree has an entry for it. Lets us test the .versions/
	// not-yet-created vs. empty cases independently.
	missing map[string]struct{}
}

func newFakeFiler() *fsFakeFiler {
	return &fsFakeFiler{
		tree:    map[string][]*filer_pb.Entry{},
		missing: map[string]struct{}{},
	}
}

func (c *fsFakeFiler) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listErr != nil {
		err := c.listErr
		c.listErr = nil
		return nil, err
	}
	// Real-filer behavior on a missing parent: stream creation succeeds,
	// the first Recv() surfaces ErrNotFound. SeaweedList passes that
	// through unwrapped, which is what filerSiblingLister's
	// errors.Is(err, filer_pb.ErrNotFound) check depends on.
	if _, missing := c.missing[in.Directory]; missing {
		return &fsListStream{recvErr: filer_pb.ErrNotFound, ctx: ctx}, nil
	}
	src := c.tree[in.Directory]
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
	return &fsListStream{responses: resps, ctx: ctx}, nil
}

func (c *fsFakeFiler) LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lookupErr != nil {
		err := c.lookupErr
		c.lookupErr = nil
		return nil, err
	}
	for _, e := range c.tree[in.Directory] {
		if e != nil && e.Name == in.Name {
			return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
		}
	}
	return nil, filer_pb.ErrNotFound
}

func (c *fsFakeFiler) put(dir string, entries ...*filer_pb.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tree[dir] = append(c.tree[dir], entries...)
}

func (c *fsFakeFiler) markMissing(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.missing[dir] = struct{}{}
}

func newLister(c *fsFakeFiler) *filerSiblingLister {
	return &filerSiblingLister{client: c, bucketsPath: "/buckets"}
}

func versionEntry(name, versionID string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte(versionID),
		},
	}
}

func TestSurvivors_NoVersionsNoNullReturnsZero(t *testing.T) {
	f := newFakeFiler()
	// .versions/ doesn't exist (NotFound) and the bare key doesn't exist either.
	f.markMissing("/buckets/b/k" + s3_constants.VersionsFolder)
	l := newLister(f)

	s, err := l.Survivors(context.Background(), "b", "k")
	require.NoError(t, err)
	assert.Equal(t, 0, s.Count)
	assert.False(t, s.HasNullVersion)
	assert.Nil(t, s.LoneEntry)
}

func TestSurvivors_OneVersionPopulatesLoneEntry(t *testing.T) {
	// Count == 1 means LoneEntry must be set; the dispatcher's
	// sole-survivor path uses it to compare against the marker version.
	f := newFakeFiler()
	dir := "/buckets/b/k" + s3_constants.VersionsFolder
	f.put(dir, versionEntry("v_aaa", "aaa"))
	f.markMissing("/buckets/b") // bare key absent
	l := newLister(f)

	s, err := l.Survivors(context.Background(), "b", "k")
	require.NoError(t, err)
	assert.Equal(t, 1, s.Count)
	require.NotNil(t, s.LoneEntry)
	assert.Equal(t, "v_aaa", s.LoneEntry.Name)
	assert.False(t, s.HasNullVersion)
}

func TestSurvivors_MoreThanOneClearsLoneEntry(t *testing.T) {
	// Listing caps at 2; once Count > 1, LoneEntry must be reset to nil
	// so the caller doesn't mistakenly treat the second-arrival entry as
	// the sole survivor.
	f := newFakeFiler()
	dir := "/buckets/b/k" + s3_constants.VersionsFolder
	f.put(dir, versionEntry("v_aaa", "aaa"), versionEntry("v_bbb", "bbb"))
	f.markMissing("/buckets/b")
	l := newLister(f)

	s, err := l.Survivors(context.Background(), "b", "k")
	require.NoError(t, err)
	assert.Equal(t, 2, s.Count)
	assert.Nil(t, s.LoneEntry, "Count > 1 must clear LoneEntry")
}

func TestSurvivors_NullVersionDetectedFromBareRegularFile(t *testing.T) {
	// The bare <bucket>/<key> path resolves to a regular file when the
	// object pre-dates versioning; that file is the null version.
	f := newFakeFiler()
	f.markMissing("/buckets/b/k" + s3_constants.VersionsFolder)
	f.put("/buckets/b", &filer_pb.Entry{Name: "k", IsDirectory: false, Attributes: &filer_pb.FuseAttributes{}})
	l := newLister(f)

	s, err := l.Survivors(context.Background(), "b", "k")
	require.NoError(t, err)
	assert.True(t, s.HasNullVersion)
}

func TestSurvivors_DirectoryKeyMarkerCountsAsNull(t *testing.T) {
	// An explicit S3 directory-key marker (IsDirectory=true with
	// Attributes.Mime set) is also the null version. The listing path
	// downstream treats both the regular-file and the directory-marker
	// shape as null; Survivors must agree. The lister's NewFullPath
	// trims the trailing slash from objectKey before splitting, so the
	// stored entry Name has no trailing slash.
	f := newFakeFiler()
	f.markMissing("/buckets/b/dir" + s3_constants.VersionsFolder)
	f.put("/buckets/b", &filer_pb.Entry{
		Name:        "dir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mime: "application/x-directory"},
	})
	l := newLister(f)

	s, err := l.Survivors(context.Background(), "b", "dir/")
	require.NoError(t, err)
	assert.True(t, s.HasNullVersion, "directory-key marker must count as null version")
}

func TestSurvivors_PlainDirectoryDoesNotCount(t *testing.T) {
	// A plain directory (no Mime, just IsDirectory=true) is implicit-only
	// and must NOT register as a null version — otherwise normal prefix
	// directories would shadow real null-version detection.
	f := newFakeFiler()
	f.markMissing("/buckets/b/dir" + s3_constants.VersionsFolder)
	f.put("/buckets/b", &filer_pb.Entry{
		Name:        "dir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{}, // no Mime
	})
	l := newLister(f)

	s, err := l.Survivors(context.Background(), "b", "dir/")
	require.NoError(t, err)
	assert.False(t, s.HasNullVersion, "plain directory must not count as null version")
}

func TestSurvivors_VersionsListingErrorPropagates(t *testing.T) {
	// A non-NotFound error from the .versions/ listing must surface; the
	// dispatcher promotes this to a retry instead of a NOOP.
	f := newFakeFiler()
	f.listErr = errors.New("transport boom")
	l := newLister(f)

	_, err := l.Survivors(context.Background(), "b", "k")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transport boom")
}

func TestSurvivors_NullLookupErrorPropagates(t *testing.T) {
	// A non-NotFound error from the bare-key lookup must surface; we
	// can't conclude "no null" from a transport failure.
	f := newFakeFiler()
	f.markMissing("/buckets/b/k" + s3_constants.VersionsFolder)
	f.lookupErr = errors.New("lookup boom")
	l := newLister(f)

	_, err := l.Survivors(context.Background(), "b", "k")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lookup boom")
}

func TestListVersions_NotFoundReturnsNil(t *testing.T) {
	// A hard-deleted .versions/ container collapses to (nil, nil) so
	// callers can treat it as "no versions" without special-casing.
	f := newFakeFiler()
	f.markMissing("/buckets/b/k" + s3_constants.VersionsFolder)
	l := newLister(f)

	versions, err := l.ListVersions(context.Background(), "b", "k")
	require.NoError(t, err)
	assert.Nil(t, versions)
}

func TestListVersions_FiltersOutDirectoriesAndMissingVersionId(t *testing.T) {
	// Subdirectories and entries without ExtVersionIdKey aren't real
	// versions; the bootstrap walker writes them sometimes so the
	// listing must filter rather than treat them as data.
	f := newFakeFiler()
	dir := "/buckets/b/k" + s3_constants.VersionsFolder
	f.put(dir,
		versionEntry("v_aaa", "aaa"),
		&filer_pb.Entry{Name: "subdir", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
		&filer_pb.Entry{Name: "v_no_attrs"},                                       // nil Attributes
		&filer_pb.Entry{Name: "v_no_ext", Attributes: &filer_pb.FuseAttributes{}}, // missing Extended
		&filer_pb.Entry{Name: "v_empty_ext", Attributes: &filer_pb.FuseAttributes{}, Extended: map[string][]byte{s3_constants.ExtVersionIdKey: nil}}, // empty value
		versionEntry("v_zzz", "zzz"),
	)
	l := newLister(f)

	versions, err := l.ListVersions(context.Background(), "b", "k")
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "v_aaa", versions[0].Name)
	assert.Equal(t, "v_zzz", versions[1].Name)
}

func TestListVersions_PaginatesAcrossPages(t *testing.T) {
	// pageSize is 1024 inside the lister; populate just over the boundary
	// to force a second listing call. The fake's StartFromFileName
	// handling mirrors the real filer.
	f := newFakeFiler()
	dir := "/buckets/b/k" + s3_constants.VersionsFolder
	const total = 1030
	entries := make([]*filer_pb.Entry, 0, total)
	for i := 0; i < total; i++ {
		entries = append(entries, versionEntry(fmt.Sprintf("v_%05d", i), fmt.Sprintf("id_%05d", i)))
	}
	f.put(dir, entries...)
	l := newLister(f)

	versions, err := l.ListVersions(context.Background(), "b", "k")
	require.NoError(t, err)
	require.Len(t, versions, total)
	// Order is preserved across pages.
	assert.Equal(t, "v_00000", versions[0].Name)
	assert.Equal(t, fmt.Sprintf("v_%05d", total-1), versions[total-1].Name)
}

func TestListVersions_ErrorPropagates(t *testing.T) {
	f := newFakeFiler()
	f.listErr = errors.New("list boom")
	l := newLister(f)

	versions, err := l.ListVersions(context.Background(), "b", "k")
	assert.Nil(t, versions)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "list boom")
}

func TestLookupNullVersion_RegularFileExplicitFalse(t *testing.T) {
	// A bare regular file with no ExtVersionIdKey is the null version
	// but not "explicit" — the suspended-versioning write path flags
	// explicit nulls with ExtVersionIdKey="null".
	f := newFakeFiler()
	f.put("/buckets/b", &filer_pb.Entry{Name: "k", IsDirectory: false, Attributes: &filer_pb.FuseAttributes{}})
	l := newLister(f)

	entry, explicit, err := l.LookupNullVersion(context.Background(), "b", "k")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "k", entry.Name)
	assert.False(t, explicit)
}

func TestLookupNullVersion_ExplicitNullFlag(t *testing.T) {
	// ExtVersionIdKey="null" is the marker the suspended-versioning
	// write path sets; the lister surfaces it via the explicit return.
	f := newFakeFiler()
	f.put("/buckets/b", &filer_pb.Entry{
		Name:       "k",
		Attributes: &filer_pb.FuseAttributes{},
		Extended:   map[string][]byte{s3_constants.ExtVersionIdKey: []byte("null")},
	})
	l := newLister(f)

	entry, explicit, err := l.LookupNullVersion(context.Background(), "b", "k")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.True(t, explicit)
}

func TestLookupNullVersion_DirectoryKeyMarkerQualifies(t *testing.T) {
	// NewFullPath strips a trailing slash before splitting; the entry
	// Name as stored has no slash.
	f := newFakeFiler()
	f.put("/buckets/b", &filer_pb.Entry{
		Name:        "dir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{Mime: "application/x-directory"},
	})
	l := newLister(f)

	entry, _, err := l.LookupNullVersion(context.Background(), "b", "dir/")
	require.NoError(t, err)
	require.NotNil(t, entry)
}

func TestLookupNullVersion_PlainDirectoryRejected(t *testing.T) {
	// A plain directory must not be returned — otherwise the dispatcher
	// would treat any prefix directory as a null version.
	f := newFakeFiler()
	f.put("/buckets/b", &filer_pb.Entry{
		Name:        "dir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{},
	})
	l := newLister(f)

	entry, explicit, err := l.LookupNullVersion(context.Background(), "b", "dir/")
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.False(t, explicit)
}

func TestLookupNullVersion_NotFoundCollapses(t *testing.T) {
	f := newFakeFiler()
	l := newLister(f) // empty tree -> NotFound on lookup

	entry, explicit, err := l.LookupNullVersion(context.Background(), "b", "k")
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.False(t, explicit)
}

func TestLookupNullVersion_TransportErrorPropagates(t *testing.T) {
	f := newFakeFiler()
	f.lookupErr = errors.New("lookup boom")
	l := newLister(f)

	_, _, err := l.LookupNullVersion(context.Background(), "b", "k")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lookup boom")
}

func TestLookupVersion_EmptyVersionIdReturnsNil(t *testing.T) {
	// An empty versionID is a no-op — neither error nor result.
	f := newFakeFiler()
	l := newLister(f)

	entry, err := l.LookupVersion(context.Background(), "b", "k", "")
	require.NoError(t, err)
	assert.Nil(t, entry)
}

func TestLookupVersion_AppliesVPrefix(t *testing.T) {
	// File name on disk is "v_<id>"; the caller passes only the id.
	f := newFakeFiler()
	dir := "/buckets/b/k" + s3_constants.VersionsFolder
	f.put(dir, versionEntry("v_abc123", "abc123"))
	l := newLister(f)

	entry, err := l.LookupVersion(context.Background(), "b", "k", "abc123")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "v_abc123", entry.Name)
}

func TestLookupVersion_NotFoundCollapses(t *testing.T) {
	// Hard-delete between pointer update and lookup is the documented
	// race; collapse to (nil, nil) so the dispatcher continues without
	// retry-storming.
	f := newFakeFiler()
	l := newLister(f)

	entry, err := l.LookupVersion(context.Background(), "b", "k", "abc")
	require.NoError(t, err)
	assert.Nil(t, entry)
}

func TestLookupVersion_TransportErrorPropagates(t *testing.T) {
	f := newFakeFiler()
	f.lookupErr = errors.New("lookup boom")
	l := newLister(f)

	_, err := l.LookupVersion(context.Background(), "b", "k", "abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lookup boom")
}
