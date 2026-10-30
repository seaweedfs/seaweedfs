package command

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type lookupEntryFilerClient struct {
	entries map[string]*filer_pb.Entry
}

func (c *lookupEntryFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(&lookupEntrySeaweedClient{entries: c.entries})
}
func (c *lookupEntryFilerClient) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}
func (c *lookupEntryFilerClient) GetDataCenter() string { return "" }

type lookupEntrySeaweedClient struct {
	filer_pb.SeaweedFilerClient
	entries map[string]*filer_pb.Entry
}

func (c *lookupEntrySeaweedClient) LookupDirectoryEntry(ctx context.Context, in *filer_pb.LookupDirectoryEntryRequest, opts ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	entry, found := c.entries[in.Directory+"/"+in.Name]
	if !found {
		return nil, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
}

func (c *lookupEntrySeaweedClient) ListEntries(_ context.Context, in *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.ListEntriesResponse], error) {
	var entries []*filer_pb.Entry
	for fullPath, entry := range c.entries {
		if dir, _ := util.FullPath(fullPath).DirAndName(); dir == in.Directory {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	if in.StartFromFileName != "" {
		idx := sort.Search(len(entries), func(i int) bool { return entries[i].Name >= in.StartFromFileName })
		if !in.InclusiveStartFrom {
			for idx < len(entries) && entries[idx].Name == in.StartFromFileName {
				idx++
			}
		}
		entries = entries[idx:]
	}
	limit := in.Limit
	if limit == 0 {
		limit = filer.PaginationSize // the filer substitutes DirListingLimit for 0
	}
	if uint64(len(entries)) > uint64(limit) {
		entries = entries[:limit]
	}
	return &verifyTestStream{entries: entries}, nil
}

type recordSink struct {
	created map[string]*filer_pb.Entry
	deleted []string
}

func (s *recordSink) GetName() string { return "record" }
func (s *recordSink) Initialize(configuration util.Configuration, prefix string) error {
	return nil
}
func (s *recordSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	s.deleted = append(s.deleted, key)
	return nil
}
func (s *recordSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	s.created[key] = entry
	return nil
}
func (s *recordSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (bool, error) {
	return true, nil
}
func (s *recordSink) GetSinkToDirectory() string            { return "/" }
func (s *recordSink) GetDestinationIdentity() string        { return "record" }
func (s *recordSink) SetSourceFiler(fs *source.FilerSource) {}
func (s *recordSink) IsIncremental() bool                   { return false }

func newFlattener(entries map[string]*filer_pb.Entry) (*s3VersionFlattener, *recordSink) {
	rs := &recordSink{created: make(map[string]*filer_pb.Entry)}
	f := &s3VersionFlattener{
		filerSource:   &lookupEntryFilerClient{entries: entries},
		dataSink:      rs,
		sourcePath:    "/buckets",
		targetPath:    "/",
		doDeleteFiles: true,
	}
	return f, rs
}

func event(dir, parent string, oldEntry, newEntry *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      oldEntry,
			NewEntry:      newEntry,
			NewParentPath: parent,
		},
	}
}

func TestFlattenDropsVersionFileEvents(t *testing.T) {
	f, rs := newFlattener(map[string]*filer_pb.Entry{})
	v := &filer_pb.Entry{Name: "v_abc", Chunks: []*filer_pb.FileChunk{{FileId: "1,01"}}}

	for _, resp := range []*filer_pb.SubscribeMetadataResponse{
		event("/buckets/b/k.versions", "", nil, v),
		event("/buckets/b/k.versions", "", v, nil),
		event("/buckets/b/k.versions", "/buckets/b/k.versions", v, v),
	} {
		handled, err := f.process(resp)
		require.NoError(t, err)
		require.True(t, handled, "version file events should be consumed")
	}
	require.Empty(t, rs.created)
	require.Empty(t, rs.deleted)
}

func TestFlattenPassesThroughNormalEvents(t *testing.T) {
	f, rs := newFlattener(nil)

	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, &filer_pb.Entry{Name: "k"}))
	require.NoError(t, err)
	require.False(t, handled)

	// a regular file literally named v_foo outside a .versions dir is not a
	// version file
	handled, err = f.process(event("/buckets/b", "/buckets/b", nil, &filer_pb.Entry{Name: "v_notes"}))
	require.NoError(t, err)
	require.False(t, handled)

	// a plain file whose name ends with .versions is not a versions directory
	handled, err = f.process(event("/buckets/b", "/buckets/b", nil, &filer_pb.Entry{Name: "x.versions"}))
	require.NoError(t, err)
	require.False(t, handled)
	require.Empty(t, rs.created)
}

func TestFlattenUploadsLatestVersion(t *testing.T) {
	version := &filer_pb.Entry{
		Name:       "v_2",
		Attributes: &filer_pb.FuseAttributes{Mtime: 7},
		Chunks:     []*filer_pb.FileChunk{{FileId: "1,02"}},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{"/buckets/b/k.versions/v_2": version})

	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_2")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, version, rs.created["/b/k"])
	require.Empty(t, rs.deleted)
}

func TestFlattenDeletesOnLatestDeleteMarker(t *testing.T) {
	f, rs := newFlattener(nil)
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionIsDeleteMarker: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", dir, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"/b/k"}, rs.deleted)
	require.Empty(t, rs.created)
}

func TestFlattenDeleteHonorsDoDeleteFiles(t *testing.T) {
	f, rs := newFlattener(nil)
	f.doDeleteFiles = false
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionIsDeleteMarker: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", dir, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, rs.deleted)
}

func TestFlattenDeleteOnClearedPointerAndRemovedDir(t *testing.T) {
	f, rs := newFlattener(nil)
	withPointer := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_1")},
	}
	cleared := &filer_pb.Entry{Name: "k.versions", IsDirectory: true, Extended: map[string][]byte{}}

	handled, err := f.process(event("/buckets/b", "/buckets/b", withPointer, cleared))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"/b/k"}, rs.deleted)

	rs.deleted = nil
	handled, err = f.process(event("/buckets/b", "", cleared, nil))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"/b/k"}, rs.deleted)
}

func TestFlattenFreshVersionsDirIsSkipped(t *testing.T) {
	f, rs := newFlattener(nil)
	// a .versions dir create carries no pointer yet — the update that sets it
	// follows in the same write
	dir := &filer_pb.Entry{Name: "k.versions", IsDirectory: true}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, rs.deleted)
	require.Empty(t, rs.created)
}

func TestFlattenNullVersionIsLatest(t *testing.T) {
	nullObject := &filer_pb.Entry{
		Name:       "k",
		Attributes: &filer_pb.FuseAttributes{Mtime: 9},
		Chunks:     []*filer_pb.FileChunk{{FileId: "1,09"}},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{"/buckets/b/k": nullObject})
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtNullVersionIsLatestKey: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, nullObject, rs.created["/b/k"])
}

func TestFlattenNullVersionDeletedRescansNewestVersion(t *testing.T) {
	// deleting ?versionId=null removes the regular object without touching the
	// null-is-latest pointer: the flattened key must fall back to the newest
	// remaining version, the same rescan the S3 read path does
	older := &filer_pb.Entry{Name: "v_00000000000000010000000000000000", Chunks: []*filer_pb.FileChunk{{FileId: "1,01"}}}
	newer := &filer_pb.Entry{Name: "v_00000000000000020000000000000000", Chunks: []*filer_pb.FileChunk{{FileId: "1,02"}}}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/k.versions/" + older.Name: older,
		"/buckets/b/k.versions/" + newer.Name: newer,
	})
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtNullVersionIsLatestKey: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, newer, rs.created["/b/k"])
	require.Empty(t, rs.deleted)
}

func TestFlattenNullVersionDeletedNewestIsDeleteMarker(t *testing.T) {
	marker := &filer_pb.Entry{
		Name:     "v_00000000000000020000000000000000",
		Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	older := &filer_pb.Entry{Name: "v_00000000000000010000000000000000", Chunks: []*filer_pb.FileChunk{{FileId: "1,01"}}}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/k.versions/" + older.Name:  older,
		"/buckets/b/k.versions/" + marker.Name: marker,
	})
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtNullVersionIsLatestKey: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"/b/k"}, rs.deleted)
	require.Empty(t, rs.created)
}

func TestFlattenNullVersionDeletedNoVersionsLeftDeletes(t *testing.T) {
	f, rs := newFlattener(map[string]*filer_pb.Entry{})
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtNullVersionIsLatestKey: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"/b/k"}, rs.deleted)
}

func TestFlattenNullVersionDeletedScansPastOnePage(t *testing.T) {
	// old-format version names sort oldest-first and the filer caps one
	// listing at DirListingLimit, so with a long history the newest version
	// sits beyond the first page — the rescan must read every page
	entries := map[string]*filer_pb.Entry{}
	var newest *filer_pb.Entry
	for i := 1; i <= filer.PaginationSize+1; i++ {
		v := &filer_pb.Entry{
			Name:   fmt.Sprintf("v_%032x", i),
			Chunks: []*filer_pb.FileChunk{{FileId: fmt.Sprintf("1,%02x", i)}},
		}
		entries["/buckets/b/k.versions/"+v.Name] = v
		newest = v
	}
	f, rs := newFlattener(entries)
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtNullVersionIsLatestKey: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, newest, rs.created["/b/k"])
}

func TestFlattenVersionFileEventRefreshesCurrentVersion(t *testing.T) {
	// a tag-only update touches the version file, not the pointer: re-resolving
	// the pointer must still upload the current version to the flat key
	version := &filer_pb.Entry{Name: "v_2", Chunks: []*filer_pb.FileChunk{{FileId: "1,02"}}}
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_2")},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/k.versions":     dir,
		"/buckets/b/k.versions/v_2": version,
	})

	tagUpdate := &filer_pb.Entry{Name: "v_2", Extended: map[string][]byte{"X-Amz-Tagging-env": []byte("prod")}}
	handled, err := f.process(event("/buckets/b/k.versions", "/buckets/b/k.versions", tagUpdate, tagUpdate))
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, version, rs.created["/b/k"])
}

func TestFlattenExcludedObjectNameIsNotCopiedOrDeleted(t *testing.T) {
	f, rs := newFlattener(nil)
	f.excludeFileNames = wildcard.CompileWildcardMatchers("*.tmp")

	dir := &filer_pb.Entry{
		Name:        "notes.tmp.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionIsDeleteMarker: []byte("true")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", dir, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, rs.deleted, "delete on the excluded flat key must be skipped")

	handled, err = f.process(event("/buckets/b", "", dir, nil))
	require.NoError(t, err)
	require.True(t, handled)
	require.Empty(t, rs.deleted)
}

func TestFlattenRegularPathFollowsVersionsPointer(t *testing.T) {
	// deleting the null object must not remove the flattened key while the
	// .versions pointer names a live version
	version := &filer_pb.Entry{Name: "v_2", Chunks: []*filer_pb.FileChunk{{FileId: "1,02"}}}
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_2")},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/k.versions":     dir,
		"/buckets/b/k.versions/v_2": version,
	})

	handled, err := f.process(event("/buckets/b", "", &filer_pb.Entry{Name: "k"}, nil))
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, version, rs.created["/b/k"])
	require.Empty(t, rs.deleted)
}

func TestFlattenRenameLeavesDeleteForGenericPath(t *testing.T) {
	// rename a -> b where only b carries a .versions directory: the flattener
	// refreshes b through the pointer but the old flattened key held the moved
	// file, so the event must reach the generic path as a lone delete of a
	version := &filer_pb.Entry{Name: "v_9", Chunks: []*filer_pb.FileChunk{{FileId: "1,09"}}}
	dir := &filer_pb.Entry{
		Name:        "b.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_9")},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/b.versions":     dir,
		"/buckets/b/b.versions/v_9": version,
	})

	resp := event("/buckets/b", "/buckets/b", &filer_pb.Entry{Name: "a"}, &filer_pb.Entry{Name: "b"})
	handled, err := f.process(resp)
	require.NoError(t, err)
	require.False(t, handled, "old side has no .versions sibling — generic path must delete a")
	require.NotNil(t, resp.EventNotification.OldEntry)
	require.Nil(t, resp.EventNotification.NewEntry)
	require.Same(t, version, rs.created["/b/b"])
}

func TestFlattenRenameLeavesCreateForGenericPath(t *testing.T) {
	// rename a -> b where only a carries a .versions directory: the pointer
	// still decides flattened a, so the generic path must see a lone create
	// of b and never delete the flattened old key
	version := &filer_pb.Entry{Name: "v_2", Chunks: []*filer_pb.FileChunk{{FileId: "1,02"}}}
	dir := &filer_pb.Entry{
		Name:        "a.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_2")},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/a.versions":     dir,
		"/buckets/b/a.versions/v_2": version,
	})

	resp := event("/buckets/b", "/buckets/b", &filer_pb.Entry{Name: "a"}, &filer_pb.Entry{Name: "b"})
	handled, err := f.process(resp)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, resp.EventNotification.OldEntry)
	require.NotNil(t, resp.EventNotification.NewEntry)
	require.Same(t, version, rs.created["/b/a"])
	require.Empty(t, rs.deleted)
}

func TestFlattenRenameBothSidesVersioned(t *testing.T) {
	versionA := &filer_pb.Entry{Name: "v_2", Chunks: []*filer_pb.FileChunk{{FileId: "1,02"}}}
	versionB := &filer_pb.Entry{Name: "v_9", Chunks: []*filer_pb.FileChunk{{FileId: "1,09"}}}
	f, rs := newFlattener(map[string]*filer_pb.Entry{
		"/buckets/b/a.versions": {
			Name:        "a.versions",
			IsDirectory: true,
			Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_2")},
		},
		"/buckets/b/a.versions/v_2": versionA,
		"/buckets/b/b.versions": {
			Name:        "b.versions",
			IsDirectory: true,
			Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_9")},
		},
		"/buckets/b/b.versions/v_9": versionB,
	})

	resp := event("/buckets/b", "/buckets/b", &filer_pb.Entry{Name: "a"}, &filer_pb.Entry{Name: "b"})
	handled, err := f.process(resp)
	require.NoError(t, err)
	require.True(t, handled)
	require.NotNil(t, resp.EventNotification.OldEntry)
	require.NotNil(t, resp.EventNotification.NewEntry)
	require.Same(t, versionA, rs.created["/b/a"])
	require.Same(t, versionB, rs.created["/b/b"])
}

func TestFlattenDeleteMarkerVersionEntry(t *testing.T) {
	marker := &filer_pb.Entry{
		Name:     "v_3",
		Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	f, rs := newFlattener(map[string]*filer_pb.Entry{"/buckets/b/k.versions/v_3": marker})
	dir := &filer_pb.Entry{
		Name:        "k.versions",
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtLatestVersionFileNameKey: []byte("v_3")},
	}
	handled, err := f.process(event("/buckets/b", "/buckets/b", nil, dir))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"/b/k"}, rs.deleted)
}
