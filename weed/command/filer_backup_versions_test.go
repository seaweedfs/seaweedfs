package command

import (
	"context"
	"testing"

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
