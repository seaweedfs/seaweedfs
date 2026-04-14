package nfs

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gonfs "github.com/willscott/go-nfs"
	gonfsfile "github.com/willscott/go-nfs/file"
	"google.golang.org/grpc"
)

type fakeListEntriesClient struct {
	responses []*filer_pb.ListEntriesResponse
	index     int
}

func (c *fakeListEntriesClient) Recv() (*filer_pb.ListEntriesResponse, error) {
	if c.index >= len(c.responses) {
		return nil, io.EOF
	}
	resp := c.responses[c.index]
	c.index++
	return resp, nil
}

type fakeNFSFilerClient struct {
	kv           map[string][]byte
	entries      map[util.FullPath]*filer_pb.Entry
	directories  map[util.FullPath][]*filer_pb.Entry
	updateResult map[util.FullPath]*filer_pb.Entry
	statistics   *filer_pb.StatisticsResponse
	updates      []*filer_pb.UpdateEntryRequest
}

func (f *fakeNFSFilerClient) KvGet(_ context.Context, in *filer_pb.KvGetRequest, _ ...grpc.CallOption) (*filer_pb.KvGetResponse, error) {
	if value, found := f.kv[string(in.Key)]; found {
		return &filer_pb.KvGetResponse{Value: value}, nil
	}
	return &filer_pb.KvGetResponse{}, nil
}

func (f *fakeNFSFilerClient) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	fullPath := util.NewFullPath(in.Directory, in.Name)
	if entry, found := f.entries[fullPath]; found {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (f *fakeNFSFilerClient) ListEntries(_ context.Context, in *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (nfsListEntriesClient, error) {
	entries := f.directories[util.FullPath(in.Directory)]
	responses := make([]*filer_pb.ListEntriesResponse, 0, len(entries))
	for _, entry := range entries {
		responses = append(responses, &filer_pb.ListEntriesResponse{Entry: entry})
	}
	return &fakeListEntriesClient{responses: responses}, nil
}

func (f *fakeNFSFilerClient) UpdateEntry(_ context.Context, in *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	f.updates = append(f.updates, in)

	fullPath := util.NewFullPath(in.Directory, in.Entry.Name)
	updatedEntry := f.updateResult[fullPath]
	if updatedEntry == nil {
		updatedEntry = in.Entry
	}
	f.entries[fullPath] = updatedEntry
	if updatedEntry.Attributes != nil && updatedEntry.Attributes.Inode != 0 {
		record := &filer.InodeIndexRecord{
			Generation: 1,
			Paths:      []string{string(fullPath)},
		}
		value, err := record.Encode()
		if err != nil {
			return nil, err
		}
		f.kv[string(filer.InodeIndexKey(updatedEntry.Attributes.Inode))] = value
	}

	return &filer_pb.UpdateEntryResponse{
		MetadataEvent: &filer_pb.SubscribeMetadataResponse{
			EventNotification: &filer_pb.EventNotification{
				NewEntry: updatedEntry,
			},
		},
	}, nil
}

func (f *fakeNFSFilerClient) Statistics(_ context.Context, _ *filer_pb.StatisticsRequest, _ ...grpc.CallOption) (*filer_pb.StatisticsResponse, error) {
	return f.statistics, nil
}

func testEntry(name string, isDirectory bool, inode uint64, mode uint32, content []byte) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: isDirectory,
		Content:     content,
		Attributes: &filer_pb.FuseAttributes{
			Inode:    inode,
			FileMode: mode,
			FileSize: uint64(len(content)),
		},
	}
}

func testIndexRecord(t *testing.T, inode uint64, generation uint64, path util.FullPath) []byte {
	t.Helper()
	record := &filer.InodeIndexRecord{
		Generation: generation,
		Paths:      []string{string(path)},
	}
	value, err := record.Encode()
	require.NoError(t, err)
	return value
}

func newTestServer(t *testing.T, exportRoot string, client *fakeNFSFilerClient) *Server {
	t.Helper()

	server, err := NewServer(&Option{
		Filer:         pb.ServerAddress("test-filer:8888"),
		FilerRootPath: exportRoot,
		Port:          2049,
	})
	require.NoError(t, err)

	server.withInternalClient = func(_ bool, fn func(nfsFilerClient) error) error {
		return fn(client)
	}
	server.withFilerClient = func(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
		return errors.New("test does not provide full filer client")
	}

	return server
}

func TestHandlerMountAndFileHandleRoundTrip(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 5, "/exports"),
			string(filer.InodeIndexKey(202)): testIndexRecord(t, 202, 9, "/exports/demo.txt"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports":          testEntry("exports", true, 101, uint32(0755), nil),
			"/exports/demo.txt": testEntry("demo.txt", false, 202, uint32(0644), []byte("hello")),
		},
	}

	server := newTestServer(t, "/exports", client)
	handler, err := server.newHandler()
	require.NoError(t, err)

	status, filesystem, authFlavors := handler.Mount(context.Background(), nil, gonfs.MountRequest{Dirpath: []byte("/exports")})
	require.Equal(t, gonfs.MountStatusOk, status)
	require.NotNil(t, filesystem)
	assert.Equal(t, []gonfs.AuthFlavor{gonfs.AuthFlavorNull, gonfs.AuthFlavorUnix}, authFlavors)

	handle := handler.ToHandle(filesystem, []string{"demo.txt"})
	require.NotEmpty(t, handle)

	resolvedFS, path, err := handler.FromHandle(handle)
	require.NoError(t, err)
	assert.Same(t, handler.rootFS, resolvedFS)
	assert.Equal(t, []string{"demo.txt"}, path)
}

func TestHandlerRejectsUnexpectedMountPath(t *testing.T) {
	client := &fakeNFSFilerClient{
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports": testEntry("exports", true, 101, uint32(0755), nil),
		},
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
		},
	}

	server := newTestServer(t, "/exports", client)
	handler, err := server.newHandler()
	require.NoError(t, err)

	status, filesystem, _ := handler.Mount(context.Background(), nil, gonfs.MountRequest{Dirpath: []byte("/wrong")})
	assert.Equal(t, gonfs.MountStatusErrNoEnt, status)
	assert.Nil(t, filesystem)
}

func TestSeaweedFileSystemBackfillsLegacyInodeOnStat(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
			string(filer.InodeIndexKey(303)): testIndexRecord(t, 303, 7, "/exports/legacy.txt"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports":            testEntry("exports", true, 101, uint32(0755), nil),
			"/exports/legacy.txt": testEntry("legacy.txt", false, 0, uint32(0644), []byte("abc")),
		},
		updateResult: map[util.FullPath]*filer_pb.Entry{
			"/exports/legacy.txt": testEntry("legacy.txt", false, 303, uint32(0644), []byte("abc")),
		},
	}

	server := newTestServer(t, "/exports", client)
	handler, err := server.newHandler()
	require.NoError(t, err)

	info, err := handler.rootFS.Lstat("/legacy.txt")
	require.NoError(t, err)
	require.Len(t, client.updates, 1)
	assert.Equal(t, int64(3), info.Size())

	nfsInfo, ok := info.Sys().(*gonfsfile.FileInfo)
	require.True(t, ok)
	assert.Equal(t, uint64(303), nfsInfo.Fileid)
}

func TestSeaweedFileSystemReadsInlineContent(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
			string(filer.InodeIndexKey(202)): testIndexRecord(t, 202, 3, "/exports/demo.txt"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports":          testEntry("exports", true, 101, uint32(0755), nil),
			"/exports/demo.txt": testEntry("demo.txt", false, 202, uint32(0644), []byte("hello")),
		},
	}

	server := newTestServer(t, "/exports", client)
	handler, err := server.newHandler()
	require.NoError(t, err)

	file, err := handler.rootFS.Open("/demo.txt")
	require.NoError(t, err)
	defer file.Close()

	buf := make([]byte, 5)
	n, err := file.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf))
}

func TestSeaweedFileSystemReadDirAndFSStat(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
			string(filer.InodeIndexKey(202)): testIndexRecord(t, 202, 2, "/exports/b.txt"),
			string(filer.InodeIndexKey(303)): testIndexRecord(t, 303, 3, "/exports/a.txt"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports":       testEntry("exports", true, 101, uint32(0755), nil),
			"/exports/b.txt": testEntry("b.txt", false, 202, uint32(0644), []byte("b")),
			"/exports/a.txt": testEntry("a.txt", false, 303, uint32(0644), []byte("aa")),
		},
		directories: map[util.FullPath][]*filer_pb.Entry{
			"/exports": {
				testEntry("b.txt", false, 202, uint32(0644), []byte("b")),
				testEntry("a.txt", false, 303, uint32(0644), []byte("aa")),
			},
		},
		statistics: &filer_pb.StatisticsResponse{
			TotalSize: 100,
			UsedSize:  40,
			FileCount: 3,
		},
	}

	server := newTestServer(t, "/exports", client)
	handler, err := server.newHandler()
	require.NoError(t, err)

	entries, err := handler.rootFS.ReadDir("/")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "a.txt", entries[0].Name())
	assert.Equal(t, "b.txt", entries[1].Name())

	var stat gonfs.FSStat
	err = handler.FSStat(context.Background(), handler.rootFS, &stat)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), stat.TotalSize)
	assert.Equal(t, uint64(60), stat.FreeSize)
	assert.Equal(t, uint64(60), stat.AvailableSize)
	assert.Equal(t, uint64(3), stat.TotalFiles)
}
