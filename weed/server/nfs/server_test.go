package nfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gonfs "github.com/willscott/go-nfs"
	gonfsfile "github.com/willscott/go-nfs/file"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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
	updateResult map[util.FullPath]*filer_pb.Entry
	statistics   *filer_pb.StatisticsResponse
	creates      []*filer_pb.CreateEntryRequest
	updates      []*filer_pb.UpdateEntryRequest
	deletes      []*filer_pb.DeleteEntryRequest
	renames      []*filer_pb.AtomicRenameEntryRequest
	nextInode    uint64
}

type fakeChunkUploadCall struct {
	assignRequest *filer_pb.AssignVolumeRequest
	uploadOption  *operation.UploadOption
	uploadURL     string
	data          []byte
}

type fakeChunkUploader struct {
	fileID string
	result *operation.UploadResult
	err    error
	calls  []fakeChunkUploadCall
}

func (f *fakeNFSFilerClient) KvGet(_ context.Context, in *filer_pb.KvGetRequest, _ ...grpc.CallOption) (*filer_pb.KvGetResponse, error) {
	if value, found := f.kv[string(in.Key)]; found {
		return &filer_pb.KvGetResponse{Value: value}, nil
	}
	return &filer_pb.KvGetResponse{}, nil
}

func (f *fakeNFSFilerClient) LookupDirectoryEntry(_ context.Context, in *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	fullPath := util.NewFullPath(in.Directory, in.Name)
	if entry := f.materializeEntry(fullPath); entry != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: entry}, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (f *fakeNFSFilerClient) ListEntries(_ context.Context, in *filer_pb.ListEntriesRequest, _ ...grpc.CallOption) (nfsListEntriesClient, error) {
	requestedDir := util.FullPath(in.Directory)
	var entries []*filer_pb.Entry
	for fullPath, entry := range f.entries {
		dir, _ := fullPath.DirAndName()
		if util.FullPath(dir) != requestedDir {
			continue
		}
		if materialized := f.materializeEntry(fullPath); materialized != nil {
			entries = append(entries, materialized)
		} else {
			entries = append(entries, cloneEntry(entry))
		}
	}
	responses := make([]*filer_pb.ListEntriesResponse, 0, len(entries))
	for _, entry := range entries {
		responses = append(responses, &filer_pb.ListEntriesResponse{Entry: entry})
	}
	return &fakeListEntriesClient{responses: responses}, nil
}

func (f *fakeNFSFilerClient) CreateEntry(_ context.Context, in *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	f.creates = append(f.creates, in)

	fullPath := util.NewFullPath(in.Directory, in.Entry.Name)
	if _, found := f.entries[fullPath]; found {
		return &filer_pb.CreateEntryResponse{
			Error:     "entry already exists",
			ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS,
		}, nil
	}

	entry := cloneEntry(in.Entry)
	storedEntry := f.persistEntry(fullPath, entry, false)
	return &filer_pb.CreateEntryResponse{
		MetadataEvent: &filer_pb.SubscribeMetadataResponse{
			EventNotification: &filer_pb.EventNotification{
				NewEntry: cloneEntry(storedEntry),
			},
		},
	}, nil
}

func (f *fakeNFSFilerClient) UpdateEntry(_ context.Context, in *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	f.updates = append(f.updates, in)

	fullPath := util.NewFullPath(in.Directory, in.Entry.Name)
	updatedEntry := f.updateResult[fullPath]
	if updatedEntry == nil {
		updatedEntry = cloneEntry(in.Entry)
	}
	storedEntry := f.persistEntry(fullPath, updatedEntry, false)

	return &filer_pb.UpdateEntryResponse{
		MetadataEvent: &filer_pb.SubscribeMetadataResponse{
			EventNotification: &filer_pb.EventNotification{
				NewEntry: cloneEntry(storedEntry),
			},
		},
	}, nil
}

func (f *fakeNFSFilerClient) DeleteEntry(_ context.Context, in *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	f.deletes = append(f.deletes, in)

	fullPath := util.NewFullPath(in.Directory, in.Name)
	entry, found := f.entries[fullPath]
	if !found {
		return &filer_pb.DeleteEntryResponse{Error: filer_pb.ErrNotFound.Error()}, nil
	}

	if len(entry.GetHardLinkId()) > 0 {
		f.decrementHardLink(entry.GetHardLinkId())
	}
	if inode := entry.GetAttributes().GetInode(); inode != 0 {
		f.removeInodeIndexPath(fullPath, inode)
	}
	delete(f.entries, fullPath)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *fakeNFSFilerClient) AtomicRenameEntry(_ context.Context, in *filer_pb.AtomicRenameEntryRequest, _ ...grpc.CallOption) (*filer_pb.AtomicRenameEntryResponse, error) {
	f.renames = append(f.renames, in)

	oldPath := util.NewFullPath(in.OldDirectory, in.OldName)
	entry, found := f.entries[oldPath]
	if !found {
		return nil, filer_pb.ErrNotFound
	}
	delete(f.entries, oldPath)
	if inode := entry.GetAttributes().GetInode(); inode != 0 {
		f.removeInodeIndexPath(oldPath, inode)
	}

	newPath := util.NewFullPath(in.NewDirectory, in.NewName)
	renamed := cloneEntry(entry)
	renamed.Name = in.NewName
	renamed = f.persistEntry(newPath, renamed, true)

	return &filer_pb.AtomicRenameEntryResponse{}, nil
}

func (f *fakeNFSFilerClient) Statistics(_ context.Context, _ *filer_pb.StatisticsRequest, _ ...grpc.CallOption) (*filer_pb.StatisticsResponse, error) {
	return f.statistics, nil
}

func (f *fakeNFSFilerClient) persistEntry(fullPath util.FullPath, entry *filer_pb.Entry, preserveZeroInode bool) *filer_pb.Entry {
	if f.entries == nil {
		f.entries = make(map[util.FullPath]*filer_pb.Entry)
	}
	if f.kv == nil {
		f.kv = make(map[string][]byte)
	}

	cloned := cloneEntry(entry)
	if cloned.Attributes == nil {
		cloned.Attributes = &filer_pb.FuseAttributes{}
	}
	if !preserveZeroInode && cloned.Attributes.Inode == 0 {
		cloned.Attributes.Inode = f.allocateInode()
	}
	cloned.Name = fullPath.Name()
	f.entries[fullPath] = cloned

	if cloned.Attributes.Inode != 0 {
		f.addInodeIndexPath(fullPath, cloned.Attributes.Inode)
	}
	if len(cloned.GetHardLinkId()) > 0 {
		f.storeHardLinkBlob(fullPath, cloned)
	}
	return cloned
}

func (f *fakeNFSFilerClient) materializeEntry(fullPath util.FullPath) *filer_pb.Entry {
	entry, found := f.entries[fullPath]
	if !found || entry == nil {
		return nil
	}
	cloned := cloneEntry(entry)
	if len(cloned.GetHardLinkId()) == 0 {
		return cloned
	}

	value, found := f.kv[string(cloned.GetHardLinkId())]
	if !found {
		return cloned
	}

	dir, _ := fullPath.DirAndName()
	fsEntry := filer.FromPbEntry(dir, cloned)
	if err := fsEntry.DecodeAttributesAndChunks(value); err != nil {
		return cloned
	}
	fsEntry.FullPath = fullPath
	return fsEntry.ToProtoEntry()
}

func (f *fakeNFSFilerClient) addInodeIndexPath(fullPath util.FullPath, inode uint64) {
	if inode == 0 {
		return
	}

	record := &filer.InodeIndexRecord{Generation: filer.InodeIndexInitialGeneration}
	if value, found := f.kv[string(filer.InodeIndexKey(inode))]; found {
		if decoded, err := filer.DecodeInodeIndexRecord(value); err == nil {
			record = decoded
		}
	}
	record.Paths = append(record.Paths, string(fullPath))
	value, err := record.Encode()
	if err == nil {
		f.kv[string(filer.InodeIndexKey(inode))] = value
	}
}

func (f *fakeNFSFilerClient) removeInodeIndexPath(fullPath util.FullPath, inode uint64) {
	if inode == 0 {
		return
	}

	key := string(filer.InodeIndexKey(inode))
	value, found := f.kv[key]
	if !found {
		return
	}
	record, err := filer.DecodeInodeIndexRecord(value)
	if err != nil {
		delete(f.kv, key)
		return
	}
	var kept []string
	for _, path := range record.Paths {
		if util.FullPath(path) != fullPath {
			kept = append(kept, path)
		}
	}
	record.Paths = kept
	if len(record.Paths) == 0 {
		delete(f.kv, key)
		return
	}
	value, err = record.Encode()
	if err == nil {
		f.kv[key] = value
	}
}

func (f *fakeNFSFilerClient) storeHardLinkBlob(fullPath util.FullPath, entry *filer_pb.Entry) {
	dir, _ := fullPath.DirAndName()
	fsEntry := filer.FromPbEntry(dir, cloneEntry(entry))
	fsEntry.FullPath = fullPath
	value, err := fsEntry.EncodeAttributesAndChunks()
	if err == nil {
		f.kv[string(entry.GetHardLinkId())] = value
	}
}

func (f *fakeNFSFilerClient) decrementHardLink(hardLinkID []byte) {
	value, found := f.kv[string(hardLinkID)]
	if !found {
		return
	}

	fsEntry := &filer.Entry{}
	if err := fsEntry.DecodeAttributesAndChunks(value); err != nil {
		return
	}
	fsEntry.HardLinkCounter--
	if fsEntry.HardLinkCounter <= 0 {
		delete(f.kv, string(hardLinkID))
		return
	}
	value, err := fsEntry.EncodeAttributesAndChunks()
	if err == nil {
		f.kv[string(hardLinkID)] = value
	}
}

func (f *fakeNFSFilerClient) allocateInode() uint64 {
	if f.nextInode == 0 {
		f.nextInode = 1000
	}
	f.nextInode++
	return f.nextInode
}

func (u *fakeChunkUploader) UploadWithRetry(_ filer_pb.FilerClient, assignRequest *filer_pb.AssignVolumeRequest, uploadOption *operation.UploadOption, genFileUrlFn func(host, fileId string) string, reader io.Reader) (string, *operation.UploadResult, error, []byte) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", nil, err, nil
	}

	fileID := u.fileID
	if fileID == "" {
		fileID = "7,abc"
	}
	result := u.result
	if result == nil {
		result = &operation.UploadResult{
			Size:       uint32(len(data)),
			ContentMd5: "etag",
		}
	}

	var assignClone *filer_pb.AssignVolumeRequest
	if assignRequest != nil {
		assignClone, _ = proto.Clone(assignRequest).(*filer_pb.AssignVolumeRequest)
	}
	var optionClone *operation.UploadOption
	if uploadOption != nil {
		copied := *uploadOption
		optionClone = &copied
	}

	u.calls = append(u.calls, fakeChunkUploadCall{
		assignRequest: assignClone,
		uploadOption:  optionClone,
		uploadURL:     genFileUrlFn("volume.example:8080", fileID),
		data:          bytes.Clone(data),
	})
	return fileID, result, u.err, data
}

func cloneEntry(entry *filer_pb.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	cloned, _ := proto.Clone(entry).(*filer_pb.Entry)
	return cloned
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

func TestSeaweedFileSystemSupportsNamespaceMutations(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports": testEntry("exports", true, 101, uint32(0755), nil),
		},
	}

	server := newTestServer(t, "/exports", client)
	handler, err := server.newHandler()
	require.NoError(t, err)

	err = handler.rootFS.MkdirAll("/docs", 0o755)
	require.NoError(t, err)

	file, err := handler.rootFS.Create("/docs/note.txt")
	require.NoError(t, err)
	_, err = file.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	err = handler.rootFS.Chmod("/docs/note.txt", 0o600)
	require.NoError(t, err)

	err = handler.rootFS.Rename("/docs/note.txt", "/docs/final.txt")
	require.NoError(t, err)

	truncateFile, err := handler.rootFS.OpenFile("/docs/final.txt", os.O_WRONLY|os.O_EXCL, 0)
	require.NoError(t, err)
	require.NoError(t, truncateFile.Truncate(2))
	require.NoError(t, truncateFile.Close())

	readFile, err := handler.rootFS.Open("/docs/final.txt")
	require.NoError(t, err)
	defer readFile.Close()

	buf := make([]byte, 2)
	n, err := readFile.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "he", string(buf))

	info, err := handler.rootFS.Stat("/docs/final.txt")
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	assert.Equal(t, int64(2), info.Size())

	err = handler.rootFS.Remove("/docs/final.txt")
	require.NoError(t, err)
	_, err = handler.rootFS.Stat("/docs/final.txt")
	require.ErrorIs(t, err, os.ErrNotExist)

	require.Len(t, client.creates, 2)
	require.Len(t, client.updates, 3)
	require.Len(t, client.renames, 1)
	require.Len(t, client.deletes, 1)
}

func TestSeaweedFileSystemUploadsLargeWritesAsChunks(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports": testEntry("exports", true, 101, uint32(0755), nil),
		},
	}
	uploader := &fakeChunkUploader{fileID: "9,xyz"}

	server := newTestServer(t, "/exports", client)
	server.option.VolumeServerAccess = "filerProxy"
	server.newUploader = func() (chunkUploader, error) {
		return uploader, nil
	}

	handler, err := server.newHandler()
	require.NoError(t, err)

	require.NoError(t, handler.rootFS.MkdirAll("/docs", 0o755))

	file, err := handler.rootFS.Create("/docs/big.bin")
	require.NoError(t, err)

	payload := bytes.Repeat([]byte("a"), maxInlineWriteSize+1)
	n, err := file.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, file.Close())

	require.Len(t, uploader.calls, 1)
	call := uploader.calls[0]
	require.NotNil(t, call.assignRequest)
	require.NotNil(t, call.uploadOption)
	assert.Equal(t, "/exports/docs/big.bin", call.assignRequest.Path)
	assert.Equal(t, "big.bin", call.uploadOption.Filename)
	assert.Equal(t, "http://test-filer:8888/?proxyChunkId=9,xyz", call.uploadURL)
	assert.Equal(t, payload, call.data)

	entry := client.entries["/exports/docs/big.bin"]
	require.NotNil(t, entry)
	require.Len(t, entry.GetChunks(), 1)
	assert.Nil(t, entry.Content)
	assert.Equal(t, uint64(len(payload)), entry.GetAttributes().GetFileSize())
	assert.Equal(t, "9,xyz", entry.GetChunks()[0].GetFileId())
}
