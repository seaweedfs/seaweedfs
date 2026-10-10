package weed_server

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// renameRemoteClient records the remote calls a rename makes. It cannot copy;
// copyingRenameRemoteClient adds CopyFile.
type renameRemoteClient struct {
	remote_storage.RemoteStorageClient
	mu      sync.Mutex
	calls   []string
	copyErr error
}

func (c *renameRemoteClient) record(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *renameRemoteClient) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *renameRemoteClient) StatFile(*remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	return nil, remote_storage.ErrRemoteObjectNotFound
}

func (c *renameRemoteClient) ListDirectory(context.Context, *remote_pb.RemoteStorageLocation, remote_storage.VisitFunc) error {
	return nil
}

func (c *renameRemoteClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) error {
	c.record("delete " + loc.Bucket + loc.Path)
	return nil
}

func (c *renameRemoteClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) error {
	c.record("rmdir " + loc.Bucket + loc.Path)
	return nil
}

type copyingRenameRemoteClient struct {
	*renameRemoteClient
}

func (c copyingRenameRemoteClient) CopyFile(src, dst *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	if c.copyErr != nil {
		return nil, c.copyErr
	}
	c.record("copy " + src.Bucket + src.Path + " " + dst.Bucket + dst.Path)
	return &filer_pb.RemoteEntry{StorageName: "r1", RemoteSize: 10, RemoteMtime: 1800000000, RemoteETag: `"copy"`}, nil
}

// newRenameRemoteTestServer mounts /buckets/b, /buckets/c and /data/m on r1,
// through client.
func newRenameRemoteTestServer(t *testing.T, client remote_storage.RemoteStorageClient) (*FilerServer, *renameTestStore) {
	t.Helper()
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	f.BuildGuardedRemoteClient = func(context.Context, *remote_pb.RemoteConf, bool) (remote_storage.RemoteStorageClient, error) {
		return client, nil
	}
	mapping, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/b": {Name: "r1", Bucket: "origin", Path: "/"},
		"/buckets/c": {Name: "r1", Bucket: "other", Path: "/"},
		"/data/m":    {Name: "r1", Bucket: "origin", Path: "/m"},
	}})
	require.NoError(t, err)
	conf, err := proto.Marshal(&remote_pb.RemoteConf{Name: "r1", Type: "s3"})
	require.NoError(t, err)
	for name, content := range map[string][]byte{filer.REMOTE_STORAGE_MOUNT_FILE: mapping, "r1" + filer.REMOTE_STORAGE_CONF_SUFFIX: conf} {
		entry := newFileEntry(filer.DirectoryEtcRemote+"/"+name, 1)
		entry.Content = content
		store.entries[string(entry.FullPath)] = entry
	}
	require.NoError(t, f.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(f))
	for _, dir := range []string{"/buckets", "/buckets/b", "/buckets/b/src", "/buckets/b/dst", "/buckets/c", "/data", "/data/m"} {
		store.entries[dir] = newDirectoryEntry(dir, 2)
	}
	return &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}, store
}

func remoteOnlyRenameEntry(path string, inode uint64) *filer.Entry {
	entry := newFileEntry(path, inode)
	entry.FileSize = 10
	entry.Remote = &filer_pb.RemoteEntry{StorageName: "r1", RemoteSize: 10, RemoteMtime: 1700000000, RemoteETag: `"orig"`}
	return entry
}

func renameFile(server *FilerServer, oldPath, newPath util.FullPath) error {
	oldDir, oldName := oldPath.DirAndName()
	newDir, newName := newPath.DirAndName()
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: oldDir,
		OldName:      oldName,
		NewDirectory: newDir,
		NewName:      newName,
	})
	return err
}

func TestRenameRemoteOnlyEntryCopiesBeforeDeletingTheOldObject(t *testing.T) {
	client := copyingRenameRemoteClient{&renameRemoteClient{}}
	server, store := newRenameRemoteTestServer(t, client)
	store.entries["/buckets/b/src/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/src/a.jpg", 101)

	require.NoError(t, renameFile(server, "/buckets/b/src/a.jpg", "/buckets/b/dst/a.jpg"))

	assert.Equal(t, []string{"copy origin/src/a.jpg origin/dst/a.jpg", "delete origin/src/a.jpg"}, client.recorded())
	_, err := store.FindEntry(context.Background(), "/buckets/b/src/a.jpg")
	assert.ErrorIs(t, err, filer_pb.ErrNotFound)
	dst, err := store.FindEntry(context.Background(), "/buckets/b/dst/a.jpg")
	require.NoError(t, err)
	assert.Equal(t, `"copy"`, dst.Remote.RemoteETag)
	assert.Equal(t, uint64(101), dst.Attr.Inode)
}

func TestRenameRemoteOnlyEntryOverTargetKeepsTheCopy(t *testing.T) {
	client := copyingRenameRemoteClient{&renameRemoteClient{}}
	server, store := newRenameRemoteTestServer(t, client)
	store.entries["/buckets/b/src/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/src/a.jpg", 101)
	store.entries["/buckets/b/dst/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/dst/a.jpg", 202)
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	require.NoError(t, renameFile(server, "/buckets/b/src/a.jpg", "/buckets/b/dst/a.jpg"))

	// the target's delete must not take the copy that replaced its object
	assert.Equal(t, []string{"copy origin/src/a.jpg origin/dst/a.jpg", "delete origin/src/a.jpg"}, client.recorded())
	events := queue.snapshot()
	require.Len(t, events, 2)
	assert.True(t, filer.IsMetadataOnlyDelete(events[0].notification.OldEntry), "the target's delete event must leave the remote object alone")
	assert.False(t, filer.IsMetadataOnlyDelete(events[1].notification.OldEntry))
	dst, err := store.FindEntry(context.Background(), "/buckets/b/dst/a.jpg")
	require.NoError(t, err)
	assert.Equal(t, uint64(101), dst.Attr.Inode)
	assert.Equal(t, `"copy"`, dst.Remote.RemoteETag)
}

func TestRenameRemoteOnlyEntryCopyFailureChangesNothing(t *testing.T) {
	client := copyingRenameRemoteClient{&renameRemoteClient{copyErr: errors.New("copy refused")}}
	server, store := newRenameRemoteTestServer(t, client)
	store.entries["/buckets/b/src/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/src/a.jpg", 101)
	store.entries["/buckets/b/dst/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/dst/a.jpg", 202)

	err := renameFile(server, "/buckets/b/src/a.jpg", "/buckets/b/dst/a.jpg")
	require.ErrorContains(t, err, "copy refused")

	assert.Empty(t, client.recorded())
	src, err := store.FindEntry(context.Background(), "/buckets/b/src/a.jpg")
	require.NoError(t, err)
	assert.Equal(t, `"orig"`, src.Remote.RemoteETag)
	dst, err := store.FindEntry(context.Background(), "/buckets/b/dst/a.jpg")
	require.NoError(t, err)
	assert.Equal(t, uint64(202), dst.Attr.Inode)
}

func TestRenameRemoteOnlyEntryWithoutCopierIsRefused(t *testing.T) {
	client := &renameRemoteClient{}
	server, store := newRenameRemoteTestServer(t, client)
	store.entries["/buckets/b/src/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/src/a.jpg", 101)

	err := renameFile(server, "/buckets/b/src/a.jpg", "/buckets/b/dst/a.jpg")
	require.ErrorContains(t, err, "cannot copy")

	assert.Empty(t, client.recorded())
	_, err = store.FindEntry(context.Background(), "/buckets/b/src/a.jpg")
	require.NoError(t, err)
	_, err = store.FindEntry(context.Background(), "/buckets/b/dst/a.jpg")
	assert.ErrorIs(t, err, filer_pb.ErrNotFound)
}

func TestRenameRemoteOnlyEntryOutOfItsMountIsRefused(t *testing.T) {
	for _, paths := range [][2]util.FullPath{
		{"/buckets/b/src/a.jpg", "/buckets/c/a.jpg"},
		{"/data/m/a.jpg", "/data/a.jpg"},
	} {
		t.Run(string(paths[1]), func(t *testing.T) {
			client := copyingRenameRemoteClient{&renameRemoteClient{}}
			server, store := newRenameRemoteTestServer(t, client)
			store.entries[string(paths[0])] = remoteOnlyRenameEntry(string(paths[0]), 101)

			err := renameFile(server, paths[0], paths[1])
			require.ErrorContains(t, err, "cannot be renamed out of it")

			assert.Empty(t, client.recorded())
			_, err = store.FindEntry(context.Background(), paths[0])
			require.NoError(t, err)
			_, err = store.FindEntry(context.Background(), paths[1])
			assert.ErrorIs(t, err, filer_pb.ErrNotFound)
		})
	}
}

func TestRenameEmptyRemoteEntryDoesNotCopy(t *testing.T) {
	// nothing to lose: no copy, and no refusal even out of the mount or
	// without a copier; the sync writes an empty object at a new key
	client := &renameRemoteClient{}
	server, store := newRenameRemoteTestServer(t, client)
	for _, path := range []string{"/buckets/b/src/empty", "/data/m/empty"} {
		entry := remoteOnlyRenameEntry(path, 101)
		entry.FileSize = 0
		entry.Remote.RemoteSize = 0
		store.entries[path] = entry
	}

	require.NoError(t, renameFile(server, "/buckets/b/src/empty", "/buckets/b/dst/empty"))
	require.NoError(t, renameFile(server, "/data/m/empty", "/data/empty"))

	assert.Equal(t, []string{"delete origin/src/empty", "delete origin/m/empty"}, client.recorded())
}

func TestRenameRemoteOnlyEntryFailingAfterTheCopyKeepsTheContent(t *testing.T) {
	client := copyingRenameRemoteClient{&renameRemoteClient{}}
	server, store := newRenameRemoteTestServer(t, client)
	store.entries["/buckets/b/src/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/src/a.jpg", 101)
	store.commitErr = errors.New("commit failed")

	err := renameFile(server, "/buckets/b/src/a.jpg", "/buckets/b/dst/a.jpg")
	require.ErrorContains(t, err, "commit failed")

	// remote changes are not rolled back with the store: the old object is
	// gone, but only after the copy at the new key was written
	assert.Equal(t, []string{"copy origin/src/a.jpg origin/dst/a.jpg", "delete origin/src/a.jpg"}, client.recorded())
}

func TestRenameEntryWithLocalDataDoesNotCopy(t *testing.T) {
	client := copyingRenameRemoteClient{&renameRemoteClient{}}
	server, store := newRenameRemoteTestServer(t, client)
	entry := remoteOnlyRenameEntry("/data/m/a.jpg", 101)
	entry.Chunks = []*filer_pb.FileChunk{{FileId: "1,0123456789", Size: 10}}
	store.entries["/data/m/a.jpg"] = entry

	// even out of the mount: the old object goes, the content stays local
	require.NoError(t, renameFile(server, "/data/m/a.jpg", "/data/a.jpg"))

	assert.Equal(t, []string{"delete origin/m/a.jpg"}, client.recorded())
	dst, err := store.FindEntry(context.Background(), "/data/a.jpg")
	require.NoError(t, err)
	assert.Equal(t, `"orig"`, dst.Remote.RemoteETag)
	assert.Len(t, dst.GetChunks(), 1)
}

func TestRenameDirectoryCopiesRemoteOnlyChildren(t *testing.T) {
	client := copyingRenameRemoteClient{&renameRemoteClient{}}
	server, store := newRenameRemoteTestServer(t, client)
	store.entries["/buckets/b/src/a.jpg"] = remoteOnlyRenameEntry("/buckets/b/src/a.jpg", 101)
	local := remoteOnlyRenameEntry("/buckets/b/src/b.jpg", 102)
	local.Content = []byte("0123456789")
	store.entries["/buckets/b/src/b.jpg"] = local

	require.NoError(t, renameFile(server, "/buckets/b/src", "/buckets/b/moved"))

	assert.Equal(t, []string{
		"copy origin/src/a.jpg origin/moved/a.jpg",
		"delete origin/src/a.jpg",
		"delete origin/src/b.jpg",
		"rmdir origin/src",
	}, client.recorded())
	moved, err := store.FindEntry(context.Background(), "/buckets/b/moved/a.jpg")
	require.NoError(t, err)
	assert.Equal(t, `"copy"`, moved.Remote.RemoteETag)
	moved, err = store.FindEntry(context.Background(), "/buckets/b/moved/b.jpg")
	require.NoError(t, err)
	assert.Equal(t, `"orig"`, moved.Remote.RemoteETag)
}
