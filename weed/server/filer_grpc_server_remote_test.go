package weed_server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRemoteNotFoundCrossesTheVolumeServerHop(t *testing.T) {
	loc := &remote_pb.RemoteStorageLocation{Name: "gcs1", Bucket: "bucket", Path: "/obj.bin"}
	tests := map[string]struct {
		volumeErr error
		notFound  bool
	}{
		"remote object gone":       {volumeRemoteReadError(loc, fmt.Errorf("read: %w", remote_storage.ErrRemoteObjectNotFound)), true},
		"remote read failure":      {volumeRemoteReadError(loc, errors.New("googleapi: Error 403: forbidden")), false},
		"not found for other uses": {status.Error(codes.NotFound, "volume not found"), false},
		"deadline":                 {status.Error(codes.DeadlineExceeded, "context deadline exceeded"), false},
		"unavailable":              {status.Error(codes.Unavailable, "connection refused"), false},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			filerErr := fetchAndWriteError("volume:8080", loc.Path, tt.volumeErr)
			assert.Equal(t, tt.notFound, errors.Is(filerErr, remote_storage.ErrRemoteObjectNotFound))
			assert.Equal(t, tt.notFound, status.Code(cacheRemoteObjectError(filerErr)) == codes.NotFound)
		})
	}
}

// mountTestRemoteClient is a remote where every object is gone; it records
// every DeleteFile.
type mountTestRemoteClient struct {
	remote_storage.RemoteStorageClient
	mu      sync.Mutex
	deleted []string
}

func (c *mountTestRemoteClient) StatFile(*remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	return nil, remote_storage.ErrRemoteObjectNotFound
}

func (c *mountTestRemoteClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, loc.Path)
	return nil
}

func (c *mountTestRemoteClient) deletedPaths() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.deleted...)
}

// newRemoteMountTestServer mounts /buckets/b on a fresh mountTestRemoteClient.
func newRemoteMountTestServer(t *testing.T) (*FilerServer, *renameTestStore, *mountTestRemoteClient) {
	t.Helper()
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	client := &mountTestRemoteClient{}
	f.BuildGuardedRemoteClient = func(context.Context, *remote_pb.RemoteConf, bool) (remote_storage.RemoteStorageClient, error) {
		return client, nil
	}
	mapping, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/b": {Name: "r1", Bucket: "origin", Path: "/"},
	}})
	require.NoError(t, err)
	conf, err := proto.Marshal(&remote_pb.RemoteConf{Name: "r1", Type: "s3"})
	require.NoError(t, err)
	for i, c := range map[string][]byte{filer.REMOTE_STORAGE_MOUNT_FILE: mapping, "r1" + filer.REMOTE_STORAGE_CONF_SUFFIX: conf} {
		entry := newFileEntry(filer.DirectoryEtcRemote+"/"+i, 1)
		entry.Content = c
		store.entries[string(entry.FullPath)] = entry
	}
	require.NoError(t, f.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(f))
	return &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}, store, client
}

func remoteOnlyFileEntry(path string) *filer.Entry {
	entry := newFileEntry(path, 10)
	entry.FileSize = 10
	entry.Remote = &filer_pb.RemoteEntry{RemoteSize: 10, RemoteMtime: 1700000000}
	dir, _ := entry.FullPath.DirAndName()
	return filer.FromPbEntry(dir, entry.ToProtoEntry())
}

func TestPruneEntryMissingFromRemote(t *testing.T) {
	gone := fmt.Errorf("volume server v fetchAndWrite /obj.bin: %w", remote_storage.ErrRemoteObjectNotFound)
	retainUntil := func(d time.Duration) []byte { return []byte(strconv.FormatInt(time.Now().Add(d).Unix(), 10)) }
	tests := []struct {
		name     string
		fetchErr error
		mutate   func(stored *filer.Entry)
		pruned   bool
	}{
		{"remote confirms the object gone", gone, nil, true},
		{"expired retention", gone, func(e *filer.Entry) {
			e.Extended = map[string][]byte{s3_constants.ExtObjectLockModeKey: []byte(s3_constants.RetentionModeGovernance), s3_constants.ExtRetentionUntilDateKey: retainUntil(-time.Hour)}
		}, true},
		{"deadline", context.DeadlineExceeded, nil, false},
		{"unavailable", fmt.Errorf("fetchAndWrite: %v", status.Error(codes.Unavailable, "connection refused")), nil, false},
		{"permission denied", errors.New("googleapi: Error 403: forbidden"), nil, false},
		{"entry vanished", filer_pb.ErrNotFound, nil, false},
		{"cached locally", gone, func(e *filer.Entry) { e.Chunks = []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 10}} }, false},
		{"inline content", gone, func(e *filer.Entry) { e.Content = []byte("0123456789") }, false},
		{"hard link", gone, func(e *filer.Entry) { e.HardLinkId = filer.HardLinkId([]byte("link")) }, false},
		{"directory", gone, func(e *filer.Entry) { e.Mode |= os.ModeDir }, false},
		{"object version", gone, func(e *filer.Entry) {
			e.FullPath = util.FullPath("/buckets/b/obj.bin" + s3_constants.VersionsFolder + "/v_1")
		}, false},
		{"active retention", gone, func(e *filer.Entry) {
			e.Extended = map[string][]byte{s3_constants.ExtObjectLockModeKey: []byte(s3_constants.RetentionModeGovernance), s3_constants.ExtRetentionUntilDateKey: retainUntil(time.Hour)}
		}, false},
		{"legal hold", gone, func(e *filer.Entry) {
			e.Extended = map[string][]byte{s3_constants.ExtLegalHoldKey: []byte(s3_constants.LegalHoldOn)}
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store, client := newRemoteMountTestServer(t)
			entry := remoteOnlyFileEntry("/buckets/b/obj.bin")
			if tt.mutate != nil {
				tt.mutate(entry)
			}
			store.entries[string(entry.FullPath)] = entry.ShallowClone()

			assert.Equal(t, tt.pruned, fs.pruneEntryMissingFromRemote(context.Background(), entry.FullPath, entry, tt.fetchErr))

			_, err := fs.filer.FindEntry(context.Background(), entry.FullPath)
			assert.Equal(t, tt.pruned, errors.Is(err, filer_pb.ErrNotFound), "find: %v", err)
			assert.Empty(t, client.deletedPaths())
		})
	}

	t.Run("entry rewritten since the fetch", func(t *testing.T) {
		fs, store, _ := newRemoteMountTestServer(t)
		fetched := remoteOnlyFileEntry("/buckets/b/obj.bin")
		rewritten := fetched.ShallowClone()
		rewritten.Remote = nil
		rewritten.Chunks = []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 10}}
		store.entries[string(fetched.FullPath)] = rewritten

		assert.False(t, fs.pruneEntryMissingFromRemote(context.Background(), fetched.FullPath, fetched, gone))
	})
}

// TestPruneLeavesTheRemoteAlone also deletes an entry the ordinary way, so a
// harness that never reached the remote could not pass it.
func TestPruneLeavesTheRemoteAlone(t *testing.T) {
	fs, store, client := newRemoteMountTestServer(t)
	pruned := remoteOnlyFileEntry("/buckets/b/pruned.bin")
	deleted := remoteOnlyFileEntry("/buckets/b/deleted.bin")
	store.entries[string(pruned.FullPath)] = pruned.ShallowClone()
	store.entries[string(deleted.FullPath)] = deleted.ShallowClone()
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	require.True(t, fs.pruneEntryMissingFromRemote(context.Background(), pruned.FullPath, pruned, remote_storage.ErrRemoteObjectNotFound))
	events := queue.snapshot()
	require.Len(t, events, 1)
	assert.False(t, events[0].notification.IsFromOtherCluster, "filer.sync must replicate the prune")
	assert.Contains(t, events[0].notification.OldEntry.Extended, filer.ExtKeepRemoteObjectKey, "filer.remote.sync must not delete the remote object")

	require.NoError(t, fs.filer.DeleteEntryMetaAndData(context.Background(), deleted.FullPath, false, false, false, false, nil, 0))
	assert.Equal(t, []string{"/deleted.bin"}, client.deletedPaths())
}

func TestReplicatedMetadataOnlyDeleteStaysMarked(t *testing.T) {
	for _, keep := range []bool{true, false} {
		t.Run(fmt.Sprintf("keep_remote_object=%v", keep), func(t *testing.T) {
			fs, store, client := newRemoteMountTestServer(t)
			entry := remoteOnlyFileEntry("/buckets/b/obj.bin")
			store.entries[string(entry.FullPath)] = entry.ShallowClone()

			resp, err := fs.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
				Directory:          "/buckets/b",
				Name:               "obj.bin",
				IsFromOtherCluster: true,
				KeepRemoteObject:   keep,
			})
			require.NoError(t, err)
			require.Empty(t, resp.Error)
			assert.Equal(t, keep, filer.IsMetadataOnlyDelete(resp.MetadataEvent.EventNotification.OldEntry))
			assert.Empty(t, client.deletedPaths())
		})
	}
}

func TestPruneOnANonOwnerLeavesTheDeleteToTheOwner(t *testing.T) {
	fs, store, _ := newRemoteMountTestServer(t)
	const self = pb.ServerAddress("127.0.0.1:18888")
	fs.option.Host = self
	fs.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	fs.filer.Dlm = lock_manager.NewDistributedLockManager(self)
	fs.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{self, "127.0.0.1:18889"}, 1)
	var entry *filer.Entry
	for i := 0; i < 4000 && entry == nil; i++ {
		p := util.FullPath(fmt.Sprintf("/buckets/b/obj-%d.bin", i))
		if fs.filer.Dlm.LockRing.WriteOwner(entryRouteKey(p)) != self {
			entry = remoteOnlyFileEntry(string(p))
		}
	}
	require.NotNil(t, entry, "no path owned by the peer")
	store.entries[string(entry.FullPath)] = entry.ShallowClone()

	assert.False(t, fs.pruneEntryMissingFromRemote(context.Background(), entry.FullPath, entry, remote_storage.ErrRemoteObjectNotFound))
	assert.Contains(t, store.entries, string(entry.FullPath), "only the write owner may delete the entry")
}
