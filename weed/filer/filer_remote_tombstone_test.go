package filer

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMountedTestFiler(t *testing.T, storageType string, stub remote_storage.RemoteStorageClient, listingTtlSeconds int32) (*Filer, *stubFilerStore) {
	t.Helper()
	if stub != nil {
		t.Cleanup(registerStubMaker(t, storageType, stub))
	}
	conf := &remote_pb.RemoteConf{Name: "tombstonestore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:                   "tombstonestore",
		Bucket:                 "mybucket",
		Path:                   "/",
		ListingCacheTtlSeconds: listingTtlSeconds,
	})
	store := newStubFilerStore()
	return newTestFiler(t, store, rs), store
}

func putRemoteSyncOffset(t *testing.T, store *stubFilerStore, dir string, offsetTsNs int64) {
	t.Helper()
	buf := make([]byte, 8)
	util.Uint64toBytes(buf, uint64(offsetTsNs))
	require.NoError(t, store.KvPut(context.Background(), remote_storage.SyncOffsetKey(dir), buf))
}

func TestRemoteDeletionTombstones_BlocksAndReleases(t *testing.T) {
	tombs := newRemoteDeletionTombstones()

	tombs.add("/m/a.txt", false, 100)
	assert.Equal(t, int64(100), tombs.blockedSince("/m/a.txt"))
	assert.Zero(t, tombs.blockedSince("/m/b.txt"))

	// a delete of a directory blocks its whole subtree
	tombs.add("/m/dir", true, 200)
	assert.Equal(t, int64(200), tombs.blockedSince("/m/dir"))
	assert.Equal(t, int64(200), tombs.blockedSince("/m/dir/deep/x.txt"))
	assert.Zero(t, tombs.blockedSince("/m/dirx/y.txt"))

	// a rewrite at or after the delete lifts only that file's tombstone
	tombs.clear("/m/a.txt", 100)
	assert.Zero(t, tombs.blockedSince("/m/a.txt"))
	tombs.add("/m/a.txt", false, 300)
	tombs.clear("/m/a.txt", 250)
	assert.Equal(t, int64(300), tombs.blockedSince("/m/a.txt"), "older create must not lift newer delete")

	tombs.releaseThrough("/m/dir/deep/x.txt", 200)
	assert.Zero(t, tombs.blockedSince("/m/dir/deep/x.txt"))
	assert.Zero(t, tombs.blockedSince("/m/dir"))
}

func TestMaybeLazyFetchFromRemote_SkipsTombstonedPath(t *testing.T) {
	const storageType = "stub_tomb_fetch"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 11},
		},
	}
	f, _ := newMountedTestFiler(t, storageType, stub, 0)

	// a delete under the mount tombstones the path; the remote object is
	// still there until the write-back daemon consumes the delete event
	filePath := util.FullPath("/buckets/mybucket/dir/a.txt")
	f.noteRemoteDeletion(filePath, false, time.Now().UnixNano())

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), filePath)
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls, "tombstoned path must not reach the remote")
}

func TestMaybeLazyFetchFromRemote_SyncOffsetReleasesTombstone(t *testing.T) {
	const storageType = "stub_tomb_release"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 11},
		},
	}
	f, store := newMountedTestFiler(t, storageType, stub, 0)

	filePath := util.FullPath("/buckets/mybucket/dir/a.txt")
	deleteTsNs := time.Now().UnixNano()
	f.noteRemoteDeletion(filePath, false, deleteTsNs)

	// the daemon's persisted watermark is behind the delete: still blocked
	putRemoteSyncOffset(t, store, "/buckets/mybucket", deleteTsNs-1)
	entry, err := f.maybeLazyFetchFromRemote(context.Background(), filePath)
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)

	// once the watermark passes the delete event, the remote delete has
	// landed and the lookup may consult the remote again
	putRemoteSyncOffset(t, store, "/buckets/mybucket", deleteTsNs+int64(remoteDeletionConfirmGrace))
	entry, err = f.maybeLazyFetchFromRemote(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, 1, stub.statCalls)
}

func TestDeleteEntryMetaAndData_TombstonesPath(t *testing.T) {
	const storageType = "stub_tomb_delete"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 11},
		},
	}
	f, store := newMountedTestFiler(t, storageType, stub, 0)

	filePath := util.FullPath("/buckets/mybucket/dir/a.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 11,
		},
	}

	require.NoError(t, f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0))

	// the deleted path must not resurrect through the lazy fetch even while
	// the remote object is still present
	entry, err := f.FindEntry(context.Background(), filePath)
	assert.ErrorIs(t, err, filer_pb.ErrNotFound)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)

	// a peer-observed create at the path lifts the tombstone
	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket/dir",
		TsNs:      time.Now().UnixNano(),
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "a.txt"},
		},
	})
	entry, err = f.maybeLazyFetchFromRemote(context.Background(), filePath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, 1, stub.statCalls)
}

func TestOnMetadataChangeEvent_PeerDeleteTombstones(t *testing.T) {
	const storageType = "stub_tomb_peer"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 11},
		},
	}
	f, _ := newMountedTestFiler(t, storageType, stub, 0)

	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket/dir",
		TsNs:      time.Now().UnixNano(),
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "a.txt"},
		},
	})

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/dir/a.txt")
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)
}

func TestOnMetadataChangeEvent_PeerDirDeleteTombstonesSubtree(t *testing.T) {
	const storageType = "stub_tomb_peer_dir"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 11},
		},
	}
	f, _ := newMountedTestFiler(t, storageType, stub, 0)

	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		TsNs:      time.Now().UnixNano(),
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "dir", IsDirectory: true},
		},
	})

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/dir/deep/a.txt")
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)
}

func TestMaybeLazyListFromRemote_SkipsTombstonedChild(t *testing.T) {
	const storageType = "stub_tomb_list"
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			if err := visitFn("/", "deleted.txt", false, &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10}); err != nil {
				return err
			}
			return visitFn("/", "fresh.txt", false, &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 20})
		},
	}
	f, store := newMountedTestFiler(t, storageType, stub, 300)

	f.noteRemoteDeletion("/buckets/mybucket/deleted.txt", false, time.Now().UnixNano())

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 1, stub.listDirCalls)

	assert.Nil(t, store.getEntry("/buckets/mybucket/deleted.txt"), "deleted child must not resurrect through a listing")
	require.NotNil(t, store.getEntry("/buckets/mybucket/fresh.txt"), "other remote objects still list")
}
