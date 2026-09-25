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

	tombs.addFromEvent("/m/a.txt", false, 100)
	ts, _ := tombs.blockedSince("/m/a.txt")
	assert.Equal(t, int64(100), ts)
	ts, _ = tombs.blockedSince("/m/b.txt")
	assert.Zero(t, ts)

	// a delete of a directory blocks its whole subtree
	tombs.addFromEvent("/m/dir", true, 200)
	ts, _ = tombs.blockedSince("/m/dir")
	assert.Equal(t, int64(200), ts)
	ts, _ = tombs.blockedSince("/m/dir/deep/x.txt")
	assert.Equal(t, int64(200), ts)
	ts, _ = tombs.blockedSince("/m/dirx/y.txt")
	assert.Zero(t, ts)

	// a rewrite at or after the delete lifts only that file's tombstone
	tombs.clear("/m/a.txt", 100)
	ts, _ = tombs.blockedSince("/m/a.txt")
	assert.Zero(t, ts)
	tombs.addFromEvent("/m/a.txt", false, 300)
	tombs.clear("/m/a.txt", 250)
	ts, _ = tombs.blockedSince("/m/a.txt")
	assert.Equal(t, int64(300), ts, "older create must not lift newer delete")

	tombs.releaseThrough("/m/dir/deep/x.txt", 200)
	ts, _ = tombs.blockedSince("/m/dir/deep/x.txt")
	assert.Zero(t, ts)
	ts, _ = tombs.blockedSince("/m/dir")
	assert.Zero(t, ts)
}

func TestRemoteDeletionTombstones_AncestorSubsumesAndCovers(t *testing.T) {
	tombs := newRemoteDeletionTombstones()

	// a child tombstone recorded before its ancestor is dropped once the
	// ancestor's newer delete covers the whole subtree
	tombs.addFromEvent("/m/dir/a.txt", false, 100)
	tombs.addFromEvent("/m/dir", true, 200)
	ts, _ := tombs.blockedSince("/m/dir")
	assert.Equal(t, int64(200), ts)
	ts, _ = tombs.blockedSince("/m/dir/a.txt")
	assert.Equal(t, int64(200), ts)
	_, exists := tombs.files["/m/dir/a.txt"]
	assert.False(t, exists, "descendant tombstone is subsumed by the ancestor")

	// adds under the covered subtree are skipped while the ancestor stands
	tombs.addFromEvent("/m/dir/b.txt", false, 150)
	_, exists = tombs.files["/m/dir/b.txt"]
	assert.False(t, exists)
	// ...but a child deleted after the ancestor still records its own tombstone
	tombs.addFromEvent("/m/dir/c.txt", false, 300)
	ts, _ = tombs.blockedSince("/m/dir/c.txt")
	assert.Equal(t, int64(300), ts)
}

func TestRemoteDeletionTombstones_PendingIgnoresOffset(t *testing.T) {
	f, store := newMountedTestFiler(t, "stub_tomb_pending_offset", nil, 0)

	// recorded before its event lands: a later event may already have moved
	// the mount's watermark past the local timestamp, so the offset cannot
	// vouch for this delete yet
	filePath := "/buckets/mybucket/dir/a.txt"
	now := time.Now().UnixNano()
	f.noteRemoteDeletion(util.FullPath(filePath), false, now)
	putRemoteSyncOffset(t, store, "/buckets/mybucket", now+100)
	assert.True(t, f.isRemoteDeletionPending(context.Background(), util.FullPath(filePath), "/buckets/mybucket"))

	// once the delete event stamps the real timestamp, the watermark releases it
	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket/dir",
		TsNs:      now + 50,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "a.txt"},
		},
	})
	assert.False(t, f.isRemoteDeletionPending(context.Background(), util.FullPath(filePath), "/buckets/mybucket"))
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
	assert.Equal(t, 0, stub.statCalls, "the remote object must not even be consulted")
}

func TestMaybeLazyFetchFromRemote_NewerRemoteMtimeStillBlocked(t *testing.T) {
	const storageType = "stub_tomb_regen"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: time.Now().Unix() + 60, RemoteSize: 11},
		},
	}
	f, _ := newMountedTestFiler(t, storageType, stub, 0)

	// a remote object whose mtime postdates the delete is still hidden: the
	// remote clock cannot distinguish a new generation from the pending
	// delete's target
	filePath := util.FullPath("/buckets/mybucket/dir/a.txt")
	f.noteRemoteDeletion(filePath, false, time.Now().UnixNano())

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), filePath)
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)
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
	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket/dir",
		TsNs:      deleteTsNs,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "a.txt"},
		},
	})

	// the daemon's persisted watermark is behind the delete: still blocked
	putRemoteSyncOffset(t, store, "/buckets/mybucket", deleteTsNs-1)
	entry, err := f.maybeLazyFetchFromRemote(context.Background(), filePath)
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)

	// once the watermark reaches the delete event's own timestamp, the
	// remote delete has landed and the lookup may consult the remote again
	putRemoteSyncOffset(t, store, "/buckets/mybucket", deleteTsNs)
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

func TestMaybeLazyListFromRemote_RecreatedDirStillHidesChildren(t *testing.T) {
	const storageType = "stub_tomb_recreate"
	deleteTs := time.Now().UnixNano()
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			if err := visitFn("/", "stale.txt", false, &filer_pb.RemoteEntry{RemoteMtime: deleteTs/int64(time.Second) - 10, RemoteSize: 10}); err != nil {
				return err
			}
			return visitFn("/", "fresh.txt", false, &filer_pb.RemoteEntry{RemoteMtime: deleteTs/int64(time.Second) + 10, RemoteSize: 20})
		},
	}
	f, store := newMountedTestFiler(t, storageType, stub, 300)

	// the directory is deleted then recreated; its remote children stay
	// hidden — old or new mtime alike — until the remote delete lands
	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		TsNs:      deleteTs,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "dir", IsDirectory: true},
		},
	})
	f.onMetadataChangeEvent(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/mybucket",
		TsNs:      deleteTs + 1,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "dir", IsDirectory: true},
		},
	})

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket/dir"))

	assert.Nil(t, store.getEntry("/buckets/mybucket/dir/stale.txt"))
	assert.Nil(t, store.getEntry("/buckets/mybucket/dir/fresh.txt"))

	// after the write-back daemon confirms the remote delete, listing merges again
	f.remoteTombstones.releaseThrough("/buckets/mybucket/dir/stale.txt", deleteTs)
	delete(store.getEntry("/buckets/mybucket/dir").Extended, xattrRemoteListingSyncedAt)
	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket/dir"))
	assert.NotNil(t, store.getEntry("/buckets/mybucket/dir/stale.txt"))
	assert.NotNil(t, store.getEntry("/buckets/mybucket/dir/fresh.txt"))
}

func TestRemoteDeletionRebuildStartTsNs_UsesOldestMountOffset(t *testing.T) {
	f, store := newMountedTestFiler(t, "stub_rebuild", nil, 0)
	f.RemoteStorage.mapDirectoryToRemoteStorage("/buckets/other", &remote_pb.RemoteStorageLocation{
		Name: "tombstonestore", Bucket: "other", Path: "/",
	})
	mounts := f.RemoteStorage.MountedDirectories()
	require.Len(t, mounts, 2)

	now := time.Now().UnixNano()
	putRemoteSyncOffset(t, store, "/buckets/mybucket", now-200)
	putRemoteSyncOffset(t, store, "/buckets/other", now-300)
	assert.Equal(t, now-300, f.remoteDeletionRebuildStartTsNs(context.Background(), mounts),
		"rebuild must replay from the least-synced mount")

	// a mount whose offset was never written replays only within the TTL
	require.NoError(t, store.KvDelete(context.Background(), remote_storage.SyncOffsetKey("/buckets/other")))
	floor := f.remoteDeletionRebuildStartTsNs(context.Background(), mounts)
	assert.GreaterOrEqual(t, floor, time.Now().Add(-remoteDeletionTombstoneTTL-time.Second).UnixNano())

	// an offset older than the TTL floor is raised to it
	putRemoteSyncOffset(t, store, "/buckets/other", time.Now().Add(-remoteDeletionTombstoneTTL-time.Hour).UnixNano())
	assert.Greater(t, f.remoteDeletionRebuildStartTsNs(context.Background(), mounts),
		time.Now().Add(-remoteDeletionTombstoneTTL-time.Hour).UnixNano())
}

func TestRebuildRemoteDeletionTombstones_EmptyLogReleasesGate(t *testing.T) {
	f, _ := newMountedTestFiler(t, "stub_rebuild_empty", nil, 0)

	f.RebuildRemoteDeletionTombstones(context.Background())
	done := f.remoteTombstonesDone.Load()
	require.NotNil(t, done, "the gate must be set synchronously")

	select {
	case <-*done:
	case <-time.After(10 * time.Second):
		t.Fatal("rebuild never released the gate")
	}
	assert.Nil(t, f.remoteTombstonesDone.Load())
	ts, _ := f.remoteTombstones.blockedSince("/buckets/mybucket/a.txt")
	assert.Zero(t, ts)
}

func TestMaybeLazyFetchFromRemote_WaitsForRebuild(t *testing.T) {
	const storageType = "stub_tomb_pending"
	stub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 10},
		},
	}
	f, _ := newMountedTestFiler(t, storageType, stub, 0)

	// an unfinished rebuild blocks the fetch until the context gives up
	gate := make(chan struct{})
	f.remoteTombstonesDone.Store(&gate)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	entry, err := f.maybeLazyFetchFromRemote(ctx, "/buckets/mybucket/a.txt")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, entry)
	assert.Equal(t, 0, stub.statCalls)

	// once the rebuild finishes, the fetch proceeds
	close(gate)
	entry, err = f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/a.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, 1, stub.statCalls)
}
