package filer

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// traverseStubClient extends stubRemoteClient with a configurable Traverse
type traverseStubClient struct {
	stubRemoteClient
	traverseEntries []traverseEntry
}

type traverseEntry struct {
	dir         string
	name        string
	isDirectory bool
	remote      *filer_pb.RemoteEntry
}

func (c *traverseStubClient) Traverse(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	for _, e := range c.traverseEntries {
		if err := visitFn(e.dir, e.name, e.isDirectory, e.remote); err != nil {
			return err
		}
	}
	return nil
}

func TestRemoteMetaSyncer_SyncsNewEntries(t *testing.T) {
	const storageType = "stub_syncer_new"
	stub := &traverseStubClient{
		traverseEntries: []traverseEntry{
			{
				dir:  "/",
				name: "newfile.txt",
				remote: &filer_pb.RemoteEntry{
					RemoteMtime: 1700000000, RemoteSize: 500, RemoteETag: "etag1",
					StorageName: storageType,
				},
			},
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: storageType, Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: storageType, Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	syncer := &RemoteMetaSyncer{filer: f, interval: time.Minute, quit: make(chan struct{})}
	syncer.syncAllMounts()

	// Verify entry was created
	entry, err := store.FindEntry(context.Background(), "/buckets/mybucket/newfile.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, uint64(500), entry.FileSize)
	assert.Equal(t, "etag1", entry.Remote.RemoteETag)
}

func TestRemoteMetaSyncer_UpdatesChangedEntries(t *testing.T) {
	const storageType = "stub_syncer_update"
	stub := &traverseStubClient{
		traverseEntries: []traverseEntry{
			{
				dir:  "/",
				name: "changed.txt",
				remote: &filer_pb.RemoteEntry{
					RemoteMtime: 1700000002, RemoteSize: 999, RemoteETag: "etag_new",
					StorageName: storageType,
				},
			},
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: storageType, Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: storageType, Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// Pre-populate with an older entry
	oldEntry := &Entry{
		FullPath: "/buckets/mybucket/changed.txt",
		Attr:     Attr{FileSize: 100, Mtime: time.Unix(1700000000, 0)},
		Remote:   &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 100, RemoteETag: "etag_old", StorageName: storageType},
	}
	store.InsertEntry(context.Background(), oldEntry)

	syncer := &RemoteMetaSyncer{filer: f, interval: time.Minute, quit: make(chan struct{})}
	syncer.syncAllMounts()

	// Verify entry was updated
	entry, err := store.FindEntry(context.Background(), "/buckets/mybucket/changed.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(999), entry.FileSize)
	assert.Equal(t, "etag_new", entry.Remote.RemoteETag)
}

func TestRemoteMetaSyncer_SkipsLocalOnlyEntries(t *testing.T) {
	const storageType = "stub_syncer_skip"
	stub := &traverseStubClient{
		traverseEntries: []traverseEntry{
			{
				dir:  "/",
				name: "localonly.txt",
				remote: &filer_pb.RemoteEntry{
					RemoteMtime: 1700000002, RemoteSize: 999, RemoteETag: "remote_etag",
					StorageName: storageType,
				},
			},
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: storageType, Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: storageType, Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// Pre-populate with a local-only entry (Remote == nil)
	localEntry := &Entry{
		FullPath: "/buckets/mybucket/localonly.txt",
		Attr:     Attr{FileSize: 42, Mtime: time.Unix(1700000000, 0)},
		Remote:   nil, // local-only
	}
	store.InsertEntry(context.Background(), localEntry)

	syncer := &RemoteMetaSyncer{filer: f, interval: time.Minute, quit: make(chan struct{})}
	syncer.syncAllMounts()

	// Verify entry was NOT overwritten
	entry, err := store.FindEntry(context.Background(), "/buckets/mybucket/localonly.txt")
	require.NoError(t, err)
	assert.Equal(t, uint64(42), entry.FileSize)
	assert.Nil(t, entry.Remote)
}

func TestRemoteMetaSyncer_StopsOnQuit(t *testing.T) {
	syncer := &RemoteMetaSyncer{
		filer:    newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage()),
		interval: 50 * time.Millisecond,
		quit:     make(chan struct{}),
	}

	done := make(chan struct{})
	go func() {
		syncer.loop()
		close(done)
	}()

	syncer.Stop()

	select {
	case <-done:
		// good, loop exited
	case <-time.After(2 * time.Second):
		t.Fatal("syncer did not stop within timeout")
	}
}

func TestGetAllMountMappings(t *testing.T) {
	rs := NewFilerRemoteStorage()
	rs.mapDirectoryToRemoteStorage("/buckets/b1", &remote_pb.RemoteStorageLocation{
		Name: "s1", Bucket: "b1", Path: "/",
	})
	rs.mapDirectoryToRemoteStorage("/buckets/b2", &remote_pb.RemoteStorageLocation{
		Name: "s2", Bucket: "b2", Path: "/data",
	})

	mappings := rs.GetAllMountMappings()
	assert.Len(t, mappings, 2)
	assert.Equal(t, "b1", mappings["/buckets/b1"].Bucket)
	assert.Equal(t, "b2", mappings["/buckets/b2"].Bucket)
}
