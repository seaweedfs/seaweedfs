package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaybeReloadRemoteStorage_TriggersOnEtcRemoteEvent(t *testing.T) {
	const storageType = "stub_reload_trigger"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	rs := NewFilerRemoteStorage()
	conf := &remote_pb.RemoteConf{Name: "myremote", Type: storageType}
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/b1", &remote_pb.RemoteStorageLocation{
		Name: "myremote", Bucket: "b1", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// Verify mount exists before reset
	_, loc := f.RemoteStorage.FindMountDirectory("/buckets/b1/file.txt")
	require.NotNil(t, loc, "mount should exist before reload")

	// Trigger reload with an event on /etc/remote
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: DirectoryEtcRemote,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "mount.mapping"},
		},
	}
	f.maybeReloadRemoteStorageConfigurationAndMapping(event)

	// After reload with no data in the store, the mount is gone (Reset clears it,
	// and LoadRemoteStorageConfigurationsAndMapping finds nothing in stub store)
	_, loc2 := f.RemoteStorage.FindMountDirectory("/buckets/b1/file.txt")
	assert.Nil(t, loc2, "after reload with empty store, mount should be gone")
}

func TestMaybeReloadRemoteStorage_IgnoresUnrelatedEvent(t *testing.T) {
	rs := NewFilerRemoteStorage()
	rs.mapDirectoryToRemoteStorage("/buckets/b1", &remote_pb.RemoteStorageLocation{
		Name: "s", Bucket: "b1", Path: "/",
	})
	rs.storageNameToConf["s"] = &remote_pb.RemoteConf{Name: "s", Type: "s3"}

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	event := &filer_pb.SubscribeMetadataResponse{
		Directory: DirectoryEtcSeaweedFS,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "filer.conf"},
		},
	}
	f.maybeReloadRemoteStorageConfigurationAndMapping(event)

	// Mount should still be there (not reloaded)
	_, loc := f.RemoteStorage.FindMountDirectory("/buckets/b1/file.txt")
	assert.NotNil(t, loc, "unrelated event should not trigger reload")
}

func TestMaybeReloadRemoteStorage_NewParentPathChecked(t *testing.T) {
	rs := NewFilerRemoteStorage()
	rs.mapDirectoryToRemoteStorage("/buckets/b1", &remote_pb.RemoteStorageLocation{
		Name: "s", Bucket: "b1", Path: "/",
	})
	rs.storageNameToConf["s"] = &remote_pb.RemoteConf{Name: "s", Type: "s3"}

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// Event with NewParentPath set to /etc/remote should trigger reload
	event := &filer_pb.SubscribeMetadataResponse{
		Directory: "/some/other/dir",
		EventNotification: &filer_pb.EventNotification{
			NewParentPath: DirectoryEtcRemote,
			NewEntry:      &filer_pb.Entry{Name: "test.conf"},
		},
	}
	f.maybeReloadRemoteStorageConfigurationAndMapping(event)

	// After reload with empty store, mount should be cleared
	_, loc := f.RemoteStorage.FindMountDirectory("/buckets/b1/file.txt")
	assert.Nil(t, loc, "reload via NewParentPath should clear mounts")
}

func TestMaybeReloadRemoteStorage_NewMountBecomesAvailable(t *testing.T) {
	const storageType = "stub_reload_new"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1, RemoteSize: 100},
	}
	defer registerStubMaker(t, storageType, stub)()

	rs := NewFilerRemoteStorage()
	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// Initially no mounts
	_, loc := f.RemoteStorage.FindMountDirectory("/buckets/newbucket/file.txt")
	assert.Nil(t, loc, "no mount initially")

	// Simulate adding a mount by directly setting it up (simulating what
	// LoadRemoteStorageConfigurationsAndMapping would do after reading from store)
	f.RemoteStorage.storageNameToConf["newremote"] = &remote_pb.RemoteConf{Name: "newremote", Type: storageType}
	f.RemoteStorage.mapDirectoryToRemoteStorage("/buckets/newbucket", &remote_pb.RemoteStorageLocation{
		Name: "newremote", Bucket: "newbucket", Path: "/",
	})

	// Now FindMountDirectory should find it
	mountDir, loc := f.RemoteStorage.FindMountDirectory("/buckets/newbucket/file.txt")
	require.NotNil(t, loc, "new mount should be available after setup")
	assert.Equal(t, util.FullPath("/buckets/newbucket"), mountDir)

	// And FindRemoteStorageClient should work
	client, _, found := f.RemoteStorage.FindRemoteStorageClient("/buckets/newbucket/file.txt")
	assert.True(t, found, "should find remote storage client for new mount")
	assert.NotNil(t, client)
}

func TestFilerRemoteStorage_Reset(t *testing.T) {
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf["s1"] = &remote_pb.RemoteConf{Name: "s1", Type: "s3"}
	rs.mapDirectoryToRemoteStorage("/buckets/b1", &remote_pb.RemoteStorageLocation{
		Name: "s1", Bucket: "b1", Path: "/",
	})

	// Verify data is there
	_, loc := rs.FindMountDirectory("/buckets/b1/file.txt")
	require.NotNil(t, loc)
	_, ok := rs.storageNameToConf["s1"]
	require.True(t, ok)

	// Reset
	rs.Reset()

	// Verify data is cleared
	_, loc2 := rs.FindMountDirectory("/buckets/b1/file.txt")
	assert.Nil(t, loc2, "rules should be cleared after Reset")
	_, ok2 := rs.storageNameToConf["s1"]
	assert.False(t, ok2, "storageNameToConf should be cleared after Reset")
}

// Verify that GetRemoteStorageClient still resolves after maker is registered.
func TestFilerRemoteStorage_GetRemoteStorageClient_AfterMakerRegistered(t *testing.T) {
	const storageType = "stub_get_client"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	rs := NewFilerRemoteStorage()
	rs.storageNameToConf["test"] = &remote_pb.RemoteConf{Name: "test", Type: storageType}

	client, conf, found := rs.GetRemoteStorageClient("test")
	assert.True(t, found)
	assert.NotNil(t, client)
	assert.Equal(t, "test", conf.Name)

	// Unknown name
	_, _, found2 := rs.GetRemoteStorageClient("nonexistent")
	assert.False(t, found2)
}

// Verify that a registered maker that's cleaned up doesn't prevent next test from working.
// This also tests the cleanup func returned by registerStubMaker.
func TestRegisterStubMaker_Cleanup(t *testing.T) {
	const storageType = "stub_cleanup_test"

	_, exists := remote_storage.RemoteStorageClientMakers[storageType]
	assert.False(t, exists, "should not exist before registration")

	cleanup := registerStubMaker(t, storageType, &stubRemoteClient{})
	_, exists = remote_storage.RemoteStorageClientMakers[storageType]
	assert.True(t, exists, "should exist after registration")

	cleanup()
	_, exists = remote_storage.RemoteStorageClientMakers[storageType]
	assert.False(t, exists, "should be cleaned up")
}
