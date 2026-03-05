package filer

import (
	"context"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRemoteListingFiler(t *testing.T, storageType string, client *stubRemoteClient) (*Filer, func()) {
	t.Helper()
	cleanup := registerStubMaker(t, storageType, client)

	// Use storageType as the conf name so each test gets its own cached client
	conf := &remote_pb.RemoteConf{Name: storageType, Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: storageType, Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)
	return f, cleanup
}

func TestMaybeMergeRemoteListings_AddsNewRemoteEntries(t *testing.T) {
	const storageType = "stub_listing_add"
	stub := &stubRemoteClient{
		listDirResult: []*remote_storage.RemoteListing{
			{Name: "remote_file.txt", Size: 100, Mtime: 1700000000, ETag: "abc", StorageName: storageType},
			{Name: "remote_dir", IsDirectory: true, StorageName: storageType},
		},
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	localNames := map[string]struct{}{}
	var collected []*Entry
	count, _ := f.maybeMergeRemoteListings(context.Background(), "/buckets/mybucket", localNames, "", 100, "",
		func(entry *Entry) (bool, error) {
			collected = append(collected, entry)
			return true, nil
		})

	assert.Equal(t, int64(2), count)
	require.Len(t, collected, 2)
	assert.Equal(t, "remote_file.txt", collected[0].Name())
	assert.Equal(t, uint64(100), collected[0].FileSize)
	assert.Equal(t, "remote_dir", collected[1].Name())
	assert.True(t, collected[1].IsDirectory())
}

func TestMaybeMergeRemoteListings_DeduplicatesExisting(t *testing.T) {
	const storageType = "stub_listing_dedup"
	stub := &stubRemoteClient{
		listDirResult: []*remote_storage.RemoteListing{
			{Name: "existing.txt", Size: 100, StorageName: storageType},
			{Name: "new.txt", Size: 200, StorageName: storageType},
		},
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	localNames := map[string]struct{}{
		"existing.txt": {},
	}
	var collected []*Entry
	count, _ := f.maybeMergeRemoteListings(context.Background(), "/buckets/mybucket", localNames, "", 100, "",
		func(entry *Entry) (bool, error) {
			collected = append(collected, entry)
			return true, nil
		})

	assert.Equal(t, int64(1), count)
	require.Len(t, collected, 1)
	assert.Equal(t, "new.txt", collected[0].Name())
}

func TestMaybeMergeRemoteListings_NotUnderMount(t *testing.T) {
	const storageType = "stub_listing_nomount"
	stub := &stubRemoteClient{
		listDirResult: []*remote_storage.RemoteListing{
			{Name: "file.txt", Size: 100, StorageName: storageType},
		},
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	count, _ := f.maybeMergeRemoteListings(context.Background(), "/not/a/mount", map[string]struct{}{}, "", 100, "",
		func(entry *Entry) (bool, error) {
			t.Error("should not be called for non-mounted path")
			return true, nil
		})
	assert.Equal(t, int64(0), count)
}

func TestMaybeMergeRemoteListings_RemoteErrorGraceful(t *testing.T) {
	const storageType = "stub_listing_err"
	stub := &stubRemoteClient{
		listDirErr: fmt.Errorf("remote down"),
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	count, _ := f.maybeMergeRemoteListings(context.Background(), "/buckets/mybucket", map[string]struct{}{}, "", 100, "",
		func(entry *Entry) (bool, error) {
			t.Error("should not be called on error")
			return true, nil
		})
	assert.Equal(t, int64(0), count)
}

func TestMaybeMergeRemoteListings_PrefixFiltering(t *testing.T) {
	const storageType = "stub_listing_prefix"
	stub := &stubRemoteClient{
		listDirResult: []*remote_storage.RemoteListing{
			{Name: "abc_file.txt", Size: 100, StorageName: storageType},
			{Name: "xyz_file.txt", Size: 200, StorageName: storageType},
		},
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	var collected []*Entry
	count, _ := f.maybeMergeRemoteListings(context.Background(), "/buckets/mybucket", map[string]struct{}{}, "", 100, "abc",
		func(entry *Entry) (bool, error) {
			collected = append(collected, entry)
			return true, nil
		})

	assert.Equal(t, int64(1), count)
	require.Len(t, collected, 1)
	assert.Equal(t, "abc_file.txt", collected[0].Name())
}

func TestMaybeMergeRemoteListings_PaginationRespected(t *testing.T) {
	const storageType = "stub_listing_page"
	stub := &stubRemoteClient{
		listDirResult: []*remote_storage.RemoteListing{
			{Name: "a.txt", Size: 100, StorageName: storageType},
			{Name: "b.txt", Size: 200, StorageName: storageType},
			{Name: "c.txt", Size: 300, StorageName: storageType},
		},
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	// startFileName="a.txt" should skip "a.txt"
	var collected []*Entry
	count, _ := f.maybeMergeRemoteListings(context.Background(), "/buckets/mybucket", map[string]struct{}{}, "a.txt", 100, "",
		func(entry *Entry) (bool, error) {
			collected = append(collected, entry)
			return true, nil
		})

	assert.Equal(t, int64(2), count)
	require.Len(t, collected, 2)
	assert.Equal(t, "b.txt", collected[0].Name())
	assert.Equal(t, "c.txt", collected[1].Name())
}

func TestMaybeMergeRemoteListings_LimitRespected(t *testing.T) {
	const storageType = "stub_listing_limit"
	stub := &stubRemoteClient{
		listDirResult: []*remote_storage.RemoteListing{
			{Name: "a.txt", Size: 100, StorageName: storageType},
			{Name: "b.txt", Size: 200, StorageName: storageType},
			{Name: "c.txt", Size: 300, StorageName: storageType},
		},
	}
	f, cleanup := setupRemoteListingFiler(t, storageType, stub)
	defer cleanup()

	var collected []*Entry
	count, _ := f.maybeMergeRemoteListings(context.Background(), "/buckets/mybucket", map[string]struct{}{}, "", 2, "",
		func(entry *Entry) (bool, error) {
			collected = append(collected, entry)
			return true, nil
		})

	assert.Equal(t, int64(2), count)
	require.Len(t, collected, 2)
}

func TestMaybeMergeRemoteListings_NilRemoteStorage(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, nil)

	count, _ := f.maybeMergeRemoteListings(context.Background(), "/any/path", map[string]struct{}{}, "", 100, "",
		func(entry *Entry) (bool, error) {
			t.Error("should not be called")
			return true, nil
		})
	assert.Equal(t, int64(0), count)
}

func TestBuildRemoteOnlyEntry_File(t *testing.T) {
	re := &remote_storage.RemoteListing{
		Name: "test.txt", Size: 1234, Mtime: 1700000000, ETag: "abc123", StorageName: "mystore",
	}
	entry := buildRemoteOnlyEntry("/buckets/mybucket", re)

	assert.Equal(t, util.FullPath("/buckets/mybucket/test.txt"), entry.FullPath)
	assert.Equal(t, uint64(1234), entry.FileSize)
	assert.False(t, entry.IsDirectory())
	require.NotNil(t, entry.Remote)
	assert.Equal(t, int64(1234), entry.Remote.RemoteSize)
	assert.Equal(t, "abc123", entry.Remote.RemoteETag)
	assert.Equal(t, "mystore", entry.Remote.StorageName)
}

func TestBuildRemoteOnlyEntry_Directory(t *testing.T) {
	re := &remote_storage.RemoteListing{
		Name: "subdir", IsDirectory: true, StorageName: "mystore",
	}
	entry := buildRemoteOnlyEntry("/buckets/mybucket", re)

	assert.Equal(t, util.FullPath("/buckets/mybucket/subdir"), entry.FullPath)
	assert.True(t, entry.IsDirectory())
	assert.Nil(t, entry.Remote)
}
