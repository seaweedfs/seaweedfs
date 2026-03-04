package filer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteEntryNeedsRevalidation_Fresh(t *testing.T) {
	f := newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage())
	f.RemoteEntryTTL = 5 * time.Minute

	entry := &Entry{
		Remote: &filer_pb.RemoteEntry{
			LastLocalSyncTsNs: time.Now().UnixNano(), // just synced
		},
	}
	assert.False(t, f.remoteEntryNeedsRevalidation(entry))
}

func TestRemoteEntryNeedsRevalidation_Stale(t *testing.T) {
	f := newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage())
	f.RemoteEntryTTL = 5 * time.Minute

	entry := &Entry{
		Remote: &filer_pb.RemoteEntry{
			LastLocalSyncTsNs: time.Now().Add(-10 * time.Minute).UnixNano(), // 10 min ago
		},
	}
	assert.True(t, f.remoteEntryNeedsRevalidation(entry))
}

func TestRemoteEntryNeedsRevalidation_NoTTL(t *testing.T) {
	f := newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage())
	f.RemoteEntryTTL = 0 // disabled

	entry := &Entry{
		Remote: &filer_pb.RemoteEntry{
			LastLocalSyncTsNs: time.Now().Add(-1 * time.Hour).UnixNano(),
		},
	}
	assert.False(t, f.remoteEntryNeedsRevalidation(entry))
}

func TestRemoteEntryNeedsRevalidation_NoRemote(t *testing.T) {
	f := newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage())
	f.RemoteEntryTTL = 5 * time.Minute

	entry := &Entry{Remote: nil}
	assert.False(t, f.remoteEntryNeedsRevalidation(entry))
}

func TestRemoteEntryNeedsRevalidation_ZeroSyncTs(t *testing.T) {
	f := newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage())
	f.RemoteEntryTTL = 5 * time.Minute

	entry := &Entry{
		Remote: &filer_pb.RemoteEntry{
			LastLocalSyncTsNs: 0, // never synced via cache
		},
	}
	assert.False(t, f.remoteEntryNeedsRevalidation(entry))
}

func TestRevalidateRemoteEntry_Unchanged(t *testing.T) {
	const storageType = "stub_reval_unchanged"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{
			RemoteMtime: 1700000000, RemoteSize: 500, RemoteETag: "same_etag",
			StorageName: storageType,
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
	f.RemoteEntryTTL = 5 * time.Minute

	oldSyncTs := time.Now().Add(-10 * time.Minute).UnixNano()
	existing := &Entry{
		FullPath: "/buckets/mybucket/file.txt",
		Attr:     Attr{FileSize: 500, Mtime: time.Unix(1700000000, 0)},
		Remote: &filer_pb.RemoteEntry{
			RemoteMtime:       1700000000,
			RemoteSize:        500,
			RemoteETag:        "same_etag",
			StorageName:       storageType,
			LastLocalSyncTsNs: oldSyncTs,
		},
	}
	store.InsertEntry(context.Background(), existing)

	result, err := f.revalidateRemoteEntry(context.Background(), "/buckets/mybucket/file.txt", existing)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "same_etag", result.Remote.RemoteETag)
	// LastLocalSyncTsNs should be updated
	assert.True(t, result.Remote.LastLocalSyncTsNs > oldSyncTs)
}

func TestRevalidateRemoteEntry_Changed(t *testing.T) {
	const storageType = "stub_reval_changed"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{
			RemoteMtime: 1700000099, RemoteSize: 999, RemoteETag: "new_etag",
			StorageName: storageType,
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

	existing := &Entry{
		FullPath: "/buckets/mybucket/file.txt",
		Attr:     Attr{FileSize: 500, Mtime: time.Unix(1700000000, 0)},
		Remote: &filer_pb.RemoteEntry{
			RemoteMtime:       1700000000,
			RemoteSize:        500,
			RemoteETag:        "old_etag",
			StorageName:       storageType,
			LastLocalSyncTsNs: time.Now().Add(-10 * time.Minute).UnixNano(),
		},
	}
	store.InsertEntry(context.Background(), existing)

	result, err := f.revalidateRemoteEntry(context.Background(), "/buckets/mybucket/file.txt", existing)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "new_etag", result.Remote.RemoteETag)
	assert.Equal(t, uint64(999), result.FileSize)
	assert.Nil(t, result.Chunks)
}

func TestRevalidateRemoteEntry_Deleted(t *testing.T) {
	const storageType = "stub_reval_deleted"
	stub := &stubRemoteClient{
		statErr: remote_storage.ErrRemoteObjectNotFound,
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

	existing := &Entry{
		FullPath: "/buckets/mybucket/file.txt",
		Attr:     Attr{FileSize: 500},
		Remote: &filer_pb.RemoteEntry{
			RemoteMtime:       1700000000,
			RemoteSize:        500,
			RemoteETag:        "old_etag",
			StorageName:       storageType,
			LastLocalSyncTsNs: time.Now().Add(-10 * time.Minute).UnixNano(),
		},
	}
	store.InsertEntry(context.Background(), existing)

	result, err := f.revalidateRemoteEntry(context.Background(), "/buckets/mybucket/file.txt", existing)
	assert.Nil(t, result)
	assert.True(t, errors.Is(err, filer_pb.ErrNotFound))
}

func TestRevalidateRemoteEntry_ErrorServesStale(t *testing.T) {
	const storageType = "stub_reval_stale"
	stub := &stubRemoteClient{
		statErr: errors.New("network error"),
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

	existing := &Entry{
		FullPath: "/buckets/mybucket/file.txt",
		Attr:     Attr{FileSize: 500},
		Remote: &filer_pb.RemoteEntry{
			RemoteMtime:       1700000000,
			RemoteSize:        500,
			RemoteETag:        "old_etag",
			StorageName:       storageType,
			LastLocalSyncTsNs: time.Now().Add(-10 * time.Minute).UnixNano(),
		},
	}
	store.InsertEntry(context.Background(), existing)

	result, err := f.revalidateRemoteEntry(context.Background(), "/buckets/mybucket/file.txt", existing)
	require.NoError(t, err)
	require.NotNil(t, result, "should serve stale entry on error")
	assert.Equal(t, "old_etag", result.Remote.RemoteETag)
}
