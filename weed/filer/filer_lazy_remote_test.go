package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- minimal FilerStore stub ---

type stubFilerStore struct {
	mu      sync.Mutex
	entries map[string]*Entry
	kv      map[string][]byte
	insertErr error
	deleteErrByPath map[string]error
}

func newStubFilerStore() *stubFilerStore {
	return &stubFilerStore{
		entries: make(map[string]*Entry),
		kv: make(map[string][]byte),
		deleteErrByPath: make(map[string]error),
	}
}

func (s *stubFilerStore) GetName() string { return "stub" }
func (s *stubFilerStore) Initialize(util.Configuration, string) error { return nil }
func (s *stubFilerStore) Shutdown() {}
func (s *stubFilerStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *stubFilerStore) CommitTransaction(context.Context) error    { return nil }
func (s *stubFilerStore) RollbackTransaction(context.Context) error  { return nil }
func (s *stubFilerStore) KvPut(_ context.Context, key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[string(key)] = append([]byte(nil), value...)
	return nil
}
func (s *stubFilerStore) KvGet(_ context.Context, key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, found := s.kv[string(key)]
	if !found {
		return nil, ErrKvNotFound
	}
	return append([]byte(nil), value...), nil
}
func (s *stubFilerStore) KvDelete(_ context.Context, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kv, string(key))
	return nil
}
func (s *stubFilerStore) DeleteFolderChildren(context.Context, util.FullPath) error { return nil }
func (s *stubFilerStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *stubFilerStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (string, error) {
	return "", nil
}

func (s *stubFilerStore) InsertEntry(_ context.Context, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.insertErr != nil {
		return s.insertErr
	}
	s.entries[string(entry.FullPath)] = entry
	return nil
}

func (s *stubFilerStore) UpdateEntry(_ context.Context, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[string(entry.FullPath)] = entry
	return nil
}

func (s *stubFilerStore) FindEntry(_ context.Context, p util.FullPath) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[string(p)]; ok {
		return e, nil
	}
	return nil, filer_pb.ErrNotFound
}

func (s *stubFilerStore) DeleteEntry(_ context.Context, p util.FullPath) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if deleteErr, found := s.deleteErrByPath[string(p)]; found && deleteErr != nil {
		return deleteErr
	}
	delete(s.entries, string(p))
	return nil
}

// --- minimal RemoteStorageClient stub ---

type stubRemoteClient struct {
	statResult *filer_pb.RemoteEntry
	statErr    error
	deleteErr  error
	removeErr  error

	deleteCalls []*remote_pb.RemoteStorageLocation
	removeCalls []*remote_pb.RemoteStorageLocation
}

func (c *stubRemoteClient) StatFile(*remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	return c.statResult, c.statErr
}
func (c *stubRemoteClient) Traverse(*remote_pb.RemoteStorageLocation, remote_storage.VisitFunc) error {
	return nil
}
func (c *stubRemoteClient) ReadFile(*remote_pb.RemoteStorageLocation, int64, int64) ([]byte, error) {
	return nil, nil
}
func (c *stubRemoteClient) WriteDirectory(*remote_pb.RemoteStorageLocation, *filer_pb.Entry) error {
	return nil
}
func (c *stubRemoteClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) error {
	c.removeCalls = append(c.removeCalls, &remote_pb.RemoteStorageLocation{
		Name:   loc.Name,
		Bucket: loc.Bucket,
		Path:   loc.Path,
	})
	return c.removeErr
}
func (c *stubRemoteClient) WriteFile(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, io.Reader) (*filer_pb.RemoteEntry, error) {
	return nil, nil
}
func (c *stubRemoteClient) UpdateFileMetadata(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, *filer_pb.Entry) error {
	return nil
}
func (c *stubRemoteClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) error {
	c.deleteCalls = append(c.deleteCalls, &remote_pb.RemoteStorageLocation{
		Name:   loc.Name,
		Bucket: loc.Bucket,
		Path:   loc.Path,
	})
	return c.deleteErr
}
func (c *stubRemoteClient) ListBuckets() ([]*remote_storage.Bucket, error)    { return nil, nil }
func (c *stubRemoteClient) CreateBucket(string) error                         { return nil }
func (c *stubRemoteClient) DeleteBucket(string) error                         { return nil }

// --- stub RemoteStorageClientMaker ---

type stubClientMaker struct {
	client remote_storage.RemoteStorageClient
}

func (m *stubClientMaker) Make(*remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return m.client, nil
}
func (m *stubClientMaker) HasBucket() bool { return true }

// --- test filer factory ---

func newTestFiler(t *testing.T, store *stubFilerStore, rs *FilerRemoteStorage) *Filer {
	t.Helper()
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	mc := wdclient.NewMasterClient(
		dialOption, "test", cluster.FilerType,
		pb.ServerAddress("localhost:0"), "", "",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{}),
	)
	f := &Filer{
		RemoteStorage:       rs,
		Store:               NewFilerStoreWrapper(store),
		MaxFilenameLength:   255,
		MasterClient:        mc,
		fileIdDeletionQueue: util.NewUnboundedQueue(),
		LocalMetaLogBuffer: log_buffer.NewLogBuffer("test", time.Minute,
			func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, func() {}),
	}
	return f
}

// registerStubMaker registers a stub RemoteStorageClientMaker for the given
// type string and returns a cleanup function that restores the previous maker.
func registerStubMaker(t *testing.T, storageType string, client remote_storage.RemoteStorageClient) func() {
	t.Helper()
	prev := remote_storage.RemoteStorageClientMakers[storageType]
	remote_storage.RemoteStorageClientMakers[storageType] = &stubClientMaker{client: client}
	return func() {
		if prev != nil {
			remote_storage.RemoteStorageClientMakers[storageType] = prev
		} else {
			delete(remote_storage.RemoteStorageClientMakers, storageType)
		}
	}
}

// --- tests ---

func TestMaybeLazyFetchFromRemote_HitsRemoteAndPersists(t *testing.T) {
	const storageType = "stub_lazy_hit"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 1234},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "mystore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "mystore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/file.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, util.FullPath("/buckets/mybucket/file.txt"), entry.FullPath)
	assert.Equal(t, int64(1234), entry.Remote.RemoteSize)
	assert.Equal(t, uint64(1234), entry.FileSize)

	// entry must have been persisted in the store
	stored, sErr := store.FindEntry(context.Background(), "/buckets/mybucket/file.txt")
	require.NoError(t, sErr)
	assert.Equal(t, int64(1234), stored.Remote.RemoteSize)
}

func TestMaybeLazyFetchFromRemote_NotUnderMount(t *testing.T) {
	rs := NewFilerRemoteStorage()
	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/not/a/mounted/path.txt")
	require.NoError(t, err)
	assert.Nil(t, entry)
}

func TestMaybeLazyFetchFromRemote_RemoteObjectNotFound(t *testing.T) {
	const storageType = "stub_lazy_notfound"
	stub := &stubRemoteClient{statErr: remote_storage.ErrRemoteObjectNotFound}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "storenotfound", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "storenotfound",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/missing.txt")
	require.NoError(t, err)
	assert.Nil(t, entry)
}

func TestMaybeLazyFetchFromRemote_CreateEntryFailureReturnsInMemoryEntry(t *testing.T) {
	const storageType = "stub_lazy_saveerr"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 42},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "storesaveerr", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "storesaveerr",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	store.insertErr = errors.New("simulated store failure")
	f := newTestFiler(t, store, rs)

	// even with a store failure, the in-memory entry should be returned
	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/failfile.txt")
	require.NoError(t, err)
	require.NotNil(t, entry, "should return in-memory entry even when CreateEntry fails")
	assert.Equal(t, int64(42), entry.Remote.RemoteSize)
}

func TestMaybeLazyFetchFromRemote_LongestPrefixMount(t *testing.T) {
	// Register maker for the root mount
	const typeRoot = "stub_lp_root"
	stubRoot := &stubRemoteClient{statResult: &filer_pb.RemoteEntry{RemoteMtime: 1, RemoteSize: 10}}
	defer registerStubMaker(t, typeRoot, stubRoot)()

	// Register maker for the prefix mount
	const typePrefix = "stub_lp_prefix"
	stubPrefix := &stubRemoteClient{statResult: &filer_pb.RemoteEntry{RemoteMtime: 2, RemoteSize: 20}}
	defer registerStubMaker(t, typePrefix, stubPrefix)()

	rs := NewFilerRemoteStorage()
	rs.storageNameToConf["rootstore"] = &remote_pb.RemoteConf{Name: "rootstore", Type: typeRoot}
	rs.storageNameToConf["prefixstore"] = &remote_pb.RemoteConf{Name: "prefixstore", Type: typePrefix}

	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: "rootstore", Bucket: "root-bucket", Path: "/",
	})
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket/prefix", &remote_pb.RemoteStorageLocation{
		Name: "prefixstore", Bucket: "prefix-bucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// path under root mount only
	entryRoot, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/file.txt")
	require.NoError(t, err)
	require.NotNil(t, entryRoot)
	assert.Equal(t, int64(10), entryRoot.Remote.RemoteSize, "root mount should be used")

	// path under nested (longer) mount — must prefer the longer prefix
	entryPrefix, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/prefix/file.txt")
	require.NoError(t, err)
	require.NotNil(t, entryPrefix)
	assert.Equal(t, int64(20), entryPrefix.Remote.RemoteSize, "nested mount should win (longest prefix)")
}

type countingRemoteClient struct {
	stubRemoteClient
	statCalls int
}

func (c *countingRemoteClient) StatFile(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	c.statCalls++
	return c.stubRemoteClient.StatFile(loc)
}

func TestMaybeLazyFetchFromRemote_ContextGuardPreventsRecursion(t *testing.T) {
	const storageType = "stub_lazy_guard"
	countingStub := &countingRemoteClient{
		stubRemoteClient: stubRemoteClient{
			statResult: &filer_pb.RemoteEntry{RemoteMtime: 1, RemoteSize: 1},
		},
	}
	defer registerStubMaker(t, storageType, countingStub)()

	conf := &remote_pb.RemoteConf{Name: "guardstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "guardstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	guardCtx := context.WithValue(context.Background(), lazyFetchContextKey{}, true)
	entry, err := f.maybeLazyFetchFromRemote(guardCtx, "/buckets/mybucket/file.txt")
	require.NoError(t, err)
	assert.Nil(t, entry)
	assert.Equal(t, 0, countingStub.statCalls, "guard should prevent StatFile from being called")
}

func TestFindEntry_LazyFetchOnMiss(t *testing.T) {
	const storageType = "stub_lazy_findentry"
	stub := &stubRemoteClient{
		statResult: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 999},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "findentrystore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "findentrystore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// First lookup: store miss → lazy fetch
	entry, err := f.FindEntry(context.Background(), "/buckets/mybucket/obj.txt")
	require.NoError(t, err, fmt.Sprintf("unexpected err: %v", err))
	require.NotNil(t, entry)
	assert.Equal(t, uint64(999), entry.FileSize)

	// Second lookup: now in store, no remote call needed
	entry2, err2 := f.FindEntry(context.Background(), "/buckets/mybucket/obj.txt")
	require.NoError(t, err2)
	require.NotNil(t, entry2)
	assert.Equal(t, uint64(999), entry2.FileSize)
}

func TestDeleteEntryMetaAndData_RemoteOnlyFileDeletesRemoteAndMetadata(t *testing.T) {
	const storageType = "stub_lazy_delete_file"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "deletestore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "deletestore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/file.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 64,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 64},
	}
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0)
	require.NoError(t, err)

	_, findErr := store.FindEntry(context.Background(), filePath)
	require.ErrorIs(t, findErr, filer_pb.ErrNotFound)
	require.Len(t, stub.deleteCalls, 1)
	assert.Equal(t, "deletestore", stub.deleteCalls[0].Name)
	assert.Equal(t, "mybucket", stub.deleteCalls[0].Bucket)
	assert.Equal(t, "/file.txt", stub.deleteCalls[0].Path)
}

func TestDeleteEntryMetaAndData_RemoteOnlyFileNotUnderMountSkipsRemoteDelete(t *testing.T) {
	const storageType = "stub_lazy_delete_not_under_mount"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "notundermount", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "notundermount",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/no/mount/file.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 99,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 99},
	}
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0)
	require.NoError(t, err)
	require.Len(t, stub.deleteCalls, 0)
}

func TestDeleteEntryMetaAndData_RemoteMountWithoutClientResolutionKeepsMetadata(t *testing.T) {
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf["missingclient"] = &remote_pb.RemoteConf{Name: "missingclient", Type: "stub_missing_client"}
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "missingclient",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/no-client.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 51,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 51},
	}
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "resolve remote storage client")
	require.ErrorContains(t, err, string(filePath))

	stored, findErr := store.FindEntry(context.Background(), filePath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
}

func TestDeleteEntryMetaAndData_LocalDeleteFailureLeavesDurablePendingForReconcile(t *testing.T) {
	const storageType = "stub_lazy_delete_pending_reconcile"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "pendingreconcile", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "pendingreconcile",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/reconcile.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 80,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 80},
	}
	store.deleteErrByPath[string(filePath)] = errors.New("simulated local delete failure")
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "filer store delete")
	require.Len(t, stub.deleteCalls, 1)

	stored, findErr := store.FindEntry(context.Background(), filePath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)

	pendingPaths, pendingErr := f.listPendingRemoteMetadataDeletionPaths(context.Background())
	require.NoError(t, pendingErr)
	require.Equal(t, []util.FullPath{filePath}, pendingPaths)

	delete(store.deleteErrByPath, string(filePath))
	require.NoError(t, f.reconcilePendingRemoteMetadataDeletions(context.Background()))

	_, findAfterReconcileErr := store.FindEntry(context.Background(), filePath)
	require.ErrorIs(t, findAfterReconcileErr, filer_pb.ErrNotFound)
	require.Len(t, stub.deleteCalls, 1)

	pendingPaths, pendingErr = f.listPendingRemoteMetadataDeletionPaths(context.Background())
	require.NoError(t, pendingErr)
	require.Empty(t, pendingPaths)
}

func TestDeleteEntryMetaAndData_RemoteDeleteNotFoundStillDeletesMetadata(t *testing.T) {
	const storageType = "stub_lazy_delete_notfound"
	stub := &stubRemoteClient{deleteErr: remote_storage.ErrRemoteObjectNotFound}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "deletenotfound", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "deletenotfound",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/notfound.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 23,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 23},
	}
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0)
	require.NoError(t, err)
	_, findErr := store.FindEntry(context.Background(), filePath)
	require.ErrorIs(t, findErr, filer_pb.ErrNotFound)
}

func TestDeleteEntryMetaAndData_RemoteDeleteErrorKeepsMetadata(t *testing.T) {
	const storageType = "stub_lazy_delete_error"
	stub := &stubRemoteClient{deleteErr: errors.New("remote delete failed")}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "deleteerr", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "deleteerr",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/error.txt")
	store.entries[string(filePath)] = &Entry{
		FullPath: filePath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 77,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 77},
	}
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, false, nil, 0)
	require.Error(t, err)
	stored, findErr := store.FindEntry(context.Background(), filePath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
}

func TestDeleteEntryMetaAndData_DirectoryUnderMountDeletesRemoteDirectory(t *testing.T) {
	const storageType = "stub_lazy_delete_dir"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "dirstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "dirstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	dirPath := util.FullPath("/buckets/mybucket/dir")
	store.entries[string(dirPath)] = &Entry{
		FullPath: dirPath,
		Attr: Attr{
			Mtime:  time.Unix(1700000000, 0),
			Crtime: time.Unix(1700000000, 0),
			Mode:   os.ModeDir | 0755,
		},
	}
	f := newTestFiler(t, store, rs)

	err := f.doDeleteEntryMetaAndData(context.Background(), store.entries[string(dirPath)], false, false, nil)
	require.NoError(t, err)

	require.Len(t, stub.removeCalls, 1)
	assert.Equal(t, "dirstore", stub.removeCalls[0].Name)
	assert.Equal(t, "mybucket", stub.removeCalls[0].Bucket)
	assert.Equal(t, "/dir", stub.removeCalls[0].Path)
	_, findErr := store.FindEntry(context.Background(), dirPath)
	require.ErrorIs(t, findErr, filer_pb.ErrNotFound)
}
