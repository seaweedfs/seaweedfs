package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	insertErr error
}

func newStubFilerStore() *stubFilerStore {
	return &stubFilerStore{entries: make(map[string]*Entry)}
}

func (s *stubFilerStore) GetName() string { return "stub" }
func (s *stubFilerStore) Initialize(util.Configuration, string) error { return nil }
func (s *stubFilerStore) Shutdown() {}
func (s *stubFilerStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *stubFilerStore) CommitTransaction(context.Context) error    { return nil }
func (s *stubFilerStore) RollbackTransaction(context.Context) error  { return nil }
func (s *stubFilerStore) KvPut(context.Context, []byte, []byte) error { return nil }
func (s *stubFilerStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, ErrKvNotFound
}
func (s *stubFilerStore) KvDelete(context.Context, []byte) error { return nil }
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
	delete(s.entries, string(p))
	return nil
}

// --- minimal RemoteStorageClient stub ---

type stubRemoteClient struct {
	statResult *filer_pb.RemoteEntry
	statErr    error
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
func (c *stubRemoteClient) RemoveDirectory(*remote_pb.RemoteStorageLocation) error { return nil }
func (c *stubRemoteClient) WriteFile(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, io.Reader) (*filer_pb.RemoteEntry, error) {
	return nil, nil
}
func (c *stubRemoteClient) UpdateFileMetadata(*remote_pb.RemoteStorageLocation, *filer_pb.Entry, *filer_pb.Entry) error {
	return nil
}
func (c *stubRemoteClient) DeleteFile(*remote_pb.RemoteStorageLocation) error { return nil }
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

func TestMaybeLazyFetchFromRemote_NetworkErrorPropagated(t *testing.T) {
	const storageType = "stub_lazy_neterr"
	networkErr := fmt.Errorf("connection refused")
	stub := &stubRemoteClient{statErr: networkErr}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "neterr", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: "neterr", Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	entry, err := f.maybeLazyFetchFromRemote(context.Background(), "/buckets/mybucket/file.txt")
	assert.Nil(t, entry)
	require.Error(t, err, "network error should be propagated")
	assert.Contains(t, err.Error(), "connection refused")
}

func TestFindEntry_RemoteErrorPropagated(t *testing.T) {
	const storageType = "stub_findentry_err"
	remoteErr := fmt.Errorf("remote auth failure")
	stub := &stubRemoteClient{statErr: remoteErr}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "autherr", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: "autherr", Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	entry, err := f.FindEntry(context.Background(), "/buckets/mybucket/file.txt")
	assert.Nil(t, entry)
	require.Error(t, err, "FindEntry should propagate remote errors")
	assert.Contains(t, err.Error(), "remote auth failure")
	// Verify it's NOT ErrNotFound
	assert.False(t, errors.Is(err, filer_pb.ErrNotFound), "should not be ErrNotFound")
}

func TestFindEntry_RemoteNotFoundStillReturnsErrNotFound(t *testing.T) {
	const storageType = "stub_findentry_notfound"
	stub := &stubRemoteClient{statErr: remote_storage.ErrRemoteObjectNotFound}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "notfound", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name: "notfound", Bucket: "mybucket", Path: "/",
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	entry, err := f.FindEntry(context.Background(), "/buckets/mybucket/file.txt")
	assert.Nil(t, entry)
	// When remote says "not found", the original ErrNotFound from the store is returned
	assert.True(t, errors.Is(err, filer_pb.ErrNotFound), "genuine not-found should still return ErrNotFound")
}
