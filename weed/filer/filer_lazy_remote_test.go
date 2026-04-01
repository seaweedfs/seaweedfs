package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
	mu              sync.Mutex
	entries         map[string]*Entry
	kv              map[string][]byte
	insertErr       error
	deleteErrByPath map[string]error
}

func newStubFilerStore() *stubFilerStore {
	return &stubFilerStore{
		entries:         make(map[string]*Entry),
		kv:              make(map[string][]byte),
		deleteErrByPath: make(map[string]error),
	}
}

func (s *stubFilerStore) GetName() string                             { return "stub" }
func (s *stubFilerStore) Initialize(util.Configuration, string) error { return nil }
func (s *stubFilerStore) Shutdown()                                   {}
func (s *stubFilerStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *stubFilerStore) CommitTransaction(context.Context) error   { return nil }
func (s *stubFilerStore) RollbackTransaction(context.Context) error { return nil }
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
func (s *stubFilerStore) DeleteFolderChildren(_ context.Context, dirPath util.FullPath) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(dirPath) + "/"
	for k := range s.entries {
		if strings.HasPrefix(k, prefix) {
			delete(s.entries, k)
		}
	}
	return nil
}
func (s *stubFilerStore) listDirectoryChildNames(dirPath util.FullPath, startFileName string, includeStartFile bool, namePrefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirPrefix := string(dirPath) + "/"
	var names []string
	for k := range s.entries {
		if !strings.HasPrefix(k, dirPrefix) {
			continue
		}
		rest := k[len(dirPrefix):]
		if strings.Contains(rest, "/") {
			continue
		}
		if namePrefix != "" && !strings.HasPrefix(rest, namePrefix) {
			continue
		}
		if rest > startFileName || (includeStartFile && rest == startFileName) {
			names = append(names, rest)
		}
	}
	sort.Strings(names)
	return names
}
func (s *stubFilerStore) getEntry(path string) *Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[path]
}
func (s *stubFilerStore) ListDirectoryEntries(_ context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {
	names := s.listDirectoryChildNames(dirPath, startFileName, includeStartFile, "")
	dirPrefix := string(dirPath) + "/"
	lastFileName := ""
	for i, name := range names {
		if int64(i) >= limit {
			break
		}
		entry := s.getEntry(dirPrefix + name)
		cont, err := eachEntryFunc(entry)
		if err != nil {
			return lastFileName, err
		}
		lastFileName = name
		if !cont {
			break
		}
	}
	return lastFileName, nil
}
func (s *stubFilerStore) ListDirectoryPrefixedEntries(_ context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (string, error) {
	names := s.listDirectoryChildNames(dirPath, startFileName, includeStartFile, prefix)
	dirPrefix := string(dirPath) + "/"
	lastFileName := ""
	for i, name := range names {
		if int64(i) >= limit {
			break
		}
		entry := s.getEntry(dirPrefix + name)
		cont, err := eachEntryFunc(entry)
		if err != nil {
			return lastFileName, err
		}
		lastFileName = name
		if !cont {
			break
		}
	}
	return lastFileName, nil
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

	listDirFn    func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error
	listDirCalls int
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
func (c *stubRemoteClient) ListDirectory(_ context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	c.listDirCalls++
	if c.listDirFn != nil {
		return c.listDirFn(loc, visitFn)
	}
	return nil
}
func (c *stubRemoteClient) ListBuckets() ([]*remote_storage.Bucket, error) { return nil, nil }
func (c *stubRemoteClient) CreateBucket(string) error                      { return nil }
func (c *stubRemoteClient) DeleteBucket(string) error                      { return nil }

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
		FilerConf:           NewFilerConf(),
		MaxFilenameLength:   255,
		MasterClient:        mc,
		fileIdDeletionQueue: util.NewUnboundedQueue(),
		deletionQuit:        make(chan struct{}),
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

func TestDeleteEntryMetaAndData_IsFromOtherClusterSkipsRemoteDelete(t *testing.T) {
	const storageType = "stub_lazy_delete_other_cluster"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "othercluster", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "othercluster",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/replicated.txt")
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

	// isFromOtherCluster=true simulates a replicated delete from another filer
	err := f.DeleteEntryMetaAndData(context.Background(), filePath, false, false, false, true, nil, 0)
	require.NoError(t, err)

	// Local metadata should be deleted
	_, findErr := store.FindEntry(context.Background(), filePath)
	require.ErrorIs(t, findErr, filer_pb.ErrNotFound)
	// Remote should NOT have been called — the originating filer handles that
	require.Len(t, stub.deleteCalls, 0)
	require.Len(t, stub.removeCalls, 0)
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

func TestDeleteEntryMetaAndData_LocalDeleteFailurePreservesMetadata(t *testing.T) {
	const storageType = "stub_lazy_delete_local_fail"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "localfail", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "localfail",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	filePath := util.FullPath("/buckets/mybucket/localfail.txt")
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

	// Local metadata should still exist since local delete failed
	stored, findErr := store.FindEntry(context.Background(), filePath)
	require.NoError(t, findErr)
	require.NotNil(t, stored)
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

func TestDeleteEntryMetaAndData_RecursiveFolderDeleteRemotesChildren(t *testing.T) {
	const storageType = "stub_lazy_delete_folder_children"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "childstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "childstore",
		Bucket: "mybucket",
		Path:   "/",
	})

	store := newStubFilerStore()
	dirPath := util.FullPath("/buckets/mybucket/subdir")
	store.entries[string(dirPath)] = &Entry{
		FullPath: dirPath,
		Attr: Attr{
			Mtime:  time.Unix(1700000000, 0),
			Crtime: time.Unix(1700000000, 0),
			Mode:   os.ModeDir | 0755,
		},
	}
	childPath := util.FullPath("/buckets/mybucket/subdir/child.txt")
	store.entries[string(childPath)] = &Entry{
		FullPath: childPath,
		Attr: Attr{
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1700000000, 0),
			Mode:     0644,
			FileSize: 50,
		},
		Remote: &filer_pb.RemoteEntry{RemoteMtime: 1700000000, RemoteSize: 50},
	}
	f := newTestFiler(t, store, rs)

	err := f.DeleteEntryMetaAndData(context.Background(), dirPath, true, false, false, false, nil, 0)
	require.NoError(t, err)

	// Child file should have been deleted from remote
	require.Len(t, stub.deleteCalls, 1)
	assert.Equal(t, "/subdir/child.txt", stub.deleteCalls[0].Path)
	// Directory itself should also have been deleted from remote
	require.Len(t, stub.removeCalls, 1)
	assert.Equal(t, "/subdir", stub.removeCalls[0].Path)
}

// --- lazy listing tests ---

func TestMaybeLazyListFromRemote_PopulatesStoreFromRemote(t *testing.T) {
	const storageType = "stub_lazy_list_populate"
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			if err := visitFn("/", "subdir", true, nil); err != nil {
				return err
			}
			if err := visitFn("/", "file.txt", false, &filer_pb.RemoteEntry{
				RemoteMtime: 1700000000,
				RemoteSize:  42,
				RemoteETag:  "abc",
				StorageName: "myliststore",
			}); err != nil {
				return err
			}
			return nil
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "myliststore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:                   "myliststore",
		Bucket:                 "mybucket",
		Path:                   "/",
		ListingCacheTtlSeconds: 300,
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 1, stub.listDirCalls)

	// Check that the file was persisted
	fileEntry := store.getEntry("/buckets/mybucket/file.txt")
	require.NotNil(t, fileEntry, "file.txt should be persisted")
	assert.Equal(t, uint64(42), fileEntry.FileSize)
	assert.NotNil(t, fileEntry.Remote)

	// Check that the subdirectory was persisted
	dirEntry := store.getEntry("/buckets/mybucket/subdir")
	require.NotNil(t, dirEntry, "subdir should be persisted")
	assert.True(t, dirEntry.IsDirectory())
}

func TestMaybeLazyListFromRemote_DisabledWhenTTLZero(t *testing.T) {
	const storageType = "stub_lazy_list_disabled"
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			return visitFn("/", "file.txt", false, &filer_pb.RemoteEntry{
				RemoteMtime: 1700000000, RemoteSize: 10,
			})
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "disabledstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "disabledstore",
		Bucket: "mybucket",
		Path:   "/",
		// ListingCacheTtlSeconds defaults to 0 → disabled
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 0, stub.listDirCalls, "should not call remote when TTL is 0")
}

func TestMaybeLazyListFromRemote_TTLCachePreventsSecondCall(t *testing.T) {
	const storageType = "stub_lazy_list_ttl"
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			return visitFn("/", "file.txt", false, &filer_pb.RemoteEntry{
				RemoteMtime: 1700000000, RemoteSize: 10,
			})
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "ttlstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:                   "ttlstore",
		Bucket:                 "mybucket",
		Path:                   "/",
		ListingCacheTtlSeconds: 300,
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// First call should hit remote
	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 1, stub.listDirCalls)

	// Second call within TTL should be a no-op
	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 1, stub.listDirCalls, "should not call remote again within TTL")
}

func TestMaybeLazyListFromRemote_NotUnderMount(t *testing.T) {
	rs := NewFilerRemoteStorage()
	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/not/a/mount"))
}

func TestMaybeLazyListFromRemote_SkipsLocalOnlyEntries(t *testing.T) {
	const storageType = "stub_lazy_list_skiplocal"
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			// Remote has a file called "local.txt" too
			return visitFn("/", "local.txt", false, &filer_pb.RemoteEntry{
				RemoteMtime: 1700000000, RemoteSize: 99,
			})
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "skipstore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:                   "skipstore",
		Bucket:                 "mybucket",
		Path:                   "/",
		ListingCacheTtlSeconds: 300,
	})

	store := newStubFilerStore()
	// Pre-populate a local-only entry (no Remote field)
	store.entries["/buckets/mybucket/local.txt"] = &Entry{
		FullPath: "/buckets/mybucket/local.txt",
		Attr:     Attr{Mode: 0644, FileSize: 50},
	}
	f := newTestFiler(t, store, rs)

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))

	// Local entry should NOT have been overwritten
	localEntry := store.getEntry("/buckets/mybucket/local.txt")
	require.NotNil(t, localEntry)
	assert.Equal(t, uint64(50), localEntry.FileSize, "local-only entry should not be overwritten")
	assert.Nil(t, localEntry.Remote, "local-only entry should keep nil Remote")
}

func TestMaybeLazyListFromRemote_MergesExistingRemoteEntry(t *testing.T) {
	const storageType = "stub_lazy_list_merge"
	stub := &stubRemoteClient{
		listDirFn: func(loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
			return visitFn("/", "cached.txt", false, &filer_pb.RemoteEntry{
				RemoteMtime: 1700000099, // updated mtime
				RemoteSize:  200,        // updated size
				RemoteETag:  "new-etag",
				StorageName: "mergestore",
			})
		},
	}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "mergestore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:                   "mergestore",
		Bucket:                 "mybucket",
		Path:                   "/",
		ListingCacheTtlSeconds: 300,
	})

	store := newStubFilerStore()
	// Pre-populate an existing remote-backed entry with chunks and extended attrs
	existingChunks := []*filer_pb.FileChunk{
		{FileId: "1,abc123", Size: 100, Offset: 0},
	}
	store.entries["/buckets/mybucket/cached.txt"] = &Entry{
		FullPath: "/buckets/mybucket/cached.txt",
		Attr: Attr{
			Mode:     0644,
			FileSize: 100,
			Uid:      1000,
			Gid:      1000,
			Mtime:    time.Unix(1700000000, 0),
			Crtime:   time.Unix(1699000000, 0),
		},
		Chunks: existingChunks,
		Extended: map[string][]byte{
			"user.custom": []byte("myvalue"),
		},
		Remote: &filer_pb.RemoteEntry{
			RemoteMtime: 1700000000,
			RemoteSize:  100,
			RemoteETag:  "old-etag",
			StorageName: "mergestore",
		},
	}
	f := newTestFiler(t, store, rs)

	f.maybeLazyListFromRemote(context.Background(), util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 1, stub.listDirCalls)

	merged := store.getEntry("/buckets/mybucket/cached.txt")
	require.NotNil(t, merged)

	// Remote metadata should be updated
	assert.Equal(t, int64(1700000099), merged.Remote.RemoteMtime)
	assert.Equal(t, int64(200), merged.Remote.RemoteSize)
	assert.Equal(t, "new-etag", merged.Remote.RemoteETag)
	assert.Equal(t, uint64(200), merged.FileSize)
	assert.Equal(t, time.Unix(1700000099, 0), merged.Mtime)

	// Local state should be preserved
	assert.Equal(t, existingChunks, merged.Chunks, "chunks must be preserved")
	assert.Equal(t, []byte("myvalue"), merged.Extended["user.custom"], "extended attrs must be preserved")
	assert.Equal(t, uint32(1000), merged.Uid, "uid must be preserved")
	assert.Equal(t, uint32(1000), merged.Gid, "gid must be preserved")
	assert.Equal(t, os.FileMode(0644), merged.Mode, "mode must be preserved")
	assert.Equal(t, time.Unix(1699000000, 0), merged.Crtime, "crtime must be preserved")
}

func TestMaybeLazyListFromRemote_ContextGuardPreventsRecursion(t *testing.T) {
	const storageType = "stub_lazy_list_guard"
	stub := &stubRemoteClient{}
	defer registerStubMaker(t, storageType, stub)()

	conf := &remote_pb.RemoteConf{Name: "guardliststore", Type: storageType}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:                   "guardliststore",
		Bucket:                 "mybucket",
		Path:                   "/",
		ListingCacheTtlSeconds: 300,
	})

	store := newStubFilerStore()
	f := newTestFiler(t, store, rs)

	// With lazyListContextKey set, should be a no-op
	guardCtx := context.WithValue(context.Background(), lazyListContextKey{}, true)
	f.maybeLazyListFromRemote(guardCtx, util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 0, stub.listDirCalls)

	// With lazyFetchContextKey set, should also be a no-op
	fetchCtx := context.WithValue(context.Background(), lazyFetchContextKey{}, true)
	f.maybeLazyListFromRemote(fetchCtx, util.FullPath("/buckets/mybucket"))
	assert.Equal(t, 0, stub.listDirCalls)
}
