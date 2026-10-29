package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestFilerRemoteStorage_FindRemoteStorageClient(t *testing.T) {
	conf := &remote_pb.RemoteConf{
		Name: "s7",
		Type: "s3",
	}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf

	rs.mapDirectoryToRemoteStorage("/a/b/c", &remote_pb.RemoteStorageLocation{
		Name:   "s7",
		Bucket: "some",
		Path:   "/dir",
	})

	_, _, found := rs.FindRemoteStorageClient("/a/b/c/d/e/f")
	assert.Equal(t, true, found, "find storage client")

	_, _, found2 := rs.FindRemoteStorageClient("/a/b")
	assert.Equal(t, false, found2, "should not find storage client")

	_, _, found3 := rs.FindRemoteStorageClient("/a/b/c")
	assert.Equal(t, false, found3, "should not find storage client")

	_, _, found4 := rs.FindRemoteStorageClient("/a/b/cc")
	assert.Equal(t, false, found4, "should not find storage client")
}

func TestFilerRemoteStorage_FindMountDirectory_LongestPrefixWins(t *testing.T) {
	conf := &remote_pb.RemoteConf{Name: "store", Type: "s3"}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf

	rs.mapDirectoryToRemoteStorage("/buckets/mybucket", &remote_pb.RemoteStorageLocation{
		Name:   "store",
		Bucket: "bucket-root",
		Path:   "/",
	})
	rs.mapDirectoryToRemoteStorage("/buckets/mybucket/prefix", &remote_pb.RemoteStorageLocation{
		Name:   "store",
		Bucket: "bucket-prefix",
		Path:   "/",
	})

	tests := []struct {
		path       string
		wantMount  string
		wantBucket string
	}{
		{"/buckets/mybucket/file.txt", "/buckets/mybucket", "bucket-root"},
		{"/buckets/mybucket/prefix/file.txt", "/buckets/mybucket/prefix", "bucket-prefix"},
		{"/buckets/mybucket/prefix/sub/file.txt", "/buckets/mybucket/prefix", "bucket-prefix"},
	}
	for _, tt := range tests {
		mountDir, loc := rs.FindMountDirectory(util.FullPath(tt.path))
		assert.Equal(t, util.FullPath(tt.wantMount), mountDir, "mount dir for %s", tt.path)
		if assert.NotNil(t, loc, "location for %s", tt.path) {
			assert.Equal(t, tt.wantBucket, loc.Bucket, "bucket for %s", tt.path)
		}
	}
}

// TestLoadRemoteStorageConfigurationsAndMapping_RejectsBlockedEndpoint
// reproduces the unauthenticated-plant SSRF: a RemoteConf whose S3 endpoint is
// a loopback address is written under /etc/remote. With the SSRF deny-list
// validator injected (as the filer server does), the conf is dropped at load
// so the lazy-fetch path can never resolve a client for it; a conf whose
// endpoint passes validation is loaded as before.
func TestLoadRemoteStorageConfigurationsAndMapping_RejectsBlockedEndpoint(t *testing.T) {
	loopbackConf := &remote_pb.RemoteConf{
		Name:       "evil",
		Type:       "s3",
		S3Endpoint: "http://127.0.0.1:8000",
		S3Region:   "us-east-1",
	}
	okConf := &remote_pb.RemoteConf{
		Name:       "good",
		Type:       "s3",
		S3Endpoint: "https://s3.example.com",
		S3Region:   "us-east-1",
	}
	loopbackBytes, _ := proto.Marshal(loopbackConf)
	okBytes, _ := proto.Marshal(okConf)

	store := newStubFilerStore()
	store.entries[string(DirectoryEtcRemote)+"/evil.conf"] = &Entry{
		FullPath: util.FullPath(string(DirectoryEtcRemote) + "/evil.conf"),
		Attr:     Attr{Mode: 0644},
		Content:  loopbackBytes,
	}
	store.entries[string(DirectoryEtcRemote)+"/good.conf"] = &Entry{
		FullPath: util.FullPath(string(DirectoryEtcRemote) + "/good.conf"),
		Attr:     Attr{Mode: 0644},
		Content:  okBytes,
	}

	rs := NewFilerRemoteStorage()
	// Validator mirrors the filer server injection: reject loopback endpoints.
	rs.SetConfValidator(func(_ context.Context, conf *remote_pb.RemoteConf) error {
		if conf.GetS3Endpoint() == "http://127.0.0.1:8000" {
			return assertError("reject remote endpoint: loopback")
		}
		return nil
	})
	f := newTestFiler(t, store, rs)

	require.NoError(t, f.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(f))

	_, _, foundEvil := f.RemoteStorage.GetRemoteStorageClient("evil")
	assert.False(t, foundEvil, "loopback-endpoint conf must be dropped at load")
	_, _, foundGood := f.RemoteStorage.GetRemoteStorageClient("good")
	assert.True(t, foundGood, "valid-endpoint conf must still be loaded")
}

// assertError is a tiny error type so the test validator can return a sentinel
// without importing fmt.
type assertError string

func (e assertError) Error() string { return string(e) }

// Every reload drops the cached clients of remotes that are no longer
// configured, also when /etc/remote is gone entirely, so a removed remote keeps
// no client and no open connections.
func TestLoadRemoteStorageConfigurationsAndMapping_DropsClientsOfRemovedRemotes(t *testing.T) {
	remote_storage.ResetRemoteStorageClients()
	t.Cleanup(remote_storage.ResetRemoteStorageClients)
	closed := map[string]int{}
	cache := func(name string) {
		conf := &remote_pb.RemoteConf{Name: name, Type: "s3", S3Endpoint: "https://s3.example.com", S3Region: "us-east-1"}
		_, err := remote_storage.GetRemoteStorageWith("guarded", conf, "", func(*remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, func(), error) {
			return &stubRemoteClient{}, func() { closed[name]++ }, nil
		})
		require.NoError(t, err)
	}

	// /etc/remote holds "good" only: the client of "gone" is dropped and closed, "good"'s kept
	goodBytes, _ := proto.Marshal(&remote_pb.RemoteConf{Name: "good", Type: "s3", S3Endpoint: "https://s3.example.com", S3Region: "us-east-1"})
	store := newStubFilerStore()
	store.entries[string(DirectoryEtcRemote)+"/good.conf"] = &Entry{
		FullPath: util.FullPath(string(DirectoryEtcRemote) + "/good.conf"), Attr: Attr{Mode: 0644}, Content: goodBytes,
	}
	cache("good")
	cache("gone")
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	require.NoError(t, f.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(f))
	assert.Zero(t, closed["good"], "a configured remote keeps its client")
	assert.Positive(t, closed["gone"], "a removed remote's client is closed")

	// no /etc/remote at all (the listing answers ErrNotFound): every remote is gone
	empty := newTestFiler(t, newStubFilerStore(), NewFilerRemoteStorage())
	empty.Store = NewFilerStoreWrapper(etcRemoteNotFoundStore{newStubFilerStore()})
	require.NoError(t, empty.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(empty))
	assert.Positive(t, closed["good"], "with no /etc/remote, no remote keeps a client")
}

// a store in which /etc/remote does not exist: its listing answers filer_pb.ErrNotFound
type etcRemoteNotFoundStore struct{ *stubFilerStore }

func (s etcRemoteNotFoundStore) ListDirectoryEntries(context.Context, util.FullPath, string, bool, int64, ListEachEntryFunc) (string, error) {
	return "", filer_pb.ErrNotFound
}

func (s etcRemoteNotFoundStore) ListDirectoryPrefixedEntries(context.Context, util.FullPath, string, bool, int64, string, ListEachEntryFunc) (string, error) {
	return "", filer_pb.ErrNotFound
}
