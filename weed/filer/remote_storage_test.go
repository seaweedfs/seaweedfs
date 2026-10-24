package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
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
