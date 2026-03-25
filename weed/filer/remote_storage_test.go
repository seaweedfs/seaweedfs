package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
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
