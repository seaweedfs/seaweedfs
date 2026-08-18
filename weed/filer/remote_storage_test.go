package filer

import (
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

func TestRemoteStorageMapping_SkipRemoteDeleteSurvivesMountMappingRoundTrip(t *testing.T) {
	stored, err := proto.Marshal(&remote_pb.RemoteStorageMapping{
		Mappings: map[string]*remote_pb.RemoteStorageLocation{
			"/buckets/mybucket": {
				Name:             "store",
				Bucket:           "mybucket",
				Path:             "/",
				SkipRemoteDelete: true,
			},
			"/buckets/other": {
				Name:   "store",
				Bucket: "other",
				Path:   "/",
			},
		},
	})
	require.NoError(t, err)

	mappings, err := UnmarshalRemoteStorageMappings(stored)
	require.NoError(t, err)

	conf := &remote_pb.RemoteConf{Name: "store", Type: "s3"}
	rs := NewFilerRemoteStorage()
	rs.storageNameToConf[conf.Name] = conf
	for dir, loc := range mappings.Mappings {
		rs.mapDirectoryToRemoteStorage(util.FullPath(dir), loc)
	}

	_, skipLoc := rs.FindMountDirectory("/buckets/mybucket/file.txt")
	require.NotNil(t, skipLoc)
	assert.True(t, skipLoc.SkipRemoteDelete, "skipRemoteDelete should survive mount mapping serialisation")

	_, plainLoc := rs.FindMountDirectory("/buckets/other/file.txt")
	require.NotNil(t, plainLoc)
	assert.False(t, plainLoc.SkipRemoteDelete, "mounts without the flag should keep propagating deletes")
}
