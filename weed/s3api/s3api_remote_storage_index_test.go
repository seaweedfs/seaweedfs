package s3api

import (
	"io"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stubMount(name string) mountEntry {
	return mountEntry{
		loc:    &remote_pb.RemoteStorageLocation{Name: name, Bucket: "bucket", Path: "/"},
		client: &stubRemoteClient{},
	}
}

type stubRemoteClient struct{}

func (s *stubRemoteClient) Traverse(_ *remote_pb.RemoteStorageLocation, _ remote_storage.VisitFunc) error {
	return nil
}
func (s *stubRemoteClient) StatFile(_ *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	return nil, nil
}
func (s *stubRemoteClient) ReadFile(_ *remote_pb.RemoteStorageLocation, _, _ int64) ([]byte, error) {
	return nil, nil
}
func (s *stubRemoteClient) WriteDirectory(_ *remote_pb.RemoteStorageLocation, _ *filer_pb.Entry) error {
	return nil
}
func (s *stubRemoteClient) RemoveDirectory(_ *remote_pb.RemoteStorageLocation) error { return nil }
func (s *stubRemoteClient) WriteFile(_ *remote_pb.RemoteStorageLocation, _ *filer_pb.Entry, _ io.Reader) (*filer_pb.RemoteEntry, error) {
	return nil, nil
}
func (s *stubRemoteClient) UpdateFileMetadata(_ *remote_pb.RemoteStorageLocation, _, _ *filer_pb.Entry) error {
	return nil
}
func (s *stubRemoteClient) DeleteFile(_ *remote_pb.RemoteStorageLocation) error { return nil }
func (s *stubRemoteClient) ListBuckets() ([]*remote_storage.Bucket, error)      { return nil, nil }
func (s *stubRemoteClient) CreateBucket(_ string) error                         { return nil }
func (s *stubRemoteClient) DeleteBucket(_ string) error                         { return nil }

func populatedIndex(mounts map[string]mountEntry) *remoteStorageIndex {
	idx := newRemoteStorageIndex()
	idx.mounts = mounts
	return idx
}

func TestRemoteStorageIndex_isEmpty(t *testing.T) {
	assert.True(t, newRemoteStorageIndex().isEmpty())

	idx := populatedIndex(map[string]mountEntry{
		"/buckets/mybucket": stubMount("remote1"),
	})
	assert.False(t, idx.isEmpty())
}

func TestRemoteStorageIndex_findForPath(t *testing.T) {
	mount := stubMount("remote1")
	idx := populatedIndex(map[string]mountEntry{
		"/buckets/mybucket": mount,
	})

	tests := []struct {
		name        string
		path        string
		wantFound   bool
		wantRelPath string
	}{
		{
			name:        "exact bucket root",
			path:        "/buckets/mybucket",
			wantFound:   true,
			wantRelPath: "",
		},
		{
			name:        "object under bucket",
			path:        "/buckets/mybucket/folder/file.txt",
			wantFound:   true,
			wantRelPath: "/folder/file.txt",
		},
		{
			name:        "bucket root with trailing slash",
			path:        "/buckets/mybucket/",
			wantFound:   true,
			wantRelPath: "",
		},
		{
			name:      "different bucket",
			path:      "/buckets/otherbucket/file.txt",
			wantFound: false,
		},
		{
			name:      "partial prefix match should not match",
			path:      "/buckets/mybucket2/file.txt",
			wantFound: false,
		},
		{
			name:      "empty path",
			path:      "",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, loc, relPath, found := idx.findForPath(tt.path)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				require.NotNil(t, client)
				require.NotNil(t, loc)
				assert.Equal(t, tt.wantRelPath, relPath)
			} else {
				assert.Nil(t, client)
				assert.Nil(t, loc)
			}
		})
	}
}

func TestRemoteStorageIndex_findForPath_longestPrefixWins(t *testing.T) {
	shallowMount := stubMount("shallow")
	deepMount := stubMount("deep")
	idx := populatedIndex(map[string]mountEntry{
		"/buckets/mybucket":        shallowMount,
		"/buckets/mybucket/prefix": deepMount,
	})

	_, loc, relPath, found := idx.findForPath("/buckets/mybucket/prefix/file.txt")
	require.True(t, found)
	assert.Equal(t, "deep", loc.Name, "deeper mount should win")
	assert.Equal(t, "/file.txt", relPath)
}

func TestRemoteStorageIndex_findForPath_emptyIndex(t *testing.T) {
	_, _, _, found := newRemoteStorageIndex().findForPath("/buckets/mybucket/file.txt")
	assert.False(t, found)
}

func TestIsRemoteBacked(t *testing.T) {
	const bucketsPath = "/buckets"

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{BucketsPath: bucketsPath},
	}

	assert.False(t, s3a.isRemoteBacked("mybucket"), "nil index should return false")

	s3a.remoteStorageIdx = newRemoteStorageIndex()
	assert.False(t, s3a.isRemoteBacked("mybucket"), "empty index should return false")

	s3a.remoteStorageIdx = populatedIndex(map[string]mountEntry{
		"/buckets/mybucket": stubMount("remote1"),
	})
	assert.True(t, s3a.isRemoteBacked("mybucket"))
	assert.False(t, s3a.isRemoteBacked("otherbucket"))
}
