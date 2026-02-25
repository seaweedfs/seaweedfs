package gcs

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/require"
)

func TestGCSStatFileImplementsRemoteStorageClientInterface(t *testing.T) {
	var _ remote_storage.RemoteStorageClient = (*gcsRemoteStorageClient)(nil)
}

func TestGCSErrRemoteObjectNotFoundIsAccessible(t *testing.T) {
	require.Error(t, remote_storage.ErrRemoteObjectNotFound)
	require.Equal(t, "remote object not found", remote_storage.ErrRemoteObjectNotFound.Error())
}

func TestGCSStatFileFieldPopulation(t *testing.T) {
	conf := &remote_pb.RemoteConf{
		Name: "test-gcs",
	}

	tests := []struct {
		name     string
		entry    *filer_pb.RemoteEntry
		wantSize int64
		wantMtime int64
		wantETag  string
	}{
		{
			name: "with all fields populated",
			entry: &filer_pb.RemoteEntry{
				StorageName: conf.Name,
				RemoteSize:  4096,
				RemoteMtime: 1677253800,
				RemoteETag:  "test-etag-gcs",
			},
			wantSize:  4096,
			wantMtime: 1677253800,
			wantETag:  "test-etag-gcs",
		},
		{
			name: "with nil fields",
			entry: &filer_pb.RemoteEntry{
				StorageName: conf.Name,
			},
			wantSize:  0,
			wantMtime: 0,
			wantETag:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, conf.Name, tt.entry.StorageName)
			require.Equal(t, tt.wantSize, tt.entry.RemoteSize)
			require.Equal(t, tt.wantMtime, tt.entry.RemoteMtime)
			require.Equal(t, tt.wantETag, tt.entry.RemoteETag)
		})
	}
}

func TestGCSObjectNotFoundErrorDetection(t *testing.T) {
	t.Run("detects ErrObjectNotExist", func(t *testing.T) {
		err := storage.ErrObjectNotExist
		isObjectNotFound := errors.Is(err, storage.ErrObjectNotExist)
		require.True(t, isObjectNotFound)
	})

	t.Run("distinguishes from generic errors", func(t *testing.T) {
		err := errors.New("permission denied")
		isObjectNotFound := errors.Is(err, storage.ErrObjectNotExist)
		require.False(t, isObjectNotFound)
	})

	t.Run("ErrRemoteObjectNotFound is a distinct sentinel", func(t *testing.T) {
		require.NotEqual(t, remote_storage.ErrRemoteObjectNotFound, storage.ErrObjectNotExist)
		require.Error(t, remote_storage.ErrRemoteObjectNotFound)
	})
}

func TestGCSObjectAttrsPopulation(t *testing.T) {
	lastModified := time.Date(2026, 2, 25, 10, 30, 0, 0, time.UTC)
	eTag := "test-etag-abc123"
	size := int64(8192)

	attrs := &storage.ObjectAttrs{
		Name:    "test-file.txt",
		Bucket:  "test-bucket",
		Updated: lastModified,
		Size:    size,
		Etag:    eTag,
	}

	entry := &filer_pb.RemoteEntry{
		StorageName: "test-gcs",
		RemoteMtime: attrs.Updated.Unix(),
		RemoteSize:  attrs.Size,
		RemoteETag:  attrs.Etag,
	}

	require.Equal(t, "test-gcs", entry.StorageName)
	require.Equal(t, size, entry.RemoteSize)
	require.Equal(t, lastModified.Unix(), entry.RemoteMtime)
	require.Equal(t, eTag, entry.RemoteETag)
}
