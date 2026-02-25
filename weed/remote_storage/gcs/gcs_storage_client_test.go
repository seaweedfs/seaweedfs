package gcs

import (
	"testing"

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
