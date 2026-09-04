package weed_server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeDeleteStatusErrorDistinguishesAbsentFromTransportFailure(t *testing.T) {
	notFound := volumeDeleteStatusError(fmt.Errorf("delete volume 17 not found on disk: %w", storage.ErrVolumeNotFound))
	assert.Equal(t, codes.NotFound, status.Code(notFound))
	assert.Contains(t, notFound.Error(), "not found", "the store message must survive for callers that match on it")

	transport := errors.New("connection reset")
	require.ErrorIs(t, volumeDeleteStatusError(transport), transport)
	assert.NotEqual(t, codes.NotFound, status.Code(transport))

	notEmpty := volumeDeleteStatusError(storage.ErrVolumeNotEmpty)
	assert.Equal(t, codes.FailedPrecondition, status.Code(notEmpty))
	assert.Contains(t, notEmpty.Error(), "volume not empty")
}

func TestVolumeDeleteMapsAbsentStoreVolumeToNotFound(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}

	_, err := vs.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{VolumeId: 17})
	assert.Equal(t, codes.NotFound, status.Code(err), err)
}
