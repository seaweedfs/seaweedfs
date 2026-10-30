package weed_server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/stretchr/testify/assert"
)

func TestVolumeMountRefusesAVolumeBeingCopied(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}
	vs.volumeCopiesInFlight.Store(uint32(17), struct{}{})

	_, err := vs.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{VolumeId: 17})
	assert.Equal(t, codes.Unavailable, status.Code(err), err)

	vs.volumeCopiesInFlight.Delete(uint32(17))
	_, err = vs.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{VolumeId: 17})
	assert.NotEqual(t, codes.Unavailable, status.Code(err), err)
}

func TestVolumeConfigureRefusesAVolumeBeingCopied(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}
	vs.volumeCopiesInFlight.Store(uint32(17), struct{}{})

	_, err := vs.VolumeConfigure(context.Background(), &volume_server_pb.VolumeConfigureRequest{VolumeId: 17, Replication: "000"})
	assert.Equal(t, codes.Unavailable, status.Code(err), err)
}

func TestAllocateVolumeRefusesAVolumeBeingCopied(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}
	vs.volumeCopiesInFlight.Store(uint32(17), struct{}{})

	_, err := vs.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{VolumeId: 17})
	assert.Equal(t, codes.Unavailable, status.Code(err), err)
}

func TestAdoptStagedVolumeRefusesAVolumeBeingCopied(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}
	vs.volumeCopiesInFlight.Store(uint32(17), struct{}{})

	_, err := vs.adoptStagedVolume(&volume_server_pb.VolumeEcShardsToVolumeRequest{VolumeId: 17})
	assert.Equal(t, codes.Unavailable, status.Code(err), err)
}

func TestVolumeLoadGuardSkipsACopyInFlight(t *testing.T) {
	vs := &VolumeServer{store: newTraversalTestStore(t.TempDir())}
	vs.volumeCopiesInFlight.Store(uint32(17), struct{}{})

	release, skip := vs.volumeLoadGuard(17)
	release()
	assert.True(t, skip)

	vs.volumeCopiesInFlight.Delete(uint32(17))
	release, skip = vs.volumeLoadGuard(17)
	assert.False(t, skip)
	release()
}
