package weed_server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestVolumeMountSucceedsWhileHoldingTheClaim(t *testing.T) {
	dir := t.TempDir()
	volume, err := storage.NewVolume(dir, dir, "", 17, storage.NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	require.NoError(t, err)
	volume.Close()

	vs := &VolumeServer{store: newTraversalTestStore(dir)}
	vs.store.SetVolumeLoadGuard(vs.volumeLoadGuard)

	_, err = vs.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{VolumeId: 17})
	assert.NoError(t, err)
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
