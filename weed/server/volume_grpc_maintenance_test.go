package weed_server

import (
	"context"
	"os"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMaintenanceModeServer builds a volume server whose store holds volume vid
// in collection and has maintenance mode switched on.
func newMaintenanceModeServer(t *testing.T, vid needle.VolumeId, collection string) (*VolumeServer, string) {
	t.Helper()
	dir := t.TempDir()
	store := newTraversalTestStore(dir)
	t.Cleanup(store.Close)
	require.NoError(t, store.AddVolume(vid, collection, storage.NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))
	require.NoError(t, store.State.Update(&volume_server_pb.VolumeServerState{Maintenance: true}))

	vs := &VolumeServer{
		store:          store,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	require.True(t, vs.MaintenanceMode())
	return vs, dir
}

type fakeReadonlyAcceptingMaster struct {
	master_pb.UnimplementedSeaweedServer
}

func (s *fakeReadonlyAcceptingMaster) VolumeMarkReadonly(context.Context, *master_pb.VolumeMarkReadonlyRequest) (*master_pb.VolumeMarkReadonlyResponse, error) {
	return &master_pb.VolumeMarkReadonlyResponse{}, nil
}

// Evacuating a server in maintenance mode marks each volume readonly on the
// source, copies it, then deletes the source (issue #11066). Those source-side
// RPCs remove data or restrict the server further, so maintenance mode must
// let them through — otherwise the mode defeats the evacuation it exists for.
func TestMaintenanceModeAllowsVolumeMarkReadonly(t *testing.T) {
	vid := needle.VolumeId(1)
	vs, _ := newMaintenanceModeServer(t, vid, "")
	vs.setCurrentMaster(startFakeMasterServerForLeaderLookup(t, &fakeReadonlyAcceptingMaster{}))

	_, err := vs.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: uint32(vid)})
	require.NoError(t, err)
	assert.True(t, vs.store.GetVolume(vid).IsReadOnly())
}

func TestMaintenanceModeAllowsVolumeDelete(t *testing.T) {
	vid := needle.VolumeId(2)
	vs, _ := newMaintenanceModeServer(t, vid, "")

	_, err := vs.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{VolumeId: uint32(vid)})
	require.NoError(t, err)
	assert.Nil(t, vs.store.GetVolume(vid), "volume should be gone from the store")
}

func TestMaintenanceModeAllowsVolumeEcShardsDelete(t *testing.T) {
	const collection = "ec-maint"
	vid := needle.VolumeId(3)
	vs, dir := newMaintenanceModeServer(t, needle.VolumeId(99), "")

	base := erasure_coding.EcShardFileName(collection, dir, int(vid))
	require.NoError(t, os.WriteFile(base+".ecx", make([]byte, 16), 0o644))
	for _, id := range []int{0, 1} {
		require.NoError(t, os.WriteFile(base+erasure_coding.ToExt(id), []byte("s"), 0o644))
	}

	_, err := vs.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
		VolumeId:   uint32(vid),
		Collection: collection,
		ShardIds:   []uint32{0, 1},
	})
	require.NoError(t, err)
	assert.False(t, util.FileExists(base+erasure_coding.ToExt(0)))
	assert.False(t, util.FileExists(base+erasure_coding.ToExt(1)))
}

// Maintenance mode keeps rejecting RPCs that add data to the server or reopen
// it for writes; only the removal/restriction path above is exempt.
func TestMaintenanceModeStillBlocksWrites(t *testing.T) {
	vid := needle.VolumeId(4)
	vs, _ := newMaintenanceModeServer(t, vid, "")
	wantErr := vs.CheckMaintenanceMode().Error()
	ctx := context.Background()

	_, err := vs.AllocateVolume(ctx, &volume_server_pb.AllocateVolumeRequest{VolumeId: 5, Replication: "000", DiskType: string(types.HardDriveType)})
	assert.EqualError(t, err, wantErr, "AllocateVolume")

	_, err = vs.WriteNeedleBlob(ctx, &volume_server_pb.WriteNeedleBlobRequest{VolumeId: uint32(vid), NeedleId: 1, Size: 1, NeedleBlob: []byte{0}})
	assert.EqualError(t, err, wantErr, "WriteNeedleBlob")

	_, err = vs.BatchDelete(ctx, &volume_server_pb.BatchDeleteRequest{FileIds: []string{"4,01637037d6"}})
	assert.EqualError(t, err, wantErr, "BatchDelete")

	_, err = vs.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{VolumeId: uint32(vid)})
	assert.EqualError(t, err, wantErr, "VolumeMarkWritable")
}
