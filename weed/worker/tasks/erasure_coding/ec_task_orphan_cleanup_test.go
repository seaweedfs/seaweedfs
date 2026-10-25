package erasure_coding

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cleanupOrphanSourceReplicas must not delete a source replica that has come
// back writable: it may hold writes the EC shards do not contain. A still
// readonly replica is the genuine orphan and is deleted.
func TestCleanupOrphanSkipsWritableSourceReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(955)
	framework.AllocateVolume(t, grpcClient, volumeID, "")

	clusterInfo := &types.ClusterInfo{
		ActiveTopology: buildSingleNodeTopology(t, clusterHarness.VolumeServerAddress(), volumeID),
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	metric := &types.VolumeHealthMetrics{VolumeID: volumeID, Collection: ""}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Writable source replica: must be left intact.
	deleted, err := cleanupOrphanSourceReplicas(ctx, clusterInfo, metric, 0)
	require.NoError(t, err)
	require.Equal(t, 0, deleted, "a writable source replica must not be deleted")
	require.True(t, volumeExists(t, grpcClient, volumeID), "writable source volume must still exist")

	// Mark readonly: now it is a genuine orphan and must be deleted.
	_, err = grpcClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: volumeID})
	require.NoError(t, err)

	deleted, err = cleanupOrphanSourceReplicas(ctx, clusterInfo, metric, 0)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "a readonly orphan source replica must be deleted")
	require.False(t, volumeExists(t, grpcClient, volumeID), "readonly orphan source volume must be deleted")
}

func volumeExists(t *testing.T, client volume_server_pb.VolumeServerClient, volumeID uint32) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.VolumeStatus(ctx, &volume_server_pb.VolumeStatusRequest{VolumeId: volumeID})
	return err == nil
}

// buildSingleNodeTopology builds an ActiveTopology with one node (at serverAddr)
// holding the regular volume, so cleanupOrphanSourceReplicas resolves the
// replica to the live test server and its gRPC calls reach it.
func buildSingleNodeTopology(t *testing.T, serverAddr string, volumeID uint32) *topology.ActiveTopology {
	t.Helper()
	at := topology.NewActiveTopology(10)
	topologyInfo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id: serverAddr,
					DiskInfos: map[string]*master_pb.DiskInfo{
						"hdd": {
							DiskId: 0,
							VolumeInfos: []*master_pb.VolumeInformationMessage{
								{Id: volumeID, Collection: "", DiskId: 0},
							},
						},
					},
				}},
			}},
		}},
	}
	require.NoError(t, at.UpdateTopology(topologyInfo))
	return at
}
