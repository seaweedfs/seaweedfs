package erasure_coding

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestReplicasPendingDelete(t *testing.T) {
	replicas := []string{"a:8080", "b:8080", "c:8080"}

	require.Equal(t, replicas, replicasPendingDelete(replicas, nil),
		"nil swept set leaves the list unchanged")
	require.ElementsMatch(t, []string{"a:8080", "c:8080"},
		replicasPendingDelete(replicas, map[string]bool{"b:8080": true}),
		"swept node is excluded")
	require.Empty(t,
		replicasPendingDelete(replicas, map[string]bool{"a:8080": true, "b:8080": true, "c:8080": true}),
		"all swept yields nothing left to delete")
}

// The pre-distribute sweep must delete a 0-byte stub replica (so its shared
// <collection>_<vid>.vif can't be clobbered by a later original-delete and
// can't shadow the incoming EC files) while leaving a data-bearing replica
// untouched — VolumeDelete(OnlyEmpty=true) is the guard. A data-bearing
// replica is deleted later, only after the EC shard set is verified.
func TestSweepEmptyReplicasDeletesStubKeepsData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clusterHarness := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	server := clusterHarness.VolumeServerAddress()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// An empty allocated volume is a superblock-only stub.
	const stubVol = uint32(9479)
	framework.AllocateVolume(t, grpcClient, stubVol, "ec-stub")

	stubTask := newSweepTaskForTest(server, stubVol, "ec-stub", dialOption)
	stubTask.sweepEmptyReplicas(ctx)

	require.True(t, stubTask.emptyReplicasDeleted[server], "stub server should be recorded as swept")
	_, err := grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: stubVol})
	require.Error(t, err, "empty stub volume must be deleted by the sweep")

	// A volume holding data must survive the sweep.
	const dataVol = uint32(9480)
	framework.AllocateVolume(t, grpcClient, dataVol, "ec-data")
	httpClient := framework.NewHTTPClient()
	fid := framework.NewFileID(dataVol, 948000, 0x9480CAFE)
	upResp := framework.UploadBytes(t, httpClient, clusterHarness.VolumeAdminURL(), fid, []byte("real-data-keep-me"))
	_ = framework.ReadAllAndClose(t, upResp)
	require.Equal(t, http.StatusCreated, upResp.StatusCode)

	dataTask := newSweepTaskForTest(server, dataVol, "ec-data", dialOption)
	dataTask.sweepEmptyReplicas(ctx)

	require.False(t, dataTask.emptyReplicasDeleted[server], "data-bearing server must not be recorded as swept")
	_, err = grpcClient.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{VolumeId: dataVol})
	require.NoError(t, err, "data-bearing volume must survive the sweep")
}

func newSweepTaskForTest(server string, volumeID uint32, collection string, dialOption grpc.DialOption) *ErasureCodingTask {
	task := NewErasureCodingTask("sweep-"+collection, server, volumeID, collection, dialOption)
	task.sources = []*worker_pb.TaskSource{{Node: server, VolumeId: volumeID}}
	return task
}
