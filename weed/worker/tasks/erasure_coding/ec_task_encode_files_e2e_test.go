package erasure_coding

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// End-to-end on two servers: a real replica on A and a 0-byte stub on B
// (interrupted-encode leftover). Empty live-idx vs non-empty must refuse
// encode; originals stay, t.server is not rebound to the stub.
func TestEcEncodeRefusesOnEmptyStubReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMultiVolumeCluster(t, matrix.P1(), 2)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	const (
		volumeID   = uint32(9490)
		collection = "ec-e2e"
	)
	addrA := serverAddress(cluster, 0)
	addrB := serverAddress(cluster, 1)

	connA, clientA := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer connA.Close()
	connB, clientB := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer connB.Close()

	// Server A: real source replica with data.
	framework.AllocateVolume(t, clientA, volumeID, collection)
	httpClient := framework.NewHTTPClient()
	for i := 0; i < 8; i++ {
		fid := framework.NewFileID(volumeID, uint64(948000+i), 0x9490CA00+uint32(i))
		payload := make([]byte, 4096)
		for j := range payload {
			payload[j] = byte(i + 1)
		}
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fid, payload)
		_ = framework.ReadAllAndClose(t, resp)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}

	// Server B: empty stub replica of the same volume id.
	framework.AllocateVolume(t, clientB, volumeID, collection)

	dataShards := int(erasure_coding.DataShardsCount)
	totalShards := int(erasure_coding.DataShardsCount + erasure_coding.ParityShardsCount)
	aShards := shardRange(0, dataShards)           // 0..DataShardsCount-1 on A
	bShards := shardRange(dataShards, totalShards) // parity range on B

	task := NewErasureCodingTask("ec-e2e", addrA, volumeID, collection, dialOption)
	params := &worker_pb.TaskParams{
		VolumeId:   volumeID,
		Collection: collection,
		Sources: []*worker_pb.TaskSource{
			{Node: addrA, VolumeId: volumeID},
			{Node: addrB, VolumeId: volumeID},
		},
		Targets: []*worker_pb.TaskTarget{
			{Node: addrA, ShardIds: aShards},
			{Node: addrB, ShardIds: bShards},
		},
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:   erasure_coding.DataShardsCount,
				ParityShards: erasure_coding.ParityShardsCount,
				WorkingDir:   t.TempDir(),
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	err := task.Execute(ctx, params)
	require.Error(t, err, "empty live-idx vs non-empty must refuse encode")
	require.Contains(t, err.Error(), "no union")
	require.NotEqual(t, addrB, task.server, "after gate t.server is not a stub URL")

	dirA := filepath.Join(cluster.BaseDir(), "volume0")
	dirB := filepath.Join(cluster.BaseDir(), "volume1")
	base := fmt.Sprintf("%s_%d", collection, volumeID)
	requirePresent(t, dirA, base+".dat")
	requirePresent(t, dirB, base+".dat")
}

// serverAddress builds the SeaweedFS ip:httpPort.grpcPort address the worker's
// gRPC client decodes, from the multi-cluster's separate admin and grpc ports.
func serverAddress(c *framework.MultiVolumeCluster, index int) string {
	_, grpcPort, err := net.SplitHostPort(c.VolumeGRPCAddress(index))
	if err != nil {
		panic(err)
	}
	return c.VolumeAdminAddress(index) + "." + grpcPort
}

func shardRange(start, end int) []uint32 {
	ids := make([]uint32, 0, end-start)
	for i := start; i < end; i++ {
		ids = append(ids, uint32(i))
	}
	return ids
}

func requirePresent(t *testing.T, dir, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	require.NoError(t, err, "expected %s to be present in %s", name, dir)
}

func requireAbsent(t *testing.T, dir, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, name))
	require.True(t, os.IsNotExist(err), "expected %s to be absent in %s, stat err=%v", name, dir, err)
}
