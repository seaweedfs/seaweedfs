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

// End-to-end on two servers: a volume with a real replica on server A and a
// 0-byte stub replica of the same id on server B (an interrupted-encode
// leftover). After a full EC encode the cluster must end in exactly one valid
// layout — the complete shard set split across A and B, each with .ecx/.vif —
// and every wrong file must be gone: both regular .dat files (source deleted
// after verify, stub swept before distribute), no shard on the wrong server,
// and B's shared <collection>_<vid>.vif intact rather than clobbered by the
// stub's delete.
func TestEcEncodeLeavesRightFilesAndRemovesStubAndSource(t *testing.T) {
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
	aShards := shardRange(0, dataShards)            // 0..DataShardsCount-1 on A
	bShards := shardRange(dataShards, totalShards)  // parity range on B

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

	require.NoError(t, task.Execute(ctx, params))

	dirA := filepath.Join(cluster.BaseDir(), "volume0")
	dirB := filepath.Join(cluster.BaseDir(), "volume1")
	base := fmt.Sprintf("%s_%d", collection, volumeID)

	// Both original regular volumes are gone: A's source deleted after verify,
	// B's stub swept before distribute.
	requireAbsent(t, dirA, base+".dat")
	requireAbsent(t, dirA, base+".idx")
	requireAbsent(t, dirB, base+".dat")
	requireAbsent(t, dirB, base+".idx")

	// Each server holds exactly its assigned shards, plus index/info sidecars.
	for _, id := range aShards {
		requirePresent(t, dirA, fmt.Sprintf("%s.ec%02d", base, id))
	}
	for _, id := range bShards {
		requireAbsent(t, dirA, fmt.Sprintf("%s.ec%02d", base, id))
	}
	requirePresent(t, dirA, base+".ecx")
	requirePresent(t, dirA, base+".vif")

	for _, id := range bShards {
		requirePresent(t, dirB, fmt.Sprintf("%s.ec%02d", base, id))
	}
	for _, id := range aShards {
		requireAbsent(t, dirB, fmt.Sprintf("%s.ec%02d", base, id))
	}
	requirePresent(t, dirB, base+".ecx")
	// The shared .vif must survive on B: the stub was deleted before the EC
	// files landed, so deleteOriginalVolume never ran removeVolumeFiles there.
	requirePresent(t, dirB, base+".vif")
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
