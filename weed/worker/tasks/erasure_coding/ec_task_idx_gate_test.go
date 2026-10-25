package erasure_coding

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_replica"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestEcEncodeAbortsOnEqualFileCountDifferentIds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster, task, params, ctx := twoReplicaTask(t, 9610, "ec-idx-mismatch")
	httpClient := framework.NewHTTPClient()
	const volumeID = uint32(9610)

	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(0), volumeID, []uint64{1, 2, 3})
	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(1), volumeID, []uint64{1, 2, 4})

	require.NoError(t, task.markReplicasReadonly(ctx))
	err := task.syncAndSelectSourceReplica()
	require.Error(t, err, "equal FileCount with different ids must abort")
	require.Contains(t, err.Error(), "no union")

	execErr := task.Execute(ctx, params)
	require.Error(t, execErr)
	require.Contains(t, execErr.Error(), "no union")
	require.NotContains(t, execErr.Error(), "building union")

	dirA := cluster.VolumeDiskDir(0, 0)
	dirB := cluster.VolumeDiskDir(1, 0)
	base := fmt.Sprintf("%s_%d", "ec-idx-mismatch", volumeID)
	requirePresent(t, dirA, base+".dat")
	requirePresent(t, dirB, base+".dat")
	require.True(t, idxHasNeedle(t, filepath.Join(dirA, base+".idx"), 3))
	require.False(t, idxHasNeedle(t, filepath.Join(dirA, base+".idx"), 4))
	require.True(t, idxHasNeedle(t, filepath.Join(dirB, base+".idx"), 4))
	require.False(t, idxHasNeedle(t, filepath.Join(dirB, base+".idx"), 3))
}

func TestEcEncodeAllowsVacuumOffsetMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster, task, params, ctx := twoReplicaTask(t, 9611, "ec-idx-vacuum")
	httpClient := framework.NewHTTPClient()
	const volumeID = uint32(9611)

	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(0), volumeID, []uint64{10, 11, 12})
	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(1), volumeID, []uint64{10, 11, 12})

	// Delete the first needle so vacuum packs 11/12 to new offsets on A only.
	deletedFID := framework.NewFileID(volumeID, 10, cookieFor(10))
	for _, url := range []string{cluster.VolumeAdminURL(0), cluster.VolumeAdminURL(1)} {
		req, err := http.NewRequest(http.MethodDelete, url+"/"+deletedFID, nil)
		require.NoError(t, err)
		resp := framework.DoRequest(t, httpClient, req)
		_ = framework.ReadAllAndClose(t, resp)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
	}

	connA, clientA := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer connA.Close()
	compactVolumeOnce(t, clientA, volumeID)

	offA := needleOffset(t, filepath.Join(cluster.VolumeDiskDir(0, 0), "ec-idx-vacuum_9611.idx"), 11)
	offB := needleOffset(t, filepath.Join(cluster.VolumeDiskDir(1, 0), "ec-idx-vacuum_9611.idx"), 11)
	require.NotEqual(t, offA, offB, "vacuum should rewrite offsets on one replica")

	require.NoError(t, task.Execute(ctx, params))
}

func TestEcEncodeAbortsWhenIdxRpcFails(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster, task, _, ctx := twoReplicaTask(t, 9612, "ec-idx-rpc")
	httpClient := framework.NewHTTPClient()
	const volumeID = uint32(9612)
	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(0), volumeID, []uint64{1})
	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(1), volumeID, []uint64{1})

	require.NoError(t, task.markReplicasReadonly(ctx))

	locs := []wdclient.Location{
		{Url: serverAddress(cluster, 0)},
		{Url: "127.0.0.1:1"},
	}
	_, err := volume_replica.SelectSourceIfIndexesMatch(task.grpcDialOption, needle.VolumeId(volumeID), "ec-idx-rpc", locs, "", &bytes.Buffer{})
	require.Error(t, err)
}

func TestEcEncodeSingleReplicaAfterReadonly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(9613)
	framework.AllocateVolume(t, grpcClient, volumeID, "ec-idx-one")
	httpClient := framework.NewHTTPClient()
	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(), volumeID, []uint64{7, 8})

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	task := NewErasureCodingTask("ec-idx-one", cluster.VolumeServerAddress(), volumeID, "ec-idx-one", dialOption)
	params := singleReplicaParams(cluster.VolumeServerAddress(), volumeID, "ec-idx-one", t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	require.NoError(t, task.markReplicasReadonly(ctx))
	require.NoError(t, task.syncAndSelectSourceReplica())
	require.NoError(t, task.Execute(ctx, params))
}

func TestHttpWriteAfterReadonlyIsNotCreated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, grpcClient := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const volumeID = uint32(9614)
	framework.AllocateVolume(t, grpcClient, volumeID, "")
	httpClient := framework.NewHTTPClient()
	uploadNeedles(t, httpClient, cluster.VolumeAdminURL(), volumeID, []uint64{1})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := grpcClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: volumeID, Persist: true})
	require.NoError(t, err)

	fid := framework.NewFileID(volumeID, 99, cookieFor(99))
	resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), fid, []byte("must-not-land"))
	_ = framework.ReadAllAndClose(t, resp)
	require.NotEqual(t, http.StatusCreated, resp.StatusCode)
	require.NotEqual(t, http.StatusNoContent, resp.StatusCode)

	idxPath := filepath.Join(cluster.BaseDir(), "volume", fmt.Sprintf("%d.idx", volumeID))
	require.False(t, idxHasNeedle(t, idxPath, 99))
}

func twoReplicaTask(t *testing.T, volumeID uint32, collection string) (*framework.MultiVolumeCluster, *ErasureCodingTask, *worker_pb.TaskParams, context.Context) {
	t.Helper()
	cluster := framework.StartMultiVolumeCluster(t, matrix.P1(), 2)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	addrA := serverAddress(cluster, 0)
	addrB := serverAddress(cluster, 1)

	connA, clientA := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	t.Cleanup(func() { connA.Close() })
	connB, clientB := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	t.Cleanup(func() { connB.Close() })
	framework.AllocateVolume(t, clientA, volumeID, collection)
	framework.AllocateVolume(t, clientB, volumeID, collection)

	dataShards := int(erasure_coding.DataShardsCount)
	totalShards := int(erasure_coding.DataShardsCount + erasure_coding.ParityShardsCount)
	task := NewErasureCodingTask("ec-idx-gate", addrA, volumeID, collection, dialOption)
	params := &worker_pb.TaskParams{
		VolumeId:   volumeID,
		Collection: collection,
		Sources: []*worker_pb.TaskSource{
			{Node: addrA, VolumeId: volumeID},
			{Node: addrB, VolumeId: volumeID},
		},
		Targets: []*worker_pb.TaskTarget{
			{Node: addrA, ShardIds: shardRange(0, dataShards)},
			{Node: addrB, ShardIds: shardRange(dataShards, totalShards)},
		},
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:   erasure_coding.DataShardsCount,
				ParityShards: erasure_coding.ParityShardsCount,
				WorkingDir:   t.TempDir(),
			},
		},
	}
	task.sources = params.Sources
	task.targets = params.Targets
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	return cluster, task, params, ctx
}

func singleReplicaParams(node string, volumeID uint32, collection, workDir string) *worker_pb.TaskParams {
	totalShards := int(erasure_coding.DataShardsCount + erasure_coding.ParityShardsCount)
	return &worker_pb.TaskParams{
		VolumeId:   volumeID,
		Collection: collection,
		Sources:    []*worker_pb.TaskSource{{Node: node, VolumeId: volumeID}},
		Targets:    []*worker_pb.TaskTarget{{Node: node, ShardIds: shardRange(0, totalShards)}},
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DataShards:   erasure_coding.DataShardsCount,
				ParityShards: erasure_coding.ParityShardsCount,
				WorkingDir:   workDir,
			},
		},
	}
}

func cookieFor(id uint64) uint32 {
	return 0xEC100000 + uint32(id)
}

func uploadNeedles(t *testing.T, client *http.Client, volumeURL string, volumeID uint32, ids []uint64) {
	t.Helper()
	for _, id := range ids {
		fid := framework.NewFileID(volumeID, id, cookieFor(id))
		resp := framework.UploadBytes(t, client, volumeURL, fid, []byte(fmt.Sprintf("payload-%d", id)))
		_ = framework.ReadAllAndClose(t, resp)
		require.Equal(t, http.StatusCreated, resp.StatusCode)
	}
}

func idxHasNeedle(t *testing.T, idxPath string, id uint64) bool {
	t.Helper()
	f, err := os.Open(idxPath)
	require.NoError(t, err)
	defer f.Close()
	want := types.NeedleId(id)
	found := false
	err = idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if key == want && !offset.IsZero() && !size.IsDeleted() {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	return found
}

func needleOffset(t *testing.T, idxPath string, id uint64) types.Offset {
	t.Helper()
	f, err := os.Open(idxPath)
	require.NoError(t, err)
	defer f.Close()
	want := types.NeedleId(id)
	var off types.Offset
	var seen bool
	err = idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		if key == want && !offset.IsZero() && !size.IsDeleted() {
			off = offset
			seen = true
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, seen, "needle %d missing in %s", id, idxPath)
	return off
}
