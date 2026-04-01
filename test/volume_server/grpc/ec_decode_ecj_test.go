package volume_server_grpc_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestEcDecodePreservesDeletedNeedles verifies that needles deleted via
// VolumeEcBlobDelete (recorded in .ecj) are correctly excluded from the
// decoded volume produced by VolumeEcShardsToVolume.
func TestEcDecodePreservesDeletedNeedles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartVolumeCluster(t, matrix.P1())
	conn, client := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress())
	defer conn.Close()

	const (
		volumeID = uint32(140)
		keyA     = uint64(990020)
		cookieA  = uint32(0xDA001122)
		keyB     = uint64(990021)
		cookieB  = uint32(0xDA003344)
	)

	framework.AllocateVolume(t, client, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fidA := framework.NewFileID(volumeID, keyA, cookieA)
	fidB := framework.NewFileID(volumeID, keyB, cookieB)
	payloadA := []byte("needle-A-should-be-deleted-after-decode")
	payloadB := []byte("needle-B-should-survive-decode")

	// Upload two needles.
	for _, tc := range []struct {
		fid     string
		payload []byte
	}{
		{fidA, payloadA},
		{fidB, payloadB},
	} {
		resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(), tc.fid, tc.payload)
		_ = framework.ReadAllAndClose(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("upload %s: expected 201, got %d", tc.fid, resp.StatusCode)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// EC encode.
	_, err := client.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsGenerate: %v", err)
	}

	// Mount all data shards so the EC volume is usable.
	_, err = client.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: "",
		ShardIds: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount: %v", err)
	}

	// Delete needle A via EC path (writes to .ecj).
	_, err = client.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId: volumeID, Collection: "",
		FileKey: keyA, Version: uint32(needle.GetCurrentVersion()),
	})
	if err != nil {
		t.Fatalf("VolumeEcBlobDelete needle A: %v", err)
	}

	// Unmount the normal volume so decode writes fresh files.
	_, err = client.VolumeUnmount(ctx, &volume_server_pb.VolumeUnmountRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeUnmount: %v", err)
	}

	// Decode EC shards back to a normal volume.
	_, err = client.VolumeEcShardsToVolume(ctx, &volume_server_pb.VolumeEcShardsToVolumeRequest{
		VolumeId: volumeID, Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsToVolume: %v", err)
	}

	// Re-mount the decoded volume.
	_, err = client.VolumeMount(ctx, &volume_server_pb.VolumeMountRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeMount: %v", err)
	}

	// Needle A should be gone (deleted via .ecj before decode).
	respA := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fidA)
	_ = framework.ReadAllAndClose(t, respA)
	if respA.StatusCode == http.StatusOK {
		t.Fatalf("needle A should be deleted after decode, but got 200")
	}

	// Needle B should still be readable.
	respB := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(), fidB)
	bodyB := framework.ReadAllAndClose(t, respB)
	if respB.StatusCode != http.StatusOK {
		t.Fatalf("needle B read: expected 200, got %d", respB.StatusCode)
	}
	if string(bodyB) != string(payloadB) {
		t.Fatalf("needle B payload mismatch: got %q, want %q", bodyB, payloadB)
	}
}

// TestEcDecodeCollectsEcjFromPeer verifies that .ecj deletion entries from a
// peer server that contributes no new data shards are still collected during
// decode. This is the regression test for the fix in collectEcShards that
// always copies .ecj from every shard location.
//
// Scenario:
//   - Server 0 holds all 10 data shards (decode target).
//   - Server 1 holds a copy of shard 0 (no new shards for server 0).
//   - A needle is deleted ONLY on server 1 (server 0's .ecj is empty).
//   - During decode on server 0, server 1's .ecj must be collected and applied.
func TestEcDecodeCollectsEcjFromPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cluster := framework.StartMultiVolumeClusterAuto(t, matrix.P1(), 2)
	conn0, client0 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(0))
	defer conn0.Close()
	conn1, client1 := framework.DialVolumeServer(t, cluster.VolumeGRPCAddress(1))
	defer conn1.Close()

	const (
		volumeID = uint32(141)
		keyA     = uint64(990030)
		cookieA  = uint32(0xDB001122)
		keyB     = uint64(990031)
		cookieB  = uint32(0xDB003344)
	)

	// Allocate and upload on server 0.
	framework.AllocateVolume(t, client0, volumeID, "")

	httpClient := framework.NewHTTPClient()
	fidA := framework.NewFileID(volumeID, keyA, cookieA)
	fidB := framework.NewFileID(volumeID, keyB, cookieB)
	payloadB := []byte("needle-B-should-survive-peer-ecj-decode")

	resp := framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fidA, []byte("needle-A-deleted-on-peer"))
	_ = framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload A: expected 201, got %d", resp.StatusCode)
	}
	resp = framework.UploadBytes(t, httpClient, cluster.VolumeAdminURL(0), fidB, payloadB)
	_ = framework.ReadAllAndClose(t, resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload B: expected 201, got %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// EC encode on server 0.
	_, err := client0.VolumeEcShardsGenerate(ctx, &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId: volumeID, Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsGenerate on server 0: %v", err)
	}

	// Build the SourceDataNode address for server 0 (format: host:adminPort.grpcPort).
	sourceDataNode := cluster.VolumeAdminAddress(0) + "." +
		strings.Split(cluster.VolumeGRPCAddress(0), ":")[1]

	// Copy shard 0 + ecx + ecj from server 0 → server 1.
	_, err = client1.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       volumeID,
		Collection:     "",
		SourceDataNode: sourceDataNode,
		ShardIds:       []uint32{0},
		CopyEcxFile:    true,
		CopyEcjFile:    true,
		CopyVifFile:    true,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsCopy 0→1: %v", err)
	}

	// Mount shard 0 on server 1 so the EC volume can accept deletions.
	_, err = client1.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: "",
		ShardIds: []uint32{0},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount on server 1: %v", err)
	}

	// Delete needle A on server 1 only (creates .ecj entry on server 1).
	_, err = client1.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
		VolumeId: volumeID, Collection: "",
		FileKey: keyA, Version: uint32(needle.GetCurrentVersion()),
	})
	if err != nil {
		t.Fatalf("VolumeEcBlobDelete needle A on server 1: %v", err)
	}

	// Mount all data shards on server 0 (the decode target).
	_, err = client0.VolumeEcShardsMount(ctx, &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId: volumeID, Collection: "",
		ShardIds: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsMount on server 0: %v", err)
	}

	// Collect .ecj from server 1 → server 0 with NO new shard IDs.
	// This is the critical path: server 1 has shard 0 which server 0 already
	// has, so needToCopyShardsInfo would be empty. Before the fix in
	// collectEcShards, this copy would be skipped entirely, losing server 1's
	// deletion entries.
	server1DataNode := cluster.VolumeAdminAddress(1) + "." +
		strings.Split(cluster.VolumeGRPCAddress(1), ":")[1]

	_, err = client0.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
		VolumeId:       volumeID,
		Collection:     "",
		SourceDataNode: server1DataNode,
		ShardIds:       []uint32{}, // No new shards — just .ecj.
		CopyEcxFile:    false,
		CopyEcjFile:    true,
		CopyVifFile:    false,
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsCopy .ecj from server 1→0: %v", err)
	}

	// Unmount the normal volume before decode.
	_, _ = client0.VolumeUnmount(ctx, &volume_server_pb.VolumeUnmountRequest{VolumeId: volumeID})

	// Decode on server 0. RebuildEcxFile should see needle A's deletion from
	// the .ecj that was collected from server 1.
	_, err = client0.VolumeEcShardsToVolume(ctx, &volume_server_pb.VolumeEcShardsToVolumeRequest{
		VolumeId: volumeID, Collection: "",
	})
	if err != nil {
		t.Fatalf("VolumeEcShardsToVolume on server 0: %v", err)
	}

	// Re-mount the decoded normal volume.
	_, err = client0.VolumeMount(ctx, &volume_server_pb.VolumeMountRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("VolumeMount on server 0: %v", err)
	}

	// Needle A should be gone — its deletion was in server 1's .ecj.
	respA := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(0), fidA)
	_ = framework.ReadAllAndClose(t, respA)
	if respA.StatusCode == http.StatusOK {
		t.Fatalf("needle A should be deleted (ecj from peer), but got 200")
	}

	// Needle B should still be readable.
	respB := framework.ReadBytes(t, httpClient, cluster.VolumeAdminURL(0), fidB)
	bodyB := framework.ReadAllAndClose(t, respB)
	if respB.StatusCode != http.StatusOK {
		t.Fatalf("needle B read: expected 200, got %d", respB.StatusCode)
	}
	if string(bodyB) != string(payloadB) {
		t.Fatalf("needle B payload mismatch: got %q, want %q", bodyB, payloadB)
	}
}
