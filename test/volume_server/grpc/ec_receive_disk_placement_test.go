package volume_server_grpc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/volume_server/framework"
	"github.com/seaweedfs/seaweedfs/test/volume_server/matrix"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// TestReceiveFileEcShardAlwaysPlacedOnFirstDisk reproduces the disk placement
// bug described in https://github.com/seaweedfs/seaweedfs/issues/9184: when the
// plugin-worker EC task sends shards via ReceiveFile, the server-side handler
// at volume_grpc_copy.go:ReceiveFile picks Locations[0] as the target directory
// for every EC shard regardless of DiskID. ReceiveFileInfo has no disk_id
// field, so the admin-planned placement cannot be honored.
//
// This test asserts the current (buggy) behavior: across multiple data dirs,
// every shard lands in the first one. When the fix for #9184 adds disk_id to
// ReceiveFileInfo and the handler honors it, this test will need to be
// updated to assert the planned disk is respected instead.
func TestReceiveFileEcShardAlwaysPlacedOnFirstDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const dataDirCount = 3
	clusterHarness := framework.StartSingleVolumeClusterWithDataDirs(t, matrix.P1(), dataDirCount)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	dataDirs := clusterHarness.VolumeDataDirs()
	if len(dataDirs) != dataDirCount {
		t.Fatalf("expected %d data dirs, got %d: %v", dataDirCount, len(dataDirs), dataDirs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const volumeID = uint32(9184)
	const collection = "ec-disk-placement"

	// Send three EC shard files to the server. In production the admin planner
	// would assign each shard to a distinct target disk; the plugin-worker
	// path has no way to communicate that intent, so all three should land on
	// the same disk — dir[0].
	shardIDs := []uint32{0, 5, 13}
	shardExt := func(id uint32) string { return fmt.Sprintf(".ec%02d", id) }

	for _, shardID := range shardIDs {
		payload := []byte(fmt.Sprintf("ec-shard-payload-%02d", shardID))
		stream, err := grpcClient.ReceiveFile(ctx)
		if err != nil {
			t.Fatalf("ReceiveFile stream create for shard %d: %v", shardID, err)
		}
		if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
			Data: &volume_server_pb.ReceiveFileRequest_Info{
				Info: &volume_server_pb.ReceiveFileInfo{
					VolumeId:   volumeID,
					Ext:        shardExt(shardID),
					Collection: collection,
					IsEcVolume: true,
					ShardId:    shardID,
					FileSize:   uint64(len(payload)),
				},
			},
		}); err != nil {
			t.Fatalf("ReceiveFile send info for shard %d: %v", shardID, err)
		}
		if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
			Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: payload},
		}); err != nil {
			t.Fatalf("ReceiveFile send content for shard %d: %v", shardID, err)
		}
		resp, err := stream.CloseAndRecv()
		if err != nil {
			t.Fatalf("ReceiveFile close for shard %d: %v", shardID, err)
		}
		if resp.GetError() != "" {
			t.Fatalf("ReceiveFile shard %d error response: %s", shardID, resp.GetError())
		}
		if resp.GetBytesWritten() != uint64(len(payload)) {
			t.Fatalf("ReceiveFile shard %d bytes_written mismatch: got %d want %d",
				shardID, resp.GetBytesWritten(), len(payload))
		}
	}

	// Scan every data dir and record which shard files live where.
	perDirShards := make(map[int][]uint32)
	for dirIdx, dir := range dataDirs {
		for _, shardID := range shardIDs {
			shardPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", collection, volumeID, shardExt(shardID)))
			if _, err := os.Stat(shardPath); err == nil {
				perDirShards[dirIdx] = append(perDirShards[dirIdx], shardID)
			} else if !os.IsNotExist(err) {
				t.Fatalf("unexpected stat error for %s: %v", shardPath, err)
			}
		}
	}

	// BUG #9184: every shard should land on dir[0] because ReceiveFile ignores
	// disk placement and unconditionally picks Locations[0]. When the fix
	// lands, update this assertion to reflect the spread across disks that a
	// disk_id-aware handler should produce.
	if got := perDirShards[0]; len(got) != len(shardIDs) {
		t.Fatalf("bug #9184 regression: expected all %d shards on dir[0], got %v (full layout: %v)",
			len(shardIDs), got, perDirShards)
	}
	for dirIdx := 1; dirIdx < dataDirCount; dirIdx++ {
		if got := perDirShards[dirIdx]; len(got) != 0 {
			t.Fatalf("bug #9184 regression: expected no shards on dir[%d], got %v (full layout: %v)",
				dirIdx, got, perDirShards)
		}
	}

	t.Logf("bug #9184 reproduced: all EC shards placed on dir[0]=%s; dirs 1..%d empty",
		dataDirs[0], dataDirCount-1)
}
