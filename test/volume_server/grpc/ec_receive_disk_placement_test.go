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

// Sends EC shards with disk_id={1, 2, 0} and verifies each lands on the
// requested disk (0 → auto-select → disk 0 for a fresh volume).
func TestReceiveFileEcShardHonorsDiskID(t *testing.T) {
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

	shards := []struct {
		shardID        uint32
		requestedDisk  uint32
		expectedDirIdx int
	}{
		{shardID: 0, requestedDisk: 1, expectedDirIdx: 1},
		{shardID: 5, requestedDisk: 2, expectedDirIdx: 2},
		{shardID: 13, requestedDisk: 0, expectedDirIdx: 0},
	}
	shardExt := func(id uint32) string { return fmt.Sprintf(".ec%02d", id) }

	for _, s := range shards {
		payload := []byte(fmt.Sprintf("ec-shard-payload-%02d", s.shardID))
		stream, err := grpcClient.ReceiveFile(ctx)
		if err != nil {
			t.Fatalf("ReceiveFile stream create for shard %d: %v", s.shardID, err)
		}
		if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
			Data: &volume_server_pb.ReceiveFileRequest_Info{
				Info: &volume_server_pb.ReceiveFileInfo{
					VolumeId:   volumeID,
					Ext:        shardExt(s.shardID),
					Collection: collection,
					IsEcVolume: true,
					ShardId:    s.shardID,
					FileSize:   uint64(len(payload)),
					DiskId:     s.requestedDisk,
				},
			},
		}); err != nil {
			t.Fatalf("ReceiveFile send info for shard %d: %v", s.shardID, err)
		}
		if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
			Data: &volume_server_pb.ReceiveFileRequest_FileContent{FileContent: payload},
		}); err != nil {
			t.Fatalf("ReceiveFile send content for shard %d: %v", s.shardID, err)
		}
		resp, err := stream.CloseAndRecv()
		if err != nil {
			t.Fatalf("ReceiveFile close for shard %d: %v", s.shardID, err)
		}
		if resp.GetError() != "" {
			t.Fatalf("ReceiveFile shard %d error response: %s", s.shardID, resp.GetError())
		}
		if resp.GetBytesWritten() != uint64(len(payload)) {
			t.Fatalf("ReceiveFile shard %d bytes_written mismatch: got %d want %d",
				s.shardID, resp.GetBytesWritten(), len(payload))
		}
	}

	// Scan every data dir and record which shard files live where.
	perDirShards := make(map[int][]uint32)
	for dirIdx, dir := range dataDirs {
		for _, s := range shards {
			shardPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", collection, volumeID, shardExt(s.shardID)))
			if _, err := os.Stat(shardPath); err == nil {
				perDirShards[dirIdx] = append(perDirShards[dirIdx], s.shardID)
			} else if !os.IsNotExist(err) {
				t.Fatalf("unexpected stat error for %s: %v", shardPath, err)
			}
		}
	}

	for _, s := range shards {
		found := false
		for _, got := range perDirShards[s.expectedDirIdx] {
			if got == s.shardID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("shard %d (requestedDisk=%d) expected on dir[%d], full layout: %v",
				s.shardID, s.requestedDisk, s.expectedDirIdx, perDirShards)
		}
		for dirIdx, ids := range perDirShards {
			if dirIdx == s.expectedDirIdx {
				continue
			}
			for _, id := range ids {
				if id == s.shardID {
					t.Fatalf("shard %d leaked onto dir[%d]: full layout: %v", s.shardID, dirIdx, perDirShards)
				}
			}
		}
	}

	t.Logf("disk_id honored: shard placement = %v", perDirShards)
}

func TestReceiveFileEcShardRejectsInvalidDiskID(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const dataDirCount = 2
	clusterHarness := framework.StartSingleVolumeClusterWithDataDirs(t, matrix.P1(), dataDirCount)
	conn, grpcClient := framework.DialVolumeServer(t, clusterHarness.VolumeGRPCAddress())
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const volumeID = uint32(91840)
	const collection = "ec-invalid-disk"
	payload := []byte("invalid-disk-id-payload")

	stream, err := grpcClient.ReceiveFile(ctx)
	if err != nil {
		t.Fatalf("ReceiveFile stream create: %v", err)
	}
	if err = stream.Send(&volume_server_pb.ReceiveFileRequest{
		Data: &volume_server_pb.ReceiveFileRequest_Info{
			Info: &volume_server_pb.ReceiveFileInfo{
				VolumeId:   volumeID,
				Ext:        ".ec00",
				Collection: collection,
				IsEcVolume: true,
				ShardId:    0,
				FileSize:   uint64(len(payload)),
				DiskId:     99,
			},
		},
	}); err != nil {
		t.Fatalf("ReceiveFile send info: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return
	}
	if resp.GetError() == "" {
		t.Fatalf("expected invalid disk_id rejection, got success response: %+v", resp)
	}
}
