package volume_move

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func ecMove(shardIds ...erasure_coding.ShardId) EcShardMove {
	return EcShardMove{
		VolumeId:   7,
		Collection: "c1",
		ShardIds:   shardIds,
		Source:     srcAddr,
		Target:     dstAddr,
		TargetDisk: 2,
	}
}

func dstShards(shardIds ...uint32) []*volume_server_pb.EcShardInfo {
	var infos []*volume_server_pb.EcShardInfo
	for _, sid := range shardIds {
		infos = append(infos, &volume_server_pb.EcShardInfo{VolumeId: 7, ShardId: sid})
	}
	return infos
}

func TestMoveEcShardsSequence(t *testing.T) {
	cluster := newFakeCluster()
	cluster.ecShards[string(dstAddr)] = dstShards(3, 4)

	err := cluster.mover().MoveEcShards(context.Background(), ecMove(3, 4), EcMoveOptions{IoBytePerSecond: 77})
	if err != nil {
		t.Fatalf("MoveEcShards: %v", err)
	}

	assertCalls(t, cluster.callList(), []string{
		"dst:8080 VolumeEcShardsCopy",
		"dst:8080 VolumeEcShardsMount",
		"dst:8080 VolumeEcShardsInfo",
		"src:8080 VolumeEcShardsUnmount",
		"src:8080 VolumeEcShardsDelete",
	})

	copyReq := cluster.ecCopyReqs[0]
	if !copyReq.CopyEcxFile || !copyReq.CopyEcjFile || !copyReq.CopyVifFile || !copyReq.CopyEcsumFile {
		t.Errorf("shard sidecars not all copied: %+v", copyReq)
	}
	if copyReq.DiskId != 2 || copyReq.SourceDataNode != string(srcAddr) || copyReq.Collection != "c1" || copyReq.IoBytePerSecond != 77 {
		t.Errorf("copy request not propagated: %+v", copyReq)
	}
}

func TestMoveEcShardsVerifyFailureKeepsSource(t *testing.T) {
	cluster := newFakeCluster()
	cluster.ecShards[string(dstAddr)] = dstShards(3) // shard 4 didn't register

	err := cluster.mover().MoveEcShards(context.Background(), ecMove(3, 4), EcMoveOptions{})
	if err == nil || !strings.Contains(err.Error(), "missing EC shard 7.4") {
		t.Fatalf("expected missing-shard error, got: %v", err)
	}

	for _, call := range cluster.callList() {
		if call == "src:8080 VolumeEcShardsUnmount" || call == "src:8080 VolumeEcShardsDelete" {
			t.Fatalf("source touched despite verification failure: %v", cluster.callList())
		}
	}
}

func TestMoveEcShardsRejectsSameServer(t *testing.T) {
	// The second target is the same server written with an explicit grpc port;
	// the guard must see through the representation difference.
	for _, target := range []pb.ServerAddress{srcAddr, pb.ServerAddress("src:8080.18080")} {
		cluster := newFakeCluster()
		move := ecMove(3)
		move.Target = target

		err := cluster.mover().MoveEcShards(context.Background(), move, EcMoveOptions{})
		if err == nil || !strings.Contains(err.Error(), "its own server") {
			t.Fatalf("target %q: expected same-server rejection, got: %v", target, err)
		}
		if len(cluster.callList()) != 0 {
			t.Fatalf("target %q: RPCs issued for a rejected move: %v", target, cluster.callList())
		}
	}
}

func TestRemoveEcShards(t *testing.T) {
	cluster := newFakeCluster()

	err := cluster.mover().RemoveEcShards(context.Background(), 7, "c1", srcAddr, []erasure_coding.ShardId{3})
	if err != nil {
		t.Fatalf("RemoveEcShards: %v", err)
	}

	assertCalls(t, cluster.callList(), []string{
		"src:8080 VolumeEcShardsUnmount",
		"src:8080 VolumeEcShardsDelete",
	})
}

func TestCopyAndMountEcShardsSameAddressMountsOnly(t *testing.T) {
	cluster := newFakeCluster()

	err := cluster.mover().CopyAndMountEcShards(context.Background(), 7, "c1", []erasure_coding.ShardId{3}, srcAddr, srcAddr, 0, 0, nil)
	if err != nil {
		t.Fatalf("CopyAndMountEcShards: %v", err)
	}

	assertCalls(t, cluster.callList(), []string{
		"src:8080 VolumeEcShardsMount",
	})
}
