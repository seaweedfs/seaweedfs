package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// ecShardSizes returns count shard sizes of size bytes each.
func ecShardSizes(count int, size int64) []int64 {
	sizes := make([]int64, count)
	for i := range sizes {
		sizes[i] = size
	}
	return sizes
}

// newStatisticsMaster returns a leader master over four nodes of 10 volume
// slots at 1MB each. Collection c1 holds a 1000 byte volume replicated to two
// nodes and an EC volume of 14 shards at 100 bytes; collection c2 holds a
// single 5000 byte volume.
func newStatisticsMaster(t *testing.T) *MasterServer {
	t.Helper()

	ms := newLeaderMaster()
	ms.option.VolumeSizeLimitMB = 1

	rack := ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 10}
	newNode := func(host string) *topology.DataNode {
		dn := rack.GetOrCreateDataNode(host, 34534, 0, host, "", maxVolumeCounts)
		// VolumeLocationList.Stats only counts nodes connected for over a minute
		dn.LastSeen = time.Now().Unix() - 61
		return dn
	}

	replicatedVolume := &master_pb.VolumeInformationMessage{
		Id: 1, Size: 1000, Collection: "c1", FileCount: 10,
		ReplicaPlacement: 1, // 001: one copy in the same rack
		Version:          uint32(needle.GetCurrentVersion()),
	}
	for _, host := range []string{"127.0.0.1", "127.0.0.2"} {
		ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{replicatedVolume}, newNode(host))
	}

	otherCollectionNode := newNode("127.0.0.3")
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 3, Size: 5000, Collection: "c2", FileCount: 50, Version: uint32(needle.GetCurrentVersion())},
	}, otherCollectionNode)

	// EC shards are not replicated here, so the volume sits on one node
	ms.Topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{
		{Id: 2, Collection: "c1", EcIndexBits: 1<<14 - 1, ShardSizes: ecShardSizes(14, 100), FileCount: 7},
	}, newNode("127.0.0.4"))

	return ms
}

func TestStatisticsLogicalSizes(t *testing.T) {
	ms := newStatisticsMaster(t)

	resp, err := ms.Statistics(context.Background(), &master_pb.StatisticsRequest{
		Collection:  "c1",
		Replication: "001",
	})
	if err != nil {
		t.Fatalf("Statistics: %v", err)
	}

	// 1000 bytes on each of two replicas, plus every shard of the EC volume
	if got, want := resp.UsedSize, uint64(2*1000+14*100); got != want {
		t.Errorf("used size: got %d, want %d", got, want)
	}
	// one replica, and the 10 data shards of the EC volume
	if got, want := resp.LogicalUsedSize, uint64(1000+10*100); got != want {
		t.Errorf("logical used size: got %d, want %d", got, want)
	}

	// four nodes of 10 slots at 1MB each
	totalSize := uint64(40 * 1024 * 1024)
	if resp.TotalSize != totalSize {
		t.Fatalf("total size: got %d, want %d", resp.TotalSize, totalSize)
	}
	// what is left over is what c2 has not taken either, and 001 writes two
	// copies of it
	clusterUsedSize := resp.UsedSize + 5000
	want := resp.LogicalUsedSize + (totalSize-clusterUsedSize)/2
	if resp.LogicalTotalSize != want {
		t.Errorf("logical total size: got %d, want %d", resp.LogicalTotalSize, want)
	}
}

func TestStatisticsReplicaCopyCount(t *testing.T) {
	ms := newStatisticsMaster(t)
	ms.option.DefaultReplicaPlacement = "010"

	for _, tc := range []struct {
		name        string
		replication string
		copies      uint64
	}{
		{"requested", "002", 3},
		{"empty falls back to the master default", "", 2},
		{"unparsable falls back to the master default", "not-a-replication", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := ms.Statistics(context.Background(), &master_pb.StatisticsRequest{
				Replication: tc.replication,
			})
			if err != nil {
				t.Fatalf("Statistics: %v", err)
			}
			want := resp.LogicalUsedSize + (resp.TotalSize-resp.UsedSize)/tc.copies
			if resp.LogicalTotalSize != want {
				t.Errorf("logical total size: got %d, want %d for %d copies",
					resp.LogicalTotalSize, want, tc.copies)
			}
		})
	}
}
