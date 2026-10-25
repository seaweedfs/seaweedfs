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

// reportDiskBytes has the first nodeCount nodes report the same filesystem
// capacity, the way a volume server does in its heartbeat.
func reportDiskBytes(ms *MasterServer, nodeCount int, totalBytes, freeBytes uint64) {
	rack := ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	for _, node := range rack.Children()[:nodeCount] {
		node.(*topology.DataNode).AdjustDiskUsageBytes(
			map[string]uint64{"": totalBytes}, map[string]uint64{"": freeBytes})
	}
}

// TestStatisticsPhysicalCapacity covers a cluster configured with more volume
// slots than its disks hold. Slots promise 40MB here; what the disks can still
// take is what gets reported.
func TestStatisticsPhysicalCapacity(t *testing.T) {
	ms := newStatisticsMaster(t)

	slotCapacity := uint64(40 * 1024 * 1024)
	statistics := func() *master_pb.StatisticsResponse {
		t.Helper()
		resp, err := ms.Statistics(context.Background(), &master_pb.StatisticsRequest{})
		if err != nil {
			t.Fatalf("Statistics: %v", err)
		}
		return resp
	}

	if got := statistics().TotalSize; got != slotCapacity {
		t.Fatalf("disks that report nothing: got %d, want %d", got, slotCapacity)
	}

	// a volume server too old to report leaves the whole cluster on its slots,
	// since the room it holds would otherwise go missing
	reportDiskBytes(ms, 3, 8<<20, 1<<20)
	if got := statistics().TotalSize; got != slotCapacity {
		t.Errorf("one disk of four reporting nothing: got %d, want %d", got, slotCapacity)
	}

	reportDiskBytes(ms, 4, 8<<20, 1<<20)
	resp := statistics()
	if want := resp.UsedSize + (4 << 20); resp.TotalSize != want {
		t.Errorf("four disks with 1MB free: got %d, want %d", resp.TotalSize, want)
	}

	reportDiskBytes(ms, 4, 1<<30, 1<<30)
	if got := statistics().TotalSize; got != slotCapacity {
		t.Errorf("disks roomier than the slots: got %d, want %d", got, slotCapacity)
	}
}
