package topology

import (
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func newDiskTypeTestNode() *DataNode {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	return rack.GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1:8080", "", map[string]uint32{"hdd": 10})
}

func ecShardOnDiskType(vid needle.VolumeId, diskType string) *erasure_coding.EcVolumeInfo {
	s := &erasure_coding.EcVolumeInfo{VolumeId: vid, DiskType: diskType, ShardsInfo: erasure_coding.NewShardsInfo()}
	s.ShardsInfo.Set(erasure_coding.ShardInfo{Id: 0, Size: 100})
	return s
}

// A heartbeat that reports a disk type the node does not have yet links a new
// Disk into dn.children. AdjustMaxVolumeCounts, AdjustDiskUsageBytes and the
// EC shard deltas did that without dn.Lock while lookups, assigns and the UI
// range dn.children under dn.RLock, which the Go runtime can abort the master
// for ("concurrent map read and map write"). Run with -race to reproduce.
func TestNewDiskTypeFromHeartbeatDoesNotRaceReaders(t *testing.T) {
	dn := newDiskTypeTestNode()
	const rounds = 100

	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				_ = dn.GetVolumes()
				_ = dn.GetEcShards()
				_ = dn.Children()
			}
		}()
	}

	for i := range rounds {
		dn.AdjustMaxVolumeCounts(map[string]uint32{fmt.Sprintf("max%d", i): 3})
		dn.AdjustDiskUsageBytes(map[string]uint64{fmt.Sprintf("bytes%d", i): 1 << 30}, map[string]uint64{fmt.Sprintf("bytes%d", i): 1 << 20})
		dn.AddOrUpdateEcShard(ecShardOnDiskType(needle.VolumeId(i+1), fmt.Sprintf("ec%d", i)))
		dn.DeleteEcShard(ecShardOnDiskType(needle.VolumeId(i+1), fmt.Sprintf("ec%d", i)))
	}
	close(done)
	readers.Wait()

	// 1 initial hdd disk + 3 new disk types per round
	if got, want := len(dn.Children()), 1+3*rounds; got != want {
		t.Fatalf("children = %d, want %d", got, want)
	}
}

// Several heartbeats introducing the same disk type at once must end up on
// one linked Disk. A second Disk created in parallel is never linked, so
// whatever is added to it is lost from the topology.
func TestConcurrentFirstUseOfDiskTypeSharesOneDisk(t *testing.T) {
	for round := range 50 {
		dn := newDiskTypeTestNode()
		const writers = 8
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				dn.AddOrUpdateEcShard(ecShardOnDiskType(needle.VolumeId(w+1), "ssd"))
			}()
		}
		close(start)
		wg.Wait()

		if got := len(dn.GetEcShards()); got != writers {
			t.Fatalf("round %d: node holds %d ec volumes, want %d", round, got, writers)
		}
	}
}
