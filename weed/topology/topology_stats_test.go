package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestCollectionVolumeStats(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := map[string]uint32{"": 25, "ssd": 12}
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

	volumeMessages := []*master_pb.VolumeInformationMessage{
		{Id: 1, Size: 1000, Collection: "", FileCount: 10, ReplicaPlacement: 0, Version: uint32(needle.GetCurrentVersion())},
		{Id: 2, Size: 2000, Collection: "c1", FileCount: 20, ReplicaPlacement: 0, Version: uint32(needle.GetCurrentVersion())},
		{Id: 3, Size: 3000, Collection: "c1", FileCount: 30, ReplicaPlacement: 0, Version: uint32(needle.GetCurrentVersion()), DiskType: "ssd"},
		{Id: 4, Size: 4000, Collection: "c2", FileCount: 40, ReplicaPlacement: 1, Version: uint32(needle.GetCurrentVersion())},
	}
	topo.SyncDataNodeRegistration(volumeMessages, dn)

	// VolumeLocationList.Stats only counts nodes connected for over a minute
	dn.LastSeen = time.Now().Unix() - 61

	allStats := topo.CollectionVolumeStats("")
	assert(t, "all collections used size", int(allStats.UsedSize), 10000)
	assert(t, "all collections file count", int(allStats.FileCount), 100)

	c1Stats := topo.CollectionVolumeStats("c1")
	assert(t, "c1 used size across disk types", int(c1Stats.UsedSize), 5000)
	assert(t, "c1 file count", int(c1Stats.FileCount), 50)

	c2Stats := topo.CollectionVolumeStats("c2")
	assert(t, "c2 used size", int(c2Stats.UsedSize), 4000)

	missingStats := topo.CollectionVolumeStats("no-such-collection")
	assert(t, "missing collection used size", int(missingStats.UsedSize), 0)
	if _, found := topo.FindCollection("no-such-collection"); found {
		t.Errorf("stats query should not create a phantom collection")
	}
}
