package topology

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Crowding asks for more room to write into, so a read-only volume has nothing
// to say about it. Growth already discounts them by intersecting with the
// writable list, so an entry for one is weight and nothing else.
func TestCrowdedVolumesAreOnesThatCanTakeWrites(t *testing.T) {
	topo := NewTopology("crowd", nil, 10000, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})

	// Both are past the growth threshold; only one can be written to.
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Size: 9500, Collection: "c", Version: 3},
		{Id: 2, Size: 9500, Collection: "c", Version: 3, ReadOnly: true},
	}, dn)
	dn.LastSeen = time.Now().Unix()

	// The channels are unbuffered and normally drained by the writable-refresh
	// loop, which is not running here.
	crowded := map[needle.VolumeId]bool{}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case v := <-topo.chanCrowdedVolumes:
				crowded[v.Id] = true
			case <-topo.chanFullVolumes:
			case <-stop:
				return
			}
		}
	}()
	// The channels are unbuffered, so every send has been received by the time
	// this returns; waiting for the collector to finish covers the recording.
	topo.CollectDeadNodeAndFullVolumes(time.Now().Unix()-1, topo.volumeSizeLimit, 0.9)
	close(stop)
	<-done // the collector owns the map until it returns

	if !crowded[needle.VolumeId(1)] {
		t.Error("a writable volume past the threshold was not reported crowded")
	}
	if crowded[needle.VolumeId(2)] {
		t.Error("a read-only volume was reported crowded, which only adds an entry growth then discounts")
	}
}

// The count growth decides on has to be unchanged by this.
func TestCrowdedCountStillMatchesWritables(t *testing.T) {
	topo := NewTopology("crowd", nil, 10000, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Size: 9500, Collection: "c", Version: 3},
	}, dn)

	rp, _ := super_block.NewReplicaPlacementFromString("000")
	vl := topo.GetVolumeLayout("c", rp, needle.EMPTY_TTL, types.HardDriveType)
	vl.SetVolumeCrowded(needle.VolumeId(1))

	writable, crowded := vl.GetWritableVolumeCount()
	if writable != 1 || crowded != 1 {
		t.Errorf("writable=%d crowded=%d, want 1 and 1", writable, crowded)
	}
}
