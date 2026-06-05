package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// TestRegisterEcShardsAdvancesMaxVolumeId guards against volume-id reuse: an
// EC-only volume must advance maxVolumeId so a heartbeat-rebuilt master cannot
// re-issue its id.
func TestRegisterEcShardsAdvancesMaxVolumeId(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 100})

	const ecVid = uint32(39848)
	if got := topo.GetMaxVolumeId(); uint32(got) >= ecVid {
		t.Fatalf("precondition: maxVolumeId already %d, want < %d", got, ecVid)
	}

	// Full-sync heartbeat reporting an EC-only volume.
	msg := buildEcShardMessage(ecVid, "prefeitura", "", 0, []erasure_coding.ShardId{0, 1, 2, 3})
	topo.SyncDataNodeEcShards([]*master_pb.VolumeEcShardInformationMessage{msg}, dn)

	if got := topo.GetMaxVolumeId(); got != needle.VolumeId(ecVid) {
		t.Fatalf("maxVolumeId after EC registration = %d, want %d", got, ecVid)
	}
}
