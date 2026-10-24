package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// A disk that registers after the listing went out has nowhere to go in it, so
// its volumes must not be streamed into a client that cannot place them. It is
// reported by the next listing instead.
func TestStreamVolumesStaysInsideTheAnnouncedTopology(t *testing.T) {
	topo := NewTopology("bounds", nil, 32*1024*1024*1024, 5, false)
	rack := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1")
	known := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 1, Collection: "c", Size: 100, Version: 3},
		{Id: 2, Collection: "c", Size: 200, Version: 3},
	}, known)

	listed := topo.ToTopologyInfo(NoVolumes())

	// The heartbeat that lands between the listing and the walk below.
	late := rack.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "", map[string]uint32{"": 100})
	topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{
		{Id: 3, Collection: "c", Size: 300, Version: 3},
	}, late)

	var streamed []uint32
	err := topo.StreamVolumes(listed, VolumeFilter{}, 10, func(b *master_pb.VolumeListStreamResponse) error {
		if b.DataNode == string(late.Id()) {
			t.Errorf("streamed a batch for %s, which the listing never named", b.DataNode)
		}
		for _, v := range b.VolumeInfos {
			streamed = append(streamed, v.Id)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !equalIds(streamed, []uint32{1, 2}) {
		t.Errorf("streamed %v, want the two volumes the listing named", streamed)
	}
}
