package topology

import (
	"testing"
)

func TestRemoveDataCenter(t *testing.T) {
	topo := setup(topologyLayout)
	topo.UnlinkChildNode(NodeId("dc2"))
	if topo.GetActiveVolumeCount() != 18 {
		t.Fail()
	}
	topo.UnlinkChildNode(NodeId("dc3"))
	if topo.GetActiveVolumeCount() != 15 {
		t.Fail()
	}
}
