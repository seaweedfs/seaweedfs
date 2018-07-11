package topology

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/kr/pretty"
	"testing"
)

func TestRemoveDataCenter(t *testing.T) {
	topo := setup(topologyLayout)
	topo.UnlinkChildNode(NodeId("dc2"))
	if topo.GetActiveVolumeCount() != 15 {
		t.Fail()
	}
	topo.UnlinkChildNode(NodeId("dc3"))
	if topo.GetActiveVolumeCount() != 12 {
		t.Fail()
	}
}

func TestHandlingVolumeServerHeartbeat(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, "127.0.0.1", 25)

	{
		volumeCount := 7
		var volumeMessages []*master_pb.VolumeInformationMessage
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(k),
				Size:             uint64(25432),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 34524,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(1),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		topo.SyncDataNodeRegistration(volumeMessages, dn)

		assert(t, "activeVolumeCount1", topo.activeVolumeCount, volumeCount)
		assert(t, "volumeCount", topo.volumeCount, volumeCount)
	}

	{
		volumeCount := 7 - 1
		var volumeMessages []*master_pb.VolumeInformationMessage
		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(k),
				Size:             uint64(254320),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 345240,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(1),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}
		topo.SyncDataNodeRegistration(volumeMessages, dn)

		assert(t, "activeVolumeCount1", topo.activeVolumeCount, volumeCount)
		assert(t, "volumeCount", topo.volumeCount, volumeCount)
	}

	topo.UnRegisterDataNode(dn)

	assert(t, "activeVolumeCount2", topo.activeVolumeCount, 0)

}

func assert(t *testing.T, message string, actual, expected int) {
	if actual != expected {
		t.Fatalf("unexpected %s: %d, expected: %d", message, actual, expected)
	}
}

func TestAddRemoveVolume(t *testing.T) {

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, "127.0.0.1", 25)

	v := storage.VolumeInfo{
		Id:               storage.VolumeId(1),
		Size:             100,
		Collection:       "xcollection",
		FileCount:        123,
		DeleteCount:      23,
		DeletedByteCount: 45,
		ReadOnly:         false,
		Version:          storage.CurrentVersion,
		ReplicaPlacement: &storage.ReplicaPlacement{},
		Ttl:              storage.EMPTY_TTL,
	}

	dn.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn)

	fmt.Printf("added volume 1 \n%# v\n", pretty.Formatter(topo.ToMap()))

	topo.UnRegisterVolumeLayout(v, dn)

	fmt.Printf("removed volume 1 \n%# v\n", pretty.Formatter(topo.ToMap()))

}
