package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"testing"
)

func TestRemoveDataCenter(t *testing.T) {
	topo := setup(topologyLayout)
	topo.UnlinkChildNode(NodeId("dc2"))
	if topo.diskUsages.usages[types.HardDriveType].activeVolumeCount != 15 {
		t.Fail()
	}
	topo.UnlinkChildNode(NodeId("dc3"))
	if topo.diskUsages.usages[types.HardDriveType].activeVolumeCount != 12 {
		t.Fail()
	}
}

func TestHandlingVolumeServerHeartbeat(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 25
	maxVolumeCounts["ssd"] = 12
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", maxVolumeCounts)

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
				Version:          uint32(needle.CurrentVersion),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		for k := 1; k <= volumeCount; k++ {
			volumeMessage := &master_pb.VolumeInformationMessage{
				Id:               uint32(volumeCount + k),
				Size:             uint64(25432),
				Collection:       "",
				FileCount:        uint64(2343),
				DeleteCount:      uint64(345),
				DeletedByteCount: 34524,
				ReadOnly:         false,
				ReplicaPlacement: uint32(0),
				Version:          uint32(needle.CurrentVersion),
				Ttl:              0,
				DiskType:         "ssd",
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}

		topo.SyncDataNodeRegistration(volumeMessages, dn)

		usageCounts := topo.diskUsages.usages[types.HardDriveType]

		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount)
		assert(t, "ssdVolumeCount", int(topo.diskUsages.usages[types.SsdType].volumeCount), volumeCount)
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
				Version:          uint32(needle.CurrentVersion),
				Ttl:              0,
			}
			volumeMessages = append(volumeMessages, volumeMessage)
		}
		topo.SyncDataNodeRegistration(volumeMessages, dn)

		//rp, _ := storage.NewReplicaPlacementFromString("000")
		//layout := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL)
		//assert(t, "writables", len(layout.writables), volumeCount)

		usageCounts := topo.diskUsages.usages[types.HardDriveType]

		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount)
	}

	{
		volumeCount := 6
		newVolumeShortMessage := &master_pb.VolumeShortInformationMessage{
			Id:               uint32(3),
			Collection:       "",
			ReplicaPlacement: uint32(0),
			Version:          uint32(needle.CurrentVersion),
			Ttl:              0,
		}
		topo.IncrementalSyncDataNodeRegistration(
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			nil,
			dn)
		rp, _ := super_block.NewReplicaPlacementFromString("000")
		layout := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
		assert(t, "writables after repeated add", len(layout.writables), volumeCount)

		usageCounts := topo.diskUsages.usages[types.HardDriveType]

		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount)

		topo.IncrementalSyncDataNodeRegistration(
			nil,
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			dn)
		assert(t, "writables after deletion", len(layout.writables), volumeCount-1)
		assert(t, "activeVolumeCount1", int(usageCounts.activeVolumeCount), volumeCount-1)
		assert(t, "volumeCount", int(usageCounts.volumeCount), volumeCount-1)

		topo.IncrementalSyncDataNodeRegistration(
			[]*master_pb.VolumeShortInformationMessage{newVolumeShortMessage},
			nil,
			dn)

		for vid := range layout.vid2location {
			println("after add volume id", vid)
		}
		for _, vid := range layout.writables {
			println("after add writable volume id", vid)
		}

		assert(t, "writables after add back", len(layout.writables), volumeCount)

	}

	topo.UnRegisterDataNode(dn)

	usageCounts := topo.diskUsages.usages[types.HardDriveType]

	assert(t, "activeVolumeCount2", int(usageCounts.activeVolumeCount), 0)

}

func assert(t *testing.T, message string, actual, expected int) {
	if actual != expected {
		t.Fatalf("unexpected %s: %d, expected: %d", message, actual, expected)
	}
}

func TestAddRemoveVolume(t *testing.T) {

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 25
	maxVolumeCounts["ssd"] = 12
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", maxVolumeCounts)

	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             100,
		Collection:       "xcollection",
		DiskType:         "ssd",
		FileCount:        123,
		DeleteCount:      23,
		DeletedByteCount: 45,
		ReadOnly:         false,
		Version:          needle.CurrentVersion,
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}

	dn.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn)
	topo.RegisterVolumeLayout(v, dn)

	if _, hasCollection := topo.FindCollection(v.Collection); !hasCollection {
		t.Errorf("collection %v should exist", v.Collection)
	}

	topo.UnRegisterVolumeLayout(v, dn)

	if _, hasCollection := topo.FindCollection(v.Collection); hasCollection {
		t.Errorf("collection %v should not exist", v.Collection)
	}

}
