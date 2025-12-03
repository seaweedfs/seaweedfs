package topology

import (
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
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
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

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
				Version:          uint32(needle.GetCurrentVersion()),
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
				Version:          uint32(needle.GetCurrentVersion()),
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
				Version:          uint32(needle.GetCurrentVersion()),
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
			Version:          uint32(needle.GetCurrentVersion()),
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
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             100,
		Collection:       "xcollection",
		DiskType:         "ssd",
		FileCount:        123,
		DeleteCount:      23,
		DeletedByteCount: 45,
		ReadOnly:         false,
		Version:          needle.GetCurrentVersion(),
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

func TestVolumeReadOnlyStatusChange(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 25
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

	// Create a writable volume
	v := storage.VolumeInfo{
		Id:               needle.VolumeId(1),
		Size:             100,
		Collection:       "",
		DiskType:         "",
		FileCount:        10,
		DeleteCount:      0,
		DeletedByteCount: 0,
		ReadOnly:         false, // Initially writable
		Version:          needle.GetCurrentVersion(),
		ReplicaPlacement: &super_block.ReplicaPlacement{},
		Ttl:              needle.EMPTY_TTL,
	}

	dn.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn)

	// Check initial active count (should be 1 since volume is writable)
	usageCounts := topo.diskUsages.usages[types.HardDriveType]
	assert(t, "initial activeVolumeCount", int(usageCounts.activeVolumeCount), 1)
	assert(t, "initial remoteVolumeCount", int(usageCounts.remoteVolumeCount), 0)

	// Change volume to read-only
	v.ReadOnly = true
	dn.UpdateVolumes([]storage.VolumeInfo{v})

	// Check active count after marking read-only (should be 0)
	usageCounts = topo.diskUsages.usages[types.HardDriveType]
	assert(t, "activeVolumeCount after read-only", int(usageCounts.activeVolumeCount), 0)

	// Change volume back to writable
	v.ReadOnly = false
	dn.UpdateVolumes([]storage.VolumeInfo{v})

	// Check active count after marking writable again (should be 1)
	usageCounts = topo.diskUsages.usages[types.HardDriveType]
	assert(t, "activeVolumeCount after writable again", int(usageCounts.activeVolumeCount), 1)
}

func TestVolumeReadOnlyAndRemoteStatusChange(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)

	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 25
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", maxVolumeCounts)

	// Create a writable, local volume
	v := storage.VolumeInfo{
		Id:                needle.VolumeId(1),
		Size:              100,
		Collection:        "",
		DiskType:          "",
		FileCount:         10,
		DeleteCount:       0,
		DeletedByteCount:  0,
		ReadOnly:          false, // Initially writable
		RemoteStorageName: "",    // Initially local
		Version:           needle.GetCurrentVersion(),
		ReplicaPlacement:  &super_block.ReplicaPlacement{},
		Ttl:               needle.EMPTY_TTL,
	}

	dn.UpdateVolumes([]storage.VolumeInfo{v})
	topo.RegisterVolumeLayout(v, dn)

	// Check initial counts
	usageCounts := topo.diskUsages.usages[types.HardDriveType]
	assert(t, "initial activeVolumeCount", int(usageCounts.activeVolumeCount), 1)
	assert(t, "initial remoteVolumeCount", int(usageCounts.remoteVolumeCount), 0)

	// Simultaneously change to read-only AND remote
	v.ReadOnly = true
	v.RemoteStorageName = "s3"
	v.RemoteStorageKey = "key1"
	dn.UpdateVolumes([]storage.VolumeInfo{v})

	// Check counts after both changes
	usageCounts = topo.diskUsages.usages[types.HardDriveType]
	assert(t, "activeVolumeCount after read-only+remote", int(usageCounts.activeVolumeCount), 0)
	assert(t, "remoteVolumeCount after read-only+remote", int(usageCounts.remoteVolumeCount), 1)

	// Change back to writable but keep remote
	v.ReadOnly = false
	dn.UpdateVolumes([]storage.VolumeInfo{v})

	// Check counts - should be writable (active=1) and still remote
	usageCounts = topo.diskUsages.usages[types.HardDriveType]
	assert(t, "activeVolumeCount after writable+remote", int(usageCounts.activeVolumeCount), 1)
	assert(t, "remoteVolumeCount after writable+remote", int(usageCounts.remoteVolumeCount), 1)

	// Change back to local AND read-only simultaneously
	v.ReadOnly = true
	v.RemoteStorageName = ""
	v.RemoteStorageKey = ""
	dn.UpdateVolumes([]storage.VolumeInfo{v})

	// Check final counts
	usageCounts = topo.diskUsages.usages[types.HardDriveType]
	assert(t, "final activeVolumeCount", int(usageCounts.activeVolumeCount), 0)
	assert(t, "final remoteVolumeCount", int(usageCounts.remoteVolumeCount), 0)
}

func TestListCollections(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("002")

	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", nil)

	topo.RegisterVolumeLayout(storage.VolumeInfo{
		Id:               needle.VolumeId(1111),
		ReplicaPlacement: rp,
	}, dn)
	topo.RegisterVolumeLayout(storage.VolumeInfo{
		Id:               needle.VolumeId(2222),
		ReplicaPlacement: rp,
		Collection:       "vol_collection_a",
	}, dn)
	topo.RegisterVolumeLayout(storage.VolumeInfo{
		Id:               needle.VolumeId(3333),
		ReplicaPlacement: rp,
		Collection:       "vol_collection_b",
	}, dn)

	topo.RegisterEcShards(&erasure_coding.EcVolumeInfo{
		VolumeId:   needle.VolumeId(4444),
		Collection: "ec_collection_a",
	}, dn)
	topo.RegisterEcShards(&erasure_coding.EcVolumeInfo{
		VolumeId:   needle.VolumeId(5555),
		Collection: "ec_collection_b",
	}, dn)

	testCases := []struct {
		name                 string
		includeNormalVolumes bool
		includeEcVolumes     bool
		want                 []string
	}{
		{
			name:                 "no volume types selected",
			includeNormalVolumes: false,
			includeEcVolumes:     false,
			want:                 nil,
		}, {
			name:                 "normal volumes",
			includeNormalVolumes: true,
			includeEcVolumes:     false,
			want:                 []string{"", "vol_collection_a", "vol_collection_b"},
		}, {
			name:                 "EC volumes",
			includeNormalVolumes: false,
			includeEcVolumes:     true,
			want:                 []string{"ec_collection_a", "ec_collection_b"},
		}, {
			name:                 "normal + EC volumes",
			includeNormalVolumes: true,
			includeEcVolumes:     true,
			want:                 []string{"", "ec_collection_a", "ec_collection_b", "vol_collection_a", "vol_collection_b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := topo.ListCollections(tc.includeNormalVolumes, tc.includeEcVolumes)

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDataNodeIdBasedIdentification(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")

	maxVolumeCounts := make(map[string]uint32)
	maxVolumeCounts[""] = 10

	// Test 1: Create a DataNode with explicit id
	dn1 := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "10.0.0.1:8080", "node-1", maxVolumeCounts)
	if string(dn1.Id()) != "node-1" {
		t.Errorf("expected node id 'node-1', got '%s'", dn1.Id())
	}
	if dn1.Ip != "10.0.0.1" {
		t.Errorf("expected ip '10.0.0.1', got '%s'", dn1.Ip)
	}

	// Test 2: Same id with different IP should return the same DataNode (K8s pod reschedule scenario)
	dn2 := rack.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "10.0.0.2:8080", "node-1", maxVolumeCounts)
	if dn1 != dn2 {
		t.Errorf("expected same DataNode for same id, got different nodes")
	}
	// IP should be updated to the new value
	if dn2.Ip != "10.0.0.2" {
		t.Errorf("expected ip to be updated to '10.0.0.2', got '%s'", dn2.Ip)
	}
	if dn2.PublicUrl != "10.0.0.2:8080" {
		t.Errorf("expected publicUrl to be updated to '10.0.0.2:8080', got '%s'", dn2.PublicUrl)
	}

	// Test 3: Different id should create a new DataNode
	dn3 := rack.GetOrCreateDataNode("10.0.0.3", 8080, 18080, "10.0.0.3:8080", "node-2", maxVolumeCounts)
	if string(dn3.Id()) != "node-2" {
		t.Errorf("expected node id 'node-2', got '%s'", dn3.Id())
	}
	if dn1 == dn3 {
		t.Errorf("expected different DataNode for different id")
	}

	// Test 4: Empty id should fall back to ip:port (backward compatibility)
	dn4 := rack.GetOrCreateDataNode("10.0.0.4", 8080, 18080, "10.0.0.4:8080", "", maxVolumeCounts)
	if string(dn4.Id()) != "10.0.0.4:8080" {
		t.Errorf("expected node id '10.0.0.4:8080' for empty id, got '%s'", dn4.Id())
	}

	// Test 5: Same ip:port with empty id should return the same DataNode
	dn5 := rack.GetOrCreateDataNode("10.0.0.4", 8080, 18080, "10.0.0.4:8080", "", maxVolumeCounts)
	if dn4 != dn5 {
		t.Errorf("expected same DataNode for same ip:port with empty id")
	}

	// Verify we have 3 unique DataNodes total:
	// - node-1 (dn1/dn2 share the same id)
	// - node-2 (dn3)
	// - 10.0.0.4:8080 (dn4/dn5 share the same ip:port)
	children := rack.Children()
	if len(children) != 3 {
		t.Errorf("expected 3 DataNodes, got %d", len(children))
	}

	// Test 6: Transition from ip:port to explicit id
	// First, the node exists with ip:port as id (dn4/dn5)
	// Now the same volume server starts sending an explicit id
	dn6 := rack.GetOrCreateDataNode("10.0.0.4", 8080, 18080, "10.0.0.4:8080", "node-4-explicit", maxVolumeCounts)
	// Should return the same DataNode instance
	if dn6 != dn4 {
		t.Errorf("expected same DataNode instance during transition")
	}
	// But the id should now be updated to the explicit id
	if string(dn6.Id()) != "node-4-explicit" {
		t.Errorf("expected node id to transition to 'node-4-explicit', got '%s'", dn6.Id())
	}
	// The node should be re-keyed in the children map
	if rack.FindDataNodeById("node-4-explicit") != dn6 {
		t.Errorf("expected to find DataNode by new explicit id")
	}
	// Old ip:port key should no longer work
	if rack.FindDataNodeById("10.0.0.4:8080") != nil {
		t.Errorf("expected old ip:port id to be removed from children map")
	}

	// Still 3 unique DataNodes (node-1, node-2, node-4-explicit)
	children = rack.Children()
	if len(children) != 3 {
		t.Errorf("expected 3 DataNodes after transition, got %d", len(children))
	}

	// Test 7: Prevent incorrect transition when a new node reuses ip:port of a node with explicit id
	// Scenario: node-1 runs at 10.0.0.1:8080, dies, new node-99 starts at same ip:port
	// The transition should NOT happen because node-1 already has an explicit id
	dn7 := rack.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "10.0.0.1:8080", "node-99", maxVolumeCounts)
	// Should create a NEW DataNode, not reuse node-1
	if dn7 == dn1 {
		t.Errorf("expected new DataNode for node-99, got reused node-1")
	}
	if string(dn7.Id()) != "node-99" {
		t.Errorf("expected node id 'node-99', got '%s'", dn7.Id())
	}
	// node-1 should still exist with its original id
	if rack.FindDataNodeById("node-1") == nil {
		t.Errorf("node-1 should still exist")
	}
	// Now we have 4 DataNodes
	children = rack.Children()
	if len(children) != 4 {
		t.Errorf("expected 4 DataNodes, got %d", len(children))
	}
}
