package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func TestGetDisksWithEffectiveCapacityNotCappedAtTenByLoad(t *testing.T) {
	t.Parallel()

	activeTopology := NewActiveTopology(0)
	if err := activeTopology.UpdateTopology(singleDiskTopologyInfoForCapacityTest()); err != nil {
		t.Fatalf("UpdateTopology: %v", err)
	}

	const pendingTasks = 32
	for i := 0; i < pendingTasks; i++ {
		taskID := fmt.Sprintf("ec-capacity-%d", i)
		err := activeTopology.AddPendingTask(TaskSpec{
			TaskID:     taskID,
			TaskType:   TaskTypeErasureCoding,
			VolumeID:   uint32(i + 1),
			VolumeSize: 1,
			Sources: []TaskSourceSpec{
				{
					ServerID:      "node-a",
					DiskID:        0,
					StorageImpact: &StorageSlotChange{},
				},
			},
			Destinations: []TaskDestinationSpec{
				{
					ServerID:      "node-a",
					DiskID:        0,
					StorageImpact: &StorageSlotChange{},
				},
			},
		})
		if err != nil {
			t.Fatalf("AddPendingTask(%s): %v", taskID, err)
		}
	}

	disks := activeTopology.GetDisksWithEffectiveCapacity(TaskTypeErasureCoding, "", 1)
	if len(disks) != 1 {
		t.Fatalf("expected disk to remain available after %d pending tasks, got %d", pendingTasks, len(disks))
	}
	if disks[0].LoadCount != pendingTasks {
		t.Fatalf("unexpected load count: got=%d want=%d", disks[0].LoadCount, pendingTasks)
	}
}

func singleDiskTopologyInfoForCapacityTest() *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		Id: "topology-test",
		DataCenterInfos: []*master_pb.DataCenterInfo{
			{
				Id: "dc1",
				RackInfos: []*master_pb.RackInfo{
					{
						Id: "rack1",
						DataNodeInfos: []*master_pb.DataNodeInfo{
							{
								Id: "node-a",
								DiskInfos: map[string]*master_pb.DiskInfo{
									"hdd": {
										DiskId:         0,
										Type:           "hdd",
										VolumeCount:    0,
										MaxVolumeCount: 1000,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
