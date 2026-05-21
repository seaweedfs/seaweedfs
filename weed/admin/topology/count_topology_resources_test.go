package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// TestCountTopologyResources_multiDiskPerNode covers the case where the master
// keys DiskInfos by disk type, so several same-type physical disks on a node
// collapse into a single DiskInfo entry. Counting len(DiskInfos) under-reports
// the physical disk count and disagrees with the per-disk activeDisk map that
// the rest of the admin topology builds via SplitByPhysicalDisk.
func TestCountTopologyResources_multiDiskPerNode(t *testing.T) {
	makeNode := func(id string) *master_pb.DataNodeInfo {
		var ecShardInfos []*master_pb.VolumeEcShardInformationMessage
		for diskId := uint32(0); diskId < 6; diskId++ {
			ecShardInfos = append(ecShardInfos, &master_pb.VolumeEcShardInformationMessage{
				Id:          diskId + 1,
				DiskId:      diskId,
				EcIndexBits: 1,
			})
		}
		return &master_pb.DataNodeInfo{
			Id: id,
			DiskInfos: map[string]*master_pb.DiskInfo{
				"": {Type: "", MaxVolumeCount: 60, EcShardInfos: ecShardInfos},
			},
		}
	}
	topo := &master_pb.TopologyInfo{
		Id: "multi_disk_topo",
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					makeNode("node1"), makeNode("node2"), makeNode("node3"),
				},
			}},
		}},
	}

	dcCount, nodeCount, diskCount := CountTopologyResources(topo)
	if dcCount != 1 {
		t.Errorf("dcCount = %d, want 1", dcCount)
	}
	if nodeCount != 3 {
		t.Errorf("nodeCount = %d, want 3", nodeCount)
	}
	if diskCount != 18 {
		t.Errorf("diskCount = %d, want 18 (6 physical disks x 3 nodes)", diskCount)
	}
}
