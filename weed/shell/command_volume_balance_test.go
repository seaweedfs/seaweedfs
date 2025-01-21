package shell

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type testMoveCase struct {
	name           string
	replication    string
	replicas       []*VolumeReplica
	sourceLocation location
	targetLocation location
	expected       bool
}

func TestIsGoodMove(t *testing.T) {

	var tests = []testMoveCase{

		{
			name:        "test 100 move to wrong data centers",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			targetLocation: location{"dc2", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       false,
		},

		{
			name:        "test 100 move to spread into proper data centers",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test move to the same node",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:       false,
		},

		{
			name:        "test move to the same rack, but existing node",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			expected:       false,
		},

		{
			name:        "test move to the same rack, a new node",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test 010 move all to the same rack",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       false,
		},

		{
			name:        "test 010 move to spread racks",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test 010 move to spread racks",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			targetLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:       true,
		},

		{
			name:        "test 011 switch which rack has more replicas",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			targetLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       true,
		},

		{
			name:        "test 011 move the lonely replica to another racks",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			targetLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       true,
		},

		{
			name:        "test 011 move to wrong racks",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			targetLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       false,
		},

		{
			name:        "test 011 move all to the same rack",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			sourceLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			targetLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:       false,
		},
	}

	for _, tt := range tests {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
		println("replication:", tt.replication, "expected", tt.expected, "name:", tt.name)
		sourceNode := &Node{
			info: tt.sourceLocation.dataNode,
			dc:   tt.sourceLocation.dc,
			rack: tt.sourceLocation.rack,
		}
		targetNode := &Node{
			info: tt.targetLocation.dataNode,
			dc:   tt.targetLocation.dc,
			rack: tt.targetLocation.rack,
		}
		if isGoodMove(replicaPlacement, tt.replicas, sourceNode, targetNode) != tt.expected {
			t.Errorf("%s: expect %v move from %v to %s, replication:%v",
				tt.name, tt.expected, tt.sourceLocation, tt.targetLocation, tt.replication)
		}
	}

}

func TestBalance(t *testing.T) {
	topologyInfo := parseOutput(topoData)
	volumeServers := collectVolumeServersByDcRackNode(topologyInfo, "", "", "")
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	diskTypes := collectVolumeDiskTypes(topologyInfo)
	c := &commandVolumeBalance{}
	if err := c.balanceVolumeServers(diskTypes, volumeReplicas, volumeServers, "ALL_COLLECTIONS"); err != nil {
		t.Errorf("balance: %v", err)
	}

}

func TestVolumeSelection(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	vids, err := collectVolumeIdsForTierChange(topologyInfo, 1000, types.ToDiskType(types.HddType), "", 20.0, 0)
	if err != nil {
		t.Errorf("collectVolumeIdsForTierChange: %v", err)
	}
	assert.Equal(t, 378, len(vids))

}

func TestDeleteEmptySelection(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Size <= super_block.SuperBlockSize && v.ModifiedAtSecond > 0 {
					fmt.Printf("empty volume %d from %s\n", v.Id, dn.Id)
				}
			}
		}
	})

}
