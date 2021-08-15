package shell

import (
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
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
	volumeServers := collectVolumeServersByDc(topologyInfo, "")
	volumeReplicas, _ := collectVolumeReplicaLocations(topologyInfo)
	diskTypes := collectVolumeDiskTypes(topologyInfo)

	if err := balanceVolumeServers(nil, diskTypes, volumeReplicas, volumeServers, 30*1024*1024*1024, "ALL_COLLECTIONS", false); err != nil {
		t.Errorf("balance: %v", err)
	}

}

func TestVolumeSelection(t *testing.T) {
	topologyInfo := parseOutput(topoData)

	vids, err := collectVolumeIdsForTierChange(nil, topologyInfo, 1000, types.ToDiskType("hdd"), "", 20.0, 0)
	if err != nil {
		t.Errorf("collectVolumeIdsForTierChange: %v", err)
	}
	assert.Equal(t, 378, len(vids))

}
