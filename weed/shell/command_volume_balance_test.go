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
	if err := c.balanceVolumeServers(diskTypes, volumeReplicas, volumeServers, nil, "ALL_COLLECTIONS"); err != nil {
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

func TestSplitCSVSet(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]bool
	}{
		{"empty returns nil", "", nil},
		{"whitespace only returns nil", "   ", nil},
		{"commas only returns nil", ",,,", nil},
		{"whitespace and commas only returns nil", " , , ", nil},
		{"single", "rack1", map[string]bool{"rack1": true}},
		{"multi", "rack1,rack2", map[string]bool{"rack1": true, "rack2": true}},
		{"trims whitespace", " rack1 , rack2 ", map[string]bool{"rack1": true, "rack2": true}},
		{"skips empty items", "rack1,,rack2,", map[string]bool{"rack1": true, "rack2": true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, splitCSVSet(tc.in))
		})
	}
}

// Regression test for the rack/node filter that previously used
// strings.Contains, which falsely matched any id that was a substring of the
// user-supplied flag value (e.g. -racks=rack10 also matched rack1).
func TestCollectVolumeServersByDcRackNode_RackFilter(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{
				{Id: "rack1", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "n1"}}},
				{Id: "rack10", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "n10"}}},
				{Id: "rack2", DataNodeInfos: []*master_pb.DataNodeInfo{{Id: "n2"}}},
			},
		}},
	}

	got := collectVolumeServersByDcRackNode(topo, "", "rack10", "")
	if assert.Len(t, got, 1, "-racks=rack10 should not match rack1") {
		assert.Equal(t, "rack10", got[0].rack)
	}

	got = collectVolumeServersByDcRackNode(topo, "", "rack1,rack2", "")
	racks := map[string]bool{}
	for _, n := range got {
		racks[n.rack] = true
	}
	assert.Equal(t, map[string]bool{"rack1": true, "rack2": true}, racks,
		"-racks=rack1,rack2 should match exactly those two, not rack10")
}

// Regression test for the -nodes filter, mirroring the rack-filter case.
// Uses bare ids (no :port suffix) so that "node1" is a true substring of
// "node10": under the old strings.Contains implementation,
// -nodes=node10 wrongly included node1 as well.
func TestCollectVolumeServersByDcRackNode_NodeFilter(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{Id: "node1"},
					{Id: "node10"},
					{Id: "node2"},
				},
			}},
		}},
	}

	got := collectVolumeServersByDcRackNode(topo, "", "", "node10")
	if assert.Len(t, got, 1, "-nodes=node10 should not match node1") {
		assert.Equal(t, "node10", got[0].info.Id)
	}

	got = collectVolumeServersByDcRackNode(topo, "", "", "node1,node2")
	nodes := map[string]bool{}
	for _, n := range got {
		nodes[n.info.Id] = true
	}
	assert.Equal(t, map[string]bool{"node1": true, "node2": true}, nodes,
		"-nodes=node1,node2 should match exactly those two, not node10")
}
