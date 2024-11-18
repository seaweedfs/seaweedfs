package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type testcase struct {
	name             string
	replication      string
	replicas         []*VolumeReplica
	possibleLocation location
	expected         bool
}

func TestSatisfyReplicaPlacementComplicated(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 100 negative",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         false,
		},
		{
			name:        "test 100 positive",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
			},
			possibleLocation: location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         true,
		},
		{
			name:        "test 022 positive",
			replication: "022",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         true,
		},
		{
			name:        "test 022 negative",
			replication: "022",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			possibleLocation: location{"dc1", "r4", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
		{
			name:        "test 210 moved from 200 positive",
			replication: "210",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc3", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			possibleLocation: location{"dc1", "r4", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         true,
		},
		{
			name:        "test 210 moved from 200 negative extra dc",
			replication: "210",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc3", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			possibleLocation: location{"dc4", "r4", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
		{
			name:        "test 210 moved from 200 negative extra data node",
			replication: "210",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc3", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
	}

	runTests(tests, t)

}

func TestSatisfyReplicaPlacement01x(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 011 same existing rack",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
		{
			name:        "test 011 negative",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         false,
		},
		{
			name:        "test 011 different existing racks",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
		{
			name:        "test 011 different existing racks negative",
			replication: "011",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         false,
		},
	}

	runTests(tests, t)

}

func TestSatisfyReplicaPlacement00x(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 001",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         true,
		},
		{
			name:        "test 002 positive",
			replication: "002",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
		{
			name:        "test 002 negative, repeat the same node",
			replication: "002",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         false,
		},
		{
			name:        "test 002 negative, enough node already",
			replication: "002",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
	}

	runTests(tests, t)

}

func TestSatisfyReplicaPlacement100(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 100",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			possibleLocation: location{"dc2", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
	}

	runTests(tests, t)

}

func runTests(tests []testcase, t *testing.T) {
	for _, tt := range tests {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
		println("replication:", tt.replication, "expected", tt.expected, "name:", tt.name)
		if satisfyReplicaPlacement(replicaPlacement, tt.replicas, tt.possibleLocation) != tt.expected {
			t.Errorf("%s: expect %v add %v to %s %+v",
				tt.name, tt.expected, tt.possibleLocation, tt.replication, tt.replicas)
		}
	}
}

func TestMisplacedChecking(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 001",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			expected: true,
		},
		{
			name:        "test 010",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			expected: false,
		},
		{
			name:        "test 011",
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
			expected: false,
		},
		{
			name:        "test 110",
			replication: "110",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			expected: true,
		},
		{
			name:        "test 100",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
		println("replication:", tt.replication, "expected", tt.expected, "name:", tt.name)
		if isMisplaced(tt.replicas, replicaPlacement) != tt.expected {
			t.Errorf("%s: expect %v %v %+v",
				tt.name, tt.expected, tt.replication, tt.replicas)
		}
	}

}

func TestPickingMisplacedVolumeToDelete(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 001",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
					info: &master_pb.VolumeInformationMessage{
						Size: 100,
					},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
					info: &master_pb.VolumeInformationMessage{
						Size: 99,
					},
				},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
		},
		{
			name:        "test 100",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
					info: &master_pb.VolumeInformationMessage{
						Size: 100,
					},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
					info: &master_pb.VolumeInformationMessage{
						Size: 99,
					},
				},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
		},
	}

	for _, tt := range tests {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
		println("replication:", tt.replication, "name:", tt.name)
		if x := pickOneMisplacedVolume(tt.replicas, replicaPlacement); x.location.dataNode.Id != tt.possibleLocation.dataNode.Id {
			t.Errorf("%s: picked %+v for replication %v",
				tt.name, x.location.dataNode.Id, tt.replication)
		} else {
			t.Logf("%s: picked %+v %v",
				tt.name, x.location.dataNode.Id, tt.replication)
		}
	}

}

func TestSatisfyReplicaCurrentLocation(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 001",
			replication: "001",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			expected: false,
		},
		{
			name:        "test 010",
			replication: "010",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			expected: true,
		},
		{
			name:        "test 011",
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
			expected: true,
		},
		{
			name:        "test 110",
			replication: "110",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
				{
					location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
				},
			},
			expected: true,
		},
		{
			name:        "test 100",
			replication: "100",
			replicas: []*VolumeReplica{
				{
					location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				},
				{
					location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
			if satisfyReplicaCurrentLocation(replicaPlacement, tt.replicas) != tt.expected {
				t.Errorf("%s: expect %v %v %+v",
					tt.name, tt.expected, tt.replication, tt.replicas)
			}
		})
	}
}
