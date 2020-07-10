package shell

import (
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

type testcase struct {
	name              string
	replication       string
	existingLocations []location
	possibleLocation  location
	expected          bool
}

func TestSatisfyReplicaPlacementComplicated(t *testing.T) {

	var tests = []testcase{
		{
			name:        "test 100 negative",
			replication: "100",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         false,
		},
		{
			name:        "test 100 positive",
			replication: "100",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			},
			possibleLocation: location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         true,
		},
		{
			name:        "test 022 positive",
			replication: "022",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         true,
		},
		{
			name:        "test 022 negative",
			replication: "022",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			},
			possibleLocation: location{"dc1", "r4", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
		{
			name:        "test 210 moved from 200 positive",
			replication: "210",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				{"dc3", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			},
			possibleLocation: location{"dc1", "r4", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         true,
		},
		{
			name:        "test 210 moved from 200 negative extra dc",
			replication: "210",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				{"dc3", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
			},
			possibleLocation: location{"dc4", "r4", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
		{
			name:        "test 210 moved from 200 negative extra data node",
			replication: "210",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				{"dc3", "r3", &master_pb.DataNodeInfo{Id: "dn3"}},
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
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
		{
			name:        "test 011 negative",
			replication: "011",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         false,
		},
		{
			name:        "test 011 different existing racks",
			replication: "011",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			},
			possibleLocation: location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
		{
			name:        "test 011 different existing racks negative",
			replication: "011",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
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
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         true,
		},
		{
			name:        "test 002 positive",
			replication: "002",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			expected:         true,
		},
		{
			name:        "test 002 negative, repeat the same node",
			replication: "002",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
			expected:         false,
		},
		{
			name:        "test 002 negative, enough node already",
			replication: "002",
			existingLocations: []location{
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}},
				{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}},
			},
			possibleLocation: location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn4"}},
			expected:         false,
		},
	}

	runTests(tests, t)

}

func runTests(tests []testcase, t *testing.T) {
	for _, tt := range tests {
		replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
		println("replication:", tt.replication, "expected", tt.expected, "name:", tt.name)
		if satisfyReplicaPlacement(replicaPlacement, tt.existingLocations, tt.possibleLocation) != tt.expected {
			t.Errorf("%s: expect %v add %v to %s %+v",
				tt.name, tt.expected, tt.possibleLocation, tt.replication, tt.existingLocations)
		}
	}
}
