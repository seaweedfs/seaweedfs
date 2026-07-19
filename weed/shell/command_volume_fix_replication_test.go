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

func TestPickOneReplicaToDeleteSkipsReadOnlySurvivor(t *testing.T) {
	replicaPlacement, _ := super_block.NewReplicaPlacementFromString("001")

	// One writable replica and one read-only replica. The trim must never
	// delete the only writable replica while the survivor is read-only:
	// VolumeStatus alone cannot prove the read-only .dat is readable, so the
	// trim is refused.
	t.Run("refuses to delete the only writable replica while a read-only survivor remains", func(t *testing.T) {
		replicas := []*VolumeReplica{
			{
				location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn-writable"}},
				info:     &master_pb.VolumeInformationMessage{Size: 90, ReadOnly: false},
			},
			{
				location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn-readonly"}},
				info:     &master_pb.VolumeInformationMessage{Size: 100, ReadOnly: true},
			},
		}
		if got := pickOneReplicaToDelete(replicas, replicaPlacement); got != nil {
			t.Fatalf("expected no replica to delete, got %s", got.location.dataNode.Id)
		}
	})

	// All survivors read-only: never trim.
	t.Run("refuses when all replicas are read-only", func(t *testing.T) {
		replicas := []*VolumeReplica{
			{
				location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
				info:     &master_pb.VolumeInformationMessage{Size: 100, ReadOnly: true},
			},
			{
				location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
				info:     &master_pb.VolumeInformationMessage{Size: 100, ReadOnly: true},
			},
		}
		if got := pickOneReplicaToDelete(replicas, replicaPlacement); got != nil {
			t.Fatalf("expected no replica to delete, got %s", got.location.dataNode.Id)
		}
	})

	// Two healthy writable replicas plus a read-only one: trim the smallest
	// writable, never the read-only one.
	t.Run("trims the smallest writable replica and never the read-only one", func(t *testing.T) {
		replicas := []*VolumeReplica{
			{
				location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn-big"}},
				info:     &master_pb.VolumeInformationMessage{Size: 100, ReadOnly: false},
			},
			{
				location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn-small"}},
				info:     &master_pb.VolumeInformationMessage{Size: 90, ReadOnly: false},
			},
			{
				location: &location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn-readonly"}},
				info:     &master_pb.VolumeInformationMessage{Size: 80, ReadOnly: true},
			},
		}
		got := pickOneReplicaToDelete(replicas, replicaPlacement)
		if got == nil {
			t.Fatalf("expected a writable replica to delete, got nil")
		}
		if got.location.dataNode.Id != "dn-small" {
			t.Fatalf("expected to delete smallest writable dn-small, got %s", got.location.dataNode.Id)
		}
	})
}

// The over-replication writable-survivor guard must NOT leak into the misplaced
// relocation path: a misplaced volume whose replicas are all read-only (e.g. a
// full volume) must still be relocated, picking the smallest replica to delete
// and recreate at a correct placement.
func TestPickOneMisplacedVolumeRelocatesReadOnlyReplicas(t *testing.T) {
	replicaPlacement, _ := super_block.NewReplicaPlacementFromString("001")
	replicas := []*VolumeReplica{
		{
			location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}},
			info:     &master_pb.VolumeInformationMessage{Size: 100, ReadOnly: true},
		},
		{
			location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}},
			info:     &master_pb.VolumeInformationMessage{Size: 99, ReadOnly: true},
		},
	}
	got := pickOneMisplacedVolume(replicas, replicaPlacement)
	if got == nil {
		t.Fatal("misplaced read-only volume must still be relocated, got nil")
	}
	if got.location.dataNode.Id != "dn2" {
		t.Fatalf("expected to relocate smallest replica dn2, got %s", got.location.dataNode.Id)
	}
}

// The over-replication trim must not strip the only replica in a required
// failure domain: with "100" and replicas in dc1 + two in dc2, deleting the
// smallest (dc1) leaves both survivors in dc2 and violates placement, so the
// trim must instead delete a dc2 writable replica.
func TestPickOneReplicaToDeletePreservesPlacement(t *testing.T) {
	replicaPlacement, _ := super_block.NewReplicaPlacementFromString("100")
	replicas := []*VolumeReplica{
		{
			location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dc1-writable"}},
			info:     &master_pb.VolumeInformationMessage{Size: 90, ReadOnly: false},
		},
		{
			location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dc2-readonly"}},
			info:     &master_pb.VolumeInformationMessage{Size: 80, ReadOnly: true},
		},
		{
			location: &location{"dc2", "r3", &master_pb.DataNodeInfo{Id: "dc2-writable"}},
			info:     &master_pb.VolumeInformationMessage{Size: 100, ReadOnly: false},
		},
	}
	got := pickOneReplicaToDelete(replicas, replicaPlacement)
	if got == nil {
		t.Fatal("expected a replica to delete")
	}
	if got.location.dataNode.Id != "dc2-writable" {
		t.Fatalf("expected to delete dc2-writable to preserve placement, got %s", got.location.dataNode.Id)
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

func TestClassifyReplicaSet(t *testing.T) {

	var tests = []struct {
		name        string
		replication string
		replicas    []*VolumeReplica
		expected    replicaFix
	}{
		{
			// The full replica count is present but two copies crowd one rack.
			// Deleting a misplaced replica first would drop the volume below its
			// intended durability, so add a well-placed replica first.
			name:        "110 misplaced without surplus adds before trimming",
			replication: "110",
			replicas: []*VolumeReplica{
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}}},
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}}},
				{location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn3"}}},
			},
			expected: replicaFixAddOneBeforeTrim,
		},
		{
			// Same misplaced set once the well-placed replica registered: now
			// there is a surplus, and the misplaced replica can be trimmed.
			name:        "110 misplaced with surplus trims",
			replication: "110",
			replicas: []*VolumeReplica{
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}}},
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}}},
				{location: &location{"dc1", "r3", &master_pb.DataNodeInfo{Id: "dn4"}}},
				{location: &location{"dc2", "r2", &master_pb.DataNodeInfo{Id: "dn3"}}},
			},
			expected: replicaFixTrimMisplaced,
		},
		{
			name:        "010 missing a replica adds",
			replication: "010",
			replicas: []*VolumeReplica{
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}}},
			},
			expected: replicaFixAddOne,
		},
		{
			// Simultaneously over-provisioned in count and lacking rack spread:
			// the missing failure domain is filled first, trims come later.
			name:        "010 surplus count but missing rack spread adds",
			replication: "010",
			replicas: []*VolumeReplica{
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}}},
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn2"}}},
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn3"}}},
			},
			expected: replicaFixAddOne,
		},
		{
			name:        "010 well placed needs nothing",
			replication: "010",
			replicas: []*VolumeReplica{
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}}},
				{location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}}},
			},
			expected: replicaFixNothing,
		},
		{
			name:        "000 duplicate copy trims",
			replication: "000",
			replicas: []*VolumeReplica{
				{location: &location{"dc1", "r1", &master_pb.DataNodeInfo{Id: "dn1"}}},
				{location: &location{"dc1", "r2", &master_pb.DataNodeInfo{Id: "dn2"}}},
			},
			expected: replicaFixTrimMisplaced,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			replicaPlacement, _ := super_block.NewReplicaPlacementFromString(tt.replication)
			if got := classifyReplicaSet(replicaPlacement, tt.replicas); got != tt.expected {
				t.Errorf("%s: expect %v got %v for %v %+v",
					tt.name, tt.expected, got, tt.replication, tt.replicas)
			}
		})
	}
}
