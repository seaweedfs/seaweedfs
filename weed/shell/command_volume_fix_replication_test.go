package shell

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		if x := misplacedVolumesToDelete(tt.replicas, replicaPlacement)[0]; x.location.dataNode.Id != tt.possibleLocation.dataNode.Id {
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
		if got := replicasToDelete(replicas, replicaPlacement); len(got) != 0 {
			t.Fatalf("expected no replica to delete, got %s", got[0].location.dataNode.Id)
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
		if got := replicasToDelete(replicas, replicaPlacement); len(got) != 0 {
			t.Fatalf("expected no replica to delete, got %s", got[0].location.dataNode.Id)
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
		got := replicasToDelete(replicas, replicaPlacement)
		if len(got) != 2 {
			t.Fatalf("expected the two writable replicas as candidates, got %d", len(got))
		}
		if got[0].location.dataNode.Id != "dn-small" {
			t.Fatalf("expected to delete smallest writable dn-small, got %s", got[0].location.dataNode.Id)
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
	got := misplacedVolumesToDelete(replicas, replicaPlacement)
	if len(got) == 0 {
		t.Fatal("misplaced read-only volume must still be relocated, got nil")
	}
	if got[0].location.dataNode.Id != "dn2" {
		t.Fatalf("expected to relocate smallest replica dn2, got %s", got[0].location.dataNode.Id)
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
	got := replicasToDelete(replicas, replicaPlacement)
	if len(got) != 1 {
		t.Fatalf("expected only dc2-writable as a candidate, got %d", len(got))
	}
	if got[0].location.dataNode.Id != "dc2-writable" {
		t.Fatalf("expected to delete dc2-writable to preserve placement, got %s", got[0].location.dataNode.Id)
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

type trimTestVolumeServer struct {
	volume_server_pb.UnimplementedVolumeServerServer
	afterIndex   func()
	index        []byte
	gone         atomic.Bool
	deletes      atomic.Int32
	statusChecks atomic.Int32
}

func (s *trimTestVolumeServer) CopyFile(_ *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {
	if s.gone.Load() {
		return status.Error(codes.NotFound, "volume not found")
	}
	index := s.index
	if index == nil {
		index = needle_map.ToBytes(1, types.ToOffset(8), 4)
	}
	if err := stream.Send(&volume_server_pb.CopyFileResponse{FileContent: index}); err != nil {
		return err
	}
	if s.afterIndex != nil {
		s.afterIndex()
	}
	return nil
}

func (s *trimTestVolumeServer) VolumeStatus(context.Context, *volume_server_pb.VolumeStatusRequest) (*volume_server_pb.VolumeStatusResponse, error) {
	s.statusChecks.Add(1)
	if s.gone.Load() {
		return nil, status.Error(codes.NotFound, "volume not found")
	}
	return &volume_server_pb.VolumeStatusResponse{}, nil
}

func (s *trimTestVolumeServer) VolumeDelete(context.Context, *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {
	s.deletes.Add(1)
	return &volume_server_pb.VolumeDeleteResponse{}, nil
}

func TestDeleteOneVolumeRechecksSurvivorsBeforeDelete(t *testing.T) {
	for _, tc := range []struct {
		name         string
		survivorGoes bool
		wantDeleted  int
	}{
		{name: "survivor stays", wantDeleted: 1},
		{name: "survivor gone after its index read", survivorGoes: true, wantDeleted: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doomed, survivor := &trimTestVolumeServer{}, &trimTestVolumeServer{}
			if tc.survivorGoes {
				// Gone after the coverage check, its second index read.
				var indexReads atomic.Int32
				survivor.afterIndex = func() {
					if indexReads.Add(1) == 2 {
						survivor.gone.Store(true)
					}
				}
			}
			commandEnv, _ := configureReplicationTestEnv()
			replicas := []*VolumeReplica{
				{location: &location{dataNode: startConfigureReplicationTestVolumeServer(t, doomed, 1)}, info: &master_pb.VolumeInformationMessage{Id: 1}},
				{location: &location{dataNode: startConfigureReplicationTestVolumeServer(t, survivor, 1)}, info: &master_pb.VolumeInformationMessage{Id: 1}},
			}
			pickDoomed := func([]*VolumeReplica, *super_block.ReplicaPlacement) []*VolumeReplica { return replicas[:1] }

			c := &commandVolumeFixReplication{}
			deleted, err := c.deleteOneVolume(commandEnv, io.Discard, true, true, []uint32{1}, map[uint32][]*VolumeReplica{1: replicas}, pickDoomed)
			if err != nil {
				t.Fatalf("deleteOneVolume: %v", err)
			}
			if tc.survivorGoes && !survivor.gone.Load() {
				t.Fatal("the survivor never served its index")
			}
			if survivor.statusChecks.Load() == 0 {
				t.Fatal("the final survivor check never ran")
			}
			if deleted != tc.wantDeleted || int(doomed.deletes.Load()) != tc.wantDeleted {
				t.Fatalf("deleted=%d VolumeDelete calls=%d, want %d", deleted, doomed.deletes.Load(), tc.wantDeleted)
			}
		})
	}
}

func trimTestIndex(entries ...needle_map.NeedleValue) []byte {
	var index []byte
	for _, entry := range entries {
		index = append(index, needle_map.ToBytes(entry.Key, entry.Offset, entry.Size)...)
	}
	return index
}

func TestDeleteOneVolumeRefusesTrimOfUncoveredNeedles(t *testing.T) {
	live := func(key types.NeedleId) needle_map.NeedleValue {
		return needle_map.NeedleValue{Key: key, Offset: types.ToOffset(int64(key) * 8), Size: 4}
	}
	deleted := func(key types.NeedleId) needle_map.NeedleValue {
		return needle_map.NeedleValue{Key: key, Offset: types.ToOffset(int64(key) * 8), Size: types.TombstoneFileSize}
	}
	for _, tc := range []struct {
		name        string
		replication string
		doomed      []needle_map.NeedleValue
		survivors   [][]needle_map.NeedleValue
		wantDeleted int
	}{
		{
			name:        "needle on no survivor",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1)}, {live(1)}},
			wantDeleted: 0,
		},
		{
			name:        "each needle on some survivor",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1)}, {live(2)}},
			wantDeleted: 1,
		},
		{
			name:        "survivor holds the delete",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1), deleted(2)}},
			wantDeleted: 1,
		},
		{
			name:        "remote-tier survivor holds the delete at offset zero",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1), {Key: 2, Size: types.TombstoneFileSize}}},
			wantDeleted: 1,
		},
		{
			name:        "needle already deleted on the doomed replica",
			doomed:      []needle_map.NeedleValue{live(1), live(2), deleted(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1)}},
			wantDeleted: 1,
		},
		{
			name:        "needle left on one of two required copies",
			replication: "001",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1), live(2)}, {live(1)}},
			wantDeleted: 0,
		},
		{
			name:        "every needle kept on two copies",
			replication: "001",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1), live(2)}, {live(2), live(1)}},
			wantDeleted: 1,
		},
		{
			name:        "delete on one survivor covers a needle short of copies",
			replication: "001",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1), live(2)}, {live(1), deleted(2)}},
			wantDeleted: 1,
		},
		{
			name:        "needle rewritten after a delete on a survivor",
			replication: "001",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1), live(2)}, {live(1), deleted(2), live(2)}},
			wantDeleted: 1,
		},
		{
			name:        "needle rewritten after a delete is short of copies",
			replication: "001",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(1)}, {live(1), deleted(2), live(2)}},
			wantDeleted: 0,
		},
		{
			name:        "short volume keeps every needle on its only survivor",
			replication: "002",
			doomed:      []needle_map.NeedleValue{live(1), live(2)},
			survivors:   [][]needle_map.NeedleValue{{live(2), live(1)}},
			wantDeleted: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commandEnv, _ := configureReplicationTestEnv()
			var placement byte
			if tc.replication != "" {
				rp, err := super_block.NewReplicaPlacementFromString(tc.replication)
				if err != nil {
					t.Fatal(err)
				}
				placement = rp.Byte()
			}
			info := func() *master_pb.VolumeInformationMessage {
				return &master_pb.VolumeInformationMessage{Id: 1, ReplicaPlacement: uint32(placement)}
			}
			doomed := &trimTestVolumeServer{index: trimTestIndex(tc.doomed...)}
			replicas := []*VolumeReplica{
				{location: &location{dataNode: startConfigureReplicationTestVolumeServer(t, doomed, 1)}, info: info()},
			}
			for _, entries := range tc.survivors {
				survivor := &trimTestVolumeServer{index: trimTestIndex(entries...)}
				replicas = append(replicas, &VolumeReplica{location: &location{dataNode: startConfigureReplicationTestVolumeServer(t, survivor, 1)}, info: info()})
			}
			pickDoomed := func([]*VolumeReplica, *super_block.ReplicaPlacement) []*VolumeReplica { return replicas[:1] }

			c := &commandVolumeFixReplication{}
			got, err := c.deleteOneVolume(commandEnv, io.Discard, true, true, []uint32{1}, map[uint32][]*VolumeReplica{1: replicas}, pickDoomed)
			if err != nil {
				t.Fatalf("deleteOneVolume: %v", err)
			}
			if got != tc.wantDeleted || int(doomed.deletes.Load()) != tc.wantDeleted {
				t.Fatalf("deleted=%d VolumeDelete calls=%d, want %d", got, doomed.deletes.Load(), tc.wantDeleted)
			}
		})
	}
}

func TestDeleteOneVolumeTrimsTheNextCandidateWhenThePickIsShort(t *testing.T) {
	live := func(keys ...types.NeedleId) []byte {
		var index []byte
		for _, key := range keys {
			index = append(index, needle_map.ToBytes(key, types.ToOffset(int64(key)*8), 4)...)
		}
		return index
	}
	rp, _ := super_block.NewReplicaPlacementFromString("001")
	for _, tc := range []struct {
		name        string
		indexes     [][]byte
		wantDeletes []int32
	}{
		{
			name:        "smallest replica holds a write the next one missed",
			indexes:     [][]byte{live(1, 2), live(1), live(1, 2)},
			wantDeletes: []int32{0, 1, 0},
		},
		{
			name:        "every replica holds a write another missed",
			indexes:     [][]byte{live(1, 2), live(1, 3), live(1, 2, 3)},
			wantDeletes: []int32{0, 0, 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			commandEnv, _ := configureReplicationTestEnv()
			var servers []*trimTestVolumeServer
			var replicas []*VolumeReplica
			for i, index := range tc.indexes {
				server := &trimTestVolumeServer{index: index}
				servers = append(servers, server)
				// The first replica is the smallest, as after a compaction.
				size := uint64(100)
				if i == 0 {
					size = 50
				}
				replicas = append(replicas, &VolumeReplica{
					location: &location{"dc1", "r1", startConfigureReplicationTestVolumeServer(t, server, 1)},
					info:     &master_pb.VolumeInformationMessage{Id: 1, ReplicaPlacement: uint32(rp.Byte()), Size: size},
				})
			}

			c := &commandVolumeFixReplication{}
			if _, err := c.deleteOneVolume(commandEnv, io.Discard, true, true, []uint32{1}, map[uint32][]*VolumeReplica{1: replicas}, replicasToDelete); err != nil {
				t.Fatalf("deleteOneVolume: %v", err)
			}
			for i, server := range servers {
				if got := server.deletes.Load(); got != tc.wantDeletes[i] {
					t.Errorf("replica %d: VolumeDelete calls=%d, want %d", i, got, tc.wantDeletes[i])
				}
			}
		})
	}
}
