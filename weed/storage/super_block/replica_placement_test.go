package super_block

import (
	"testing"
)

func TestReplicaPlacementSerialDeserial(t *testing.T) {
	rp, _ := NewReplicaPlacementFromString("001")
	newRp, _ := NewReplicaPlacementFromByte(rp.Byte())
	if rp.String() != newRp.String() {
		println("expected:", rp.String(), "actual:", newRp.String())
		t.Fail()
	}
}

func TestReplicaPlacementHasReplication(t *testing.T) {
	testCases := []struct {
		name             string
		replicaPlacement string
		want             bool
	}{
		{"empty replica placement", "", false},
		{"no replication", "000", false},
		{"same rack replication", "100", true},
		{"diff rack replication", "020", true},
		{"DC replication", "003", true},
		{"full replication", "155", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rp, err := NewReplicaPlacementFromString(tc.replicaPlacement)
			if err != nil {
				t.Errorf("failed to initialize ReplicaPlacement: %v", err)
				return
			}

			if got, want := rp.HasReplication(), tc.want; got != want {
				t.Errorf("expected %v, got %v", want, got)
			}
		})
	}
}
