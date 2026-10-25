package super_block

import (
	"fmt"
	"testing"
)

func TestReplicaPlacementFromByteMatchesString(t *testing.T) {
	for b := 0; b < 256; b++ {
		want, err := NewReplicaPlacementFromString(fmt.Sprintf("%03d", b))
		if err != nil {
			t.Fatalf("byte %d: %v", b, err)
		}
		got, err := NewReplicaPlacementFromByte(byte(b))
		if err != nil {
			t.Fatalf("byte %d: %v", b, err)
		}
		if !got.Equals(want) {
			t.Errorf("byte %d: got %+v, want %+v", b, got, want)
		}
		if got.Byte() != byte(b) {
			t.Errorf("byte %d: round trip gave %d", b, got.Byte())
		}
	}
}

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
