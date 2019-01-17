package storage

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
