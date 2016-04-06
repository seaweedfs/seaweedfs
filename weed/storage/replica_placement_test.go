package storage

import (
	"testing"
)

func TestReplicaPlacemnetSerialDeserial(t *testing.T) {
	rp, _ := NewReplicaPlacementFromString("001")
	new_rp, _ := NewReplicaPlacementFromByte(rp.Byte())
	if rp.String() != new_rp.String() {
		println("expected:", rp.String(), "actual:", new_rp.String())
		t.Fail()
	}
}
