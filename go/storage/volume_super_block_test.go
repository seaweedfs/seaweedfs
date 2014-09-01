package storage

import (
	"testing"
)

func TestSuperBlockReadWrite(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	s := &SuperBlock{
		version:          CurrentVersion,
		ReplicaPlacement: rp,
		Ttl:              uint16(35),
	}

	bytes := s.Bytes()

	if !(bytes[2] == 0 && bytes[3] == 35) {
	  println("byte[2]:", bytes[2], "byte[3]:", bytes[3])
		t.Fail()
	}

}
