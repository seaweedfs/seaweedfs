package super_block

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func TestSuperBlockReadWrite(t *testing.T) {
	rp, _ := NewReplicaPlacementFromByte(byte(001))
	ttl, _ := needle.ReadTTL("15d")
	s := &SuperBlock{
		Version:          needle.CurrentVersion,
		ReplicaPlacement: rp,
		Ttl:              ttl,
	}

	bytes := s.Bytes()

	if !(bytes[2] == 15 && bytes[3] == needle.Day) {
		println("byte[2]:", bytes[2], "byte[3]:", bytes[3])
		t.Fail()
	}

}
