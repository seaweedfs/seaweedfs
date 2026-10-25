package needle

import (
	"testing"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// .dat files written by volume server versions prior to 3.09 (before commit
// 056c480eb) store the needle checksum using the legacy CRC.Value() transform.
// Reads of such needles must still verify after an upgrade.
func TestReadNeedleTailLegacyChecksum(t *testing.T) {
	data := []byte("hello seaweed")
	rawChecksum := NewCRC(data)
	legacyChecksum := rawChecksum.Value()

	tail := make([]byte, NeedleChecksumSize+TimestampSize)
	util.Uint32toBytes(tail[0:NeedleChecksumSize], legacyChecksum)

	n := &Needle{Data: data}
	if err := n.readNeedleTail(tail, Version3); err != nil {
		t.Fatalf("legacy checksum should verify, got %v", err)
	}
	if n.Checksum != rawChecksum {
		t.Fatalf("expected raw checksum stored, got %x want %x", uint32(n.Checksum), uint32(rawChecksum))
	}
}

func TestReadNeedleTailRawChecksum(t *testing.T) {
	data := []byte("hello seaweed")
	rawChecksum := NewCRC(data)

	tail := make([]byte, NeedleChecksumSize+TimestampSize)
	util.Uint32toBytes(tail[0:NeedleChecksumSize], uint32(rawChecksum))

	n := &Needle{Data: data}
	if err := n.readNeedleTail(tail, Version3); err != nil {
		t.Fatalf("raw checksum should verify, got %v", err)
	}
}

func TestReadNeedleTailCorrupted(t *testing.T) {
	data := []byte("hello seaweed")

	tail := make([]byte, NeedleChecksumSize+TimestampSize)
	util.Uint32toBytes(tail[0:NeedleChecksumSize], 0xdeadbeef)

	n := &Needle{Data: data}
	if err := n.readNeedleTail(tail, Version3); err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
}
