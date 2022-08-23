package super_block

import (
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	SuperBlockSize = 8
)

/*
* Super block currently has 8 bytes allocated for each volume.
* Byte 0: version, 1 or 2
* Byte 1: Replica Placement strategy, 000, 001, 002, 010, etc
* Byte 2 and byte 3: Time to live. See TTL for definition
* Byte 4 and byte 5: The number of times the volume has been compacted.
* Rest bytes: Reserved
 */
type SuperBlock struct {
	Version            needle.Version
	ReplicaPlacement   *ReplicaPlacement
	Ttl                *needle.TTL
	CompactionRevision uint16
	Extra              *master_pb.SuperBlockExtra
	ExtraSize          uint16
}

func (s *SuperBlock) BlockSize() int {
	switch s.Version {
	case needle.Version2, needle.Version3:
		return SuperBlockSize + int(s.ExtraSize)
	}
	return SuperBlockSize
}

func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, SuperBlockSize)
	header[0] = byte(s.Version)
	header[1] = s.ReplicaPlacement.Byte()
	s.Ttl.ToBytes(header[2:4])
	util.Uint16toBytes(header[4:6], s.CompactionRevision)

	if s.Extra != nil {
		extraData, err := proto.Marshal(s.Extra)
		if err != nil {
			glog.Fatalf("cannot marshal super block extra %+v: %v", s.Extra, err)
		}
		extraSize := len(extraData)
		if extraSize > 256*256-2 {
			// reserve a couple of bits for future extension
			glog.Fatalf("super block extra size is %d bigger than %d", extraSize, 256*256-2)
		}
		s.ExtraSize = uint16(extraSize)
		util.Uint16toBytes(header[6:8], s.ExtraSize)

		header = append(header, extraData...)
	}

	return header
}

func (s *SuperBlock) Initialized() bool {
	return s.ReplicaPlacement != nil && s.Ttl != nil
}
