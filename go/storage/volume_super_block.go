package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/util"
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
	version          Version
	ReplicaPlacement *ReplicaPlacement
	Ttl              *TTL
	CompactRevision  uint16
}

func (s *SuperBlock) Version() Version {
	return s.version
}
func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, SuperBlockSize)
	header[0] = byte(s.version)
	header[1] = s.ReplicaPlacement.Byte()
	s.Ttl.ToBytes(header[2:4])
	util.Uint16toBytes(header[4:6], s.CompactRevision)
	return header
}

func (v *Volume) maybeWriteSuperBlock() error {
	stat, e := v.dataFile.Stat()
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %v", v.dataFile, e)
		return e
	}
	if stat.Size() == 0 {
		v.SuperBlock.version = CurrentVersion
		_, e = v.dataFile.Write(v.SuperBlock.Bytes())
		if e != nil && os.IsPermission(e) {
			//read-only, but zero length - recreate it!
			if v.dataFile, e = os.Create(v.dataFile.Name()); e == nil {
				if _, e = v.dataFile.Write(v.SuperBlock.Bytes()); e == nil {
					v.readOnly = false
				}
			}
		}
	}
	return e
}
func (v *Volume) readSuperBlock() (err error) {
	if _, err = v.dataFile.Seek(0, 0); err != nil {
		return fmt.Errorf("cannot seek to the beginning of %s: %v", v.dataFile.Name(), err)
	}
	header := make([]byte, SuperBlockSize)
	if _, e := v.dataFile.Read(header); e != nil {
		return fmt.Errorf("cannot read volume %d super block: %v", v.Id, e)
	}
	v.SuperBlock, err = ParseSuperBlock(header)
	return err
}
func ParseSuperBlock(header []byte) (superBlock SuperBlock, err error) {
	superBlock.version = Version(header[0])
	if superBlock.ReplicaPlacement, err = NewReplicaPlacementFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err.Error())
	}
	superBlock.Ttl = LoadTTLFromBytes(header[2:4])
	superBlock.CompactRevision = util.BytesToUint16(header[4:6])
	return
}
