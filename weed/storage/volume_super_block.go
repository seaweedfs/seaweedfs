package storage

import (
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	_SuperBlockSize = 8
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

func (s *SuperBlock) BlockSize() int {
	return _SuperBlockSize
}

func (s *SuperBlock) Version() Version {
	return s.version
}
func (s *SuperBlock) Bytes() []byte {
	header := make([]byte, _SuperBlockSize)
	header[0] = byte(s.version)
	header[1] = s.ReplicaPlacement.Byte()
	s.Ttl.ToBytes(header[2:4])
	util.Uint16toBytes(header[4:6], s.CompactRevision)
	return header
}

func (v *Volume) maybeWriteSuperBlock() error {
	stat, e := v.dataFile.Stat()
	if e != nil {
		glog.V(0).Infof("failed to stat datafile %s: %v", v.dataFile.Name(), e)
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
	v.SuperBlock, err = ReadSuperBlock(v.dataFile)
	return err
}

func parseSuperBlock(header []byte) (superBlock SuperBlock, err error) {
	superBlock.version = Version(header[0])
	if superBlock.ReplicaPlacement, err = NewReplicaPlacementFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err.Error())
	}
	superBlock.Ttl = LoadTTLFromBytes(header[2:4])
	superBlock.CompactRevision = util.BytesToUint16(header[4:6])
	return
}

// ReadSuperBlock reads from data file and load it into volume's super block
func ReadSuperBlock(dataFile *os.File) (superBlock SuperBlock, err error) {
	if _, err = dataFile.Seek(0, 0); err != nil {
		err = fmt.Errorf("cannot seek to the beginning of %s: %v", dataFile.Name(), err)
		return
	}
	header := make([]byte, _SuperBlockSize)
	if _, e := dataFile.Read(header); e != nil {
		err = fmt.Errorf("cannot read volume %s super block: %v", dataFile.Name(), e)
		return
	}
	return parseSuperBlock(header)
}
