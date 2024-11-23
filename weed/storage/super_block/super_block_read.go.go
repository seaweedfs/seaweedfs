package super_block

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ReadSuperBlock reads from data file and load it into volume's super block
func ReadSuperBlock(datBackend backend.BackendStorageFile) (superBlock SuperBlock, err error) {

	header := make([]byte, SuperBlockSize)
	if n, e := datBackend.ReadAt(header, 0); e != nil {
		if n != SuperBlockSize {
			err = fmt.Errorf("cannot read volume %s super block: %v", datBackend.Name(), e)
			return
		}
	}

	superBlock.Version = needle.Version(header[0])
	if superBlock.ReplicaPlacement, err = NewReplicaPlacementFromByte(header[1]); err != nil {
		err = fmt.Errorf("cannot read replica type: %s", err.Error())
		return
	}
	superBlock.Ttl = needle.LoadTTLFromBytes(header[2:4])
	superBlock.CompactionRevision = util.BytesToUint16(header[4:6])
	superBlock.ExtraSize = util.BytesToUint16(header[6:8])

	if superBlock.ExtraSize > 0 {
		// read more
		extraData := make([]byte, int(superBlock.ExtraSize))
		superBlock.Extra = &master_pb.SuperBlockExtra{}
		err = proto.Unmarshal(extraData, superBlock.Extra)
		if err != nil {
			err = fmt.Errorf("cannot read volume %s super block extra: %v", datBackend.Name(), err)
			return
		}
	}

	return
}
