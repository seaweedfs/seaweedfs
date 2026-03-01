package blockvol

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// BlockVolAdapter wraps a *BlockVol to implement the iscsi.BlockDevice interface,
// bridging the BlockVol storage engine to iSCSI target sessions.
type BlockVolAdapter struct {
	vol *BlockVol
}

// NewBlockVolAdapter creates a BlockDevice adapter for the given BlockVol.
func NewBlockVolAdapter(vol *BlockVol) *BlockVolAdapter {
	return &BlockVolAdapter{vol: vol}
}

func (a *BlockVolAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return a.vol.ReadLBA(lba, length)
}

func (a *BlockVolAdapter) WriteAt(lba uint64, data []byte) error {
	return a.vol.WriteLBA(lba, data)
}

func (a *BlockVolAdapter) Trim(lba uint64, length uint32) error {
	return a.vol.Trim(lba, length)
}

func (a *BlockVolAdapter) SyncCache() error {
	return a.vol.SyncCache()
}

func (a *BlockVolAdapter) BlockSize() uint32 {
	return a.vol.Info().BlockSize
}

func (a *BlockVolAdapter) VolumeSize() uint64 {
	return a.vol.Info().VolumeSize
}

func (a *BlockVolAdapter) IsHealthy() bool {
	return a.vol.Info().Healthy
}

// Compile-time check that BlockVolAdapter implements iscsi.BlockDevice.
var _ iscsi.BlockDevice = (*BlockVolAdapter)(nil)
