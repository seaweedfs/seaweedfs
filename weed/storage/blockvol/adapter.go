package blockvol

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// BlockVolAdapter wraps a *BlockVol to implement the iscsi.BlockDevice and
// iscsi.ALUAProvider interfaces, bridging the BlockVol storage engine to
// iSCSI target sessions.
type BlockVolAdapter struct {
	Vol   *BlockVol
	TPGID uint16 // Target Port Group ID for ALUA (1-65535)
}

// NewBlockVolAdapter creates a BlockDevice adapter for the given BlockVol.
func NewBlockVolAdapter(vol *BlockVol) *BlockVolAdapter {
	return &BlockVolAdapter{Vol: vol}
}

func (a *BlockVolAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return a.Vol.ReadLBA(lba, length)
}

func (a *BlockVolAdapter) WriteAt(lba uint64, data []byte) error {
	return a.Vol.WriteLBA(lba, data)
}

func (a *BlockVolAdapter) Trim(lba uint64, length uint32) error {
	return a.Vol.Trim(lba, length)
}

func (a *BlockVolAdapter) SyncCache() error {
	return a.Vol.SyncCache()
}

func (a *BlockVolAdapter) BlockSize() uint32 {
	return a.Vol.Info().BlockSize
}

func (a *BlockVolAdapter) VolumeSize() uint64 {
	return a.Vol.Info().VolumeSize
}

func (a *BlockVolAdapter) IsHealthy() bool {
	return a.Vol.Info().Healthy
}

// ALUAState returns the ALUA asymmetric access state based on the volume's role.
func (a *BlockVolAdapter) ALUAState() uint8 { return RoleToALUA(a.Vol.Role()) }

// TPGroupID returns the target port group ID.
func (a *BlockVolAdapter) TPGroupID() uint16 { return a.TPGID }

// DeviceNAA returns the NAA-6 device identifier derived from the volume UUID.
func (a *BlockVolAdapter) DeviceNAA() [8]byte { return UUIDToNAA(a.Vol.Info().UUID) }

// RoleToALUA maps a BlockVol Role to an ALUA asymmetric access state.
// RoleNone maps to Active/Optimized so standalone single-node targets
// (no assignment from master) can accept writes.
func RoleToALUA(r Role) uint8 {
	switch r {
	case RolePrimary, RoleNone:
		return iscsi.ALUAActiveOptimized
	case RoleReplica:
		return iscsi.ALUAStandby
	case RoleStale:
		return iscsi.ALUAUnavailable
	case RoleRebuilding, RoleDraining:
		return iscsi.ALUATransitioning
	default:
		return iscsi.ALUAStandby
	}
}

// UUIDToNAA converts a 16-byte UUID to an 8-byte NAA-6 identifier.
// NAA-6 format: nibble 6 (NAA=6) followed by 60 bits from the UUID.
func UUIDToNAA(uuid [16]byte) [8]byte {
	var naa [8]byte
	naa[0] = 0x60 | (uuid[0] & 0x0F)
	copy(naa[1:], uuid[1:8])
	return naa
}

// Compile-time checks.
var _ iscsi.BlockDevice = (*BlockVolAdapter)(nil)
var _ iscsi.ALUAProvider = (*BlockVolAdapter)(nil)
