package nvme

import (
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockerr"
)

// NVMeAdapter wraps a *BlockVol to implement BlockDevice and ANAProvider
// for the NVMe/TCP target, bridging the BlockVol storage engine to NVMe
// command handling.
type NVMeAdapter struct {
	Vol *blockvol.BlockVol
}

// NewNVMeAdapter creates a BlockDevice adapter for the given BlockVol.
func NewNVMeAdapter(vol *blockvol.BlockVol) *NVMeAdapter {
	return &NVMeAdapter{Vol: vol}
}

func (a *NVMeAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return a.Vol.ReadLBA(lba, length)
}

func (a *NVMeAdapter) WriteAt(lba uint64, data []byte) error {
	return a.Vol.WriteLBA(lba, data)
}

func (a *NVMeAdapter) Trim(lba uint64, length uint32) error {
	return a.Vol.Trim(lba, length)
}

func (a *NVMeAdapter) SyncCache() error {
	return a.Vol.SyncCache()
}

func (a *NVMeAdapter) BlockSize() uint32 {
	return a.Vol.Info().BlockSize
}

func (a *NVMeAdapter) VolumeSize() uint64 {
	return a.Vol.Info().VolumeSize
}

func (a *NVMeAdapter) IsHealthy() bool {
	return a.Vol.Info().Healthy
}

// ANAState returns the ANA state based on the volume's role.
func (a *NVMeAdapter) ANAState() uint8 {
	return RoleToANAState(a.Vol.Role())
}

// ANAGroupID returns the ANA group ID (always 1 for single-group MVP).
func (a *NVMeAdapter) ANAGroupID() uint16 { return 1 }

// DeviceNGUID returns a 16-byte NGUID derived from the volume UUID.
func (a *NVMeAdapter) DeviceNGUID() [16]byte {
	return UUIDToNGUID(a.Vol.Info().UUID)
}

// WALPressure returns the current WAL usage fraction (0.0–1.0).
func (a *NVMeAdapter) WALPressure() float64 {
	return a.Vol.WALUsedFraction()
}

// Compile-time checks.
var _ BlockDevice = (*NVMeAdapter)(nil)
var _ ANAProvider = (*NVMeAdapter)(nil)
var _ WALPressureProvider = (*NVMeAdapter)(nil)

// RoleToANAState maps a BlockVol Role to an NVMe ANA state.
func RoleToANAState(r blockvol.Role) uint8 {
	switch r {
	case blockvol.RolePrimary, blockvol.RoleNone:
		return anaOptimized
	case blockvol.RoleReplica:
		return anaInaccessible
	case blockvol.RoleStale:
		return anaPersistentLoss
	case blockvol.RoleRebuilding, blockvol.RoleDraining:
		return anaInaccessible
	default:
		return anaInaccessible
	}
}

// UUIDToNGUID converts a 16-byte UUID to a 16-byte NGUID.
// Uses NAA-6 pattern for first 8 bytes (compatible with iSCSI UUIDToNAA),
// copies remaining bytes as-is.
func UUIDToNGUID(uuid [16]byte) [16]byte {
	var nguid [16]byte
	nguid[0] = 0x60 | (uuid[0] & 0x0F)
	copy(nguid[1:8], uuid[1:8])
	copy(nguid[8:16], uuid[8:16])
	return nguid
}

// mapBlockError maps BlockVol errors to NVMe status words.
func mapBlockError(err error) StatusWord {
	if err == nil {
		return StatusSuccess
	}

	// Check known sentinel errors from blockvol and blockerr packages.
	switch {
	case errors.Is(err, blockvol.ErrLeaseExpired):
		return StatusNSNotReadyDNR // DNR=1: fencing is permanent
	case errors.Is(err, blockvol.ErrEpochRegression):
		return StatusInternalErrorDNR // DNR=1: stale controller
	case errors.Is(err, blockerr.ErrDurabilityBarrierFailed):
		return StatusInternalError // DNR=0: replica may recover
	case errors.Is(err, blockerr.ErrDurabilityQuorumLost):
		return StatusInternalError // DNR=0: quorum may heal
	case errors.Is(err, blockvol.ErrWALFull):
		return StatusNSNotReady // DNR=0: transient pressure
	case errors.Is(err, blockvol.ErrNotPrimary):
		return StatusNSNotReady // DNR=0: may be transitioning
	}

	// Heuristic for I/O errors (no dedicated sentinels yet).
	msg := err.Error()
	if strings.Contains(msg, "write") || strings.Contains(msg, "Write") {
		return StatusMediaWriteFault
	}
	if strings.Contains(msg, "read") || strings.Contains(msg, "Read") {
		return StatusMediaReadError
	}

	return StatusInternalError
}
