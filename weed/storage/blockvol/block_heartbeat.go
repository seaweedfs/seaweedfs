package blockvol

import (
	"math"
	"time"
)

// BlockVolumeInfoMessage is the heartbeat status for one block volume.
// Mirrors the proto message that will be generated from master.proto.
type BlockVolumeInfoMessage struct {
	Path            string  // volume file path (unique ID on this server)
	VolumeSize      uint64  // logical size in bytes
	BlockSize       uint32  // block size in bytes
	Epoch           uint64  // current fencing epoch
	Role            uint32  // blockvol.Role as uint32 for wire compat
	WalHeadLsn      uint64  // WAL head LSN
	CheckpointLsn   uint64  // last flushed LSN
	HasLease        bool    // whether volume holds a valid lease
	DiskType        string  // e.g., "ssd", "hdd"
	ReplicaDataAddr string  // receiver data listen addr (VS reports in heartbeat)
	ReplicaCtrlAddr string  // receiver ctrl listen addr
	HealthScore     float64 // CP8-2: 0.0-1.0
	ScrubErrors     int64   // CP8-2: lifetime scrub error count
	LastScrubTime   int64   // CP8-2: unix seconds
	ReplicaDegraded bool    // CP8-2: true if any replica shipper degraded
	DurabilityMode  string  // CP8-3-1: "best_effort", "sync_all", "sync_quorum"
	NvmeAddr             string                 // NVMe/TCP target address (ip:port), empty if NVMe disabled
	NQN                  string                 // NVMe subsystem NQN, empty if NVMe disabled
	ReplicaShipperStates []ReplicaShipperStatus // CP13-7: per-replica state from primary's shipper group
}

// BlockVolumeShortInfoMessage is used for delta heartbeats
// (new/deleted block volumes).
type BlockVolumeShortInfoMessage struct {
	Path       string
	VolumeSize uint64
	BlockSize  uint32
	DiskType   string
}

// BlockVolumeAssignment carries a role/epoch/lease assignment
// from master to volume server for one block volume.
type BlockVolumeAssignment struct {
	Path            string        // which block volume
	Epoch           uint64        // new epoch
	Role            uint32        // target role (blockvol.Role as uint32)
	LeaseTtlMs      uint32        // lease TTL in milliseconds (0 = no lease)
	ReplicaDataAddr string        // where primary ships WAL data (scalar, RF=2 compat)
	ReplicaCtrlAddr string        // where primary sends barriers (scalar, RF=2 compat)
	ReplicaServerID string        // V2: stable server identity for scalar replica (from registry)
	RebuildAddr     string        // where rebuild server listens
	ReplicaAddrs    []ReplicaAddr // CP8-2: multi-replica addrs (precedence over scalar)
}

// ToBlockVolumeInfoMessage converts a BlockVol's current state
// to a heartbeat info message. diskType is caller-supplied metadata
// (e.g. "ssd", "hdd") since the volume itself does not track disk type.
func ToBlockVolumeInfoMessage(path, diskType string, vol *BlockVol) BlockVolumeInfoMessage {
	info := vol.Info()
	status := vol.Status()
	hs := vol.HealthStats()
	return BlockVolumeInfoMessage{
		Path:            path,
		VolumeSize:      info.VolumeSize,
		BlockSize:       info.BlockSize,
		Epoch:           status.Epoch,
		Role:            RoleToWire(status.Role),
		WalHeadLsn:      status.WALHeadLSN,
		CheckpointLsn:   status.CheckpointLSN,
		HasLease:        status.HasLease,
		DiskType:        diskType,
		HealthScore:     status.HealthScore,
		ScrubErrors:     hs.ScrubErrors,
		LastScrubTime:   hs.LastScrubTime,
		ReplicaDegraded:      status.ReplicaDegraded,
		DurabilityMode:       vol.DurabilityMode().String(),
		ReplicaShipperStates: vol.ReplicaShipperStates(),
	}
}

// ToBlockVolumeShortInfoMessage returns a short info message
// for delta heartbeats. diskType is caller-supplied metadata.
func ToBlockVolumeShortInfoMessage(path, diskType string, vol *BlockVol) BlockVolumeShortInfoMessage {
	info := vol.Info()
	return BlockVolumeShortInfoMessage{
		Path:       path,
		VolumeSize: info.VolumeSize,
		BlockSize:  info.BlockSize,
		DiskType:   diskType,
	}
}

// maxValidRole is the highest defined Role value.
const maxValidRole = uint32(RoleDraining)

// RoleFromWire converts a uint32 wire role to blockvol.Role.
// Unknown values are mapped to RoleNone to prevent invalid roles
// from propagating through the system.
func RoleFromWire(r uint32) Role {
	if r > maxValidRole {
		return RoleNone
	}
	return Role(r)
}

// RoleToWire converts a blockvol.Role to uint32 for wire.
func RoleToWire(r Role) uint32 {
	return uint32(r)
}

// LeaseTTLFromWire converts milliseconds to time.Duration.
func LeaseTTLFromWire(ms uint32) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// LeaseTTLToWire converts time.Duration to milliseconds.
// Durations exceeding ~49.7 days are clamped to math.MaxUint32.
func LeaseTTLToWire(d time.Duration) uint32 {
	ms := d.Milliseconds()
	if ms > math.MaxUint32 {
		return math.MaxUint32
	}
	if ms < 0 {
		return 0
	}
	return uint32(ms)
}
