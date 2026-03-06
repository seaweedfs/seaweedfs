package blockvol

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockerr"
)

// DurabilityMode controls the write-ACK contract for replicated block volumes.
//   - DurabilityBestEffort (0): local fsync = ACK; replica failures degrade shippers
//   - DurabilitySyncAll (1): all replica barriers must succeed, else write returns error
//   - DurabilitySyncQuorum (2): quorum of RF nodes must be durable (RF>=3 only)
type DurabilityMode uint8

const (
	DurabilityBestEffort DurabilityMode = 0 // zero-value = backward compat
	DurabilitySyncAll    DurabilityMode = 1
	DurabilitySyncQuorum DurabilityMode = 2
)

// Re-export shared error sentinels from blockerr so existing callers
// (e.g. dist_group_commit.go, tests) continue to compile.
var (
	ErrDurabilityBarrierFailed = blockerr.ErrDurabilityBarrierFailed
	ErrDurabilityQuorumLost    = blockerr.ErrDurabilityQuorumLost
	ErrInvalidDurabilityMode   = errors.New("blockvol: invalid durability mode")
	ErrSyncQuorumRequiresRF3   = errors.New("blockvol: sync_quorum requires replica_factor >= 3")
)

// ParseDurabilityMode converts a string to DurabilityMode.
// Empty string is treated as "best_effort" for backward compatibility.
func ParseDurabilityMode(s string) (DurabilityMode, error) {
	switch s {
	case "", "best_effort":
		return DurabilityBestEffort, nil
	case "sync_all":
		return DurabilitySyncAll, nil
	case "sync_quorum":
		return DurabilitySyncQuorum, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrInvalidDurabilityMode, s)
	}
}

// String returns the canonical string representation.
func (m DurabilityMode) String() string {
	switch m {
	case DurabilityBestEffort:
		return "best_effort"
	case DurabilitySyncAll:
		return "sync_all"
	case DurabilitySyncQuorum:
		return "sync_quorum"
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

// Validate checks that the mode is valid for the given replica factor.
func (m DurabilityMode) Validate(replicaFactor int) error {
	switch m {
	case DurabilityBestEffort, DurabilitySyncAll:
		return nil
	case DurabilitySyncQuorum:
		if replicaFactor < 3 {
			return ErrSyncQuorumRequiresRF3
		}
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrInvalidDurabilityMode, m)
	}
}

// IsStrict returns true for sync_all and sync_quorum modes.
func (m DurabilityMode) IsStrict() bool {
	return m == DurabilitySyncAll || m == DurabilitySyncQuorum
}

// RequiredReplicas returns the minimum number of replicas that must be
// successfully provisioned at create time for the given mode + RF.
//   - best_effort: 0 (partial create OK)
//   - sync_all: replicaFactor-1 (all replicas required)
//   - sync_quorum: quorum-1 = RF/2 (e.g., RF=3 -> 1 replica needed)
func (m DurabilityMode) RequiredReplicas(replicaFactor int) int {
	switch m {
	case DurabilitySyncAll:
		if replicaFactor <= 1 {
			return 0
		}
		return replicaFactor - 1
	case DurabilitySyncQuorum:
		// quorum = RF/2+1 nodes (including primary). Replicas needed = quorum-1.
		if replicaFactor <= 1 {
			return 0
		}
		return replicaFactor/2 + 1 - 1 // = RF/2
	default:
		return 0
	}
}
