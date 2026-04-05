package weed_server

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// BlockPromotionEvidence is the fresh on-demand evidence returned by a volume
// server when the master queries it at failover time. It reads live local
// facts from blockvol status and V2 core projection — never from cached or
// stale heartbeat data.
//
// Truth owner: local blockvol (storage facts) + V2 engine (eligibility
// semantics). The master only consumes this evidence; it does not own or
// synthesize it.
type BlockPromotionEvidence struct {
	Path                 string
	Epoch                uint64
	CommittedLSN         uint64
	WALHeadLSN           uint64
	CheckpointLSN        uint64
	EngineProjectionMode string  // pure V2 engine local projection, empty if no core
	Eligible             bool
	Reason               string  // why ineligible, or empty
	HealthScore          float64
}

// QueryBlockPromotionEvidence returns fresh promotion evidence for one volume
// on this volume server. The master calls this at failover time instead of
// relying on stale heartbeat data.
//
// Returns live blockvol.Status() plus V2 core projection when present.
// Eligibility is determined locally by the V2 engine — the master does not
// override it.
//
// When V2 core projection is absent, the node is reported as ineligible with
// reason "missing_engine_projection". This is fail-closed by design: the V2
// evidence path does not silently fall back to V1 eligibility semantics.
// Legacy V1 promotion (without engine projection) is handled by T3's explicit
// rollout-gated flag, not by this evidence layer.
func (bs *BlockService) QueryBlockPromotionEvidence(path string, expectedEpoch uint64) (BlockPromotionEvidence, error) {
	if bs == nil {
		return BlockPromotionEvidence{}, fmt.Errorf("block service not enabled")
	}
	if path == "" {
		return BlockPromotionEvidence{}, fmt.Errorf("volume path is required")
	}

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok || vol == nil {
		return BlockPromotionEvidence{}, fmt.Errorf("volume %q not found on this server", path)
	}

	status := vol.Status()
	evidence := BlockPromotionEvidence{
		Path:          path,
		Epoch:         status.Epoch,
		CommittedLSN:  status.WALHeadLSN, // V1 fallback: overridden below when core is present
		WALHeadLSN:    status.WALHeadLSN,
		CheckpointLSN: status.CheckpointLSN,
		HealthScore:   status.HealthScore,
	}

	// V2 core projection: when present, use engine-derived boundaries and
	// eligibility semantics. This is the authoritative source.
	proj, hasCore := bs.CoreProjection(path)
	if hasCore {
		evidence.EngineProjectionMode = string(proj.Mode.Name)
		// Use engine CommittedLSN unconditionally when core is present.
		// A legitimate CommittedLSN == 0 means no data is committed yet;
		// falling back to WALHeadLSN would overstate durability.
		evidence.CommittedLSN = proj.Boundary.CommittedLSN
		// Eligibility from V2 engine: only publish_healthy and replica_ready
		// are eligible for promotion.
		switch proj.Mode.Name {
		case "publish_healthy", "replica_ready":
			evidence.Eligible = true
		default:
			evidence.Eligible = false
			evidence.Reason = fmt.Sprintf("engine_projection_mode=%s", proj.Mode.Name)
			if proj.Mode.Reason != "" {
				evidence.Reason += ": " + proj.Mode.Reason
			}
		}
	} else {
		// Fail-closed: no V2 core projection means no V2 semantic evidence.
		// The node cannot be reported as eligible through the V2 evidence
		// path. Legacy V1 promotion uses a separate rollout-gated path.
		evidence.Eligible = false
		evidence.Reason = "missing_engine_projection"
	}

	// Epoch staleness check: overrides any eligibility set above.
	if expectedEpoch != 0 && status.Epoch != expectedEpoch {
		evidence.Eligible = false
		evidence.Reason = fmt.Sprintf("epoch_mismatch: local=%d expected=%d", status.Epoch, expectedEpoch)
	}

	// Role check: only primary and replica roles are eligible for promotion.
	switch status.Role {
	case blockvol.RolePrimary, blockvol.RoleReplica:
		// eligible (unless overridden above)
	default:
		evidence.Eligible = false
		if evidence.Reason == "" {
			evidence.Reason = fmt.Sprintf("role=%s", status.Role)
		}
	}

	return evidence, nil
}
