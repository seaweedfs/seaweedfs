package blockvol

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidAssignment = errors.New("blockvol: invalid assignment transition")
	ErrDrainTimeout      = errors.New("blockvol: drain timeout waiting for in-flight ops")
	ErrEpochRegression   = errors.New("blockvol: epoch regression")
)

const defaultDrainTimeout = 10 * time.Second

// HandleAssignment processes a role/epoch/lease assignment from master.
// Serialized with vol.assignMu to prevent concurrent assignment races.
func HandleAssignment(vol *BlockVol, newEpoch uint64, newRole Role, leaseTTL time.Duration) error {
	vol.assignMu.Lock()
	defer vol.assignMu.Unlock()

	current := vol.Role()

	// Same role -> refresh lease and update epoch if bumped.
	if current == newRole {
		if newEpoch > vol.Epoch() {
			if err := vol.SetEpoch(newEpoch); err != nil {
				return fmt.Errorf("assignment refresh: set epoch: %w", err)
			}
			vol.SetMasterEpoch(newEpoch)
		}
		if current == RolePrimary {
			vol.lease.Grant(leaseTTL)
		}
		return nil
	}

	switch {
	case current == RoleReplica && newRole == RolePrimary:
		return promote(vol, newEpoch, leaseTTL)
	case current == RolePrimary && newRole == RoleStale:
		return demote(vol, newEpoch)
	case current == RoleStale && newRole == RoleRebuilding:
		// Rebuild started externally via StartRebuild.
		return vol.SetRole(RoleRebuilding)
	case current == RoleNone && newRole == RolePrimary:
		return promote(vol, newEpoch, leaseTTL)
	case current == RoleNone && newRole == RoleReplica:
		if err := vol.SetEpoch(newEpoch); err != nil {
			return fmt.Errorf("assign replica: set epoch: %w", err)
		}
		vol.SetMasterEpoch(newEpoch)
		return vol.SetRole(RoleReplica)
	default:
		return fmt.Errorf("%w: %s -> %s", ErrInvalidAssignment, current, newRole)
	}
}

// promote transitions Replica/None -> Primary.
// Order matters: epoch durable BEFORE writes possible.
func promote(vol *BlockVol, newEpoch uint64, leaseTTL time.Duration) error {
	if newEpoch < vol.Epoch() {
		return fmt.Errorf("%w: new %d < current %d", ErrEpochRegression, newEpoch, vol.Epoch())
	}
	if err := vol.SetEpoch(newEpoch); err != nil {
		return fmt.Errorf("promote: set epoch: %w", err)
	}
	vol.SetMasterEpoch(newEpoch)
	if err := vol.SetRole(RolePrimary); err != nil {
		return fmt.Errorf("promote: set role: %w", err)
	}
	vol.lease.Grant(leaseTTL)
	return nil
}

// demote transitions Primary -> Draining -> Stale.
// Revokes lease first, drains in-flight ops, then persists new epoch.
func demote(vol *BlockVol, newEpoch uint64) error {
	// Guard epoch monotonicity before any state changes.
	if newEpoch < vol.Epoch() {
		return fmt.Errorf("%w: new %d < current %d", ErrEpochRegression, newEpoch, vol.Epoch())
	}

	// Revoke lease --writeGate blocks new writes immediately.
	vol.lease.Revoke()

	// Transition to Draining.
	if err := vol.SetRole(RoleDraining); err != nil {
		return fmt.Errorf("demote: set draining: %w", err)
	}

	// Wait for in-flight ops to drain.
	drainTTL := vol.drainTimeout
	if drainTTL == 0 {
		drainTTL = defaultDrainTimeout
	}
	deadline := time.NewTimer(drainTTL)
	defer deadline.Stop()
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for vol.opsOutstanding.Load() > 0 {
		select {
		case <-deadline.C:
			return ErrDrainTimeout
		case <-ticker.C:
		}
	}

	// Stop shipper if present.
	if vol.shipper != nil {
		vol.shipper.Stop()
	}

	// Transition Draining -> Stale.
	if err := vol.SetRole(RoleStale); err != nil {
		return fmt.Errorf("demote: set stale: %w", err)
	}

	// Persist new epoch.
	if err := vol.SetEpoch(newEpoch); err != nil {
		return fmt.Errorf("demote: set epoch: %w", err)
	}
	vol.SetMasterEpoch(newEpoch)
	return nil
}
