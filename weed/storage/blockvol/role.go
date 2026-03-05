package blockvol

import (
	"errors"
	"fmt"
)

// Role represents the replication role of a block volume.
type Role uint8

const (
	RoleNone       Role = 0 // Phase 3 default, no fencing
	RolePrimary    Role = 1
	RoleReplica    Role = 2
	RoleStale      Role = 3
	RoleRebuilding Role = 4
	RoleDraining   Role = 5
)

// String returns the role name.
func (r Role) String() string {
	switch r {
	case RoleNone:
		return "none"
	case RolePrimary:
		return "primary"
	case RoleReplica:
		return "replica"
	case RoleStale:
		return "stale"
	case RoleRebuilding:
		return "rebuilding"
	case RoleDraining:
		return "draining"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(r))
	}
}

// validTransitions maps each role to the set of roles it can transition to.
var validTransitions = map[Role]map[Role]bool{
	RoleNone:       {RolePrimary: true, RoleReplica: true, RoleRebuilding: true},
	RolePrimary:    {RoleDraining: true},
	RoleReplica:    {RolePrimary: true},
	RoleStale:      {RoleRebuilding: true, RoleReplica: true},
	RoleRebuilding: {RoleReplica: true},
	RoleDraining:   {RoleStale: true},
}

// ValidTransition returns true if transitioning from -> to is allowed.
func ValidTransition(from, to Role) bool {
	targets, ok := validTransitions[from]
	if !ok {
		return false
	}
	return targets[to]
}

// RoleChangeCallback is called when a volume's role changes.
type RoleChangeCallback func(old, new Role)

var ErrInvalidRoleTransition = errors.New("blockvol: invalid role transition")

// Role returns the current role of this volume.
func (v *BlockVol) Role() Role {
	return Role(v.role.Load())
}

// SetRole transitions the volume to a new role if the transition is valid.
// Uses CompareAndSwap to prevent TOCTOU races between concurrent callers.
// Calls the registered RoleChangeCallback after updating.
func (v *BlockVol) SetRole(r Role) error {
	for {
		old := v.role.Load()
		if !ValidTransition(Role(old), r) {
			return fmt.Errorf("%w: %s -> %s", ErrInvalidRoleTransition, Role(old), r)
		}
		if v.role.CompareAndSwap(old, uint32(r)) {
			if v.roleCallback != nil {
				v.safeCallback(Role(old), r)
			}
			return nil
		}
		// CAS failed -- another goroutine changed the role; retry.
	}
}

// safeCallback invokes the role callback with panic recovery.
// If the callback panics, the role is already updated but the panic
// is caught and returned as an error via log (callers see nil from SetRole).
func (v *BlockVol) safeCallback(old, new Role) {
	defer func() {
		if r := recover(); r != nil {
			// Role is already stored. Log but don't propagate panic.
			// In production, this would go to glog. For now, swallow it.
		}
	}()
	v.roleCallback(old, new)
}

// SetRoleCallback registers a callback to be invoked on role changes.
func (v *BlockVol) SetRoleCallback(cb RoleChangeCallback) {
	v.roleCallback = cb
}
