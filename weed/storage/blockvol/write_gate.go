package blockvol

import "errors"

var (
	ErrNotPrimary   = errors.New("blockvol: not primary")
	ErrEpochStale   = errors.New("blockvol: epoch stale")
	ErrLeaseExpired = errors.New("blockvol: lease expired")
)

// writeGate checks role, epoch, and lease before allowing a write.
// RoleNone skips all checks for Phase 3 backward compatibility.
func (v *BlockVol) writeGate() error {
	r := Role(v.role.Load())
	if r == RoleNone {
		return nil // Phase 3 compat: no fencing
	}
	if r != RolePrimary {
		return ErrNotPrimary
	}
	if v.epoch.Load() != v.masterEpoch.Load() {
		return ErrEpochStale
	}
	if !v.lease.IsValid() {
		return ErrLeaseExpired
	}
	return nil
}
