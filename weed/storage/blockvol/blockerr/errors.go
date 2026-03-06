// Package blockerr defines shared error sentinels for the blockvol subsystem.
// This package exists to break the import cycle between blockvol and iscsi:
//   blockvol -> (adapter.go) -> iscsi
// Both blockvol and iscsi can import blockerr without creating a cycle.
package blockerr

import "errors"

var (
	// ErrDurabilityBarrierFailed is returned when sync_all mode detects
	// that one or more replica barriers did not succeed.
	ErrDurabilityBarrierFailed = errors.New("blockvol: sync_all durability barrier failed")

	// ErrDurabilityQuorumLost is returned when sync_quorum mode detects
	// that fewer than quorum nodes are durable.
	ErrDurabilityQuorumLost = errors.New("blockvol: sync_quorum quorum not met")
)
