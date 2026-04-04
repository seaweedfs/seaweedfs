// Package runtime provides reusable recovery-coordination helpers for the
// V2 engine. These helpers are independent of weed/ host specifics and can
// be used by any adapter shell.
package runtime

import "sync"

// PendingExecution holds the state needed to execute a planned recovery
// action. The coordinator stores one pending execution per volume and
// matches it against incoming commands.
type PendingExecution struct {
	VolumeID  string
	ReplicaID string

	// CatchUpTarget is the target LSN for catch-up execution.
	// Used by ExecutePendingCatchUp for fail-closed target matching.
	CatchUpTarget uint64

	// RebuildTargetLSN is the target LSN for rebuild execution.
	// Used by ExecutePendingRebuild for fail-closed target matching.
	RebuildTargetLSN uint64

	// Opaque handles — the coordinator stores these but doesn't interpret them.
	// The host adapter supplies concrete types (driver, plan, IO bindings).
	Driver    interface{}
	Plan      interface{}
	CatchUpIO interface{}
	RebuildIO interface{}
}

// CancelFunc is called when a pending execution is cancelled due to
// mismatch or supersession. The reason string explains why.
type CancelFunc func(pending *PendingExecution, reason string)

// PendingCoordinator manages pending recovery executions with fail-closed
// command matching. It is safe for concurrent use.
//
// Flow:
//  1. Store() caches a planned execution after recovery planning completes
//  2. TakeCatchUp/TakeRebuild matches an incoming command against the cached plan
//  3. If the target doesn't match, the pending plan is cancelled (fail-closed)
//  4. Cancel() explicitly cancels a pending execution
type PendingCoordinator struct {
	mu       sync.Mutex
	pending  map[string]*PendingExecution
	cancelFn CancelFunc
}

// NewPendingCoordinator creates a coordinator with the given cancel callback.
// The cancel function is called when a pending execution is cancelled due to
// target mismatch or explicit cancellation.
func NewPendingCoordinator(cancelFn CancelFunc) *PendingCoordinator {
	return &PendingCoordinator{
		pending:  make(map[string]*PendingExecution),
		cancelFn: cancelFn,
	}
}

// Store caches a pending execution for a volume, replacing any previous one.
func (pc *PendingCoordinator) Store(volumeID string, pe *PendingExecution) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.pending[volumeID] = pe
}

// TakeCatchUp takes the pending execution for the volume if the catch-up
// target matches. If there's a mismatch, the pending execution is cancelled
// (fail-closed) and nil is returned. If no pending execution exists, nil
// is returned.
func (pc *PendingCoordinator) TakeCatchUp(volumeID string, targetLSN uint64) *PendingExecution {
	pc.mu.Lock()
	pe, ok := pc.pending[volumeID]
	if ok {
		delete(pc.pending, volumeID)
	}
	pc.mu.Unlock()

	if !ok || pe == nil {
		return nil
	}
	if pe.CatchUpTarget != targetLSN {
		if pc.cancelFn != nil {
			pc.cancelFn(pe, "start_catchup_target_mismatch")
		}
		return nil
	}
	return pe
}

// TakeRebuild takes the pending execution for the volume if the rebuild
// target matches. Same fail-closed semantics as TakeCatchUp.
func (pc *PendingCoordinator) TakeRebuild(volumeID string, targetLSN uint64) *PendingExecution {
	pc.mu.Lock()
	pe, ok := pc.pending[volumeID]
	if ok {
		delete(pc.pending, volumeID)
	}
	pc.mu.Unlock()

	if !ok || pe == nil {
		return nil
	}
	if pe.RebuildTargetLSN != targetLSN {
		if pc.cancelFn != nil {
			pc.cancelFn(pe, "start_rebuild_target_mismatch")
		}
		return nil
	}
	return pe
}

// Has returns true if a pending execution exists for the volume.
func (pc *PendingCoordinator) Has(volumeID string) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	_, ok := pc.pending[volumeID]
	return ok
}

// Cancel explicitly cancels and removes the pending execution for a volume.
func (pc *PendingCoordinator) Cancel(volumeID, reason string) {
	pc.mu.Lock()
	pe, ok := pc.pending[volumeID]
	if ok {
		delete(pc.pending, volumeID)
	}
	pc.mu.Unlock()

	if ok && pe != nil && pc.cancelFn != nil {
		pc.cancelFn(pe, reason)
	}
}

// Peek returns the pending execution without removing it. Returns nil if none.
func (pc *PendingCoordinator) Peek(volumeID string) *PendingExecution {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.pending[volumeID]
}

// CancelAll cancels and removes all pending executions.
func (pc *PendingCoordinator) CancelAll(reason string) {
	pc.mu.Lock()
	all := make(map[string]*PendingExecution, len(pc.pending))
	for k, v := range pc.pending {
		all[k] = v
	}
	pc.pending = make(map[string]*PendingExecution)
	pc.mu.Unlock()

	if pc.cancelFn != nil {
		for _, pe := range all {
			pc.cancelFn(pe, reason)
		}
	}
}
