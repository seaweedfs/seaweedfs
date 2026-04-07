package protocol

// Engine is the v2 protocol engine. Deterministic, side-effect free.
// Event in → state mutation + commands + projection out.
//
// Compared to engine/replication (19 events, 979 lines):
//   - 7 event types (Assignment, Readiness, SyncAck, SessionProgress,
//     SessionCompleted, SessionFailed, BarrierConfirmed)
//   - Primary decides catchup vs rebuild from SyncAck facts, not from
//     autonomous shipper/budget logic
//   - Session lifecycle is explicit (idle → issued → running → completed/failed)
//   - No per-replica bookkeeping maps — ReplicaView holds everything
type Engine struct {
	volumes map[string]*VolumeState
}

type Result struct {
	Commands   []Command
	Mode       ModeName
	ModeReason string
	Healthy    bool
}

func NewEngine() *Engine {
	return &Engine{volumes: make(map[string]*VolumeState)}
}

func (e *Engine) ApplyEvent(ev Event) Result {
	st := e.mustVolume(ev.volumeID())

	var cmds []Command

	switch v := ev.(type) {
	case AssignmentDelivered:
		cmds = e.applyAssignment(st, v)
	case ReadinessObserved:
		cmds = e.applyReadiness(st, v)
	case SyncAckReceived:
		cmds = e.applySyncAck(st, v)
	case SessionProgress:
		e.applyProgress(st, v)
	case SessionCompleted:
		cmds = e.applyCompleted(st, v)
	case SessionFailed:
		cmds = e.applyFailed(st, v)
	case BarrierConfirmed:
		e.applyBarrier(st, v)
	}

	e.deriveMode(st)

	return Result{
		Commands:   cmds,
		Mode:       st.Mode,
		ModeReason: st.ModeReason,
		Healthy:    st.Healthy,
	}
}

func (e *Engine) Volume(id string) (VolumeState, bool) {
	st, ok := e.volumes[id]
	if !ok {
		return VolumeState{}, false
	}
	return *st, true
}

// --- Assignment ---

func (e *Engine) applyAssignment(st *VolumeState, ev AssignmentDelivered) []Command {
	epochChanged := st.Epoch != ev.Epoch
	roleChanged := st.Role != ev.Role

	st.Epoch = ev.Epoch
	st.Role = ev.Role
	st.Replicas = ev.Replicas
	st.Readiness.Assigned = true

	if epochChanged || roleChanged {
		st.Readiness.RoleApplied = false
		st.Readiness.ShipperConfigured = false
		st.Readiness.ShipperConnected = false
		st.Readiness.ReceiverReady = false
		// Clear all replica sessions on epoch/role change.
		st.ReplicaStates = make(map[string]*ReplicaView)
	}

	// Ensure ReplicaView exists for each assigned replica.
	if st.ReplicaStates == nil {
		st.ReplicaStates = make(map[string]*ReplicaView)
	}
	for _, r := range ev.Replicas {
		if _, ok := st.ReplicaStates[r.ReplicaID]; !ok {
			st.ReplicaStates[r.ReplicaID] = &ReplicaView{
				ReplicaID: r.ReplicaID,
				Endpoint:  r.Endpoint,
				Session:   ReplicaSession{Kind: SessionNone, State: SessionStateIdle},
			}
		}
	}

	var cmds []Command
	cmds = append(cmds, ApplyRoleCommand{
		VolumeID: st.VolumeID,
		Epoch:    st.Epoch,
		Role:     st.Role,
	})

	if st.Role == RolePrimary && len(st.Replicas) > 0 {
		cmds = append(cmds, ConfigureShipperCommand{
			VolumeID: st.VolumeID,
			Replicas: st.Replicas,
		})
	}
	if st.Role == RoleReplica {
		cmds = append(cmds, StartReceiverCommand{VolumeID: st.VolumeID})
	}

	return cmds
}

// --- Readiness ---

func (e *Engine) applyReadiness(st *VolumeState, ev ReadinessObserved) []Command {
	if ev.RoleApplied != nil {
		st.Readiness.RoleApplied = *ev.RoleApplied
	}
	if ev.ReceiverReady != nil {
		st.Readiness.ReceiverReady = *ev.ReceiverReady
	}
	if ev.ShipperConfigured != nil {
		st.Readiness.ShipperConfigured = *ev.ShipperConfigured
	}
	if ev.ShipperConnected != nil {
		st.Readiness.ShipperConnected = *ev.ShipperConnected
	}
	return nil
}

// --- SyncAck: the core decision point ---
//
// This is where the primary decides per-replica recovery mode.
// One threshold: applied_lsn >= primary_wal_tail → WAL catch-up, else → rebuild.
// Matches Ceph's last_update >= log_tail decision.

func (e *Engine) applySyncAck(st *VolumeState, ev SyncAckReceived) []Command {
	rv := st.replicaView(ev.ReplicaID)
	if rv == nil {
		return nil
	}
	rv.LastSyncAck = ev.Ack
	st.WALTail = ev.PrimaryWALTail
	st.WALHead = ev.PrimaryWALHead

	// If replica reports durable progress, advance volume boundary.
	if ev.Ack.DurableLSN > st.DurableLSN {
		st.DurableLSN = ev.Ack.DurableLSN
	}

	// Already in an active session? Don't re-decide, just update ack.
	if rv.Session.State == SessionStateRunning || rv.Session.State == SessionStateIssued {
		return nil
	}

	// --- Primary decision ---
	decision := e.decide(ev.Ack, ev.PrimaryWALTail, ev.PrimaryWALHead)

	switch decision {
	case SessionKeepUp:
		rv.Session = ReplicaSession{Kind: SessionKeepUp, State: SessionStateIdle}
		return nil

	case SessionCatchUp:
		targetLSN := ev.PrimaryWALHead
		startLSN := ev.Ack.AppliedLSN
		if startLSN == 0 {
			startLSN = ev.Ack.DurableLSN
		}
		rv.Session = ReplicaSession{
			Kind:      SessionCatchUp,
			State:     SessionStateIssued,
			StartLSN:  startLSN,
			TargetLSN: targetLSN,
			PinLSN:    startLSN,
		}
		return []Command{IssueCatchUpCommand{
			VolumeID:  st.VolumeID,
			ReplicaID: ev.ReplicaID,
			StartLSN:  startLSN,
			TargetLSN: targetLSN,
			PinLSN:    startLSN,
		}}

	case SessionRebuild:
		rv.Session = ReplicaSession{
			Kind:      SessionRebuild,
			State:     SessionStateIssued,
			TargetLSN: ev.PrimaryWALHead,
			Reason:    "applied_lsn < primary_wal_tail",
		}
		return []Command{IssueRebuildCommand{
			VolumeID:  st.VolumeID,
			ReplicaID: ev.ReplicaID,
			TargetLSN: ev.PrimaryWALHead,
		}}
	}

	return nil
}

// decide is the one-threshold decision.
// Matches Ceph: last_update >= log_tail → log-recovery, else → backfill.
func (e *Engine) decide(ack SyncAck, primaryWALTail uint64, primaryWALHead uint64) SessionKind {
	replicaPos := ack.AppliedLSN
	if replicaPos == 0 {
		replicaPos = ack.DurableLSN
	}

	// Replica is fully caught up.
	if replicaPos >= primaryWALHead && replicaPos > 0 {
		return SessionKeepUp
	}

	// Replica is behind but within retained WAL → catch-up.
	if replicaPos >= primaryWALTail && replicaPos > 0 {
		return SessionCatchUp
	}

	// Fresh replica (pos=0) with retained WAL from the beginning.
	if replicaPos == 0 && primaryWALTail <= 1 {
		return SessionCatchUp
	}

	// Gap exceeds retained WAL → rebuild.
	return SessionRebuild
}

// --- Session lifecycle ---

func (e *Engine) applyProgress(st *VolumeState, ev SessionProgress) {
	rv := st.replicaView(ev.ReplicaID)
	if rv == nil {
		return
	}
	if rv.Session.State == SessionStateIssued {
		rv.Session.State = SessionStateRunning
	}
	rv.Session.Progress = ev.Progress
}

func (e *Engine) applyCompleted(st *VolumeState, ev SessionCompleted) []Command {
	rv := st.replicaView(ev.ReplicaID)
	if rv == nil {
		return nil
	}
	rv.Session = ReplicaSession{Kind: SessionKeepUp, State: SessionStateIdle}
	if ev.DurableLSN > st.DurableLSN {
		st.DurableLSN = ev.DurableLSN
	}
	return nil
}

func (e *Engine) applyFailed(st *VolumeState, ev SessionFailed) []Command {
	rv := st.replicaView(ev.ReplicaID)
	if rv == nil {
		return nil
	}
	rv.Session.State = SessionStateFailed
	rv.Session.Reason = ev.Reason
	// Don't auto-escalate. Wait for next SyncAck to re-decide.
	// This is the key difference from v1: failure doesn't auto-become NeedsRebuild.
	return nil
}

// --- Barrier ---

func (e *Engine) applyBarrier(st *VolumeState, ev BarrierConfirmed) {
	if ev.DurableLSN > st.DurableLSN {
		st.DurableLSN = ev.DurableLSN
	}
}

// --- Mode derivation (projection) ---
//
// This is the outward view. Derived from all replica states + boundaries.
// Matches the principle: projection is primary-derived, not assigned.

func (e *Engine) deriveMode(st *VolumeState) {
	st.Healthy = false
	st.ModeReason = ""

	switch {
	case st.anyReplicaInState(SessionRebuild):
		st.Mode = ModeNeedsRebuild
		st.ModeReason = "replica_needs_rebuild"

	case st.anyReplicaSessionFailed():
		st.Mode = ModeDegraded
		st.ModeReason = st.failedReason()

	case st.anyReplicaInState(SessionCatchUp):
		st.Mode = ModeBootstrapPending
		st.ModeReason = "recovery_in_progress"

	case st.Role == RoleReplica && st.Readiness.ReceiverReady:
		st.Mode = ModeReplicaReady
		st.ModeReason = "replica_not_primary"

	case st.Role == RolePrimary && len(st.Replicas) == 0:
		st.Mode = ModeAllocatedOnly
		st.ModeReason = "allocated_only"

	case st.Role == RolePrimary && st.primaryEligible():
		st.Mode = ModePublishHealthy
		st.Healthy = true

	case st.Readiness.Assigned:
		st.Mode = ModeBootstrapPending
		st.ModeReason = st.bootstrapReason()

	default:
		st.Mode = ModeAllocatedOnly
		st.ModeReason = "allocated_only"
	}
}

func (st *VolumeState) primaryEligible() bool {
	return st.Readiness.RoleApplied &&
		st.Readiness.ShipperConfigured &&
		st.Readiness.ShipperConnected &&
		st.DurableLSN > 0
}

func (st *VolumeState) bootstrapReason() string {
	switch {
	case !st.Readiness.RoleApplied:
		return "awaiting_role_apply"
	case st.Role == RoleReplica && !st.Readiness.ReceiverReady:
		return "awaiting_receiver_ready"
	case st.Role == RolePrimary && !st.Readiness.ShipperConfigured:
		return "awaiting_shipper_configured"
	case st.Role == RolePrimary && !st.Readiness.ShipperConnected:
		return "awaiting_shipper_connected"
	case st.Role == RolePrimary && st.DurableLSN == 0:
		return "awaiting_barrier_durability"
	default:
		return "bootstrap_pending"
	}
}

// --- Helpers ---

func (e *Engine) mustVolume(id string) *VolumeState {
	if st, ok := e.volumes[id]; ok {
		return st
	}
	st := &VolumeState{
		VolumeID:      id,
		ReplicaStates: make(map[string]*ReplicaView),
	}
	e.volumes[id] = st
	return st
}

func (st *VolumeState) replicaView(replicaID string) *ReplicaView {
	if replicaID == "" {
		return nil
	}
	return st.ReplicaStates[replicaID]
}

func (st *VolumeState) anyReplicaInState(kind SessionKind) bool {
	for _, rv := range st.ReplicaStates {
		if rv.Session.Kind == kind &&
			(rv.Session.State == SessionStateIssued || rv.Session.State == SessionStateRunning) {
			return true
		}
	}
	return false
}

func (st *VolumeState) anyReplicaSessionFailed() bool {
	for _, rv := range st.ReplicaStates {
		if rv.Session.State == SessionStateFailed {
			return true
		}
	}
	return false
}

func (st *VolumeState) failedReason() string {
	for _, rv := range st.ReplicaStates {
		if rv.Session.State == SessionStateFailed && rv.Session.Reason != "" {
			return rv.Session.Reason
		}
	}
	return "session_failed"
}
