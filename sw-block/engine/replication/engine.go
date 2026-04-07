package replication

import (
	"reflect"
	"sort"
)

// CoreEngine is the first explicit Phase 14 V2 core shell.
// It is deterministic and side-effect free: one event in, updated state and
// commands/projection out.
//
// Ownership model:
//   - master-owned assignment truth enters as assignment events
//   - primary-owned per-replica session truth enters as sync/recovery events
//   - outward mode/publication is always derived projection
type CoreEngine struct {
	volumes map[string]*VolumeState
}

// ApplyResult is the full result of handling one event.
type ApplyResult struct {
	Commands   []Command
	Projection PublicationProjection
	State      VolumeState
}

// NewCoreEngine creates an empty core.
func NewCoreEngine() *CoreEngine {
	return &CoreEngine{volumes: map[string]*VolumeState{}}
}

// ApplyEvent mutates the bounded core state and emits commands/projection.
func (e *CoreEngine) ApplyEvent(ev Event) ApplyResult {
	prevProjection, hadPrevProjection := e.Projection(ev.VolumeID())
	st := e.mustState(ev.VolumeID())
	var cmds []Command

	switch v := ev.(type) {
	case AssignmentDelivered:
		cmds = append(cmds, e.applyAssignment(st, v)...)

	case RoleApplied:
		st.Readiness.RoleApplied = true

	case ReceiverReadyObserved:
		st.Readiness.ReceiverReady = true

	case ShipperConfiguredObserved:
		st.Readiness.ShipperConfigured = true

	case ShipperConnectedObserved:
		st.Readiness.ShipperConnected = true

	case DiagnosticShippedAdvanced:
		if v.ShippedLSN > st.Boundary.DiagnosticShippedLSN {
			st.Boundary.DiagnosticShippedLSN = v.ShippedLSN
		}

	case CommittedLSNAdvanced:
		if v.CommittedLSN > st.Boundary.CommittedLSN {
			st.Boundary.CommittedLSN = v.CommittedLSN
		}

	case BarrierAccepted:
		if v.FlushedLSN > st.Boundary.DurableLSN {
			st.Boundary.DurableLSN = v.FlushedLSN
		}
		st.Boundary.LastBarrierOK = true
		st.Boundary.LastBarrierReason = ""
		st.degraded = false
		st.degradeReason = ""

	case BarrierRejected:
		st.Boundary.LastBarrierOK = false
		st.Boundary.LastBarrierReason = v.Reason
		st.degraded = true
		st.degradeReason = v.Reason
		if st.shouldInvalidate(v.Reason) {
			cmds = append(cmds, InvalidateSessionCommand{
				VolumeID: st.VolumeID,
				Reason:   v.Reason,
			})
			st.commands.InvalidationIssued = true
			st.commands.InvalidationReason = v.Reason
		}

	case SyncAckObserved:
		cmds = append(cmds, e.applySyncAckObserved(st, v)...)

	case CheckpointAdvanced:
		if v.CheckpointLSN > st.Boundary.CheckpointLSN {
			st.Boundary.CheckpointLSN = v.CheckpointLSN
		}

	case SessionStarted:
		cmds = append(cmds, e.applySessionStarted(st, v)...)

	case SessionProgressObserved:
		e.applySessionProgressObserved(st, v)

	case SessionCompleted:
		e.applySessionCompleted(st, v)

	case SessionFailed:
		cmds = append(cmds, e.applySessionFailed(st, v)...)

	case CatchUpPlanned:
		cmds = append(cmds, e.applySessionStarted(st, SessionStarted{
			ID:        v.ID,
			ReplicaID: v.ReplicaID,
			Kind:      SessionCatchUp,
			TargetLSN: v.TargetLSN,
		})...)

	case RecoveryProgressObserved:
		e.applySessionProgressObserved(st, SessionProgressObserved{
			ID:          v.ID,
			ReplicaID:   v.ReplicaID,
			AchievedLSN: v.AchievedLSN,
		})

	case CatchUpCompleted:
		e.applySessionCompleted(st, SessionCompleted{
			ID:          v.ID,
			ReplicaID:   v.ReplicaID,
			Kind:        SessionCatchUp,
			AchievedLSN: v.AchievedLSN,
		})

	case NeedsRebuildObserved:
		cmds = append(cmds, e.applyNeedsRebuildObserved(st, v)...)

	case RebuildStarted:
		cmds = append(cmds, e.applySessionStarted(st, SessionStarted{
			ID:        v.ID,
			ReplicaID: v.ReplicaID,
			Kind:      SessionRebuild,
			TargetLSN: v.TargetLSN,
		})...)

	case RebuildCommitted:
		e.applySessionCompleted(st, SessionCompleted{
			ID:            v.ID,
			ReplicaID:     v.ReplicaID,
			Kind:          SessionRebuild,
			AchievedLSN:   v.AchievedLSN,
			FlushedLSN:    v.FlushedLSN,
			CheckpointLSN: v.CheckpointLSN,
		})
	}

	e.recompute(st)
	proj := e.projectionFor(st)
	if !hadPrevProjection || !reflect.DeepEqual(prevProjection, proj) {
		cmds = append(cmds, PublishProjectionCommand{
			VolumeID:   st.VolumeID,
			Projection: proj,
		})
	}

	return ApplyResult{
		Commands:   cmds,
		Projection: proj,
		State:      st.Snapshot(),
	}
}

// State returns a detached copy of a volume state.
func (e *CoreEngine) State(volumeID string) (VolumeState, bool) {
	st, ok := e.volumes[volumeID]
	if !ok {
		return VolumeState{}, false
	}
	return st.Snapshot(), true
}

// Projection returns the current outward projection for a volume.
func (e *CoreEngine) Projection(volumeID string) (PublicationProjection, bool) {
	st, ok := e.volumes[volumeID]
	if !ok {
		return PublicationProjection{}, false
	}
	return e.projectionFor(st), true
}

func (e *CoreEngine) mustState(volumeID string) *VolumeState {
	if st, ok := e.volumes[volumeID]; ok {
		return st
	}
	st := newVolumeState(volumeID)
	e.volumes[volumeID] = st
	return st
}

func (e *CoreEngine) recompute(st *VolumeState) {
	st.refreshRecoveryAggregate()
	st.Readiness.ReplicaReady = st.Role == RoleReplica &&
		st.Readiness.RoleApplied &&
		st.Readiness.ReceiverReady
	st.Mode.Authority = RuntimeAuthorityConstrainedV1
	st.Mode.Reason = ""
	st.Publication = PublicationView{}

	switch {
	case st.hasRebuilds():
		st.Publication.Reason = defaultReason(st.aggregateRebuildReason(), "needs_rebuild")
		st.Mode.Name = ModeNeedsRebuild
		st.Mode.Reason = st.Publication.Reason
	case st.degraded:
		st.Publication.Reason = defaultReason(st.degradeReason, "degraded")
		st.Mode.Name = ModeDegraded
		st.Mode.Reason = st.Publication.Reason
	case st.recoveryActive():
		st.Publication.Reason = "recovery_in_progress"
		st.Mode.Name = ModeBootstrapPending
		st.Mode.Reason = st.Publication.Reason
	case !st.hasReplicas():
		st.Publication.Reason = "allocated_only"
		st.Mode.Name = ModeAllocatedOnly
	case st.Role == RoleReplica && st.Readiness.ReplicaReady:
		st.Publication.Reason = "replica_not_primary"
		st.Mode.Name = ModeReplicaReady
	case st.Role == RolePrimary && st.primaryEligibleForPublish():
		st.Publication.Healthy = true
		st.Mode.Name = ModePublishHealthy
	case st.Readiness.Assigned || st.Readiness.RoleApplied:
		st.Publication.Reason = st.bootstrapReason()
		st.Mode.Name = ModeBootstrapPending
		st.Mode.Reason = st.Publication.Reason
	default:
		st.Publication.Reason = "allocated_only"
		st.Mode.Name = ModeAllocatedOnly
	}
}

func (e *CoreEngine) applyAssignment(st *VolumeState, ev AssignmentDelivered) []Command {
	roleChanged := st.Role != ev.Role
	epochChanged := st.Epoch != ev.Epoch
	replicasChanged := !sameReplicaAssignments(st.DesiredReplicas, ev.Replicas)
	recoveryTargetChanged := st.recoveryTarget != ev.RecoveryTarget
	previousRecoveryTaskTargets := recoveryTaskTargetReplicaIDs(st.commands.RecoveryTaskTargets)

	st.Epoch = ev.Epoch
	st.Role = ev.Role
	st.DesiredReplicas = append([]ReplicaAssignment(nil), ev.Replicas...)
	st.recoveryTarget = ev.RecoveryTarget
	st.Readiness.Assigned = true
	st.Mode.Authority = RuntimeAuthorityConstrainedV1

	if epochChanged || roleChanged {
		st.Readiness.RoleApplied = false
	}

	switch ev.Role {
	case RoleReplica:
		if epochChanged || roleChanged {
			st.Readiness.ReceiverReady = false
		}
		st.Readiness.ShipperConfigured = false
		st.Readiness.ShipperConnected = false
	case RolePrimary:
		st.Readiness.ReceiverReady = false
		if epochChanged || roleChanged || replicasChanged {
			st.Readiness.ShipperConfigured = false
			st.Readiness.ShipperConnected = false
		}
	default:
		st.Readiness.ReceiverReady = false
		st.Readiness.ShipperConfigured = false
		st.Readiness.ShipperConnected = false
	}

	if epochChanged || roleChanged || replicasChanged || recoveryTargetChanged {
		st.degraded = false
		st.degradeReason = ""
		st.resetInvalidation()
		st.Recovery = RecoveryView{Phase: RecoveryIdle}
		st.Sync = SyncView{}
		st.ReplicaSync = nil
		st.Boundary.TargetLSN = 0
		st.Boundary.AchievedLSN = 0
		st.sessions = nil
		st.commands.RecoveryTaskEpoch = 0
		st.commands.RecoveryTaskTargets = nil
		st.commands.SessionTargets = nil
	}

	var cmds []Command
	if st.shouldApplyRole() {
		cmds = append(cmds, ApplyRoleCommand{
			VolumeID: st.VolumeID,
			Epoch:    st.Epoch,
			Role:     st.Role,
		})
		st.commands.RoleEpoch = st.Epoch
		st.commands.Role = st.Role
	}
	if st.shouldStartReceiver() {
		cmds = append(cmds, StartReceiverCommand{VolumeID: st.VolumeID})
		st.commands.ReceiverStartEpoch = st.Epoch
	}
	if st.shouldConfigureShipper() {
		cmds = append(cmds, ConfigureShipperCommand{
			VolumeID: st.VolumeID,
			Replicas: append([]ReplicaAssignment(nil), st.DesiredReplicas...),
		})
		st.commands.ShipperConfigEpoch = st.Epoch
		st.commands.ShipperConfigReplicas = append([]ReplicaAssignment(nil), st.DesiredReplicas...)
	}
	for _, replicaID := range removedRecoveryTaskReplicaIDs(previousRecoveryTaskTargets, st.recoveryTaskReplicaIDs()) {
		cmds = append(cmds, DrainRecoveryTaskCommand{
			VolumeID:  st.VolumeID,
			ReplicaID: replicaID,
			Reason:    "assignment_removed",
		})
	}
	for _, replicaID := range st.recoveryTaskReplicaIDs() {
		if !st.shouldStartRecoveryTask(replicaID) {
			continue
		}
		cmds = append(cmds, StartRecoveryTaskCommand{
			VolumeID:  st.VolumeID,
			ReplicaID: replicaID,
			Kind:      st.recoveryTarget,
		})
		st.commands.RecoveryTaskEpoch = st.Epoch
		if st.commands.RecoveryTaskTargets == nil {
			st.commands.RecoveryTaskTargets = make(map[string]SessionKind)
		}
		st.commands.RecoveryTaskTargets[replicaID] = st.recoveryTarget
	}
	return cmds
}

func (e *CoreEngine) applySessionStarted(st *VolumeState, ev SessionStarted) []Command {
	replicaID, ok := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID)
	switch ev.Kind {
	case SessionRebuild:
		reason := ev.Reason
		if reason == "" {
			reason = st.rebuildReasonForReplica(replicaID)
		}
		st.recordRebuild(replicaID, RecoveryRebuilding, reason, ev.TargetLSN)
		st.Recovery.AchievedLSN = 0
		if ev.TargetLSN > st.Recovery.TargetLSN {
			st.Recovery.TargetLSN = ev.TargetLSN
		}
		if ev.TargetLSN > st.Boundary.TargetLSN {
			st.Boundary.TargetLSN = ev.TargetLSN
		}
		if !ok {
			return nil
		}
		return st.startSessionCommand(st.VolumeID, replicaID, SessionRebuild, ev.TargetLSN)

	case SessionCatchUp:
	default:
		return nil
	}
	if ok {
		st.recordCatchUpPlan(replicaID, ev.TargetLSN)
		targetLSN, achievedLSN, _ := st.catchUpAggregate()
		st.Recovery.Phase = RecoveryCatchingUp
		st.Recovery.AchievedLSN = achievedLSN
		st.Recovery.TargetLSN = targetLSN
		st.Boundary.TargetLSN = targetLSN
		st.Boundary.AchievedLSN = achievedLSN
	} else {
		st.Recovery.Phase = RecoveryCatchingUp
		st.Recovery.AchievedLSN = 0
		if ev.TargetLSN > st.Recovery.TargetLSN {
			st.Recovery.TargetLSN = ev.TargetLSN
		}
		if ev.TargetLSN > st.Boundary.TargetLSN {
			st.Boundary.TargetLSN = ev.TargetLSN
		}
	}
	st.Recovery.Reason = ""
	if !ok {
		return nil
	}
	return st.startSessionCommand(st.VolumeID, replicaID, SessionCatchUp, ev.TargetLSN)
}

func (e *CoreEngine) applySessionProgressObserved(st *VolumeState, ev SessionProgressObserved) {
	if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID); ok && st.observeCatchUpProgress(replicaID, ev.AchievedLSN) {
		_, achievedLSN, _ := st.catchUpAggregate()
		st.Recovery.AchievedLSN = achievedLSN
		st.Boundary.AchievedLSN = achievedLSN
		return
	}
	if ev.AchievedLSN > st.Recovery.AchievedLSN {
		st.Recovery.AchievedLSN = ev.AchievedLSN
	}
	if ev.AchievedLSN > st.Boundary.AchievedLSN {
		st.Boundary.AchievedLSN = ev.AchievedLSN
	}
}

func (e *CoreEngine) applySessionCompleted(st *VolumeState, ev SessionCompleted) {
	if ev.Kind == SessionRebuild {
		st.degraded = false
		st.degradeReason = ""
		replicaID, _ := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID)
		st.clearRebuild(replicaID)
		if !st.hasRebuilds() {
			st.resetInvalidation()
		}
		st.Recovery.Phase = RecoveryIdle
		st.Recovery.Reason = ""
		if ev.FlushedLSN > st.Boundary.DurableLSN {
			st.Boundary.DurableLSN = ev.FlushedLSN
		}
		if ev.CheckpointLSN > st.Boundary.CheckpointLSN {
			st.Boundary.CheckpointLSN = ev.CheckpointLSN
		}
		achievedLSN := ev.AchievedLSN
		if achievedLSN == 0 {
			achievedLSN = maxUint64(ev.FlushedLSN, ev.CheckpointLSN)
		}
		if achievedLSN > st.Recovery.AchievedLSN {
			st.Recovery.AchievedLSN = achievedLSN
		}
		if achievedLSN > st.Boundary.AchievedLSN {
			st.Boundary.AchievedLSN = achievedLSN
		}
		st.clearSessionCommand(replicaID, SessionRebuild)
		return
	}

	if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID); ok && st.completeCatchUp(replicaID, ev.AchievedLSN) {
		targetLSN, achievedLSN, allDone := st.catchUpAggregate()
		st.Recovery.TargetLSN = targetLSN
		st.Recovery.AchievedLSN = achievedLSN
		st.Boundary.TargetLSN = targetLSN
		st.Boundary.AchievedLSN = achievedLSN
		if allDone {
			if achievedLSN > st.Boundary.DurableLSN {
				st.Boundary.DurableLSN = achievedLSN
			}
			st.Recovery.Phase = RecoveryIdle
			st.Recovery.Reason = ""
		} else {
			st.Recovery.Phase = RecoveryCatchingUp
			st.Recovery.Reason = ""
		}
		return
	}
	if ev.AchievedLSN > st.Recovery.AchievedLSN {
		st.Recovery.AchievedLSN = ev.AchievedLSN
	}
	if ev.AchievedLSN > st.Boundary.AchievedLSN {
		st.Boundary.AchievedLSN = ev.AchievedLSN
	}
	if ev.AchievedLSN > st.Boundary.DurableLSN {
		st.Boundary.DurableLSN = ev.AchievedLSN
	}
	st.Recovery.Phase = RecoveryIdle
	st.Recovery.Reason = ""
	st.clearSessionCommand("", SessionCatchUp)
}

func (e *CoreEngine) applyNeedsRebuildObserved(st *VolumeState, ev NeedsRebuildObserved) []Command {
	replicaID, _ := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID)
	st.clearCatchUp(replicaID)
	st.recordRebuild(replicaID, RecoveryNeedsRebuild, ev.Reason, 0)
	st.degraded = false
	st.degradeReason = ""
	if !st.shouldInvalidate(ev.Reason) {
		return nil
	}
	st.commands.InvalidationIssued = true
	st.commands.InvalidationReason = ev.Reason
	return []Command{InvalidateSessionCommand{
		VolumeID:  st.VolumeID,
		ReplicaID: replicaID,
		Reason:    ev.Reason,
	}}
}

func (e *CoreEngine) applySessionFailed(st *VolumeState, ev SessionFailed) []Command {
	replicaID, _ := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID)
	switch ev.Kind {
	case SessionRebuild:
		st.clearRebuild(replicaID)
		if !st.hasRebuilds() {
			st.resetInvalidation()
		}
	case SessionCatchUp:
		st.clearCatchUp(replicaID)
	}
	st.clearSessionCommand(replicaID, ev.Kind)
	st.Recovery.Phase = RecoveryIdle
	st.Recovery.Reason = ev.Reason
	st.degraded = true
	st.degradeReason = defaultReason(ev.Reason, "session_failed")
	st.Boundary.LastBarrierOK = false
	st.Boundary.LastBarrierReason = st.degradeReason
	if !st.shouldInvalidate(st.degradeReason) {
		return nil
	}
	st.commands.InvalidationIssued = true
	st.commands.InvalidationReason = st.degradeReason
	return []Command{InvalidateSessionCommand{
		VolumeID:  st.VolumeID,
		ReplicaID: replicaID,
		Reason:    st.degradeReason,
	}}
}

func (e *CoreEngine) projectionFor(st *VolumeState) PublicationProjection {
	replicaIDs := make([]string, 0, len(st.DesiredReplicas))
	for _, replica := range st.DesiredReplicas {
		replicaIDs = append(replicaIDs, replica.ReplicaID)
	}
	return PublicationProjection{
		VolumeID:    st.VolumeID,
		Epoch:       st.Epoch,
		Role:        st.Role,
		Mode:        st.Mode,
		Publication: st.Publication,
		Recovery:    st.Recovery,
		Sync:        st.Sync,
		ReplicaSync: cloneReplicaSyncView(st.ReplicaSync),
		Readiness:   st.Readiness,
		Boundary:    st.Boundary,
		ReplicaIDs:  replicaIDs,
	}
}

func (st *VolumeState) primaryEligibleForPublish() bool {
	return st.Role == RolePrimary &&
		st.Readiness.RoleApplied &&
		st.Readiness.ShipperConfigured &&
		st.Readiness.ShipperConnected &&
		st.Boundary.DurableLSN > 0
}

func (st *VolumeState) shouldApplyRole() bool {
	return st.commands.RoleEpoch != st.Epoch || st.commands.Role != st.Role
}

func (st *VolumeState) shouldStartReceiver() bool {
	return st.Role == RoleReplica &&
		st.recoveryTarget != SessionRebuild &&
		!st.Readiness.ReceiverReady &&
		st.commands.ReceiverStartEpoch != st.Epoch
}

func (st *VolumeState) shouldStartRecoveryTask(replicaID string) bool {
	if st.recoveryTarget == "" || replicaID == "" {
		return false
	}
	if st.commands.RecoveryTaskEpoch != st.Epoch {
		return true
	}
	if st.commands.RecoveryTaskTargets == nil {
		return true
	}
	return st.commands.RecoveryTaskTargets[replicaID] != st.recoveryTarget
}

func (st *VolumeState) shouldConfigureShipper() bool {
	return st.Role == RolePrimary &&
		st.hasReplicas() &&
		!st.Readiness.ShipperConfigured &&
		(st.commands.ShipperConfigEpoch != st.Epoch ||
			!sameReplicaAssignments(st.commands.ShipperConfigReplicas, st.DesiredReplicas))
}

func (st *VolumeState) shouldInvalidate(reason string) bool {
	return !st.commands.InvalidationIssued || st.commands.InvalidationReason != reason
}

func (st *VolumeState) shouldStartSessionCommand(replicaID string, kind SessionKind, targetLSN uint64) bool {
	if targetLSN == 0 || replicaID == "" {
		return false
	}
	if obs, ok := st.sessions[replicaID]; ok &&
		obs.Kind == kind &&
		obs.Completed &&
		obs.TargetLSN == targetLSN &&
		obs.AchievedLSN >= targetLSN {
		return false
	}
	if st.commands.SessionTargets == nil {
		return true
	}
	target, ok := st.commands.SessionTargets[replicaID]
	if !ok {
		return true
	}
	return target.Kind != kind || target.TargetLSN != targetLSN
}

func (st *VolumeState) startSessionCommand(volumeID, replicaID string, kind SessionKind, targetLSN uint64) []Command {
	if !st.shouldStartSessionCommand(replicaID, kind, targetLSN) {
		return nil
	}
	if st.commands.SessionTargets == nil {
		st.commands.SessionTargets = make(map[string]sessionCommandTarget)
	}
	st.commands.SessionTargets[replicaID] = sessionCommandTarget{
		Kind:      kind,
		TargetLSN: targetLSN,
	}
	switch kind {
	case SessionCatchUp:
		return []Command{StartCatchUpCommand{
			VolumeID:  volumeID,
			ReplicaID: replicaID,
			TargetLSN: targetLSN,
		}}
	case SessionRebuild:
		return []Command{StartRebuildCommand{
			VolumeID:  volumeID,
			ReplicaID: replicaID,
			TargetLSN: targetLSN,
		}}
	default:
		return nil
	}
}

func (st *VolumeState) clearSessionCommand(replicaID string, kind SessionKind) {
	if st.commands.SessionTargets == nil {
		return
	}
	if replicaID == "" {
		for id, target := range st.commands.SessionTargets {
			if target.Kind == kind {
				delete(st.commands.SessionTargets, id)
			}
		}
	} else if target, ok := st.commands.SessionTargets[replicaID]; ok && target.Kind == kind {
		delete(st.commands.SessionTargets, replicaID)
	}
	if len(st.commands.SessionTargets) == 0 {
		st.commands.SessionTargets = nil
	}
}

func (st *VolumeState) resetInvalidation() {
	st.commands.InvalidationIssued = false
	st.commands.InvalidationReason = ""
}

func (st *VolumeState) hasReplicas() bool {
	return len(st.DesiredReplicas) > 0
}

func (st *VolumeState) recoveryCommandReplicaID() (string, bool) {
	if len(st.DesiredReplicas) != 1 || st.DesiredReplicas[0].ReplicaID == "" {
		return "", false
	}
	return st.DesiredReplicas[0].ReplicaID, true
}

func (st *VolumeState) recoveryTaskReplicaIDs() []string {
	if st.recoveryTarget == "" || len(st.DesiredReplicas) == 0 {
		return nil
	}
	replicaIDs := make([]string, 0, len(st.DesiredReplicas))
	for _, replica := range st.DesiredReplicas {
		if replica.ReplicaID == "" {
			continue
		}
		replicaIDs = append(replicaIDs, replica.ReplicaID)
	}
	return replicaIDs
}

func recoveryTaskTargetReplicaIDs(targets map[string]SessionKind) []string {
	if len(targets) == 0 {
		return nil
	}
	replicaIDs := make([]string, 0, len(targets))
	for replicaID := range targets {
		if replicaID == "" {
			continue
		}
		replicaIDs = append(replicaIDs, replicaID)
	}
	sort.Strings(replicaIDs)
	return replicaIDs
}

func removedRecoveryTaskReplicaIDs(previousTargets, currentTargets []string) []string {
	if len(previousTargets) == 0 {
		return nil
	}
	current := make(map[string]struct{}, len(currentTargets))
	for _, replicaID := range currentTargets {
		current[replicaID] = struct{}{}
	}
	removed := make([]string, 0, len(previousTargets))
	for _, replicaID := range previousTargets {
		if _, ok := current[replicaID]; ok {
			continue
		}
		removed = append(removed, replicaID)
	}
	return removed
}

func (st *VolumeState) recoveryCommandReplicaIDFromEvent(replicaID string) (string, bool) {
	if replicaID != "" {
		for _, replica := range st.DesiredReplicas {
			if replica.ReplicaID == replicaID {
				return replicaID, true
			}
		}
		return "", false
	}
	return st.recoveryCommandReplicaID()
}

func (st *VolumeState) recordCatchUpPlan(replicaID string, targetLSN uint64) {
	if replicaID == "" || targetLSN == 0 {
		return
	}
	if st.sessions == nil {
		st.sessions = make(map[string]sessionObservation)
	}
	obs := st.sessions[replicaID]
	if obs.Kind != SessionCatchUp {
		obs = sessionObservation{Kind: SessionCatchUp, Phase: RecoveryCatchingUp}
	}
	if obs.Completed && obs.TargetLSN == targetLSN && obs.AchievedLSN >= targetLSN {
		return
	}
	if obs.TargetLSN != targetLSN {
		obs.AchievedLSN = 0
	}
	obs.Kind = SessionCatchUp
	obs.Phase = RecoveryCatchingUp
	obs.TargetLSN = targetLSN
	obs.Reason = ""
	obs.Completed = false
	st.sessions[replicaID] = obs
}

func (st *VolumeState) clearCatchUp(replicaID string) {
	if replicaID == "" || st.sessions == nil {
		return
	}
	if obs, ok := st.sessions[replicaID]; ok && obs.Kind == SessionCatchUp {
		delete(st.sessions, replicaID)
	}
	if len(st.sessions) == 0 {
		st.sessions = nil
	}
	st.clearSessionCommand(replicaID, SessionCatchUp)
}

func (st *VolumeState) observeCatchUpProgress(replicaID string, achievedLSN uint64) bool {
	if replicaID == "" || st.sessions == nil {
		return false
	}
	obs, ok := st.sessions[replicaID]
	if !ok || obs.Kind != SessionCatchUp {
		return false
	}
	if achievedLSN > obs.AchievedLSN {
		obs.AchievedLSN = achievedLSN
	}
	obs.Phase = RecoveryCatchingUp
	st.sessions[replicaID] = obs
	return true
}

func (st *VolumeState) completeCatchUp(replicaID string, achievedLSN uint64) bool {
	if replicaID == "" || st.sessions == nil {
		return false
	}
	obs, ok := st.sessions[replicaID]
	if !ok || obs.Kind != SessionCatchUp {
		return false
	}
	if achievedLSN > obs.AchievedLSN {
		obs.AchievedLSN = achievedLSN
	}
	if obs.TargetLSN > obs.AchievedLSN {
		obs.AchievedLSN = obs.TargetLSN
	}
	obs.Phase = RecoveryIdle
	obs.Completed = true
	st.sessions[replicaID] = obs
	return true
}

func (st *VolumeState) catchUpAggregate() (targetLSN uint64, achievedLSN uint64, allDone bool) {
	if len(st.sessions) == 0 {
		return 0, 0, true
	}
	allDone = true
	first := true
	found := false
	for _, obs := range st.sessions {
		if obs.Kind != SessionCatchUp {
			continue
		}
		found = true
		if obs.TargetLSN > targetLSN {
			targetLSN = obs.TargetLSN
		}
		if first || obs.AchievedLSN < achievedLSN {
			achievedLSN = obs.AchievedLSN
			first = false
		}
		if !obs.Completed {
			allDone = false
		}
	}
	if !found {
		return 0, 0, true
	}
	return targetLSN, achievedLSN, allDone
}

func (st *VolumeState) recordRebuild(replicaID string, phase RecoveryPhase, reason string, targetLSN uint64) {
	if replicaID == "" {
		return
	}
	if st.sessions == nil {
		st.sessions = make(map[string]sessionObservation)
	}
	if phase == "" {
		phase = RecoveryNeedsRebuild
	}
	obs := st.sessions[replicaID]
	obs.Kind = SessionRebuild
	obs.Phase = phase
	obs.Reason = reason
	obs.TargetLSN = targetLSN
	obs.Completed = false
	st.sessions[replicaID] = obs
}

func (st *VolumeState) clearRebuild(replicaID string) {
	if replicaID == "" || st.sessions == nil {
		return
	}
	if obs, ok := st.sessions[replicaID]; ok && obs.Kind == SessionRebuild {
		delete(st.sessions, replicaID)
	}
	if len(st.sessions) == 0 {
		st.sessions = nil
	}
}

func (st *VolumeState) hasRebuilds() bool {
	for _, obs := range st.sessions {
		if obs.Kind == SessionRebuild {
			return true
		}
	}
	return false
}

func (st *VolumeState) rebuildReasonForReplica(replicaID string) string {
	if replicaID == "" || st.sessions == nil {
		return ""
	}
	obs, ok := st.sessions[replicaID]
	if !ok || obs.Kind != SessionRebuild {
		return ""
	}
	return obs.Reason
}

func (st *VolumeState) aggregateRebuildReason() string {
	for _, obs := range st.sessions {
		if obs.Kind == SessionRebuild && obs.Reason != "" {
			return obs.Reason
		}
	}
	return ""
}

func (st *VolumeState) refreshRecoveryAggregate() {
	if st.hasRebuilds() {
		phase := RecoveryNeedsRebuild
		reason := ""
		targetLSN := st.Recovery.TargetLSN
		for _, obs := range st.sessions {
			if obs.Kind != SessionRebuild {
				continue
			}
			if obs.Phase == RecoveryRebuilding {
				phase = RecoveryRebuilding
			}
			if reason == "" && obs.Reason != "" {
				reason = obs.Reason
			}
			if obs.TargetLSN > targetLSN {
				targetLSN = obs.TargetLSN
			}
		}
		st.Recovery.Phase = phase
		st.Recovery.Reason = reason
		st.Recovery.TargetLSN = targetLSN
		if targetLSN > st.Boundary.TargetLSN {
			st.Boundary.TargetLSN = targetLSN
		}
		return
	}
	if targetLSN, achievedLSN, allDone := st.catchUpAggregate(); targetLSN > 0 {
		if allDone {
			st.Recovery.Phase = RecoveryIdle
		} else {
			st.Recovery.Phase = RecoveryCatchingUp
		}
		st.Recovery.Reason = ""
		st.Recovery.TargetLSN = targetLSN
		st.Recovery.AchievedLSN = achievedLSN
		if targetLSN > st.Boundary.TargetLSN {
			st.Boundary.TargetLSN = targetLSN
		}
		if achievedLSN > st.Boundary.AchievedLSN {
			st.Boundary.AchievedLSN = achievedLSN
		}
		return
	}
	if st.Recovery.Phase == RecoveryNeedsRebuild || st.Recovery.Phase == RecoveryRebuilding || st.Recovery.Phase == RecoveryCatchingUp {
		st.Recovery.Phase = RecoveryIdle
		st.Recovery.Reason = ""
	}
}

func (st *VolumeState) bootstrapReason() string {
	switch {
	case !st.Readiness.RoleApplied:
		return "awaiting_role_apply"
	case st.Role == RoleReplica && st.recoveryTarget == SessionRebuild:
		return "awaiting_rebuild_start"
	case st.Role == RoleReplica && !st.Readiness.ReceiverReady:
		return "awaiting_receiver_ready"
	case st.Role == RolePrimary && !st.Readiness.ShipperConfigured:
		return "awaiting_shipper_configured"
	case st.Role == RolePrimary && !st.Readiness.ShipperConnected:
		return "awaiting_shipper_connected"
	case st.Role == RolePrimary && st.Boundary.DurableLSN == 0:
		return "awaiting_barrier_durability"
	default:
		return "bootstrap_pending"
	}
}

func (st *VolumeState) recoveryActive() bool {
	return st.Recovery.Phase == RecoveryCatchingUp || st.Recovery.Phase == RecoveryRebuilding
}

func (e *CoreEngine) applySyncAckObserved(st *VolumeState, ev SyncAckObserved) []Command {
	syncView := SyncView{
		AckKind:        ev.AckKind,
		TargetLSN:      ev.TargetLSN,
		PrimaryTailLSN: ev.PrimaryTailLSN,
		DurableLSN:     ev.DurableLSN,
		AppliedLSN:     ev.AppliedLSN,
		Reason:         ev.Reason,
	}
	st.Sync = syncView
	if ev.ReplicaID != "" {
		if st.ReplicaSync == nil {
			st.ReplicaSync = make(ReplicaSyncView)
		}
		st.ReplicaSync[ev.ReplicaID] = syncView
	}
	if ev.DurableLSN > st.Boundary.DurableLSN {
		st.Boundary.DurableLSN = ev.DurableLSN
	}
	switch ev.AckKind {
	case SyncAckEpochMismatch:
		st.Boundary.LastBarrierOK = false
		st.Boundary.LastBarrierReason = defaultReason(ev.Reason, "epoch_mismatch")
		return nil
	}

	action, achievedLSN, ok := decideSyncAction(ev)
	if !ok {
		if ev.AckKind == SyncAckTimedOut {
			st.Boundary.LastBarrierOK = false
			st.Boundary.LastBarrierReason = defaultReason(ev.Reason, string(ev.AckKind))
		}
		return nil
	}

	switch action {
	case SyncActionKeepUp:
		st.Boundary.LastBarrierOK = true
		st.Boundary.LastBarrierReason = ""
		st.degraded = false
		st.degradeReason = ""
		st.Sync.Action = SyncActionKeepUp
		if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID); ok {
			st.clearRebuild(replicaID)
		}
		if ev.ReplicaID != "" && st.ReplicaSync != nil {
			st.ReplicaSync[ev.ReplicaID] = st.Sync
		}
		return nil

	case SyncActionCatchUp:
		st.Boundary.LastBarrierOK = false
		st.Boundary.LastBarrierReason = defaultReason(ev.Reason, string(ev.AckKind))
		st.degraded = false
		st.degradeReason = ""
		st.Sync.Action = SyncActionCatchUp
		if ev.ReplicaID != "" && st.ReplicaSync != nil {
			st.ReplicaSync[ev.ReplicaID] = st.Sync
		}
		if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID); ok {
			st.recordCatchUpPlan(replicaID, ev.TargetLSN)
			if achievedLSN > 0 {
				st.observeCatchUpProgress(replicaID, achievedLSN)
			}
			targetLSN, aggregateAchievedLSN, _ := st.catchUpAggregate()
			st.Recovery.Phase = RecoveryCatchingUp
			st.Recovery.TargetLSN = targetLSN
			st.Recovery.AchievedLSN = aggregateAchievedLSN
			st.Recovery.Reason = ""
			st.Boundary.TargetLSN = targetLSN
			st.Boundary.AchievedLSN = aggregateAchievedLSN
			return st.startSessionCommand(st.VolumeID, replicaID, SessionCatchUp, ev.TargetLSN)
		}
		st.Recovery.Phase = RecoveryCatchingUp
		st.Recovery.TargetLSN = maxUint64(st.Recovery.TargetLSN, ev.TargetLSN)
		st.Recovery.AchievedLSN = maxUint64(st.Recovery.AchievedLSN, achievedLSN)
		st.Recovery.Reason = ""
		st.Boundary.TargetLSN = maxUint64(st.Boundary.TargetLSN, ev.TargetLSN)
		st.Boundary.AchievedLSN = maxUint64(st.Boundary.AchievedLSN, achievedLSN)
		return nil

	case SyncActionRebuild:
		return e.applySyncNeedsRebuild(st, ev)
	}

	return nil
}

func defaultReason(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
}

func syncAckAchievedLSN(ev SyncAckObserved) uint64 {
	return maxUint64(ev.DurableLSN, ev.AppliedLSN)
}

func syncAckSupportsCatchUp(ev SyncAckObserved) bool {
	if ev.TargetLSN == 0 {
		return false
	}
	return maxUint64(ev.AppliedLSN, ev.DurableLSN) >= ev.PrimaryTailLSN
}

func syncAckNeedsRebuild(ev SyncAckObserved) bool {
	if ev.TargetLSN == 0 {
		return false
	}
	return !syncAckSupportsCatchUp(ev)
}

func decideSyncAction(ev SyncAckObserved) (SyncAction, uint64, bool) {
	switch ev.AckKind {
	case SyncAckQuorum:
		return SyncActionKeepUp, 0, true
	case SyncAckTimedOut:
		if syncAckNeedsRebuild(ev) {
			return SyncActionRebuild, 0, true
		}
		if syncAckSupportsCatchUp(ev) {
			return SyncActionCatchUp, syncAckAchievedLSN(ev), true
		}
		return "", 0, false
	case SyncAckTransportLost:
		return SyncActionRebuild, 0, true
	default:
		return "", 0, false
	}
}

func (e *CoreEngine) applySyncNeedsRebuild(st *VolumeState, ev SyncAckObserved) []Command {
	reason := defaultReason(ev.Reason, "needs_rebuild")
	replicaID, _ := st.recoveryCommandReplicaIDFromEvent(ev.ReplicaID)
	st.Sync.Action = SyncActionRebuild
	if ev.ReplicaID != "" && st.ReplicaSync != nil {
		st.ReplicaSync[ev.ReplicaID] = st.Sync
	}
	st.clearCatchUp(replicaID)
	st.recordRebuild(replicaID, RecoveryNeedsRebuild, reason, ev.TargetLSN)
	st.degraded = false
	st.degradeReason = ""
	st.Boundary.LastBarrierOK = false
	st.Boundary.LastBarrierReason = reason
	if st.shouldInvalidate(reason) {
		st.commands.InvalidationIssued = true
		st.commands.InvalidationReason = reason
		return []Command{InvalidateSessionCommand{
			VolumeID:  st.VolumeID,
			ReplicaID: replicaID,
			Reason:    reason,
		}}
	}
	return nil
}

func cloneReplicaSyncView(in ReplicaSyncView) ReplicaSyncView {
	if len(in) == 0 {
		return nil
	}
	out := make(ReplicaSyncView, len(in))
	for replicaID, syncView := range in {
		out[replicaID] = syncView
	}
	return out
}

func sameReplicaAssignments(left, right []ReplicaAssignment) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ReplicaID != right[i].ReplicaID {
			return false
		}
		if left[i].Endpoint != right[i].Endpoint {
			return false
		}
	}
	return true
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
