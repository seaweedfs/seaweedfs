package replication

import (
	"reflect"
	"sort"
)

// CoreEngine is the first explicit Phase 14 V2 core shell.
// It is deterministic and side-effect free: one event in, updated state and
// commands/projection out.
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

	case CheckpointAdvanced:
		if v.CheckpointLSN > st.Boundary.CheckpointLSN {
			st.Boundary.CheckpointLSN = v.CheckpointLSN
		}

	case CatchUpPlanned:
		replicaID, ok := st.recoveryCommandReplicaIDFromEvent(v.ReplicaID)
		if ok {
			st.recordCatchUpPlan(replicaID, v.TargetLSN)
			targetLSN, achievedLSN, _ := st.catchUpAggregate()
			st.Recovery.Phase = RecoveryCatchingUp
			st.Recovery.AchievedLSN = achievedLSN
			st.Recovery.TargetLSN = targetLSN
			st.Boundary.TargetLSN = targetLSN
			st.Boundary.AchievedLSN = achievedLSN
		} else {
			st.Recovery.Phase = RecoveryCatchingUp
			st.Recovery.AchievedLSN = 0
			if v.TargetLSN > st.Recovery.TargetLSN {
				st.Recovery.TargetLSN = v.TargetLSN
			}
			if v.TargetLSN > st.Boundary.TargetLSN {
				st.Boundary.TargetLSN = v.TargetLSN
			}
		}
		st.Recovery.Reason = ""
		if ok && st.shouldStartCatchUp(replicaID, v.TargetLSN) {
			cmds = append(cmds, StartCatchUpCommand{
				VolumeID:  st.VolumeID,
				ReplicaID: replicaID,
				TargetLSN: v.TargetLSN,
			})
			if st.commands.CatchUpTargets == nil {
				st.commands.CatchUpTargets = make(map[string]uint64)
			}
			st.commands.CatchUpTargets[replicaID] = v.TargetLSN
		}

	case RecoveryProgressObserved:
		if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(v.ReplicaID); ok && st.observeCatchUpProgress(replicaID, v.AchievedLSN) {
			_, achievedLSN, _ := st.catchUpAggregate()
			st.Recovery.AchievedLSN = achievedLSN
			st.Boundary.AchievedLSN = achievedLSN
		} else {
			if v.AchievedLSN > st.Recovery.AchievedLSN {
				st.Recovery.AchievedLSN = v.AchievedLSN
			}
			if v.AchievedLSN > st.Boundary.AchievedLSN {
				st.Boundary.AchievedLSN = v.AchievedLSN
			}
		}

	case CatchUpCompleted:
		if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(v.ReplicaID); ok && st.completeCatchUp(replicaID, v.AchievedLSN) {
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
				st.commands.CatchUpTargets = nil
				st.catchUps = nil
			} else {
				st.Recovery.Phase = RecoveryCatchingUp
				st.Recovery.Reason = ""
			}
		} else {
			if v.AchievedLSN > st.Recovery.AchievedLSN {
				st.Recovery.AchievedLSN = v.AchievedLSN
			}
			if v.AchievedLSN > st.Boundary.AchievedLSN {
				st.Boundary.AchievedLSN = v.AchievedLSN
			}
			if v.AchievedLSN > st.Boundary.DurableLSN {
				st.Boundary.DurableLSN = v.AchievedLSN
			}
			st.Recovery.Phase = RecoveryIdle
			st.Recovery.Reason = ""
			st.commands.CatchUpTargets = nil
		}

	case NeedsRebuildObserved:
		st.catchUps = nil
		st.commands.CatchUpTargets = nil
		st.needsRebuild = true
		st.rebuildReason = v.Reason
		st.degraded = false
		st.degradeReason = ""
		st.Recovery.Phase = RecoveryNeedsRebuild
		st.Recovery.Reason = v.Reason
		if st.shouldInvalidate(v.Reason) {
			replicaID, _ := st.recoveryCommandReplicaIDFromEvent(v.ReplicaID)
			cmds = append(cmds, InvalidateSessionCommand{
				VolumeID:  st.VolumeID,
				ReplicaID: replicaID,
				Reason:    v.Reason,
			})
			st.commands.InvalidationIssued = true
			st.commands.InvalidationReason = v.Reason
		}

	case RebuildStarted:
		st.needsRebuild = true
		st.Recovery.Phase = RecoveryRebuilding
		st.Recovery.Reason = st.rebuildReason
		st.Recovery.AchievedLSN = 0
		if v.TargetLSN > st.Recovery.TargetLSN {
			st.Recovery.TargetLSN = v.TargetLSN
		}
		if v.TargetLSN > st.Boundary.TargetLSN {
			st.Boundary.TargetLSN = v.TargetLSN
		}
		if replicaID, ok := st.recoveryCommandReplicaIDFromEvent(v.ReplicaID); ok && st.shouldStartRebuild(replicaID, v.TargetLSN) {
			cmds = append(cmds, StartRebuildCommand{
				VolumeID:  st.VolumeID,
				ReplicaID: replicaID,
				TargetLSN: v.TargetLSN,
			})
			if st.commands.RebuildTargets == nil {
				st.commands.RebuildTargets = make(map[string]uint64)
			}
			st.commands.RebuildTargets[replicaID] = v.TargetLSN
		}

	case RebuildCommitted:
		st.needsRebuild = false
		st.rebuildReason = ""
		st.degraded = false
		st.degradeReason = ""
		st.resetInvalidation()
		st.Recovery.Phase = RecoveryIdle
		st.Recovery.Reason = ""
		if v.FlushedLSN > st.Boundary.DurableLSN {
			st.Boundary.DurableLSN = v.FlushedLSN
		}
		if v.CheckpointLSN > st.Boundary.CheckpointLSN {
			st.Boundary.CheckpointLSN = v.CheckpointLSN
		}
		achievedLSN := v.AchievedLSN
		if achievedLSN == 0 {
			achievedLSN = maxUint64(v.FlushedLSN, v.CheckpointLSN)
		}
		if achievedLSN > st.Recovery.AchievedLSN {
			st.Recovery.AchievedLSN = achievedLSN
		}
		if achievedLSN > st.Boundary.AchievedLSN {
			st.Boundary.AchievedLSN = achievedLSN
		}
		st.commands.RebuildTargets = nil
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
	st.Readiness.ReplicaReady = st.Role == RoleReplica &&
		st.Readiness.RoleApplied &&
		st.Readiness.ReceiverReady
	st.Mode.Authority = RuntimeAuthorityConstrainedV1
	st.Mode.Reason = ""
	st.Publication = PublicationView{}

	switch {
	case st.needsRebuild:
		st.Publication.Reason = defaultReason(st.rebuildReason, "needs_rebuild")
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

	if epochChanged || roleChanged || recoveryTargetChanged {
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
		st.Boundary.TargetLSN = 0
		st.Boundary.AchievedLSN = 0
		st.catchUps = nil
		st.commands.RecoveryTaskEpoch = 0
		st.commands.RecoveryTaskTargets = nil
		st.commands.CatchUpTargets = nil
		st.commands.RebuildTargets = nil
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

func (st *VolumeState) shouldStartCatchUp(replicaID string, targetLSN uint64) bool {
	if targetLSN == 0 || replicaID == "" {
		return false
	}
	if st.commands.CatchUpTargets == nil {
		return true
	}
	return st.commands.CatchUpTargets[replicaID] != targetLSN
}

func (st *VolumeState) shouldStartRebuild(replicaID string, targetLSN uint64) bool {
	if targetLSN == 0 || replicaID == "" {
		return false
	}
	if st.commands.RebuildTargets == nil {
		return true
	}
	return st.commands.RebuildTargets[replicaID] != targetLSN
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
	if st.catchUps == nil {
		st.catchUps = make(map[string]catchUpObservation)
	}
	obs := st.catchUps[replicaID]
	if obs.TargetLSN != targetLSN {
		obs.AchievedLSN = 0
	}
	obs.TargetLSN = targetLSN
	obs.Completed = false
	st.catchUps[replicaID] = obs
}

func (st *VolumeState) observeCatchUpProgress(replicaID string, achievedLSN uint64) bool {
	if replicaID == "" || st.catchUps == nil {
		return false
	}
	obs, ok := st.catchUps[replicaID]
	if !ok {
		return false
	}
	if achievedLSN > obs.AchievedLSN {
		obs.AchievedLSN = achievedLSN
	}
	st.catchUps[replicaID] = obs
	return true
}

func (st *VolumeState) completeCatchUp(replicaID string, achievedLSN uint64) bool {
	if replicaID == "" || st.catchUps == nil {
		return false
	}
	obs, ok := st.catchUps[replicaID]
	if !ok {
		return false
	}
	if achievedLSN > obs.AchievedLSN {
		obs.AchievedLSN = achievedLSN
	}
	if obs.TargetLSN > obs.AchievedLSN {
		obs.AchievedLSN = obs.TargetLSN
	}
	obs.Completed = true
	st.catchUps[replicaID] = obs
	return true
}

func (st *VolumeState) catchUpAggregate() (targetLSN uint64, achievedLSN uint64, allDone bool) {
	if len(st.catchUps) == 0 {
		return 0, 0, true
	}
	allDone = true
	first := true
	for _, obs := range st.catchUps {
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
	return targetLSN, achievedLSN, allDone
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

func defaultReason(reason, fallback string) string {
	if reason != "" {
		return reason
	}
	return fallback
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
