package replication

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

	case NeedsRebuildObserved:
		st.needsRebuild = true
		st.rebuildReason = v.Reason
		st.degraded = false
		st.degradeReason = ""
		if st.shouldInvalidate(v.Reason) {
			cmds = append(cmds, InvalidateSessionCommand{
				VolumeID: st.VolumeID,
				Reason:   v.Reason,
			})
			st.commands.InvalidationIssued = true
			st.commands.InvalidationReason = v.Reason
		}

	case RebuildCommitted:
		st.needsRebuild = false
		st.rebuildReason = ""
		st.degraded = false
		st.degradeReason = ""
		st.resetInvalidation()
		if v.FlushedLSN > st.Boundary.DurableLSN {
			st.Boundary.DurableLSN = v.FlushedLSN
		}
		if v.CheckpointLSN > st.Boundary.CheckpointLSN {
			st.Boundary.CheckpointLSN = v.CheckpointLSN
		}
	}

	e.recompute(st)
	proj := e.projectionFor(st)
	cmds = append(cmds, PublishProjectionCommand{
		VolumeID:   st.VolumeID,
		Projection: proj,
	})

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
	st.Readiness.PublishHealthy = false
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
	case !st.hasReplicas():
		st.Publication.Reason = "allocated_only"
		st.Mode.Name = ModeAllocatedOnly
	case st.Role == RoleReplica && st.Readiness.ReplicaReady:
		st.Publication.Reason = "replica_not_primary"
		st.Mode.Name = ModeReplicaReady
	case st.Role == RolePrimary && st.primaryEligibleForPublish():
		st.Publication.Healthy = true
		st.Mode.Name = ModePublishHealthy
		st.Readiness.PublishHealthy = true
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

	st.Epoch = ev.Epoch
	st.Role = ev.Role
	st.DesiredReplicas = append([]ReplicaAssignment(nil), ev.Replicas...)
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

	if epochChanged || roleChanged || replicasChanged {
		st.degraded = false
		st.degradeReason = ""
		st.resetInvalidation()
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
	return cmds
}

func (e *CoreEngine) projectionFor(st *VolumeState) PublicationProjection {
	replicaIDs := make([]string, 0, len(st.DesiredReplicas))
	for _, replica := range st.DesiredReplicas {
		replicaIDs = append(replicaIDs, replica.ReplicaID)
	}
	return PublicationProjection{
		VolumeID:       st.VolumeID,
		Epoch:          st.Epoch,
		Role:           st.Role,
		Mode:           st.Mode,
		Publication:    st.Publication,
		Readiness:      st.Readiness,
		Boundary:       st.Boundary,
		ReplicaIDs:     replicaIDs,
		PublishHealthy: st.Publication.Healthy,
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
		!st.Readiness.ReceiverReady &&
		st.commands.ReceiverStartEpoch != st.Epoch
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

func (st *VolumeState) resetInvalidation() {
	st.commands.InvalidationIssued = false
	st.commands.InvalidationReason = ""
}

func (st *VolumeState) hasReplicas() bool {
	return len(st.DesiredReplicas) > 0
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
	case st.Role == RolePrimary && st.Boundary.DurableLSN == 0:
		return "awaiting_barrier_durability"
	default:
		return "bootstrap_pending"
	}
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
