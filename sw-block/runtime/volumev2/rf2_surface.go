package volumev2

// RF2SurfaceMode is the compressed outward runtime/product mode projected from
// the new runtime-owned RF2 slices.
type RF2SurfaceMode string

const (
	RF2SurfaceModeHealthy    RF2SurfaceMode = "healthy"
	RF2SurfaceModeCatchingUp RF2SurfaceMode = "catching_up"
	RF2SurfaceModeDegraded   RF2SurfaceMode = "degraded"
	RF2SurfaceModeBlocked    RF2SurfaceMode = "blocked"
)

// RF2ContinuityStatus is the bounded continuity statement exposed to a
// product-facing surface. It stays compressed and never becomes a new truth
// owner.
type RF2ContinuityStatus string

const (
	RF2ContinuityStatusUnknown RF2ContinuityStatus = "unknown"
	RF2ContinuityStatusProven  RF2ContinuityStatus = "proven"
	RF2ContinuityStatusFailed  RF2ContinuityStatus = "failed"
)

// RF2VolumeSurface is the first bounded RF2-facing runtime/product surface
// package projected from the runtime-owned failover, active Loop 2, and
// continuity slices.
type RF2VolumeSurface struct {
	VolumeName          string
	PrimaryNodeID       string
	ExpectedEpoch       uint64
	Mode                RF2SurfaceMode
	Reason              string
	ReplicationMode     Loop2RuntimeMode
	ReplicaCount        int
	HealthyReplicaCount int
	CommittedLSN        uint64
	DurableFloorLSN     uint64
	FailoverStage       FailoverStage
	FailoverNodeID      string
	FailoverError       string
	ContinuityStatus    RF2ContinuityStatus
	ContinuityNodeID    string
	ContinuityError     string
	HasLoop2            bool
	HasFailover         bool
	HasContinuity       bool
}

// RF2VolumeSurface returns the latest bounded RF2-facing surface for one volume
// if the runtime has enough local observations to project it.
func (m *InProcessRuntimeManager) RF2VolumeSurface(volumeName string) (RF2VolumeSurface, bool) {
	if m == nil || volumeName == "" {
		return RF2VolumeSurface{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	loop2, hasLoop2 := m.loop2ByVolume[volumeName]
	failover, hasFailover := m.snapshotsByName[volumeName]
	continuity, hasContinuity := m.continuityByVolume[volumeName]
	if !hasLoop2 && !hasFailover && !hasContinuity {
		return RF2VolumeSurface{}, false
	}

	surface := RF2VolumeSurface{
		VolumeName:       volumeName,
		Mode:             RF2SurfaceModeDegraded,
		ContinuityStatus: RF2ContinuityStatusUnknown,
		HasLoop2:         hasLoop2,
		HasFailover:      hasFailover,
		HasContinuity:    hasContinuity,
	}
	if hasLoop2 {
		surface.PrimaryNodeID = loop2.PrimaryNodeID
		surface.ExpectedEpoch = loop2.ExpectedEpoch
		surface.ReplicationMode = loop2.Mode
		surface.Mode = projectRF2SurfaceMode(loop2.Mode)
		surface.Reason = loop2.Reason
		surface.ReplicaCount = loop2.ReplicaCount
		surface.HealthyReplicaCount = loop2.HealthyReplicaCount
		surface.CommittedLSN = loop2.CommittedLSN
		surface.DurableFloorLSN = loop2.DurableFloorLSN
	}
	if hasFailover {
		surface.FailoverStage = failover.Stage
		surface.FailoverNodeID = failover.SelectedNodeID
		surface.FailoverError = failover.LastError
		if surface.PrimaryNodeID == "" && failover.SelectedNodeID != "" {
			surface.PrimaryNodeID = failover.SelectedNodeID
		}
		if surface.ExpectedEpoch == 0 {
			surface.ExpectedEpoch = failover.ExpectedEpoch
		}
	}
	if hasContinuity {
		if continuity.Result.SelectedPrimaryNodeID != "" {
			surface.ContinuityNodeID = continuity.Result.SelectedPrimaryNodeID
			surface.PrimaryNodeID = continuity.Result.SelectedPrimaryNodeID
		}
		if surface.ExpectedEpoch == 0 {
			surface.ExpectedEpoch = continuity.Result.ExpectedEpoch
		}
		surface.ContinuityError = continuity.LastError
		switch {
		case continuity.LastError != "":
			surface.ContinuityStatus = RF2ContinuityStatusFailed
		case continuity.Result.DataMatch:
			surface.ContinuityStatus = RF2ContinuityStatusProven
		}
	}
	return surface, true
}

func projectRF2SurfaceMode(mode Loop2RuntimeMode) RF2SurfaceMode {
	switch mode {
	case Loop2RuntimeModeKeepUp:
		return RF2SurfaceModeHealthy
	case Loop2RuntimeModeCatchingUp:
		return RF2SurfaceModeCatchingUp
	case Loop2RuntimeModeNeedsRebuild:
		return RF2SurfaceModeBlocked
	case Loop2RuntimeModeDegraded:
		fallthrough
	default:
		return RF2SurfaceModeDegraded
	}
}
