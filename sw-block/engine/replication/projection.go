package replication

// PublicationProjection is the bounded outward projection derived from one
// VolumeState. It is intentionally detached from runtime internals and is
// primary-derived projection, not assignment truth.
type PublicationProjection struct {
	VolumeID string
	Epoch    uint64
	Role     VolumeRole

	Mode        ModeView
	Publication PublicationView
	Recovery    RecoveryView
	Sync        SyncView
	ReplicaSync ReplicaSyncView
	Readiness   ReadinessView
	Boundary    BoundaryView

	ReplicaIDs []string
}
