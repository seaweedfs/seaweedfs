package replication

// PublicationProjection is the bounded outward projection derived from one
// VolumeState. It is intentionally detached from runtime internals.
type PublicationProjection struct {
	VolumeID string
	Epoch    uint64
	Role     VolumeRole

	Mode        ModeView
	Publication PublicationView
	Readiness   ReadinessView
	Boundary    BoundaryView

	ReplicaIDs     []string
	PublishHealthy bool
}
