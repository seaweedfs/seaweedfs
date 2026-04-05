package protocolv2

// ReplicaSummaryRequest asks one node for a bounded takeover/reconstruction
// summary for a specific volume. This is richer than promotion evidence, but
// still smaller than full internal engine/session state.
type ReplicaSummaryRequest struct {
	VolumeName    string
	ExpectedEpoch uint64
}

// ReplicaSummaryResponse is the bounded summary a future primary can use to
// reconstruct recovery truth. It preserves distinct boundary semantics without
// exposing raw shipper/session internals.
type ReplicaSummaryResponse struct {
	VolumeName        string
	NodeID            string
	Epoch             uint64
	Role              string
	Mode              string
	ModeReason        string
	RoleApplied       bool
	ReceiverReady     bool
	CommittedLSN      uint64
	DurableLSN        uint64
	CheckpointLSN     uint64
	TargetLSN         uint64
	AchievedLSN       uint64
	RecoveryPhase     string
	LastBarrierOK     bool
	LastBarrierReason string
	Eligible          bool
	Reason            string
}
