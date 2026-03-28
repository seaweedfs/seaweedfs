package fsmv2

type EventKind string

const (
	EventBootstrapComplete   EventKind = "BootstrapComplete"
	EventDisconnect          EventKind = "Disconnect"
	EventReconnectCatchup    EventKind = "ReconnectCatchup"
	EventReconnectRebuild    EventKind = "ReconnectRebuild"
	EventDurableProgress     EventKind = "DurableProgress"
	EventCatchupProgress     EventKind = "CatchupProgress"
	EventPromotionHealthy    EventKind = "PromotionHealthy"
	EventStartRebuild        EventKind = "StartRebuild"
	EventRebuildBaseApplied  EventKind = "RebuildBaseApplied"
	EventRetentionLost       EventKind = "RetentionLost"
	EventCatchupTimeout      EventKind = "CatchupTimeout"
	EventRebuildTooSlow      EventKind = "RebuildTooSlow"
	EventEpochChanged        EventKind = "EpochChanged"
	EventFatal               EventKind = "Fatal"
)

type Event struct {
	Kind EventKind

	Epoch uint64
	Now   uint64

	ReplicaFlushedLSN uint64
	TargetLSN         uint64
	PromotionHoldTill uint64

	SnapshotID    string
	SnapshotCpLSN uint64

	ReservationID string
	ReservationTTL uint64
}
