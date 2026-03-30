package replication

// SenderStatus is a read-only observability snapshot of one sender.
type SenderStatus struct {
	ReplicaID string
	Endpoint  Endpoint
	Epoch     uint64
	State     ReplicaState
	Stopped   bool
	Session   *SessionSnapshot // nil if no session
}

// RegistryStatus is a read-only observability snapshot of the entire registry.
type RegistryStatus struct {
	Senders    []SenderStatus
	TotalCount int
	InSync     int
	Recovering int
	Degraded   int
	Rebuilding int
}

// Status returns a full observability snapshot of the registry.
func (r *Registry) Status() RegistryStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	status := RegistryStatus{TotalCount: len(r.senders)}
	for _, s := range r.All() {
		ss := SenderStatus{
			ReplicaID: s.ReplicaID(),
			Endpoint:  s.Endpoint(),
			Epoch:     s.Epoch(),
			State:     s.State(),
			Stopped:   s.Stopped(),
			Session:   s.SessionSnapshot(),
		}
		status.Senders = append(status.Senders, ss)

		switch ss.State {
		case StateInSync:
			status.InSync++
		case StateCatchingUp, StateConnecting:
			status.Recovering++
		case StateDegraded:
			status.Degraded++
		case StateNeedsRebuild:
			status.Rebuilding++
		}
	}
	return status
}

// RecoveryEvent records a significant recovery lifecycle event for debugging.
type RecoveryEvent struct {
	ReplicaID string
	SessionID uint64
	Event     string
	Detail    string
}

// RecoveryLog collects recovery events for observability.
type RecoveryLog struct {
	events []RecoveryEvent
}

// NewRecoveryLog creates an empty recovery log.
func NewRecoveryLog() *RecoveryLog {
	return &RecoveryLog{}
}

// Record adds an event to the log.
func (rl *RecoveryLog) Record(replicaID string, sessionID uint64, event, detail string) {
	rl.events = append(rl.events, RecoveryEvent{
		ReplicaID: replicaID,
		SessionID: sessionID,
		Event:     event,
		Detail:    detail,
	})
}

// Events returns all recorded events.
func (rl *RecoveryLog) Events() []RecoveryEvent {
	return rl.events
}

// EventsFor returns events for a specific replica.
func (rl *RecoveryLog) EventsFor(replicaID string) []RecoveryEvent {
	var out []RecoveryEvent
	for _, e := range rl.events {
		if e.ReplicaID == replicaID {
			out = append(out, e)
		}
	}
	return out
}
